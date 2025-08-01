from mininet.net import Mininet
from mininet.node import Host
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
import os
from typing import Optional
import time

# WSL Note: Run with `sudo python3 Multi_VM_P2P.py` due to Mininet and tmpfs requirements.
# Ensure /mnt is writable (sudo chmod 755 /mnt) and tmpfs is supported.

# Class to represent a virtual file or folder
class VirtualFile:
    def __init__(self, file_id: str, base_path: str, size_bytes: int, is_folder: bool = False, content: str = None):
        self.file_id = file_id
        self.size_bytes = size_bytes
        self.is_folder = is_folder
        self.content = content or f"Content of {os.path.basename(file_id)} ({size_bytes} bytes)" if not is_folder else None
        self.path = os.path.join(base_path, file_id)  # Path on virtual disk

# StorageVirtualNode class for managing virtual disk and storage
class StorageVirtualNode:
    def __init__(self, total_storage_bytes: int, disk_path: str, image_path: str):
        self.total_storage = total_storage_bytes
        self.used_storage = 0
        self.files = {}  # Dictionary to store file_id: VirtualFile
        self.disk_path = disk_path  # Virtual disk mount point
        self.image_path = image_path # Path to the persistent disk image file
        self.is_running = False
        self.cwd = '/'  # Current virtual working directory

    def _rescan_filesystem(self, host: Host):
        """Scans the mounted filesystem to rebuild the internal file metadata."""
        print("Rescanning filesystem for metadata...")
        self.files.clear()
        self.used_storage = 0

        # Get a list of all files and directories inside the mount point
        find_output = host.cmd(f'find {self.disk_path} -mindepth 1')
        if not find_output:
            print("Disk is empty.")
            return # Disk is empty

        paths = find_output.strip().split('\r\n')
        for full_path in paths:
            # Get file stats: size and type
            stat_output = host.cmd(f'stat -c "%s %F" "{full_path}"')
            if not stat_output:
                continue

            size_str, file_type_str = stat_output.strip().split(maxsplit=1)
            size_bytes = int(size_str)
            is_folder = "directory" in file_type_str.lower()

            # The key for our self.files dict is the path relative to the mount point
            path_key = os.path.relpath(full_path, self.disk_path)
            if path_key == '.': continue

            # Create the VirtualFile object and add it to our metadata
            virtual_file = VirtualFile(path_key, self.disk_path, size_bytes, is_folder=is_folder)
            self.files[path_key] = virtual_file
            if not is_folder:
                self.used_storage += size_bytes

        print(f"Rescan complete. Found {len(self.files)} items, using {self.used_storage} bytes.")

    def start(self, host: Host) -> bool:
        """Start the virtual device by mounting a persistent file-backed disk."""
        if self.is_running:
            print("Virtual device already running")
            return False
        try:
            # Check if the disk image file exists.
            if "exists" not in host.cmd(f'test -f {self.image_path} && echo "exists" || echo "not found"'):
                print(f"Disk image not found. Creating new disk at {self.image_path}...")
                host.cmd(f'truncate -s {self.total_storage} {self.image_path}')
                print("Formatting disk image with ext4...")
                host.cmd(f'sudo mkfs.ext4 -F {self.image_path}')
            else:
                print(f"Found existing disk image at {self.image_path}.")

            # Create mount point directory and mount the disk image
            host.cmd(f'mkdir -p {self.disk_path}')
            host.cmd(f'sudo mount -o loop {self.image_path} {self.disk_path}')
            host.cmd(f'sudo chmod 777 {self.disk_path}')

            # Verify mount
            if host.cmd(f'mount | grep {self.disk_path}'):
                self.is_running = True
                self._rescan_filesystem(host)
                print(f"Virtual device started with persistent disk mounted at {self.disk_path}")
                return True
            else:
                print(f"Failed to mount virtual disk at {self.disk_path}")
                return False
        except Exception as e:
            print(f"Error starting virtual device: {e}")
            return False

    def stop(self, host: Host) -> bool:
        """Stop the virtual device by unmounting the persistent disk."""
        if not self.is_running:
            print("Virtual device not running")
            return False
        try:
            # Unmount the disk and remove the (now empty) mount point directory
            print(f"Unmounting persistent disk from {self.disk_path}...")
            host.cmd(f'sudo umount -l {self.disk_path}') # -l for lazy unmount
            host.cmd(f'rmdir {self.disk_path}')
            self.is_running = False
            self.used_storage = 0
            self.files.clear()
            print(f"Virtual device stopped. Data remains in {self.image_path}.")
            return True
        except Exception as e:
            print(f"Error stopping virtual device: {e}")
            return False

# New Topology for 3 VMs connected directly
class MultiVMTopo(Topo):
    "Topology with 3 VMs in a peer-to-peer triangle."
    def build(self):
        # Add three hosts with custom IP addresses on the same subnet
        vm1 = self.addHost('vm1', ip='192.168.1.1/24')
        vm2 = self.addHost('vm2', ip='192.168.1.2/24')
        #vm3 = self.addHost('vm3', ip='192.168.1.3/24')

        # Set bandwidth to 100 Mb/s and r2q to suppress HTB warnings
        link_opts = dict(bw=100, r2q=10)
        # Add direct links between them, creating a triangle.
        # This allows any VM to communicate directly with any other VM.
        self.addLink(vm1, vm2, **link_opts)
        #self.addLink(vm2, vm3, **link_opts)
        #self.addLink(vm3, vm1, **link_opts)

def transfer_file(source_vm: Host, dest_vm: Host, source_disk_path: str, dest_disk_path: str, file_size_mb: int = 10) -> float:
    """
    Transfers a file from source_vm to dest_vm and measures the transfer time.
    Returns the transfer duration in seconds.
    """
    file_name = "transfer_test.dat"
    source_file_path = f"{source_disk_path}/{file_name}"
    dest_file_path = f"{dest_disk_path}/received_{file_name}"
    source_ip = source_vm.IP() # Get IP dynamically

    print(f"\n*** Starting file transfer from {source_vm.name} ({source_ip}) to {dest_vm.name} ***")
    start_time = time.time()
    print(f"Transfer started at: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

    try:
        # Step 1: Create a file on source_vm's disk
        print(f"Creating {file_size_mb}MB file on {source_vm.name} at {source_file_path}...")
        source_vm.cmd(f'truncate -s {file_size_mb}M {source_file_path}')

        # Step 2: Start a web server on source_vm
        print(f"Starting web server on {source_vm.name}...")
        source_vm.cmd(f'python3 -m http.server 80 --directory {source_disk_path} &')
        time.sleep(1)  # Wait for server to start

        # Step 3: Download the file from dest_vm
        print(f"Downloading file to {dest_vm.name} at {dest_file_path}...")
        wget_output = dest_vm.cmd(f'wget http://{source_ip}/{file_name} -O {dest_file_path}')
        if "saved" not in wget_output.lower():
            print(f"Error: File transfer failed. wget output: {wget_output}")
            return -1

        # Step 4: Stop the web server on source_vm
        print(f"Stopping web server on {source_vm.name}...")
        source_vm.cmd('pkill -f http.server')

        # Step 5: Verify the file was transferred
        ls_output = dest_vm.cmd(f'ls {dest_file_path}')
        if file_name not in ls_output:
            print(f"Error: Transferred file not found at {dest_file_path}")
            return -1

    except Exception as e:
        print(f"Error during file transfer: {e}")
        source_vm.cmd('pkill -f http.server')  # Ensure server is stopped
        return -1

    end_time = time.time()
    transfer_duration = end_time - start_time

    # Calculate throughput in Megabits per second (Mb/s)
    # File size is in MiB, so convert to bits. Bandwidth is in Mb/s (10^6).
    file_size_bits = file_size_mb * 1024 * 1024 * 8
    throughput_mbps = (file_size_bits / transfer_duration) / (1000 * 1000) if transfer_duration > 0 else 0

    print(f"Transfer ended at:   {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
    print(f"Total duration:      {transfer_duration:.2f} seconds")
    print(f"Achieved throughput: {throughput_mbps:.2f} Mb/s (Link speed: 100 Mb/s)")
    print(f"*** File transfer from {source_vm.name} to {dest_vm.name} completed. ***")

    return transfer_duration

def run_simulation():
    """
    Sets up a Mininet network with three directly connected hosts,
    each with its own persistent virtual storage.
    Includes a timed file transfer demonstration.
    """
    topo = MultiVMTopo()
    # No switch is used, so no controller is needed.
    # Mininet handles basic routing over the direct links.
    net = Mininet(topo=topo, link=TCLink, controller=None)

    storage_nodes = {}
    hosts = {}

    try:
        net.start()
        print("*** Network started.")

        # Get host objects from the network
        vm_names = ['vm1', 'vm2']
        for vm_name in vm_names:
            hosts[vm_name] = net.get(vm_name)

        # --- Setup storage for each VM ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        virtual_disks_dir = os.path.join(script_dir, "assets")
        os.makedirs(virtual_disks_dir, exist_ok=True)

        for vm_name in vm_names:
            print(f"--- Initializing storage for {vm_name} ---")
            image_file = f"{vm_name}_disk.img"
            image_path = os.path.join(virtual_disks_dir, image_file)
            disk_path = f"/mnt/{vm_name}_disk"

            storage_node = StorageVirtualNode(
                total_storage_bytes=100 * 1024**2, # 100 MB
                disk_path=disk_path,
                image_path=image_path
            )

            if not storage_node.start(hosts[vm_name]):
                print(f"Fatal: Failed to start virtual device for {vm_name}, exiting.")
                return # Cleanup will be handled in the 'finally' block

            storage_nodes[vm_name] = storage_node
            print(f"*** Virtual storage device started on {vm_name} at {disk_path}")
            print("-" * 20)

        # --- The automatic file transfer demonstration has been disabled. ---
        # You can still perform manual transfers from the CLI.
        # print("\n*** Demonstrating timed file transfer from vm1 to vm2 ***")
        # transfer_duration = transfer_file(
        #     source_vm=hosts['vm1'],
        #     dest_vm=hosts['vm2'],
        #     source_disk_path='/mnt/vm1_disk',
        #     dest_disk_path='/mnt/vm2_disk',
        #     file_size_mb=10
        # )
        # if transfer_duration < 0:
        #     print("File transfer failed.")

        print("\n*** All virtual machines are running with persistent storage.")
        print("*** Topology: vm1 <--> vm2 <--> vm3 <--> vm1 (triangle)")
        print("\n*** You are now in the Mininet CLI.")
        print("You can run commands on each VM, e.g., 'vm1 ifconfig' or 'pingall'")
        print("\n--- Example: Checking Disk Space ---")
        print("The default working directory on each VM is root ('/'). You can check with 'vm1 pwd'.")
        print("Use the 'df -h' command on a VM's mount point:")
        print("   mininet> vm1 df -h /mnt/vm1_disk")

        print("\n--- Example: Manual File Sharing from vm1 to vm2 ---")
        print("1. (On vm1) Create a file to share in its virtual disk:")
        print("   (To create a 0-byte file) mininet> vm1 touch /mnt/vm1_disk/hello.txt")
        print("   (To create a 10MB file)  mininet> vm1 truncate -s 10M /mnt/vm1_disk/large_file.dat")

        print("\n2. (On vm1) Start a web server in the disk directory:")
        print("   mininet> vm1 python3 -m http.server 80 --directory /mnt/vm1_disk &")
        print("\n3. (On vm2) Download the file from vm1 (IP is 192.168.1.1):")
        print("   mininet> vm2 wget http://192.168.1.1/hello.txt -O /mnt/vm2_disk/file_from_vm1.txt")
        print("\n4. (On vm2) Verify the download by listing files on its disk:")
        print("   mininet> vm2 ls /mnt/vm2_disk")
        print("\n*** To exit, type 'exit' or 'quit'.")

        CLI(net)

    except Exception as e:
        print(f"An error occurred during simulation: {e}")
    finally:
        print("\n*** Shutting down simulation.")
        # Stop storage nodes
        for vm_name, storage_node in storage_nodes.items():
            if storage_node.is_running:
                print(f"--- Stopping storage for {vm_name} ---")
                storage_node.stop(hosts[vm_name])
        net.stop()
        print("*** Simulation stopped.")

if __name__ == '__main__':
    run_simulation()