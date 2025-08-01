from mininet.net import Mininet
from mininet.node import Host, RemoteController
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.cli import CLI
import os
from typing import Optional

# WSL Note: Run with `sudo python3 Virtual_machine_1.py` due to Mininet and tmpfs requirements.
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

    def _resolve_path(self, path: str) -> str:
        """Resolves a path relative to the current virtual directory."""
        if not path:
            return self.cwd
        # Treat paths starting with '~' or '/' as absolute from virtual root
        if path.startswith('/') or path.startswith('~'):
            abs_path = path.replace('~', '/', 1)
        else:
            # Path is relative to cwd
            abs_path = os.path.join(self.cwd, path)
        
        # Normalize the path to resolve '..' and '.'
        norm_path = os.path.normpath(abs_path)
        
        return norm_path

    def change_directory(self, path: str) -> tuple[bool, str]:
        """Changes the current virtual directory."""
        target_path = self._resolve_path(path)

        # Root is always a valid directory
        if target_path == '/':
            self.cwd = '/'
            return True, self.cwd

        # For other paths, check if a corresponding folder exists in our metadata
        key = target_path.lstrip('/')
        if key in self.files and self.files[key].is_folder:
            self.cwd = target_path
            return True, self.cwd
        else:
            print(f"cd: no such file or directory: {path}")
            return False, self.cwd

    def create_virtual_file(self, host: Host, path_key: str, size_bytes: int, content: str = None) -> Optional[VirtualFile]:
        """Create a file on the virtual disk."""
        if not self.is_running:
            print("Cannot create file: Virtual device not running")
            return None
        if self.used_storage + size_bytes > self.total_storage:
            print(f"Insufficient storage: {size_bytes} bytes requested, {self.total_storage - self.used_storage} bytes available")
            return None

        original_size = 0
        if path_key in self.files:
            print(f"Warning: File {path_key} already exists. Overwriting.")
            original_size = self.files[path_key].size_bytes

        # Check if parent directory exists
        parent_path, _ = os.path.split(path_key)
        if parent_path and (parent_path not in self.files or not self.files[parent_path].is_folder):
            print(f"Error: cannot create file '{path_key}': No such file or directory")
            return None
        
        virtual_file = VirtualFile(path_key, self.disk_path, size_bytes, is_folder=False, content=content)
        
        try:
            # Create a file of the specified size on the host's virtual disk using truncate.
            # This is more efficient and accurate for creating files of a specific size.
            host.cmd(f'truncate -s {virtual_file.size_bytes} {virtual_file.path}')
            # Note: The custom content is not written to keep the size accurate.
            self.used_storage = self.used_storage - original_size + size_bytes
            self.files[path_key] = virtual_file
            print(f"Created virtual file '{path_key}' ({size_bytes} bytes)")
            return virtual_file
        except Exception as e:
            print(f"Error creating file {path_key}: {e}")
            return None

    def create_virtual_folder(self, host: Host, path_key: str) -> Optional[VirtualFile]:
        """Create a folder on the virtual disk."""
        if not self.is_running:
            print("Cannot create folder: Virtual device not running")
            return None
        if path_key in self.files:
            print(f"Error: cannot create directory '{path_key}': File exists")
            return None
        
        # Check if parent directory exists
        parent_path, _ = os.path.split(path_key)
        if parent_path and (parent_path not in self.files or not self.files[parent_path].is_folder):
            print(f"Error: cannot create directory '{path_key}': No such file or directory")
            return None
        
        virtual_folder = VirtualFile(path_key, self.disk_path, 0, is_folder=True)
        
        try:
            # Create directory on the Mininet host, not the script host
            host.cmd(f'mkdir -p {virtual_folder.path}')
            self.files[path_key] = virtual_folder
            print(f"Created virtual folder '{path_key}'")
            return virtual_folder
        except Exception as e:
            print(f"Error creating folder {path_key}: {e}")
            return None

    def list_contents(self) -> str:
        """Lists the contents of the virtual disk's root, similar to 'ls -l'."""
        if not self.is_running:
            return "Virtual device is not running."

        target_dir = self.cwd.lstrip('/')
        if self.cwd == '/':
            target_dir = ''
        
        output = []
        for file_key, f in sorted(self.files.items()):
            parent_dir, basename = os.path.split(file_key)
            if parent_dir == target_dir:
                if f.is_folder:
                    output.append(f"d ---        {basename}/")
                else:
                    size_str = f"{f.size_bytes}B"
                    output.append(f"- {size_str:<10} {basename}")
        return "\n".join(output)

# Custom CLI to add storage management commands
class StorageCLI(CLI):
    def __init__(self, net, storage_node: StorageVirtualNode, host: Host, **kwargs):
        self.storage_node = storage_node
        self.host = host
        # The prompt is an attribute, not a constructor argument.
        # Call the parent constructor first.
        super(StorageCLI, self).__init__(net, **kwargs)
        # Then, set the prompt attribute.
        self.prompt = f'{self.host.name}:{self.storage_node.cwd}> '

    def do_cd(self, line: str):
        """Usage: cd <directory>
            Changes the current virtual directory. Supports '..', '/', and '~'."""
        path = line.strip()
        if not path:
            path = '/'  # 'cd' with no args goes to root
        
        success, new_path = self.storage_node.change_directory(path)
        if success:
            # Update the prompt to reflect the new directory
            self.prompt = f'mininet:{new_path}> '

    def do_mkdir(self, line: str):
        """Usage: mkdir <folder_name>
            Creates a directory on the virtual storage node."""
        folder_name = line.strip()
        if not folder_name:
            print("Usage: mkdir <folder_name>")
            return
        
        full_path = self.storage_node._resolve_path(folder_name)
        path_key = full_path.lstrip('/')
        if not path_key: # Don't allow creating root
            print("Error: cannot create directory '/': File exists")
            return
        self.storage_node.create_virtual_folder(self.host, path_key)

    def do_touch(self, line: str):
        """Usage: touch <file_name> [size_in_mb]
            Creates a file on the virtual storage node. Default size is 0MB."""
        parts = line.strip().split()
        if not parts:
            print("Usage: touch <file_name> [size_in_mb]")
            return

        file_name = parts[0]
        size_mb = 0 if len(parts) < 2 else int(parts[1])
        size_bytes = size_mb * 1024 * 1024
        
        full_path = self.storage_node._resolve_path(file_name)
        path_key = full_path.lstrip('/')
        if not path_key:
            print("Error: cannot touch '/': Is a directory")
            return
        self.storage_node.create_virtual_file(self.host, path_key, size_bytes)

    def do_ls(self, line: str):
        """Usage: ls
            Lists the contents of the virtual storage node's root directory."""
        print(self.storage_node.list_contents())

# Topology: 1 virtual device connected to a switch
class SingleVMTopo(Topo):
    "Single VM connected directly to the controller"
    def build(self):
        vm = self.addHost('vm1', cls=Host)
        # WSL Note: Simple bandwidth/delay to avoid WSL2 networking issues

def run_simulation():
    topo = SingleVMTopo()
    # Note: A Ryu controller must be running for this simulation to work.
    # Start Ryu in a separate terminal, e.g.: ryu-manager ryu.app.simple_switch_13
    c0 = RemoteController('c0', ip='127.0.0.1', port=6653)
    net = Mininet(topo=topo, link=TCLink, controller=c0)
    
    storage_node = None
    vm1 = None
    try:
        net.start()
        print("*** Network and controller started.")

        # Initialize StorageVirtualNode for the virtual device
        vm1 = net.get('vm1')

        # Define path for the persistent disk image inside a 'assets' folder.
        script_dir = os.path.dirname(os.path.abspath(__file__))
        virtual_disks_dir = os.path.join(script_dir, "assets")
        os.makedirs(virtual_disks_dir, exist_ok=True)
        image_file = "vm1_disk.img"
        image_path = os.path.join(virtual_disks_dir, image_file)

        storage_node = StorageVirtualNode(
            total_storage_bytes=100 * 1024**2,
            disk_path="/mnt/vm1_disk",
            image_path=image_path # Path to the persistent disk image
        )

        # Start the virtual device
        if not storage_node.start(vm1):
            print("Fatal: Failed to start virtual device, exiting.")
            return

        print("*** Virtual storage device started on vm1.")
        print("*** Type 'help' for a list of commands.")
        print("*** Custom commands: ls, cd <dir>, mkdir <dir>, touch <file> [size_MB]")
        print("*** To interact with the VM, type 'vm1 <command>', e.g., 'vm1 ls /mnt/vm1_disk'")
        print("*** To exit, type 'exit' or 'quit'.")
        
        StorageCLI(net, storage_node=storage_node, host=vm1)

    except Exception as e:
        print(f"Simulation error: {e}")
    finally:
        if storage_node and storage_node.is_running:
            print("\n*** Stopping virtual storage device...")
            storage_node.stop(vm1)
        net.stop()

if __name__ == '__main__':
    run_simulation()