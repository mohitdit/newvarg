#=================================
#    WINDOWS ONLY
#=================================

# import time
# import subprocess
# import sys
# import os

# # You must install this library: pip install pywinauto psutil
# try:
#     from pywinauto import Application, Desktop
#     import psutil
# except ImportError:
#     print("[CRITICAL] Library missing. Please run: pip install pywinauto psutil")
#     sys.exit(1)

# class SurfsharkBot:
#     def __init__(self):
#         self.app_path = r"C:\Program Files\Surfshark\Surfshark.exe"
#         self.app = None
#         self.main_window = None

#     def _get_window(self):
#         """
#         Private: Finds or launches the Surfshark window and brings it to front.
#         """
#         # 1. Launch/Focus the App
#         if not os.path.exists(self.app_path):
#             print(f"[!] Error: Path not found {self.app_path}")
#             return None

#         try:
#             # We launch it to ensure it comes out of the system tray
#             subprocess.Popen([self.app_path])
#             time.sleep(3) # Wait for UI to appear

#             # 2. Connect to the existing process
#             # We use backend="uia" which is best for modern Windows apps
#             self.app = Application(backend="uia").connect(path="Surfshark.exe")
            
#             # 3. Find the main window (Title usually starts with Surfshark)
#             # We use a wildcard regex because the version number changes (e.g., Surfshark 6.2.0)
#             self.main_window = self.app.window(title_re="Surfshark.*")
            
#             # 4. Restore if minimized and focus
#             if self.main_window.exists():
#                 self.main_window.restore()
#                 self.main_window.set_focus()
#                 return self.main_window
#             else:
#                 print("[!] Could not find Surfshark window title.")
#                 return None
#         except Exception as e:
#             print(f"[!] Error finding window: {e}")
#             return None

#     def disconnect(self):
#         """
#         Finds the 'Disconnect' button and clicks it.
#         """
#         print("[-] Bot: Attempting to Disconnect...")
#         win = self._get_window()
#         if not win: return

#         try:
#             # Look for a button strictly named "Disconnect"
#             # Surfshark button names are usually exposed to automation tools
#             disconnect_btn = win.child_window(title="Disconnect", control_type="Button")
            
#             if disconnect_btn.exists():
#                 disconnect_btn.click()
#                 print("    -> 'Disconnect' button clicked.")
#                 # Wait for animation to finish
#                 time.sleep(3)
#             else:
#                 print("    -> 'Disconnect' button not found. (Already disconnected?)")
#         except Exception as e:
#             print(f"[!] Disconnect failed: {e}")

#     def connect(self, server_argument="Quick-connect"):
#         """
#         Clicks 'Quick-connect' or attempts to search.
#         Note: Passing a specific argument (like 'us-nyc') is complex in GUI automation
#         without crashing, so we default to the reliable 'Quick-connect' button.
#         """
#         print(f"[+] Bot: Attempting to Connect (Target: {server_argument})...")
#         win = self._get_window()
#         if not win: return

#         try:
#             # 1. Try to find "Quick-connect" button first
#             quick_btn = win.child_window(title="Quick-connect", control_type="Button")
            
#             # 2. Try to find generic "Connect" button (sometimes label changes)
#             connect_btn = win.child_window(title="Connect", control_type="Button")

#             if quick_btn.exists():
#                 quick_btn.click()
#                 print("    -> 'Quick-connect' button clicked.")
#                 time.sleep(5) # Wait for connection
#             elif connect_btn.exists():
#                 connect_btn.click()
#                 print("    -> 'Connect' button clicked.")
#                 time.sleep(5)
#             else:
#                 print("    -> No Connect button found. (Already connected?)")

#         except Exception as e:
#             print(f"[!] Connect failed: {e}")

#     def reconnect(self, argument="Default"):
#         self.disconnect()
#         # Small buffer between actions
#         time.sleep(2)
#         self.connect(argument)

# if __name__ == "__main__":
#     bot = SurfsharkBot()
    
#     # This works like a robot user. 
#     # It will pop the window open and click the buttons.
#     bot.reconnect()



    #=======================================================================================
    #                                    LINUX ONLY
    #=======================================================================================

#=================================
#    WINDOWS ONLY
#=================================

# import time
# import subprocess
# import sys
# import os

# # You must install this library: pip install pywinauto psutil
# try:
#     from pywinauto import Application, Desktop
#     import psutil
# except ImportError:
#     print("[CRITICAL] Library missing. Please run: pip install pywinauto psutil")
#     sys.exit(1)

# class SurfsharkBot:
#     def __init__(self):
#         self.app_path = r"C:\Program Files\Surfshark\Surfshark.exe"
#         self.app = None
#         self.main_window = None

#     def _get_window(self):
#         """
#         Private: Finds or launches the Surfshark window and brings it to front.
#         """
#         # 1. Launch/Focus the App
#         if not os.path.exists(self.app_path):
#             print(f"[!] Error: Path not found {self.app_path}")
#             return None

#         try:
#             # We launch it to ensure it comes out of the system tray
#             subprocess.Popen([self.app_path])
#             time.sleep(3) # Wait for UI to appear

#             # 2. Connect to the existing process
#             # We use backend="uia" which is best for modern Windows apps
#             self.app = Application(backend="uia").connect(path="Surfshark.exe")
            
#             # 3. Find the main window (Title usually starts with Surfshark)
#             # We use a wildcard regex because the version number changes (e.g., Surfshark 6.2.0)
#             self.main_window = self.app.window(title_re="Surfshark.*")
            
#             # 4. Restore if minimized and focus
#             if self.main_window.exists():
#                 self.main_window.restore()
#                 self.main_window.set_focus()
#                 return self.main_window
#             else:
#                 print("[!] Could not find Surfshark window title.")
#                 return None
#         except Exception as e:
#             print(f"[!] Error finding window: {e}")
#             return None

#     def disconnect(self):
#         """
#         Finds the 'Disconnect' button and clicks it.
#         """
#         print("[-] Bot: Attempting to Disconnect...")
#         win = self._get_window()
#         if not win: return

#         try:
#             # Look for a button strictly named "Disconnect"
#             # Surfshark button names are usually exposed to automation tools
#             disconnect_btn = win.child_window(title="Disconnect", control_type="Button")
            
#             if disconnect_btn.exists():
#                 disconnect_btn.click()
#                 print("    -> 'Disconnect' button clicked.")
#                 # Wait for animation to finish
#                 time.sleep(3)
#             else:
#                 print("    -> 'Disconnect' button not found. (Already disconnected?)")
#         except Exception as e:
#             print(f"[!] Disconnect failed: {e}")

#     def connect(self, server_argument="Quick-connect"):
#         """
#         Clicks 'Quick-connect' or attempts to search.
#         Note: Passing a specific argument (like 'us-nyc') is complex in GUI automation
#         without crashing, so we default to the reliable 'Quick-connect' button.
#         """
#         print(f"[+] Bot: Attempting to Connect (Target: {server_argument})...")
#         win = self._get_window()
#         if not win: return

#         try:
#             # 1. Try to find "Quick-connect" button first
#             quick_btn = win.child_window(title="Quick-connect", control_type="Button")
            
#             # 2. Try to find generic "Connect" button (sometimes label changes)
#             connect_btn = win.child_window(title="Connect", control_type="Button")

#             if quick_btn.exists():
#                 quick_btn.click()
#                 print("    -> 'Quick-connect' button clicked.")
#                 time.sleep(5) # Wait for connection
#             elif connect_btn.exists():
#                 connect_btn.click()
#                 print("    -> 'Connect' button clicked.")
#                 time.sleep(5)
#             else:
#                 print("    -> No Connect button found. (Already connected?)")

#         except Exception as e:
#             print(f"[!] Connect failed: {e}")

#     def reconnect(self, argument="Default"):
#         self.disconnect()
#         # Small buffer between actions
#         time.sleep(2)
#         self.connect(argument)

# if __name__ == "__main__":
#     bot = SurfsharkBot()
    
#     # This works like a robot user. 
#     # It will pop the window open and click the buttons.
#     bot.reconnect()



    #=======================================================================================
    #                                    LINUX ONLY
    #=======================================================================================

import subprocess
import shutil
import sys
import time
import json
import os

class SurfsharkManager:
    def __init__(self, config_file="server.json"):
        self.cli_command = "surfshark-vpn"
        self.process = None # To track the running VPN process
        
        # Ensure we look for server.json in the same folder as this script
        base_path = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_path, config_file)
        self.config = self._load_config(config_path)

        if not shutil.which(self.cli_command):
            print(f"[CRITICAL] '{self.cli_command}' not found. Please install Surfshark.")
            sys.exit(1)


    def _load_config(self, filepath):
        if not os.path.exists(filepath):
            print(f"[!] Warning: Config file {filepath} not found. Using empty defaults.")
            return {}
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"[!] Error loading JSON: {e}")
            return {}

    def get_reconnect_interval_minutes(self):
        """Get the reconnect interval from config, default to 10 minutes"""
        return self.config.get("settings", {}).get("reconnect_interval_minutes", 10)

    def _get_sudo_cmd(self, args):
        """Helper to build the command list with sudo if needed."""
        if os.geteuid() == 0:
            return [self.cli_command] + args
        return ["sudo", self.cli_command] + args

    def status(self):
        """Checks the status. Returns 'Connected' or 'Not connected'."""
        cmd = self._get_sudo_cmd(["status"])
        try:
            # We wait for status because it finishes instantly
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            return result.stdout.strip()
        except Exception as e:
            return f"Error checking status: {e}"

    def disconnect(self):
        """Disconnects the VPN."""
        print("[-] Disconnecting VPN...")
        cmd = self._get_sudo_cmd(["down"])
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            self.process = None
        except Exception as e:
            print(f"[!] Error disconnecting: {e}")

    def connect(self, location_alias=None):
        """
        Connects to the VPN.
        args:
            location_alias (str): Key from server.json (e.g., 'chicago', 'new_york')
        """
        # 1. Determine the command argument (ID)
        target_arg = "attack" # Default to Quick Connect
        
        if location_alias:
            # Look up the alias in server.json
            if "locations" in self.config:
                found_id = self.config["locations"].get(location_alias)
                if found_id:
                    print(f"[*] Alias '{location_alias}' mapped to ID '{found_id}'")
                    target_arg = found_id
                else:
                    print(f"[!] Alias '{location_alias}' not found in JSON. Using default.")
            else:
                 print(f"[!] No 'locations' found in JSON. Using default.")

        # 2. Build the command
        cmd = self._get_sudo_cmd([target_arg])
        print(f"[+] Launching VPN command: {' '.join(cmd)}")

        # 3. Launch in Background (Fire and Forget)
        try:
            # We use subprocess.DEVNULL to silence the endless output logs
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL
            )
            
            # 4. CRITICAL: Wait 3 seconds to see if it crashed immediately
            time.sleep(3)
            if self.process.poll() is not None:
                # If poll() returns a number, the process is DEAD (it failed).
                print(f"[!] Connection to '{target_arg}' failed immediately.")
                
                if target_arg != "attack":
                    print("[*] Fallback: Retrying with Quick Connect ('attack')...")
                    self.connect(location_alias=None) # Recursive call with no alias
                    return

            print(" -> VPN process started in background. Stabilizing...")
            time.sleep(10) # Give it time to negotiate the connection

        except Exception as e:
            print(f"[!] Failed to launch VPN: {e}")

    def reconnect(self, location_alias=None):
        """Disconnects, waits, and connects again."""
        print("\n--- Reconnecting ---")
        self.disconnect()
        time.sleep(3) # Wait for network stack to clear
        self.connect(location_alias)

# --- usage example (only runs if you run this file directly) ---
if __name__ == "__main__":
    vpn = SurfsharkManager()

    # 1. Check Status
    print(f"Initial Status: {vpn.status()}")

    # 2. Connect to a specific location (Example: 'chicago')
    # If 'chicago' fails, it will auto-fallback to 'attack'
    vpn.connect("chicago") 
    print(f"Status after Connect: {vpn.status()}")

    # 3. Simulate work
    time.sleep(2)

    # 4. Reconnect test
    vpn.reconnect() # You can pass a different location here too
    print(f"Status after Reconnect: {vpn.status()}")

    # 5. Clean up
    vpn.disconnect()