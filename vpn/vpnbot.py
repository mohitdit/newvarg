import time
import subprocess
import sys
import os

# You must install this library: pip install pywinauto psutil
try:
    from pywinauto import Application, Desktop
    import psutil
except ImportError:
    print("[CRITICAL] Library missing. Please run: pip install pywinauto psutil")
    sys.exit(1)

class SurfsharkBot:
    def __init__(self):
        self.app_path = r"C:\Program Files\Surfshark\Surfshark.exe"
        self.app = None
        self.main_window = None

    def _get_window(self):
        """
        Private: Finds or launches the Surfshark window and brings it to front.
        """
        # 1. Launch/Focus the App
        if not os.path.exists(self.app_path):
            print(f"[!] Error: Path not found {self.app_path}")
            return None

        try:
            # We launch it to ensure it comes out of the system tray
            subprocess.Popen([self.app_path])
            time.sleep(3) # Wait for UI to appear

            # 2. Connect to the existing process
            # We use backend="uia" which is best for modern Windows apps
            self.app = Application(backend="uia").connect(path="Surfshark.exe")
            
            # 3. Find the main window (Title usually starts with Surfshark)
            # We use a wildcard regex because the version number changes (e.g., Surfshark 6.2.0)
            self.main_window = self.app.window(title_re="Surfshark.*")
            
            # 4. Restore if minimized and focus
            if self.main_window.exists():
                self.main_window.restore()
                self.main_window.set_focus()
                return self.main_window
            else:
                print("[!] Could not find Surfshark window title.")
                return None
        except Exception as e:
            print(f"[!] Error finding window: {e}")
            return None

    def disconnect(self):
        """
        Finds the 'Disconnect' button and clicks it.
        """
        print("[-] Bot: Attempting to Disconnect...")
        win = self._get_window()
        if not win: return

        try:
            # Look for a button strictly named "Disconnect"
            # Surfshark button names are usually exposed to automation tools
            disconnect_btn = win.child_window(title="Disconnect", control_type="Button")
            
            if disconnect_btn.exists():
                disconnect_btn.click()
                print("    -> 'Disconnect' button clicked.")
                # Wait for animation to finish
                time.sleep(3)
            else:
                print("    -> 'Disconnect' button not found. (Already disconnected?)")
        except Exception as e:
            print(f"[!] Disconnect failed: {e}")

    def connect(self, server_argument="Quick-connect"):
        """
        Clicks 'Quick-connect' or attempts to search.
        Note: Passing a specific argument (like 'us-nyc') is complex in GUI automation
        without crashing, so we default to the reliable 'Quick-connect' button.
        """
        print(f"[+] Bot: Attempting to Connect (Target: {server_argument})...")
        win = self._get_window()
        if not win: return

        try:
            # 1. Try to find "Quick-connect" button first
            quick_btn = win.child_window(title="Quick-connect", control_type="Button")
            
            # 2. Try to find generic "Connect" button (sometimes label changes)
            connect_btn = win.child_window(title="Connect", control_type="Button")

            if quick_btn.exists():
                quick_btn.click()
                print("    -> 'Quick-connect' button clicked.")
                time.sleep(5) # Wait for connection
            elif connect_btn.exists():
                connect_btn.click()
                print("    -> 'Connect' button clicked.")
                time.sleep(5)
            else:
                print("    -> No Connect button found. (Already connected?)")

        except Exception as e:
            print(f"[!] Connect failed: {e}")

    def reconnect(self, argument="Default"):
        self.disconnect()
        # Small buffer between actions
        time.sleep(2)
        self.connect(argument)

if __name__ == "__main__":
    bot = SurfsharkBot()
    
    # This works like a robot user. 
    # It will pop the window open and click the buttons.
    bot.reconnect()