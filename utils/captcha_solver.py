import deathbycaptcha
import os
import tempfile
from utils.logger import log

DBC_USERNAME = "hr@dharani.co.in"
DBC_PASSWORD = "Dh@r@ni@gnt99!"
async def solve_captcha(image_bytes: bytes) -> str:
    """
    Solve CAPTCHA using DeathByCaptcha HTTP API.
    Saves bytes to a temp file first to ensure library compatibility.
    """
    temp_filename = None
    try:
        # Create a temporary file to store the image
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as temp_file:
            temp_file.write(image_bytes)
            temp_filename = temp_file.name

        log.info(f"📤 Sending CAPTCHA to DeathByCaptcha (Size: {len(image_bytes)} bytes)...")
        
        # Initialize Client
        client = deathbycaptcha.HttpClient(DBC_USERNAME, DBC_PASSWORD)

        # Decode
        # default timeout is often 60 seconds
        captcha = client.decode(temp_filename, 60) 

        if captcha and "text" in captcha:
            solved = captcha["text"]
            log.info(f"🤖 DBC Solved: {solved} (ID: {captcha['captcha']})")
            return solved
        else:
            log.error("❌ DBC returned empty text.")
            return None

    except deathbycaptcha.AccessDeniedException:
        log.error("❌ DBC Access Denied: Check Username/Password")
        return None
    except Exception as e:
        log.error(f"❌ DBC Error: {e}")
        return None
    finally:
        # Cleanup temp file
        if temp_filename and os.path.exists(temp_filename):
            os.remove(temp_filename)