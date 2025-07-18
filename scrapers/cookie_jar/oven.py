import pickle
from pathlib import Path

class Oven:
    """
    Helper class that 
    """
    def __init__(self, path: str):
        """ Set path for corresponding website cookie"""
        self.path = Path(path)

    def exists(self) -> bool:
        """Check if cookie exists"""
        return self.path.exists()

    def load(self, driver):
        """Load cookie
        args:
            driver: Headless webdriver
        """
        if not self.exists():
            return False
        with self.path.open("rb") as f:
            for c in pickle.load(f):
                driver.add_cookie(c)
        return True

    def save(self, driver):
        """Save cookie
        args:
            driver: Headless webdriver
        """
        with self.path.open("wb") as f:
            pickle.dump(driver.get_cookies(), f)