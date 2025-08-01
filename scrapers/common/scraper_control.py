import sqlite3

class scraper_control:

    # ------------------------------------------------------------------
    # Status management
    # ------------------------------------------------------------------

    def make_status(db_path: str = "scraper_control.db") -> None:
        """
        Create a status table in the SQLite database if it doesn't exist.
        This is used to track scraper activity.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraper_status (
                name TEXT PRIMARY KEY,
                status TEXT CHECK( status IN ('ON','OFF') )   NOT NULL DEFAULT 'OFF',
                last_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()
    
    def get_status(name: str, db_path: str = "scraper_control.db") -> str:
        """
        Get the status of a scraper from the SQLite database.
        Returns 'OFF' if not set.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM scraper_status WHERE name = ?", (name,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else "OFF"
        

    def set_status(name: str, status: str, db_path: str = "scraper_control.db") -> None:
        """
        Set the status of a scraper in the SQLite database.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO scraper_status (name, status)
            VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET status = ?, last_run = CURRENT_TIMESTAMP
        """, (name, status, status))
        conn.commit()
        conn.close()

    # ------------------------------------------------------------------
    # Control table management
    # ------------------------------------------------------------------

    def make_control_status(db_path: str = "scraper_control.db") -> None:
        """
        Create a control table in the SQLite database if it doesn't exist.
        This is used to pause or start scrapers.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraper_control (
                name TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK(status IN ('active', 'paused')) NOT NULL DEFAULT 'active'
            )
        """)
        conn.commit()
        conn.close()

    def get_control_status(name: str, db_path: str = "scraper_control.db") -> str:
        """
        Get the control status of a scraper from the SQLite database.
        Returns 'active' if not set.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM scraper_control WHERE name = ?", (name,))
        row = cursor.fetchone()
        conn.close()
        return row[0] if row else "active"

    def set_control_status(name: str, status: str, db_path: str = "scraper_control.db") -> None:
        """
        Set the control status of a scraper in the SQLite database.
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO scraper_control (name, status)
            VALUES (?, ?)
        """, (name, status))
        conn.commit()
        conn.close()

    def init() -> None:
        """
        Initialize the database by creating necessary tables.
        """
        scraper_control.make_status()
        scraper_control.make_control_status()
        print("Database initialized with status and control tables.")
