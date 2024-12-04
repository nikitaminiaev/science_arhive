import sqlite3


class PDFArchiveDatabaseManager:
    """
    A class to manage SQLite database operations for PDF archive contents
    """

    def __init__(self, db_path='archive.db'):
        """
        Initialize the database manager

        Args:
        - db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self.cursor = None

    def create_database_schema(self):
        """
        Create the database schema with main table and full-text search table

        Returns:
        - sqlite3.Connection object or None if creation fails
        """
        try:
            # Establish database connection
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()

            # Create main table with fields
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS pdf_contents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    zip_path TEXT,
                    pdf_filename TEXT,
                    raw_text TEXT,
                    metadata TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Create virtual table for full-text search
            self.cursor.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS pdf_contents_fts 
                USING fts5(raw_text)
            ''')

            # Trigger to synchronize main table and FTS on insert
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS after_insert_pdf_contents 
                AFTER INSERT ON pdf_contents BEGIN
                    INSERT INTO pdf_contents_fts(rowid, raw_text) 
                    VALUES (NEW.id, NEW.raw_text);
                END
            ''')

            # Trigger to synchronize main table and FTS on update
            self.cursor.execute('''
                CREATE TRIGGER IF NOT EXISTS after_update_pdf_contents 
                AFTER UPDATE ON pdf_contents BEGIN
                    UPDATE pdf_contents_fts 
                    SET raw_text = NEW.raw_text 
                    WHERE rowid = NEW.id;
                END
            ''')

            # Commit changes
            self.conn.commit()
            return self.conn

        except sqlite3.Error as e:
            print(f"Error creating database: {e}")
            return None

    def insert_pdf_contents_batch(self, batch):
        """
        Insert a batch of PDF contents into the database

        Args:
        - batch: List of tuples containing (zip_path, pdf_filename, raw_text, metadata)
        """
        if not self.conn or not self.cursor:
            raise RuntimeError("Database connection not established. Call create_database_schema() first.")

        try:
            self.cursor.executemany(
                'INSERT INTO pdf_contents (zip_path, pdf_filename, raw_text, metadata) VALUES (?, ?, ?, ?)',
                batch
            )
            self.conn.commit()
        except sqlite3.Error as e:
            print(f"Error inserting batch: {e}")
            self.conn.rollback()

    def get_last_entry(self):
        """
        Retrieve the last processed entry from the database

        Returns:
        - Dictionary with the last entry (id, pdf_filename) or None if table is empty
        """
        try:
            self.cursor.execute("SELECT id, pdf_filename FROM pdf_contents ORDER BY id DESC LIMIT 1")
            row = self.cursor.fetchone()
            return {'id': row[0], 'pdf_filename': row[1]} if row else None
        except sqlite3.Error as e:
            print(f"Error retrieving last entry: {e}")
            return None
