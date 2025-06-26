import sqlite3
import json


class PDFArchiveDatabaseManager:
    """
    A class to manage SQLite database operations for PDF archive contents
    Optimized version that stores only search index without duplicating raw text
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
        Create optimized database schema with metadata table and FTS index only

        Returns:
        - sqlite3.Connection object or None if creation fails
        """
        try:
            # Establish database connection
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()

            # Create main table WITHOUT raw_text field (only metadata)
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS pdf_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    zip_path TEXT NOT NULL,
                    pdf_filename TEXT NOT NULL,
                    metadata TEXT DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(zip_path, pdf_filename)
                )
            ''')

            # Create FTS table that stores ONLY the searchable text (no duplication)
            self.cursor.execute('''
                CREATE VIRTUAL TABLE IF NOT EXISTS pdf_search_index 
                USING fts5(
                    content,
                    tokenize='porter unicode61 remove_diacritics 1',
                    prefix='2 3 4'
                )
            ''')

            # Create index for fast lookups
            self.cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_pdf_metadata_path 
                ON pdf_metadata(zip_path, pdf_filename)
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
        - batch: List of tuples containing (zip_path, pdf_filename, raw_text, metadata, file_size, pages_count)
        """
        if not self.conn or not self.cursor:
            raise RuntimeError("Database connection not established. Call create_database_schema() first.")

        try:
            # Insert metadata (without raw_text)
            metadata_batch = []
            search_batch = []
            
            for item in batch:
                zip_path, pdf_filename, raw_text = item[:3]
                base_metadata = item[3] if len(item) > 3 else {}
                file_size = item[4] if len(item) > 4 else None
                pages_count = item[5] if len(item) > 5 else None
                
                # Parse existing metadata if it's a JSON string
                if isinstance(base_metadata, str):
                    try:
                        base_metadata = json.loads(base_metadata)
                    except (json.JSONDecodeError, TypeError):
                        base_metadata = {}
                elif base_metadata is None:
                    base_metadata = {}
                
                # Add file_size and pages_count to metadata
                if file_size is not None:
                    base_metadata['file_size'] = file_size
                if pages_count is not None:
                    base_metadata['pages_count'] = pages_count
                
                # Convert back to JSON string
                metadata_json = json.dumps(base_metadata)
                
                # Prepare metadata insert
                metadata_batch.append((zip_path, pdf_filename, metadata_json))
                
                # Prepare search index insert (we'll need the ID)
                search_batch.append(raw_text)
            
            # Insert metadata first
            self.cursor.executemany(
                'INSERT OR IGNORE INTO pdf_metadata (zip_path, pdf_filename, metadata) VALUES (?, ?, ?)',
                metadata_batch
            )
            
            # Get the IDs of inserted records
            inserted_ids = []
            for zip_path, pdf_filename, _ in metadata_batch:
                self.cursor.execute(
                    'SELECT id FROM pdf_metadata WHERE zip_path = ? AND pdf_filename = ?',
                    (zip_path, pdf_filename)
                )
                result = self.cursor.fetchone()
                if result:
                    inserted_ids.append(result[0])
            
            # Insert into FTS index with explicit rowid mapping
            for i, raw_text in enumerate(search_batch):
                if i < len(inserted_ids):
                    self.cursor.execute(
                        'INSERT INTO pdf_search_index(rowid, content) VALUES (?, ?)',
                        (inserted_ids[i], raw_text)
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
            self.cursor.execute("SELECT id, pdf_filename FROM pdf_metadata ORDER BY id DESC LIMIT 1")
            row = self.cursor.fetchone()
            return {'id': row[0], 'pdf_filename': row[1]} if row else None
        except sqlite3.Error as e:
            print(f"Error retrieving last entry: {e}")
            return None

    def insert_pdf_content(self, zip_path, pdf_filename, raw_text, metadata, file_size=None, pages_count=None):
        """
        Insert a single PDF content into the database
        
        Args:
        - zip_path: Path to the ZIP archive
        - pdf_filename: Name of the PDF file
        - raw_text: Extracted text content
        - metadata: Metadata as JSON string or dict
        - file_size: Size of the PDF file (optional)
        - pages_count: Number of pages in PDF (optional)
        """
        if not self.conn or not self.cursor:
            raise RuntimeError("Database connection not established. Call create_database_schema() first.")

        try:
            # Parse metadata if it's a string
            if isinstance(metadata, str):
                try:
                    metadata_dict = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata_dict = {}
            elif metadata is None:
                metadata_dict = {}
            else:
                metadata_dict = metadata

            # Add file size and pages count to metadata
            if file_size is not None:
                metadata_dict['file_size'] = file_size
            if pages_count is not None:
                metadata_dict['pages_count'] = pages_count

            # Convert back to JSON string
            metadata_json = json.dumps(metadata_dict)

            # Insert metadata
            self.cursor.execute(
                'INSERT OR IGNORE INTO pdf_metadata (zip_path, pdf_filename, metadata) VALUES (?, ?, ?)',
                (zip_path, pdf_filename, metadata_json)
            )

            # Get the ID of the inserted or existing record
            self.cursor.execute(
                'SELECT id FROM pdf_metadata WHERE zip_path = ? AND pdf_filename = ?',
                (zip_path, pdf_filename)
            )
            result = self.cursor.fetchone()
            
            if result:
                record_id = result[0]
                # Insert into FTS index
                self.cursor.execute(
                    'INSERT OR REPLACE INTO pdf_search_index(rowid, content) VALUES (?, ?)',
                    (record_id, raw_text)
                )

            self.conn.commit()

        except sqlite3.Error as e:
            print(f"Error inserting PDF content: {e}")
            self.conn.rollback()
            raise

    def pdf_exists(self, zip_path, pdf_filename):
        """
        Check if a PDF file already exists in the database
        
        Args:
        - zip_path: Path to the ZIP archive
        - pdf_filename: Name of the PDF file
        
        Returns:
        - True if exists, False otherwise
        """
        if not self.conn or not self.cursor:
            raise RuntimeError("Database connection not established. Call create_database_schema() first.")

        try:
            self.cursor.execute(
                'SELECT id FROM pdf_metadata WHERE zip_path = ? AND pdf_filename = ? LIMIT 1',
                (zip_path, pdf_filename)
            )
            return self.cursor.fetchone() is not None

        except sqlite3.Error as e:
            print(f"Error checking PDF existence: {e}")
            return False
