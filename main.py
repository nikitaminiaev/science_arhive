import os
import zipfile
import sqlite3
import PyPDF2
# from transformers import pipeline

def extract_pdf_text_from_zip(zip_path, pdf_filename, max_chars=1000):
    """
    Extract text from the first page of a PDF within a ZIP archive
    Focuses on capturing key information efficiently

    Args:
    - zip_path: Path to the ZIP archive
    - pdf_filename: Name of the PDF file within the archive
    - max_chars: Maximum number of characters to extract (default 1000)

    Returns:
    - Extracted text from the first page, truncated to max_chars
    """
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            with zip_ref.open(pdf_filename) as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)

                # Check if the PDF has at least one page
                if len(pdf_reader.pages) > 0:
                    # Extract text from the first page
                    first_page_text = pdf_reader.pages[0].extract_text()

                    # Clean and truncate the text
                    cleaned_text = ' '.join(first_page_text.split())  # Remove extra whitespace
                    truncated_text = cleaned_text[:max_chars]

                    return truncated_text
                else:
                    return "Empty PDF"

    except Exception as e:
        return f"Error extracting text: {str(e)}"


# def summarize_text(text, max_length=150, min_length=50):
#     """Generate a concise summary of the text"""
#     summarizer = pipeline("summarization")
#     summaries = summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)
#     return summaries[0]['summary_text']


def create_pdf_contents_table(db_path='pdf_archive.db'):
    """
    Create PDF contents table in SQLite database

    Args:
    - db_path: Path to SQLite database

    Returns:
    - sqlite3.Connection object
    """
    try:
        # Establish database connection
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_contents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                zip_path TEXT,
                pdf_filename TEXT,
                raw_text TEXT
            )
        ''')

        # Commit changes and return connection
        conn.commit()
        return conn

    except sqlite3.Error as e:
        print(f"Error creating database: {e}")
        return None

def write_to_file(error, zip_path, pdf_filename='', output_file='pdf_index.txt'):
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"zip_path: {zip_path}\n")
        f.write(f"pdf_filename: {pdf_filename}\n")
        f.write(f"error: {error}\n\n")

def process_zip_archives_to_sqlite(conn, root_directory, batch_size=10):
    """
    Process ZIP archives and write PDF contents to SQLite database in batches

    Args:
    - conn: SQLite database connection
    - root_directory: Root directory containing ZIP archives
    - batch_size: Number of records to insert in a single batch
    """
    # Create cursor
    cursor = conn.cursor()

    # Batch to store records
    batch = []

    try:
        # Walk through directories
        for root, dirs, files in os.walk(root_directory):
            for file in files:
                if file.lower().endswith('.zip'):
                    zip_path = os.path.join(root, file)

                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            # Find PDF files in the archive
                            pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
                            relative_zip_path = os.path.relpath(zip_path, root_directory)
                            # Process each PDF
                            for pdf_filename in pdf_files:
                                try:
                                    # Extract raw text
                                    raw_text = extract_pdf_text_from_zip(zip_path, pdf_filename)

                                    # Add to batch
                                    batch.append((relative_zip_path, pdf_filename, raw_text))

                                    # Insert batch when it reaches batch_size
                                    if len(batch) >= batch_size:
                                        cursor.executemany(
                                            'INSERT INTO pdf_contents (zip_path, pdf_filename, raw_text) VALUES (?, ?, ?)',
                                            batch
                                        )
                                        conn.commit()
                                        batch = []  # Reset batch

                                except Exception as pdf_error:
                                    print(f"Error processing PDF {pdf_filename} in {zip_path}: {pdf_error}")
                                    write_to_file(pdf_error, zip_path, pdf_filename)

                    except Exception as zip_error:
                        print(f"Error processing ZIP archive {zip_path}: {zip_error}")
                        write_to_file(zip_error, zip_path)

        # Insert any remaining records
        if batch:
            cursor.executemany(
                'INSERT INTO pdf_contents (zip_path, pdf_filename, raw_text) VALUES (?, ?, ?)',
                batch
            )
            conn.commit()

    except Exception as e:
        print(f"Unexpected error: {e}")


# Main execution
def main():
    # Paths
    root_directory = '/path/to/your/zip/archives'
    db_path = 'pdf_archive.db'

    conn = create_pdf_contents_table(db_path)

    if conn:
        try:
            # 2. Process archives
            process_zip_archives_to_sqlite(conn, root_directory)
            print("PDF contents have been processed and stored in the database.")

        finally:
            # Always close the connection
            conn.close()
    else:
        print("Failed to create database connection.")


# Run the script
if __name__ == "__main__":
    main()
