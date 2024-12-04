import json
import os
import zipfile
import PyPDF2
from urllib.parse import unquote
from db_manager import PDFArchiveDatabaseManager

def extract_pdf_text_from_zip(zip_path, pdf_filename, max_pages=10, max_chars=10000):
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

                full_text = ""
                for page in pdf_reader.pages[:max_pages]:
                    full_text += page.extract_text()

                cleaned_text = ' '.join(full_text.split())
                truncated_text = cleaned_text[:max_chars]

                return truncated_text

    except Exception as e:
        return f"Error extracting text: {str(e)}"


def write_to_file(error, zip_path, pdf_filename='', output_file='pdf_error.txt'):
    with open(output_file, 'a', encoding='utf-8') as f:
        f.write(f"zip_path: {zip_path}\n")
        f.write(f"pdf_filename: {pdf_filename}\n")
        f.write(f"error: {error}\n\n")


def process_zip_archives_to_sqlite(db_manager, root_directory, batch_size=10):
    """
    Process ZIP archives and write PDF contents to SQLite database in batches

    Args:
    - db_manager: PDFArchiveDatabaseManager instance
    - root_directory: Root directory containing ZIP archives
    - batch_size: Number of records to insert in a single batch
    """
    batch = []
    metadata = json.dumps({"root_directory": root_directory})
    last_entry = db_manager.get_last_entry()
    last_processed_pdf = last_entry['pdf_filename'] if last_entry else None
    entity_count = last_entry['id'] if last_entry else None

    try:
        for root, dirs, files in os.walk(root_directory):
            for file in files:
                if file.lower().endswith('.zip'):
                    zip_path = os.path.join(root, file)

                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            pdf_files = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
                            relative_zip_path = os.path.relpath(zip_path, root_directory)

                            # Skip this archive if entity_count > total_pdfs
                            total_pdfs = len(pdf_files)
                            if entity_count is not None and entity_count > total_pdfs:
                                entity_count -= total_pdfs
                                continue

                            for pdf_filename in pdf_files:
                                try:
                                    unicode_filename = unquote(pdf_filename)
                                    if last_processed_pdf and unicode_filename == last_processed_pdf:
                                        last_processed_pdf = None  # Сбрасываем, как только достигли последнего обработанного
                                        continue  # Пропускаем последний обработанный файл
                                    if last_processed_pdf:  # Пока не достигли последнего файла
                                        continue

                                    raw_text = extract_pdf_text_from_zip(zip_path, pdf_filename)
                                    batch.append((relative_zip_path, unicode_filename, raw_text, metadata))

                                    if len(batch) >= batch_size:
                                        db_manager.insert_pdf_contents_batch(batch)
                                        batch = []  # Reset batch

                                except Exception as pdf_error:
                                    print(f"Error processing PDF {pdf_filename} in {zip_path}: {pdf_error}")
                                    write_to_file(pdf_error, zip_path, pdf_filename)

                    except Exception as zip_error:
                        print(f"Error processing ZIP archive {zip_path}: {zip_error}")
                        write_to_file(zip_error, zip_path)

        # Insert any remaining records
        if batch:
            db_manager.insert_pdf_contents_batch(batch)

    except Exception as e:
        print(f"Unexpected error: {e}")


def main():
    db_name = 'archive_test.db'
    root_directory = input("Введите путь к директории с ZIP-архивами: ").strip()
    db_path = input("Введите путь к базе данных (нажмите Enter для использования значения по умолчанию): ").strip()
    if not db_path:
        db_path = ''

    db_manager = PDFArchiveDatabaseManager(os.path.join(db_path, db_name))

    conn = db_manager.create_database_schema()

    if conn:
        try:
            process_zip_archives_to_sqlite(db_manager, root_directory)
            print("PDF contents have been processed and stored in the database.")

        finally:
            conn.close()
    else:
        print("Failed to create database connection.")


if __name__ == "__main__":
    main()
