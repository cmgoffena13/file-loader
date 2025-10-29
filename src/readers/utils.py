import logging
import os
from pathlib import Path
from typing import List

from src.file_processor import FileProcessingResult, FileProcessor
from src.readers.reader_factory import ReaderFactory
from src.settings import config

logger = logging.getLogger(__name__)


def process_directory() -> List[FileProcessingResult]:
    directory = Path(config.DIRECTORY_PATH)

    if not directory.exists():
        raise FileNotFoundError(f"Directory not found: {directory}")

    if not directory.is_dir():
        raise ValueError(f"Path is not a directory: {directory}")

    # Use os.scandir() for faster file discovery
    supported_extensions = set(ReaderFactory.get_supported_extensions())
    files = []

    for entry in os.scandir(directory):
        if (
            entry.is_file()
            and not entry.name.startswith(".")  # Skip hidden files
            and Path(entry.path).suffix.lower() in supported_extensions
        ):
            files.append(Path(entry.path))

    if not files:
        logger.warning(f"No files found in directory: {directory}")
        return []

    processor = FileProcessor()

    file_paths = [str(f) for f in files]

    if not file_paths:
        logger.warning(f"No files found in directory: {directory}")
        return []

    return processor.process_files_parallel(file_paths, config.ARCHIVE_PATH)
