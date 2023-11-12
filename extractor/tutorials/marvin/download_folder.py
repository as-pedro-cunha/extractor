from pathlib import Path
from extractor.config import marvin  # noqa: F401
from marvin import ai_classifier
from enum import Enum
import asyncio
from concurrent.futures import ThreadPoolExecutor
import shutil
from loguru import logger
from glob import glob


@ai_classifier
class FileCategory(Enum):
    """Represents distinct categories for files"""

    BRADESCO = "Bradesco related files"
    IMAGES = "images_others"
    ART_IMAGES = "art_images"
    SOFTWARE = "linux_software"
    PYTHON = "python"
    CSV = "csv"
    DOCUMENTS = "documents"
    AUDIO = "audio"
    VIDEO = "video"
    OTHERS = "others"
    UNKNOWN = "unknown"


TIMEOUT = 10


async def organize_files(
    file_path: str, target_path: Path, classifier, timeout: int = TIMEOUT
):
    try:
        # Run the classifier in a thread pool executor and apply a timeout
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as pool:
            category = await asyncio.wait_for(
                loop.run_in_executor(pool, classifier, file_path), timeout
            )
    except asyncio.TimeoutError:
        logger.error(f"Classification timed out for file {file_path}")
        category = FileCategory.UNKNOWN
    except Exception as e:
        logger.exception(f"Error while classifying file {file_path}: {e}")
        category = FileCategory.UNKNOWN

    logger.info(f"File {file_path} classified as {category.value}")
    await move_file(file_path, target_path, category)


async def move_file(file_path: str, target_path: Path, category: FileCategory):
    try:
        target_directory = target_path / category.value
        target_directory.mkdir(parents=True, exist_ok=True)
        shutil.move(file_path, target_directory)
    except Exception as e:
        logger.exception(f"Error moving file {file_path} to {target_directory}: {e}")


def get_files_to_organize(path: Path) -> list:
    """Retrieve a list of files to be organized."""
    # Convert Path to string for glob
    return glob(str(path))


def process_files_in_batches(files: list, target_path: Path, batch_size: int):
    for batch_files in chunks(files, batch_size):
        logger.info(f"Processing batch of {len(batch_files)} files")
        asyncio.run(process_batch(batch_files, target_path))
        logger.info("Batch processing completed")


async def process_batch(batch_files: list, target_path: Path):
    tasks = [
        organize_files(file_path, target_path, FileCategory)
        for file_path in batch_files
    ]
    await asyncio.gather(*tasks)


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def run_organizer(path: str, target_path: str, batch_size: int):
    # Expanding the tilde in paths
    expanded_path = str(Path(path).expanduser())
    expanded_target_path = str(Path(target_path).expanduser())

    files = get_files_to_organize(expanded_path)
    process_files_in_batches(files, Path(expanded_target_path), batch_size)


if __name__ == "__main__":
    download_path = "~/Downloads/*"
    target_path = "~/organized/"
    run_organizer(download_path, target_path, 10)
