import dropbox
import os

from extractor.config import DROPBOX_ACCESS_TOKEN
from extractor import PATH_DRIVE, PATH_NFE

DOWNLOADED_CACHE = PATH_DRIVE / "cache" / ".downloaded_files"
INPUT_FOLDER = PATH_NFE / "input"
OUTPUT_FOLDER = PATH_NFE / "output"


def connect_to_dropbox(access_token: str | None = None) -> dropbox.Dropbox:
    if not access_token:
        access_token = DROPBOX_ACCESS_TOKEN
    return dropbox.Dropbox(access_token)


def get_downloaded_files() -> set[str]:
    if not os.path.exists(DOWNLOADED_CACHE):
        return set()
    with open(DOWNLOADED_CACHE, "r") as file:
        return set(file.read().splitlines())


def add_to_downloaded_files(file_name: str):
    with open(DOWNLOADED_CACHE, "a") as file:
        file.write(f"{file_name}\n")


def download_file(dbx, file_metadata):
    with open(INPUT_FOLDER / file_metadata.name, "wb") as file:
        metadata, res = dbx.files_download(path=file_metadata.path_lower)
        file.write(res.content)
    add_to_downloaded_files(file_metadata.content_hash)


def save_download_urls(filename, download_url):
    file_path = OUTPUT_FOLDER / "download_urls.csv"

    # Check if file exists and is empty
    file_exists = os.path.exists(file_path)
    is_empty = not os.path.getsize(file_path) if file_exists else True

    with open(file_path, "a") as file:
        if is_empty:
            file.write("filename,download_url\n")  # Write column names
        file.write(f"{filename},{download_url}\n")


def get_files_list(dbx, folder_path: str):
    downloaded_files = get_downloaded_files()
    try:
        files = dbx.files_list_folder(folder_path).entries
        for file in files:
            if isinstance(file, dropbox.files.FileMetadata):
                if file.content_hash not in downloaded_files:
                    save_download_urls(
                        file.name.split("/")[-1],
                        dbx.files_get_temporary_link(file.path_lower).link,
                    )
                    print(f"Downloading: {file.name}")
                    download_file(dbx, file)
                else:
                    print(f"Skipping: {file.name}, already downloaded")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    folder_path = "/01. Adm/Adm. 2015/BANCOS 15/notas_fiscais"
    dbx = connect_to_dropbox()
    get_files_list(dbx, folder_path)
