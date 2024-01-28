#!/usr/bin/env python3
import os
import glob
from functools import partial
from typing import List
from multiprocessing import Pool
from tqdm import tqdm
from loguru import logger as log

from langchain.document_loaders import (
    CSVLoader,
    EverNoteLoader,
    PyMuPDFLoader,
    TextLoader,
    UnstructuredEmailLoader,
    UnstructuredEPubLoader,
    UnstructuredHTMLLoader,
    UnstructuredMarkdownLoader,
    UnstructuredODTLoader,
    UnstructuredPowerPointLoader,
    UnstructuredWordDocumentLoader,
)

from langchain.docstore.document import Document

from extractor.config import ROOT_PATH, settings
import extractor.utils as utils


# Load environment variables
persist_directory = os.environ.get("PERSIST_DIRECTORY", "db")
source_directory = os.environ.get("SOURCE_DIRECTORY", "source_documents")


# Custom document loaders
class MyElmLoader(UnstructuredEmailLoader):
    """Wrapper to fallback to text/plain when default does not work"""

    def load(self) -> List[Document]:
        """Wrapper adding fallback for elm without html"""
        try:
            try:
                doc = UnstructuredEmailLoader.load(self)
            except ValueError as e:
                if "text/html content not found in email" in str(e):
                    # Try plain text
                    self.unstructured_kwargs["content_source"] = "text/plain"
                    doc = UnstructuredEmailLoader.load(self)
                else:
                    raise
        except Exception as e:
            # Add file_path to exception message
            raise type(e)(f"{self.file_path}: {e}") from e

        return doc


# Map file extensions to document loaders and their arguments
LOADER_MAPPING = {
    ".csv": (CSVLoader, {}),
    ".doc": (UnstructuredWordDocumentLoader, {}),
    ".docx": (UnstructuredWordDocumentLoader, {}),
    ".enex": (EverNoteLoader, {}),
    ".eml": (MyElmLoader, {}),
    ".epub": (UnstructuredEPubLoader, {}),
    ".html": (UnstructuredHTMLLoader, {}),
    ".md": (UnstructuredMarkdownLoader, {}),
    ".odt": (UnstructuredODTLoader, {}),
    ".pdf": (PyMuPDFLoader, {}),
    ".ppt": (UnstructuredPowerPointLoader, {}),
    ".pptx": (UnstructuredPowerPointLoader, {}),
    ".txt": (TextLoader, {"encoding": "utf8"}),
    ".xml": (TextLoader, {"encoding": "utf8"}),
    # Add more mappings for other file extensions and loaders as needed
}


def get_files(source_dir: str, ignored_files: List[str] = []) -> List[Document]:
    """
    Loads all documents from the source documents directory, ignoring specified files
    """
    all_files = []
    for ext in LOADER_MAPPING:
        all_files.extend(
            glob.glob(os.path.join(source_dir, f"**/*{ext}"), recursive=True)
        )
    return [file_path for file_path in all_files if file_path not in ignored_files]


def load_single_document(
    file_path: str, keep_first_page_only: bool = False
) -> List[Document]:
    ext = "." + file_path.rsplit(".", 1)[-1]
    if ext in LOADER_MAPPING:
        loader_class, loader_args = LOADER_MAPPING[ext]
        loader = loader_class(file_path, **loader_args)
        documents = loader.load()
        if len(documents) > 1:
            if keep_first_page_only:
                log.info(f"Keeping only first page of {file_path}")
                documents = [documents[0]]
            else:
                log.info(f"Multiple documents found in {file_path}")
                documents = [
                    Document(
                        metadata=documents[0].metadata,
                        page_content=" ".join([doc.page_content for doc in documents]),
                    )
                ]
        return documents

    raise ValueError(f"Unsupported file extension '{ext}'")


def run(files: List[Document], keep_first_page_only: bool = False) -> List[Document]:
    """
    Loads all documents from the source documents directory, preserving the order of files.
    """
    with Pool(processes=os.cpu_count()) as pool:
        results = []
        loader_with_flag = partial(
            load_single_document, keep_first_page_only=keep_first_page_only
        )
        with tqdm(total=len(files), desc="Loading new documents", ncols=80) as pbar:
            for docs in pool.imap(loader_with_flag, files):  # type: ignore
                results.extend(docs)
                pbar.update()

    return results


if __name__ == "__main__":
    NFES_INPUT = utils.get_files(ROOT_PATH, settings["filepaths"]["nfe_input"])

    documents = run(files=NFES_INPUT)
    for document in documents:
        print(document.page_content)
