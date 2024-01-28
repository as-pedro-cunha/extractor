import os
import hashlib
from instructor import OpenAISchema
import pandas as pd
from pydantic import BaseModel
from typing import List, Callable, Iterator, Any, Tuple, Union
import csv
from loguru import logger as log
from typing import Type

from extractor import PATH_NFE


DOWNLOAD_URLS = PATH_NFE / "output" / "download_urls.csv"


def open_xml_as_txt(file_path: str) -> str:
    with open(file_path, "r") as f:
        return f.read()


def get_path(root_path: str, list_paths: List[str]) -> str:
    return os.path.join(root_path, *list_paths)


def get_files(root_path: str, list_paths: None | List[str] = None) -> List[str]:
    target_path = get_path(root_path, list_paths) if list_paths else root_path
    return [os.path.join(target_path, file) for file in os.listdir(target_path)]


def dataclass_to_pandas(
    models: List[OpenAISchema], timestamp: str, filepath: str, inserted_at: pd.Timestamp
) -> pd.DataFrame:
    # Convert your dataclass models to a DataFrame
    incoming_df = pd.DataFrame([model.dict(by_alias=True) for model in models])

    # Drop columns where all elements are NA
    incoming_df = incoming_df.dropna(axis=1, how="all")

    # create a column that is the current timestamp
    incoming_df["inserted_at"] = inserted_at

    # add the "download_url" from
    download_urls = pd.read_csv(DOWNLOAD_URLS)
    # keys: filename, download_url
    # now use the filename to match with the "input_filepath" in the incomding_df so we can add the download_url to the dataframe
    incoming_df["input_filename"] = incoming_df["input_filepath"].apply(
        lambda x: x.split("/")[-1] if isinstance(x, str) else None
    )
    # Merge the incoming_df with download_urls on the 'input_filename' and 'filename' columns
    incoming_df = incoming_df.merge(
        download_urls, left_on="input_filename", right_on="filename", how="left"
    )

    # After merging, 'download_url' from download_urls will be in incoming_df
    # You can drop the extra 'filename' column from the merge if not needed
    incoming_df.drop("filename", axis=1, inplace=True)
    incoming_df.drop("input_filename", axis=1, inplace=True)

    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        # Also clean the existing dataframe
        df = df.dropna(axis=1, how="all")
        combined_df = pd.concat([df, incoming_df], ignore_index=True)
    else:
        combined_df = incoming_df

    # Sort by the given timestamp column
    combined_df = combined_df.sort_values(by=timestamp, ascending=False)
    # Save the combined DataFrame to a CSV file
    combined_df.to_csv(filepath, index=False)

    return combined_df


def iterate_over_files(
    function_to_apply: Callable, file_contents: Union[str, List[str]]
) -> Iterator:
    file_contents = (
        list(file_contents) if isinstance(file_contents, str) else file_contents
    )
    for file_content in file_contents:
        yield function_to_apply(file_content)


def hash_of_txt(txt: str) -> str:
    return hashlib.md5(txt.encode("utf-8")).hexdigest()[0:6]


def fields_to_csv(model: BaseModel, filepath: str):
    model_dict = model.dict()
    file_exists = os.path.exists(filepath)

    with open(filepath, "a", newline="") as f:
        # Ensure that all non-numeric fields are quoted
        writer = csv.DictWriter(
            f, fieldnames=model_dict.keys(), quoting=csv.QUOTE_NONNUMERIC
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow(model_dict)


def check_unique(conditions: List[Tuple[str, Any]], filepath: str) -> bool:
    if not os.path.exists(filepath):
        return True

    conditions_dict = {condition[0]: str(condition[1]) for condition in conditions}

    with open(filepath, "r") as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if all(row.get(field) == value for field, value in conditions_dict.items()):
                log.error(f"Already exists: {conditions_dict}")
        return True


def check_short_hash(filepath: str, short_hash: str) -> bool:
    if not os.path.exists(filepath):
        return True

    with open(filepath, "r") as f:
        csv_reader = csv.DictReader(f)
        for row in csv_reader:
            if row.get("short_hash_of_input") == short_hash:
                log.error(f"Already exists: {short_hash}")
        return True


def log_filepaths(filepaths_contents: List[Tuple[str, str]]):
    log.info(f"Processing {len(filepaths_contents)} new files.")
    for filepath, _ in filepaths_contents:
        filename = os.path.basename(filepath)
        log.info(f"{filename=}")


def load_fresh_nfes(
    input_filesdir: str, output_file: str, keep_first_page_only: bool = False
) -> List[Tuple[str, str]]:
    from extractor import loader

    output_filepaths = []
    if os.path.exists(output_file):
        with open(output_file, "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                output_filepaths.append(row.get("input_filepath"))

    input_files = loader.get_files(input_filesdir)
    documents = loader.run(input_files, keep_first_page_only)

    filepaths_contents = [
        (document.metadata["source"], document.page_content)
        for document in documents
        if document.metadata["source"] not in output_filepaths
    ]

    log_filepaths(filepaths_contents)

    return filepaths_contents


def get_string_from_basemodel(model: Type[BaseModel]) -> str:
    aliases = [
        field_info.alias
        for name, field_info in model.__fields__.items()
        if field_info.alias
    ]
    return " ".join(aliases)


def validate_date(v):
    if not v:
        raise ValueError("Data não pode ser vazia")
    if len(v) != 10:
        raise ValueError("Data deve ter 10 caracteres")
    if v[2] != "/" or v[5] != "/":
        raise ValueError("Data deve estar no formato dd/mm/aaaa")
    return v


def validate_time(v):
    if not v:
        raise ValueError("Horário não pode ser vazio")
    if len(v) != 8:
        raise ValueError("Horário deve ter 8 caracteres")
    if v[2] != ":":
        raise ValueError("Horário deve estar no formato hh:mm:ss")
    return v
