import pandas as pd
import pyairtable
import numpy as np
from loguru import logger as log

from extractor.config import AIRTABLE_PERSONAL_ACCESS_TOKEN
from pyairtable import Table
from extractor import PATH_NFE

NFE_OUTPUT_FILE = PATH_NFE / "output" / "nfe.csv"

BASE_ID = "appuENAyrYsTpD4qj"


def get_base(personal_token: str | None = None, base_id: None | str = None):
    if not personal_token:
        personal_token = AIRTABLE_PERSONAL_ACCESS_TOKEN
    if not base_id:
        base_id = BASE_ID
    return pyairtable.Base(personal_token, base_id)


def get_table(name: str) -> Table:
    return get_base().table(name)


def retrieve_table_data(table: Table) -> pd.DataFrame:
    records = [record for record in table.iterate()][0]
    inner_records = [record["fields"] for record in records]
    return pd.DataFrame.from_records(inner_records)


def load_nfe_output_file(filter: bool = True):
    df = pd.read_csv(NFE_OUTPUT_FILE)
    if filter:
        # filter only the maximum inserted_at (pd.Timestamp  column)
        # let only the values with the highest inserted_at
        maximum_inserted_at = df["inserted_at"].max()
        log.info(f"Filtering records with inserted_at: {maximum_inserted_at}")
        df = df.loc[df["inserted_at"] == maximum_inserted_at]
    return df


def format_df(df: pd.DataFrame, schema: list[str]):
    df_to_insert = pd.DataFrame(columns=schema)
    df_to_insert["data"] = df["data"]
    df_to_insert["num_nf"] = df["num_nf"]
    df_to_insert["valor"] = df["valor"]
    df_to_insert["forma"] = df["forma"]
    df_to_insert["obs"] = df["categoria"]
    df_to_insert["fornecedor"] = df["fornecedor_id"]
    df_to_insert["chave_de_acesso"] = df["chave_de_acesso"]
    df_to_insert["download_url"] = df["download_url"]
    df_to_insert["tabela"] = "base_despesas"
    df_to_insert["banco"] = "recdtO0RHZOmV8ld2"
    df_to_insert["estado"] = "0. Importado"
    df_to_insert = df_to_insert.fillna("")
    return df_to_insert


def make_records(df: pd.DataFrame):
    records = []
    for _, row in df.iterrows():
        record = {
            "tabela": row["tabela"],
            "banco": [row["banco"]],
            "estado": row["estado"],
            "data": row["data"],
            "fornecedor": [row["fornecedor"]],
            "num_nf": int(row["num_nf"]) if row["num_nf"] != "" else 0,
            "valor": row["valor"],
            "obs": row["obs"],
            "chave_de_acesso": row["chave_de_acesso"],
            "nf": (
                [{"url": row["download_url"]}]
                if row["download_url"] not in ["", np.nan]
                else []
            ),
        }
        # Remove keys with empty or NaN values
        record = {k: v for k, v in record.items() if v not in ["", np.nan, [""]]}
        records.append(record)
    return records


def check_for_duplicates(df_to_insert, existing_df):
    # df_to_insert["data"] = pd.to_datetime(existing_df["data"])
    # existing_df["data"] = pd.to_datetime(existing_df["data"])
    unique_df = df_to_insert.drop_duplicates(subset=["data", "valor", "num_nf"])
    unique_df = unique_df[
        ~unique_df.set_index(["data", "valor", "num_nf"]).index.isin(
            existing_df.set_index(["data", "valor", "num_nf"]).index
        )
    ]
    return unique_df


def save_records(df, table):
    records = make_records(df)
    for record in records:
        log.info(f"Saving record: {record}")
        table.create(record)


if __name__ == "__main__":
    table_name = "base_geral"
    table = get_table(table_name)
    existing_df = retrieve_table_data(table)
    schema = [
        "data",
        "num_nf",
        "valor",
        "obs",
        "forma",
        "estado",
        "tabela",
        "banco",
        "fornecedor",
    ]

    incoming_df = load_nfe_output_file()
    # Match the CNPJ field in fornecedor_df with cnpj_do_vendedor from incoming_df and from there get the Record field from fornecedor_df
    fornecedor_table = get_table("cadastro_fornecedores")
    fornecedor_df = retrieve_table_data(fornecedor_table)
    fornecedor_df.rename(columns={"CNPJ": "cnpj_do_vendedor"}, inplace=True)
    incoming_df = pd.merge(
        incoming_df,
        fornecedor_df[["cnpj_do_vendedor", "record"]],
        on="cnpj_do_vendedor",
        how="left",
    ).rename(columns={"record": "fornecedor_id"})

    incoming_df = format_df(incoming_df, schema)
    incoming_df = check_for_duplicates(incoming_df, existing_df)

    save_records(incoming_df, table)
