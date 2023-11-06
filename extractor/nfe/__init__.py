import asyncio
from loguru import logger as log
import os
from pydantic import Field
from instructor import OpenAISchema
from typing import Optional


from extractor.config import settings, ROOT_PATH
import extractor.utils as utils
from extractor.utils import llm

TIMEOUT_RETRIES = settings["llm"]["timeout_retries"]
TIMEOUT_SEC = settings["llm"]["timeout_sec"]

NFES_FILES = utils.get_files(ROOT_PATH, settings["filepaths"]["nfe_input"])
NFES_INPUT = utils.get_path(ROOT_PATH, settings["filepaths"]["nfe_input"])
NFES_OUTPUT = utils.get_path(ROOT_PATH, settings["filepaths"]["nfe_output"])


class NfeCampos(OpenAISchema):
    data_da_emissao: str = Field(
        ..., description="data_da_emissao em formato dd/mm/aaaa"
    )
    categoria_itens_nota: str = Field(
        ...,
        description="Entenda a partir da descricao dos itens da nota e escreva qual e a categoria do servico/produto listados na nota, pode escrever mais de uma categoria se necessario. Use no maximo 5 palavras, seja bem conciso",
    )
    numero_da_nota: Optional[int] = Field(
        ...,
        description="numero da nota fiscal, pode estar apos o N ou Numero da Nota fiscal ou algo similar",
    )
    valor_liquido_total: float
    razao_social_vendedor: str = Field(
        ...,
        description="razao social do vendedor, nao da empresa do software de emissao (Emissor NF-e, ex Rensoftware <- nao e o que quero), mas sim da empresa que emitiu a nota, responsavel pela venda dos serviços/produtos. Escreva tudo em letras maisculas",
    )
    cidade_e_estado_do_vendedor: str = Field(
        ..., description="cidade e estado do vendedor -> cidade/UF"
    )
    cnpj_ou_cpf_do_vendedor: str = Field(
        ...,
        description="CNPJ ou CPF do vendedor, se for CPF, colocar no formato: XXX.XXX.XXX-XX se for CNPJ, colocar no formato: XX.XXX.XXX/XXXX-XX",
    )
    telefone_do_vendedor: str = Field(
        ...,
        description="telefone do vendedor em formato (xx) xxxxx-xxxx, nao use esse telefone: 11 3722-1011",
    )
    email_do_vendedor: Optional[str] = Field(
        ...,
        description="e-mail do vendedor",
    )
    forma_de_pagamento: Optional[str] = Field(
        ...,
        description="Dados relacionado a forma de pagamento, pix, ted, numero da conta, parcelas, etc...,",
    )
    dados_da_conta: Optional[str] = Field(
        ...,
        description="Se houver informacao a respeito Agencia, Conta, Banco, Numero do PIX, Boleto, Adicionar aqui",
    )
    numero_de_parcelas: Optional[int] = Field(
        ..., description="Se houver informacao a respeito de parcelas, adicionar aqui"
    )
    input_filepath: str = Field(
        ...,
        description="adicionar o caminho do arquivo fornecido (informaçao apos as palavras: input_filepath)",
    )


async def process_nfe_document(
    document: str, input_filepath: str, temperature: float = 0.1, **kwargs
) -> NfeCampos:
    """
    Process an NFE document by calling the OpenAI API with a specified timeout and retries.
    Returns NfeCampos model or None if failed after retries.
    """
    system_prompt = (
        "Você e um leitor de documentos de notas fiscais brasileiras. "
        "Sua funçao e somente retornar os valores respectivos das chaves especificadas. "
        "Caso algum valor nao exista na nota deixe o campo vazio. "
    )
    user_prompt = (
        f"Adicione esse campo tambem: input_filepath: {input_filepath} "
        f"Segue a nota fiscal: \n{document}"
    )

    full_prompt = (
        utils.get_string_from_basemodel(NfeCampos) + system_prompt + user_prompt
    )

    model = llm.chose_cheapest_model_given_limit(full_prompt)

    completion = await llm.completion_with_backoff(
        async_timeout=TIMEOUT_SEC,
        model=model,
        functions=[NfeCampos.openai_schema],
        temperature=temperature,
        function_call={"name": NfeCampos.openai_schema["name"]},
        max_retries=2,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )
    try:
        model = NfeCampos.from_response(completion)
    except Exception as e:
        # !FIXME: pydantic_core._pydantic_core.ValidationError
        log.error(f"Failed to parse response from OpenAI API: {e}")
        log.error(f"Response: {completion}")
        return
    log.info(
        f"Total tokens spent for NFe: {model.numero_da_nota} was {completion['usage']['total_tokens']}"
    )
    return model


@log.catch
async def run():
    filepaths_contents = utils.load_fresh_nfes(
        NFES_INPUT, NFES_OUTPUT, keep_first_page_only=False
    )

    if not filepaths_contents:
        return

    tasks = [
        asyncio.create_task(process_nfe_document(content, filepath))
        for filepath, content in filepaths_contents
    ]
    filepaths = [filepath for filepath, _ in filepaths_contents]

    for task, filepath in zip(asyncio.as_completed(tasks), filepaths):
        nfe = await task
        if not nfe:
            log.info(f"Skipping file: {filepath} due to failed API call.")
            continue
        filename = os.path.basename(filepath)
        log.info(f"Processing file: {filename}")
        file_info = f"{filename=} {nfe.numero_da_nota=} {nfe.razao_social_vendedor=}"

        utils.dataclass_to_pandas(
            [nfe], timestamp="data_da_emissao", filepath=NFES_OUTPUT
        )
        log.info(f"Saving fields to csv of file: {file_info}")


if __name__ == "__main__":
    asyncio.run(run())
