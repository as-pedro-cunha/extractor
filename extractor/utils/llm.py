import asyncio
import tiktoken
from loguru import logger as log
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)  # for exponential backoff

from extractor.config import openai

MODELS_LIMITS = {
    # "gpt-3.5-turbo": 4000,
    # "gpt-3.5-turbo-16k": 16000,
    "gpt-4-turbo-preview": 128000,
    # "gpt-4": 8192, -> overpriced
    # "gpt-4-32k": 32000, -> overpriced
}


def num_tokens_from_string(string: str, encoding_name: str, beta: float = 1.2) -> int:
    encoding = tiktoken.encoding_for_model(encoding_name)
    num_tokens = len(encoding.encode(string)) * beta
    return round(num_tokens, 0)


def chose_cheapest_model_given_limit(prompt: str, max_limit: int = 16001):
    for model, limit in MODELS_LIMITS.items():
        tokens = num_tokens_from_string(prompt, model)
        if tokens <= limit and tokens <= max_limit:
            log.info(f"Chose model {model=} with a prompt of {tokens=}.")
            return model
    log.error(f"Could not find a cheaper model that fit the token limits {tokens=}.")


@retry(
    wait=wait_random_exponential(min=60, max=120),
    stop=stop_after_attempt(20),
    retry=retry_if_exception_type(Exception),
)
async def completion_with_backoff(async_timeout, **kwargs):
    try:
        return await asyncio.wait_for(
            openai.ChatCompletion.acreate(
                **kwargs,
            ),
            timeout=async_timeout,
        )
    except Exception as e:
        log.error("Failed to get completion from OpenAI API.")
        log.error(f"Exception: {e}")
        raise e


if __name__ == "__main__":
    prompt = "Hello world, let's test tiktoken."
    num_tokens_from_string(prompt, "gpt-3.5-turbo-16k")
    chose_cheapest_model_given_limit(prompt, 16000)
