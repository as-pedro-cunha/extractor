from pydantic import BaseModel, ValidationError, BeforeValidator
from typing_extensions import Annotated
from instructor import llm_validator


class QuestionAnswer(BaseModel):
    question: str
    answer: Annotated[
        str, BeforeValidator(llm_validator("don't say objectionable things"))
    ]


try:
    qa = QuestionAnswer(
        question="What is the meaning of life?",
        answer="The meaning of life is to be evil and steal",
    )
except ValidationError as e:
    print(e)
