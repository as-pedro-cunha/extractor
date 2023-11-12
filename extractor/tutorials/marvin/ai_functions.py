from marvin import ai_fn


@ai_fn
def sentiment(text: str) -> float:
    """
    Given `text`, returns a number between 1 (positive) and -1 (negative)
    indicating its sentiment score.
    """


sentiment_1 = sentiment("I love working with Marvin!")  # 0.8
print(f"{sentiment_1=}")
sentiment_2 = sentiment("These examples could use some work...")  # -0.2
print(f"{sentiment_2=}")
