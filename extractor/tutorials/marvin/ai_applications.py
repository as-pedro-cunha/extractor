from datetime import datetime
from pydantic import BaseModel
from marvin import AIApplication
from marvin.tools import tool
import marvin
import os
import requests
from typing import Optional

marvin.settings.llm_model = "openai/gpt-4-1106-preview"

PIRATE_WEATHER = os.environ["PIRATE_WEATHER"]


@tool
def get_climate(api_key: Optional[str] = PIRATE_WEATHER):
    """Get the climate information from the Pirate Weather API."""
    your_lat = -23.4958
    your_long = -46.9656
    r = requests.get(
        f"https://api.pirateweather.net/forecast/{api_key}/{your_lat},{your_long}?&units=si"
    )
    try:
        return r.json()["currently"]
    except Exception as e:
        print(e)
        return "Climate information not available"


class ClimateInfo(BaseModel):
    temperature: float
    apparent_temperature: float
    humidity: float
    wind_speed: float
    cloud_cover: float
    rain: bool


# create models to represent the state of our ToDo app
class ToDo(BaseModel):
    title: str
    description: str
    climate_info: ClimateInfo
    due_date: datetime = None
    done: bool = False


class ToDoState(BaseModel):
    todos: list[ToDo] = []


todo_app = AIApplication(
    state=ToDoState(),
    description=(
        "A simple to-do tracker. Users will give instructions to add, remove, and "
        "update their to-dos. Add the climate information in the response and also "
        "in the todo item created and in the response."
    ),
    tools=[get_climate],
)

# invoke the application by adding a todo
response = todo_app("I need to go to the store tomorrow at 5pm")


print(f"Response: {response.content}\n")
# Response: Got it! I've added a new task to your to-do list. You need to go to the store tomorrow at 5pm.


print(f"App state: {todo_app.state.model_dump_json()}")
# App state: {
#   "todos": [
#     {
#       "title": "Go to the store",
#       "description": "Buy groceries",
#       "due_date": "2023-07-12T17:00:00+00:00",
#       "done": false
#     }
#   ]
# }
