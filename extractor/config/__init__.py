import instructor
import openai
import marvin
from dotenv import load_dotenv
import os
import toml

load_dotenv()


openai.api_key = os.environ["OPENAI_API_KEY"]
marvin.settings.openai.api_key = os.environ["OPENAI_API_KEY"]
DROPBOX_ACCESS_TOKEN = os.environ["DROPBOX_ACCESS_TOKEN"]
AIRTABLE_PERSONAL_ACCESS_TOKEN = os.environ["AIRTABLE_PERSONAL_ACCESS_TOKEN"]

instructor.patch()

CONFIG_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.abspath(os.path.join(CONFIG_PATH, ".."))

SETTINGS_FILEPATH = os.path.join(CONFIG_PATH, "settings.toml")

settings = toml.load(SETTINGS_FILEPATH)
