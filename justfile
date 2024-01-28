dropbox:
    $(which python) extractor/drive/download.py

openai:
    $(which python) extractor/nfe/__init__.py

airtable:
    $(which python) extractor/airtable/__init__.py

all: dropbox openai airtable