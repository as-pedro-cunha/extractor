# NFE Extractor

NFE Extractor is a Python application designed to extract and process data from Brazilian fiscal documents (Notas Fiscais Eletrônicas - NFEs). It leverages the OpenAI API and Instructor (that uses OpenAI functions on the backend) to parse document content and extract specific fields into structured data.

## Features

- Extracts various fields from NFEs such as issue date, item categories, seller's corporate name, and more.
- Utilizes the OpenAI API for processing documents with natural language understanding.
- Retries on timeout errors and logs appropriate warnings and errors.
- Saves the processed data in CSV format for easy use and analysis.

## Structure

The project is structured into two main directories:

- `extractor`: Contains the core logic for NFE document processing.
- `config`: Houses configuration settings and environment variable management.
- `loader`: Contains the logic for loading NFE documents from the filesystem.
- `utils`: Contains utility functions for logging and other miscellaneous tasks.

## Core Components

### `extractor/nfe/__init__.py`

This is the main module of the application, which includes:

- Definition of `NfeCampos` Pydantic model for structured data extraction.
- Asynchronous function `process_nfe_document` to process the NFE document via OpenAI API.
- Main coroutine `run` which orchestrates the document processing and saving of data.

### `extractor/config/__init__.py`

This module manages the configuration settings for the application. It:

- Loads environment variables.
- Initializes OpenAI API configuration.
- Defines paths for the settings file and the root of the project.

## Setup

1. Ensure you have Python 3.10+ installed.
2. Install required dependencies using `poetry install`.
3. Set up your OpenAI API key in the environment variable `OPENAI_API_KEY`.

## Usage

Add the NFE documents you want to process to the `nfe/input` directory.

Run the application using the following command:

```bash
inv nfe
```

Check the `nfe/output` directory for the processed data in CSV format.
