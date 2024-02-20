## How to run the solution

Python 3.11 via pyenv was used for the task.

Steps to run the solution:

1. Create a virtual environment: `python -m venv .venv`
2. Activate virtualenv: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt`
4. Start data source server: `python -m uvicorn api_data_source.main:app --reload`
5. Run etl_client.py module: `python etl_client.py`
