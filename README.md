# NBA-Analysis
Breaking down basic stats of the NBA via api-sports API and displayed with duckdb.

# How to run
To run locally: have your python version be 3.12.1, then run `pip install -r requirements.txt`, and finally run `python app.py`
To run with docker: enter the command `docker compose run app`

# Code Layout
The app.py is made up of two sections, the first being a number of helper functions that pull team and game data from api-sports, organizes the data, and saves them as parquet files. The second section is the Task class that contains SQL queries that achieve the requested analysis. The results are simply displayed with duckdb.