
# Web Scrapper ETL Project

## Case-study:

Jazzy Investment, a client of 10Alytics, is a stockbroking firm dealing in the issuance and trading of stocks on behalf of its customers in the Nigeria Stock Exchange. To facilitate the analytics team in analyzing market trends and making informed decisions on company stocks, they require daily stock exchange data published on [this website](https://afx.kwayisi.org/ngx/).

## Task:

As part of the 10Alytics data engineering training program, your task is to build a web scraper (data pipeline) to Extract, Transform, and Load (ETL) the listed companies/securities data from [https://afx.kwayisi.org/ngx/](https://afx.kwayisi.org/ngx/) into a PostgreSQL database. The required data is under the heading "Listed companies/securities."

This data will be consumed by Jazzy Investment in the database for their stock trading analytics.

## Requirements:

1. The data of company stocks listed is assumed to be updated on the website every day, and your pipeline will run once every day. Ensure newly extracted data is integrated with existing data in the database each time your pipeline runs.

2. The first page of the website shows only 100 of 157 listings (click on the Next>> button at the bottom of the table) to confirm this. You are expected to get data for the remaining 57 listings and add them to the first 100.

3. Your transformation should include a column called `Date` which shows the date for which the data was scrapped.

4. For companies that do not have a value for volume traded for a particular day, then the value for volume should be the mean volume of all companies that traded that day.

## Technical Details

### Programming Language:
- Python

### Libraries Used:
- Pandas
- SQLAlchemy
- Psycopg2


## Project Steps:

### 1. Database Connection:

- A PostgreSQL database connection is established using the `psycopg2` library. The database credentials include the username, password, host, and database name.

### 2. Table Creation:

- A table named 'stock' is created in the PostgreSQL database using the `create_engine` and `execute` functions.

### 3. Web Scraping:

- Data is extracted from the [Nigeria Stock Exchange website](https://afx.kwayisi.org/ngx/) using  Pandas.

### 4. Data Transformation:

- The two DataFrames obtained from the web scraping process are concatenated, and missing values in the 'Volume' column are filled with the mean volume of all companies that traded on a particular day.


### 6. Clean-up:

- Cursors and connections are closed to ensure proper resource management.


```

## How to Run the Project:

1. Clone the repository: `git clone <repository_url>`
2. Navigate to the project directory: `cd web-scraper-etl-project`
3. Create a virtual environment: `virtualenv venv` (activate it with `source venv/bin/activate` or `venv\Scripts\activate` on Windows)
4. Install dependencies: `pip install -r requirements.txt`
5. Run the Python script: `python web_scraper_etl.py`

---
