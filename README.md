# FREDflow Project

## Overview
FREDflow is an ETL workflow designed to populate University of South Florida (USF) database servers with economic data. This data is used for dimensional modeling, analytic SQL, and other data warehousing activities in the classroom. The primary data source is the Federal Reserve Economic Data (FRED) portal via its Python API, with additional sources to be integrated in the future.

## Coordinator
- **Don Berndt** (dberndt@usf.edu)

## Contributors
- **Anish Babu Gogineni** (agogineni@usf.edu)
- **Shreya Chakraborty** (chakraborty528@usf.edu)

## Getting Started
### 1. Request an API Key
To access the FRED API, you need an API key. Follow these steps to obtain one:
1. Visit the [FRED API registration page](https://fredaccount.stlouisfed.org/apikeys).
2. Sign up for an account if you don’t have one.
3. Request an API key.
4. Once approved, you will receive a key that allows access to the FRED API.

### 2. Set Up the Project
#### Clone the Repository
```bash
git clone https://github.com/dberndt-usf/FREDflow.git
cd FREDflow
```

<!-- #### Install Dependencies
Ensure you have Python installed (preferably 3.8+). Then, install the required libraries:
```bash
pip install -r requirements.txt
``` -->

#### Configure the API Key
1. Navigate to the `config/` directory.
2. Open or create a `fredfloe_config.csv` file and add the following content:
   ```csv
   key,value
   api_key,<your_api_key_here>
   ```

### 3. Configure FRED Series
1. Navigate to the `config/` directory.
2. Open or create a `fredflow_series.csv` file and add the following content:
   ```csv
   code,name,granularity
   GDP,"Gross Domestic Product",QUARTERLY
   ```
   
### 4. Configure Database Connections (Oracle)
1. Navigate to the `config/` directory.
2. Open or create an `oracle_db.csv` file and add the following content:
   ```csv
   user,password,host,port,sid,name
   ```
3. Use the CREATE TABLE statements in the file  `sql-ddl.txt` if needed for the target schema.
4. Use the CREATE FUNCTION statements to compile "UPSERT" functions.

Note: Set the `oracle_db_enabled` parameter in the `main.py` file to False to
jusf fetch the data and skip database processing.


### 5. Run the Script
Execute the main script to fetch and process FRED data:
```bash
python main.py
```

## Troubleshooting
### EOFError: Ran out of input
If you encounter an `EOFError` while loading pickle files, it may be due to empty or corrupted files. Try the following:
- Delete all files inside the `pickle/` directory and rerun the script.
- Ensure your API key is correctly configured.
- Check if your network connection is stable while fetching data from FRED.

## Future Enhancements
- Add more data sources beyond FRED.
- Optimize ETL workflow for better performance.
- Implement automated data validation and error handling.

## License
Copyright 2025 - Present by University of South Florida (USF).

For any inquiries or contributions, feel free to reach out to the coordinator or contributors.

