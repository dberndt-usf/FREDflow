# Import required resources.
import csv
from fredapi import Fred
from fred import *
from orcldb import *

def config_fred_api(config_path, verbosity):
    # Read config file for FRED API key.
    if verbosity > 0:
        print("\nConfiguring FRED API ...")
    with open(config_path + 'fredflow_config.csv', 'r') as file:
        csvreader = csv.reader(file)
        # Skip headers on first line.
        next(csvreader)
        for row in csvreader:
            if row[0] == 'api_key':
                fredapi_key = row[1]
    return Fred(api_key = fredapi_key)



def config_fred_dict(config_path, verbosity):
    # Read config file for FRED series code and name
    # and return a dictionary.
    fred_dict = {}
    if verbosity > 0:
        print("\nConfiguring FRED series dictionary ...")
    with open(config_path + 'fred_series.csv', 'r') as file:
        csvreader = csv.reader(file)
        # Skip headers on first line.
        next(csvreader)
        for row in csvreader:
            fred_dict[row[0]] = row[1]
    return fred_dict



def config_fred_series(config_path, new_codes,
                       pickled_series, verbosity):
    # Read config file for FRED series granularity,
    # instantiate FREDSeries objects and return a list.
    new_series = []
    if verbosity > 0:
        print("\nConfiguring new FRED series ...")
    with open(config_path + 'fred_series.csv', 'r') as file:
        csvreader = csv.reader(file)
        # Skip headers on first line.
        next(csvreader)
        for row in csvreader:
            if row[0] in new_codes:
                # Instantiate new series object.
                new_series.append(FREDSeries(row[0], # Code
                                             row[1], # Name
                                             row[2], # Granularity
                                             row[3])) # Lookback
    return new_series



def reconfig_pickled_series(config_path, pickled_series, verbosity):
    with open(config_path + 'fred_series.csv', 'r') as file:
        csvreader = csv.reader(file)
        # Skip headers on first line.
        next(csvreader)
        for row in csvreader:
            # For existing series,
            # check for parameter changes like lookback.
            pkl_pds = None
            for pds in pickled_series:
                if pds.code() == row[0]: # Code
                    pkl_pds = pds
            if pkl_pds is not None:
                if pkl_pds.lookback() != int(row[3]): # Lookback
                    print("Reconfiguring", pkl_pds.code(),
                          "lookback from", pkl_pds.lookback(),
                          "to", row[3], "...")
                    pkl_pds.lookback(int(row[3]))
                else:
                    print("No lookback parameter changes for",
                          pkl_pds.code(), "...")



def config_oracle_databases(config_path, verbosity):
    # Read config file for Oracle databases
    # and return a list.
    oracle_db_list = []
    if verbosity > 0:
        print("\nConfiguring Oracle DB ...")
    with open(config_path + 'oracle_db.csv', 'r') as file:
        csvreader = csv.reader(file)
        # Skip headers on first line.
        next(csvreader)
        for row in csvreader:
            oracle_db_list.append(OracleDB(
                row[0], # user
                row[1], # password
                row[2], # host
                row[3], # port
                row[4], # sid
                row[5] # name
            ))
    return oracle_db_list
