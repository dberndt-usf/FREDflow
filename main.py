# The FREDflow project implements and ETL workflow
# for populating USF database servers with economic data.
# This data is used for dimensional modeling, analytic SQL
# and other data warehousing activities in the classroom.
# The Federal Reserve Economic Data (FRED) portal and
# Python API is the source of most of the data.
# Other sources will be added in the future.
#
# Coordinator: dberndt@usf.edu
# Contributors:
#
# Copyright 2025 - Present by University of South Florida (USF)

# Import required resources.
import csv
import pickle
import os
from fred import *
from fredapi import Fred

# Set basic parameters like debug verbosity level.
verbosity = 3
sleep_secs = 6
pickle_path = 'pickle/'

# Read config file for FRED API key.
if verbosity > 0:
    print("\nInitializing FREDflow ...")
with open('config/config.csv', 'r') as file:
    csvreader = csv.reader(file)
    for row in csvreader:
        if row[0] == 'api_key':
            fredapi_key = row[1]
fred = Fred(api_key=fredapi_key)

# Create a dictionary of the target FRED series,
# including code and name (add new series here).
if verbosity > 0:
    print("\nCreating dictionary of FRED series ...")
fred_dict = {"DFF": "Federal Funds Effective Rate",
             "GDP": "Gross Domestic Product",
             "CORESTICKM159SFRBATL": "Sticky CPI",
             "UNRATE": "Unemployment Rate"}
fred_codes = list(fred_dict.keys())
if verbosity > 1:
    print(fred_dict)
    print(fred_codes)

# Read pickled FRED series objects.
if verbosity > 0:
    print("\nLoading pickled FRED series ...")
pickled_series = []
pickled_codes = set()
files = os.listdir(pickle_path)
for file in files:
    if os.path.isfile(os.path.join(pickle_path, file)):
        with open(os.path.join(pickle_path, file), 'rb') as f:
            pfs = pickle.load(f)
            pickled_series.append(pfs)
            pickled_codes.add(pfs.code())
if verbosity > 1:
    print(pickled_codes)
    print(pickled_series)

# Instantiate FRED series objects.
if verbosity > 0:
    print("\nInstantiating new FRED series ...")
fred_series = []
fred_series.append(FREDSeries('Federal Funds Effective Rate', 'DFF'))
fred_series.append(FREDSeries('Gross Domestic Product', 'GDP'))
fred_series.append(FREDSeries('Sticky CPI', 'CORESTICKM159SFRBATL'))
fred_series.append(FREDSeries('Unemployment Rate', 'UNRATE'))
if verbosity > 0:
    for fs in fred_series:
        fs.show()

print("\nFetching data ...")
for fs in fred_series:
    ds = fs.fetch(fred)
    if verbosity > 2:
        print(fs.name(), "-", fs.code())
        print(ds.head())
        print(ds.tail())
        # Pickle the FRED series for persistence.
        with open('pickle/' + fs.code() + '.pkl', 'wb') as pkl_file:
            pickle.dump(fs, pkl_file)
    time.sleep(sleep_secs)

