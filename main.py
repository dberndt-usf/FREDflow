# The FREDflow project implements and ETL workflow
# for populating USF database servers with economic data.
# This data is used for dimensional modeling, analytic SQL
# and other data warehousing activities in the classroom.
# The Federal Reserve Economic Data (FRED) portal and
# Python API is the source of most of the data.
# Other sources will be added in the future.
#
# Coordinator: Don Berndt (dberndt@usf.edu)
# Contributors:
# Shreya Chakraborty (chakraborty528@usf.edu)
# Anish Babu Gogineni (agogineni@usf.edu)
#
# Copyright 2025 - Present by University of South Florida (USF)

# Import required resources.
import pandas as pd
import pickle
import os
from config import *
from fred import *

# Set basic parameters like debug verbosity level.
verbosity = 3
sleep_secs = 6
config_path = 'config/'
pickle_path = 'pickle/'
oracle_db_enabled = True
db_push_enabled = True
# Temporarily omit some series such as ['DFF']
skip_list =[]

if verbosity > 0:
    print("\nInitializing FREDflow ...")
# Initialise FRED API key.
fred = config_fred_api(config_path, verbosity)
# Create a dictionary of FRED series with code and name.
fred_dict = config_fred_dict(config_path, verbosity)
fred_codes = set(fred_dict.keys())
if verbosity > 1:
    print(fred_dict)
    print(fred_codes)
    print(skip_list)

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
    if len(fred_codes - pickled_codes) > 0:
        print(fred_codes - pickled_codes)
    else:
        print("No new FRED series.")
# Instantiate the newly added series after loading pickled series.
new_series = config_fred_series(config_path,
                                fred_codes - pickled_codes,
                                verbosity)
fred_series = pickled_series + new_series
if verbosity > 1:
    for fs in fred_series:
        fs.show()

# Establish database connections, if enabled.
# The FRED data could be downloaded for computations
# and not persistent storage.
if oracle_db_enabled:
    oracle_db_list = config_oracle_databases(config_path, verbosity)
    if verbosity > 2:
        for odb in oracle_db_list:
            odb.show()
else:
    oracle_db_list = []
if verbosity > 1:
    for odb in oracle_db_list:
        if odb.ping():
            print("Database server", odb.host(), "up and running ...")
        else:
            print("Database server", odb.host(), "down and unreachable ...")

# Fetch the data from the FRED API and
# push to one or more databases.
print("\nFetching FRED series ...")
for fs in fred_series:
    # Skip some series for development work.
    if fs.code() in skip_list:
        break
    try:
        ds = fs.fetch(fred)
    except Exception as e:
        print(f"Unexpected error: {e}")
        ds = None
    if ds is not None:
        if verbosity > 2:
            print("\nFRED Series:", fs.name(), "-", fs.code())
            print("Length:", len(ds))
            print("Head:")
            print(ds.head())
            print("Tail:")
            print(ds.tail())
        # Pickle the FRED series for persistence.
        with open('pickle/' + fs.code() + '.pkl', 'wb') as pkl_file:
            pickle.dump(fs, pkl_file)
        # Push via UPSERT operations to the specified database.
        # Push to specified Oracle databases.
        if db_push_enabled:
            for odb in oracle_db_list:
                # Process only the required data using a bookmark.
                bmark = odb.bookmark(fs)
                if bmark is not None:
                    adjusted_bmark = bmark - pd.Timedelta(days=fs.lookback())
                    print("Bookmarks:", bmark, adjusted_bmark)
                    odb.upsert(fs, ds[adjusted_bmark:])
                else:
                    odb.upsert(fs, ds)
    time.sleep(sleep_secs)
