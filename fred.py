# This module defines the FREDSeries class that
# is instantiated foe each series being fetched.
# A variety of methods support the workflow.

# Import required resources.
import time

# This is the base class for FRED time series
# using Pandas series.
class FREDSeries:
    # Static variables.
    pds_kind = 'PDS'
    pds_title = 'Pandas Series'

    def __init__(self, name, code, verbosity = 0):
        self.pds_name = str(name)
        self.pds_code = str(code)
        self.pds_verbosity = verbosity
        self.pds_fetch_timestamp = None

    def code(self):
        return self.pds_code

    # Fetch the FRED data and return the series.
    def fetch(self, fred):
        if self.verbosity() > 0:
            print("Fetching", self.name(), "data ...")
        # Fetch the entire data series
        fred_series = fred.get_series(self.code())
        if self.verbosity() > 1:
            print("Fetched", fred_series.size, "values.")
        # Filter out any NaN values
        fred_series_no_nan = fred_series.dropna()
        if self.verbosity() > 2:
            print(fred_series_no_nan.head())
            print(fred_series_no_nan.tail())
        # Return non-NaN of None and update fetch timestamp.
        self.pds_fetch_timestamp = time.time()
        if fred_series_no_nan.size > 0:
            return fred_series_no_nan
        else:
            return None

    def kind(self):
        return self.pds_kind

    def name(self):
        return self.pds_name

    def show(self):
        print("Name:", self.name(),
              "Code:", self.code(),
              "Kind:", self.kind(),
              "Title:", self.title(),
              "Fetch Timestamp:", self.pds_fetch_timestamp,
              "Verbosity:", self.verbosity())

    def title(self):
        return self.pds_title

    def verbosity(self, v=None):
        if v is not None:
            self.pds_verbosity = v
        return self.pds_verbosity
