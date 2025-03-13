# This module defines the FREDSeries class that
# is instantiated foe each series being fetched.
# A variety of methods support the workflow.

# Import required resources.
import datetime
import time

# This is the base class for FRED time series
# using Pandas series.
class FREDSeries:
    # Static variables.
    pds_kind = 'PDS'
    pds_title = 'Pandas Series'

    def __init__(self, code, name, granularity, lookback = 0, verbosity = 0):
        self.pds_code = str(code)
        self.pds_name = str(name)
        self.pds_granularity = str(granularity)
        self.pds_lookback = int(lookback)
        self.pds_verbosity = int(verbosity)
        self.pds_first_fetch = None
        self.pds_last_fetch = None
        self.pds_fetch_tally = 0

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
        if self.pds_first_fetch is None:
            self.pds_first_fetch = time.time()
        self.pds_last_fetch = time.time()
        self.pds_fetch_tally += 1
        if fred_series_no_nan.size > 0:
            return fred_series_no_nan
        else:
            return None

    def granularity(self):
        return self.pds_granularity

    def kind(self):
        return self.pds_kind

    def lookback(self):
        return self.pds_lookback

    def name(self):
        return self.pds_name

    def show(self):
        print("Name:", self.name(),
              "Code:", self.code(),
              "Granularity", self.granularity(),
              "Kind:", self.kind(),
              "Title:", self.title(),
              "Verbosity:", self.verbosity())
        print("First Fetch:", self.pds_first_fetch,
              "Last Fetch:", self.pds_last_fetch,
              "Fetch Tally:", self.pds_fetch_tally,
              "Wait:", self.wait())

    def title(self):
        return self.pds_title

    def verbosity(self, v = None):
        if v is not None:
            self.pds_verbosity = v
        return self.pds_verbosity

    # Return the elapsed days since the last fetch as
    # the length of the waiting period.
    def wait(self):
        if self.pds_last_fetch is not None:
            fetch_datetime = datetime.datetime.fromtimestamp(self.pds_last_fetch)
            current_datetime = datetime.datetime.fromtimestamp(time.time())
            time_diff = current_datetime - fetch_datetime
            return time_diff.days
        else:
            return None
