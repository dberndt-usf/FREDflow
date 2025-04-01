import oracledb

# This is the base class for an Oracle database
# that includes connection information and methods.
class OracleDB:
    # Static variables.
    odb_kind = 'ODB'
    odb_title = 'Oracle DB'

    def __init__(self, user, password, host, port, sid, name, verbosity = 0):
        self.odb_user = str(user)
        self.odb_password = str(password)
        self.odb_host = str(host)
        self.odb_port = int(port)
        self.odb_sid = str(sid)
        self.odb_name = str(name)
        self.odb_verbosity = int(verbosity)

    # Returns the bookmark data for a given FRED series,
    # providing a place to begin UPSERTs for new data.
    def bookmark(self, fred_series):
        if self.verbosity() > 0:
            print("Fetching", fred_series.code(), "DB bookmark ...")
        max_release_date = None
        with oracledb.connect(
                user=self.odb_user,
                password=self.odb_password,
                host=self.odb_host,
                port=self.odb_port,
                service_name=self.odb_name) as connection:
            # SELECT the maximum release data as a bookmark.
            sql_stmt = \
                "SELECT MAX(release_date) FROM " + fred_series.code()
            with connection.cursor() as cursor:
                cursor.execute(sql_stmt)
                # Fetch the single row from the result set.
                row = cursor.fetchone()
                if row:
                    max_release_date = row  # Unpack the tuple into variables.
        return max_release_date[0]

    # Establishes and returns an Oracle database connection.
    def connection(self):
        oracle_conn = oracledb.connect(
            user = self.odb_user,
            password = self.odb_password,
            host = self.odb_host,
            port =  self.odb_port,
            service_name  = self.odb_name)
        return oracle_conn

    # Returns a row count for the specified FRED series
    # for use in workflow management and dashboards.
    def count(self, fred_series):
        if self.verbosity() > 0:
            print("Fetching", fred_series.code(), "DB row count ...")
        row_count = None
        with oracledb.connect(
                user=self.odb_user,
                password=self.odb_password,
                host=self.odb_host,
                port=self.odb_port,
                service_name=self.odb_name) as connection:
            # SELECT COUNT(*) te get a row count.
            sql_stmt = \
                "SELECT COUNT(*) FROM " + fred_series.code()
            with connection.cursor() as cursor:
                cursor.execute(sql_stmt)
                # Fetch the single row from the result set.
                row = cursor.fetchone()
                if row:
                    row_count = row  # Unpack the tuple into variables.
        return row_count[0]

    # Returns the host name being used for the database connection.
    def host(self):
        return self.odb_host

    # Returns the kind of object instantiated.
    def kind(self):
        return self.odb_kind

    # Returns the service name being used for the database connection.
    def name(self):
        return self.odb_name

    # Pings the database as status check and returns True
    # if the database is up and running.
    # A basic Oracle query (SELECT * FROM dual) is used.
    def ping(self):
        if self.verbosity() > 0:
            print("Pinging database server", self.host(), "...")
        response = False
        with oracledb.connect(
                user=self.odb_user,
                password=self.odb_password,
                host=self.odb_host,
                port=self.odb_port,
                service_name=self.odb_name) as connection:
            # Use a classic Oracle DBMS test query.
            sql_stmt = "SELECT * FROM dual"
            with connection.cursor() as cursor:
                cursor.execute(sql_stmt)
                # Fetch the single row from the result set.
                row = cursor.fetchone()
                if row:
                    dummy = row  # Unpack the tuple into variables.
                    # Check for Oracle dummy attribute default of X.
                    if dummy[0] == 'X':
                        response = True
        return response

    # Returns the port number being used for the database connection.
    def port(self):
        return self.odb_port

    # Shows back information about the database connection.
    def show(self):
        print("Name:", self.title(),
              "Code:", self.kind(),
              "User", self.user(),
              "Verbosity:", self.verbosity())
        print("Host:", self.host(),
              "Port:", self.port(),
              "SID:", self.sid(),
              "Name:", self.name())

    # Returns the SID (system identifier) being used for the database connection.
    def sid(self):
        return self.odb_sid

    # Returns  the title of the instantiated object.
    def title(self):
        return self.odb_title

    # Takes a FRED series and fetched Pandas series for
    # updating the data table for the series (via UPSERT operations).
    # The FREDflow log is also updated.
    def upsert(self, fred_series, pandas_series):
        if self.verbosity() > 0:
            print("Loading", len(pds_series), "row(s) of",
                  fred_series.code(), "via UPSERT operations ...")
        with oracledb.connect(
                user=self.odb_user,
                password=self.odb_password,
                host=self.odb_host,
                port=self.odb_port,
                service_name=self.odb_name) as connection:
            # INSERT into FREDflow log with start time.
            sql_stmt1 = \
                "INSERT INTO fredflow_logs (fred_series, row_tally) " + \
                "VALUES (:1, :2)"
            sql_data1 = [fred_series.code(), 0]
            with connection.cursor() as cursor:
                cursor.execute(sql_stmt1, sql_data1)
                connection.commit()
            # Process the pandas series row-by-row.
            row_tally = 0
            for pds_tstamp, pds_val in pandas_series.items():
                # Convert timestamp to a date string.
                pds_date = str(pds_tstamp)[0:10]
                sql_parameters = [fred_series.code(), pds_date, pds_val]
                with connection.cursor() as cursor:
                    ret_val = 0
                    if fred_series.granularity() == 'QUARTERLY':
                        ret_val = cursor.callfunc(
                            "UPSERT_QTR_FRED_SERIES",
                            int,
                            sql_parameters
                    )
                    elif fred_series.granularity() == 'MONTHLY':
                        ret_val = cursor.callfunc(
                            "UPSERT_MON_FRED_SERIES",
                            int,
                            sql_parameters
                    )
                    elif fred_series.granularity() == 'DAILY':
                        ret_val = cursor.callfunc(
                            "UPSERT_DAY_FRED_SERIES",
                            int,
                            sql_parameters
                    )
                    else:
                        print("WARNING: Granularity of ",
                              fred_series.code(),
                              " unknown.")
                    # Check return value and commit if successful.
                    if ret_val > 0:
                        row_tally += 1
                        connection.commit()
                    else:
                        print("WARNING: UPSERT for ",
                              fred_series.code(), " failed.")
            # UPDATE FREDflow log with stop time.
            sql_stmt2 = \
                "UPDATE fredflow_logs SET " + \
                "row_tally = :1, "  + \
                "stop_tstamp = SYSTIMESTAMP " + \
                "WHERE fred_series = :2 " + \
                "AND stop_tstamp IS NULL"
            sql_data2 = [row_tally, fred_series.code()]
            with connection.cursor() as cursor:
                cursor.execute(sql_stmt2, sql_data2)
                connection.commit()

    # Returns the user being used for the database connection.
    def user(self):
        return self.odb_user

    # Returns and optionally sets the verbosity level for the object.
    def verbosity(self, v = None):
        if v is not None:
            self.odb_verbosity = v
        return self.odb_verbosity
