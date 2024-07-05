import psycopg2
import configparser

class postgresInserter:
    def __init__(self, data, config_file) -> None:
        """
        Initialize the PostgresInserter with data to insert and configuration file path.

        Args:
        - data (list of dicts): Data to be inserted into the database.
        - config_file (str): Path to the configuration file containing PostgreSQL connection details.
        """
        self.data = data
        self.config_file = config_file
        
        # Read PostgreSQL configuration from the config file
        config = configparser.ConfigParser()
        config.read(self.config_file)
        self.database_config = config['postgresql']

    def makeRecords(self):
        """
        Convert each dictionary in self.data into a tuple of values for insertion.

        Returns:
        - list of tuples: List of tuples, each representing a record to be inserted into the database.
        """
        allVals = []
        for d in self.data:
            val = tuple(d.values()) # Convert dictionary values to a tuple
            allVals.append(val) # Append tuple to list
        return allVals
    

    def connect(self):
        """
        Establish a connection to the PostgreSQL database using parameters from the configuration.

        Returns:
        - psycopg2 connection object: Connection to the PostgreSQL database.
        """

        conn = psycopg2.connect(
                                database = self.database_config['database'],
                                user = self.database_config['user'],
                                password = self.database_config['password'],
                                host = self.database_config['host'],
                                port = self.database_config['port']
                                )
        return conn
    

    def insert(self):

        """
        Insert all records from self.data into the 'user_logins' table in the PostgreSQL database.

        Returns:
        - None
        """

        allVals = self.makeRecords() # Get all records as tuples
        sqlQuery = """
                INSERT INTO user_logins (user_id, app_version, device_type, locale, create_date, masked_ip, masked_device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        conn = self.connect() # Connect to the PostgreSQL database
        cur = conn.cursor()
        cur.executemany(sqlQuery, allVals)  # Execute the insert query with all records
        conn.commit()
        cur.close()
        conn.close() # Close the database connection
        return None

    