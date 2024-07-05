import psycopg2
import configparser 


class TableCreator:
    def __init__(self, config_file):
        """
        Initialize the TableCreator with configuration file path.

        Args:
        - config_file (str): Path to the configuration file containing PostgreSQL connection details.
        """
        self.config_file = config_file

    def connect(self):
        """
        Establish a connection to the PostgreSQL database using parameters from the configuration file.

        Returns:
        - psycopg2 connection object: Connection to the PostgreSQL database.
        """
        config = configparser.ConfigParser()
        config.read(self.config_file)
        database_config = config['postgresql'] # Retrieve PostgreSQL configuration section
        conn = psycopg2.connect(
                                database=database_config['database'],
                                user=database_config['user'],
                                password=database_config['password'],
                                host=database_config['host'],
                                port=database_config['port']
                                )
        return conn

    def createTable(self):
        """
        Create the 'user_logins' table in the PostgreSQL database if it doesn't exist.

        Returns:
        - None
        """

        conn = self.connect()  # Connect to the PostgreSQL database
        cur = conn.cursor() # Create a cursor object to execute SQL queries

        # SQL query to create 'user_logins' table with specified columns and primary key
        cur.execute("""
                CREATE TABLE IF NOT EXISTS user_logins(
                    user_id varchar(128) ,
                    device_type varchar(32),
                    masked_ip varchar(256),
                    masked_device_id varchar(256),
                    locale  varchar(32),
                    app_version  varchar(32),
                    create_date timestamp,
                    PRIMARY KEY (user_id, create_date)
                    )
            """)
        
        conn.commit() # Commit the transaction
        cur.close()
        conn.close() # Close the database connection
        return None