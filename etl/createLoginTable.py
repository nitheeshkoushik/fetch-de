import psycopg2
import configparser 


class TableCreator:
    def __init__(self, config_file):
        self.config_file = config_file

    def connect(self):
        config = configparser.ConfigParser()
        config.read(self.config_file)
        database_config = config['postgresql']
        conn = psycopg2.connect(
                                database=database_config['database'],
                                user=database_config['user'],
                                password=database_config['password'],
                                host=database_config['host'],
                                port=database_config['port']
                                )
        return conn

    def createTable(self):
        conn = self.connect()
        cur = conn.cursor()
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
        
        conn.commit()
        cur.close()
        conn.close()
        return None