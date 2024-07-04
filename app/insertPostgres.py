import psycopg2
import configparser

class postgresInserter:
    def __init__(self, data, config_file) -> None:
        self.data = data
        self.config_file = config_file
        
        config = configparser.ConfigParser()
        config.read(self.config_file)
        self.database_config = config['postgresql']

    def makeRecords(self):
        allVals = []
        for d in self.data:
            val = tuple(d.values())
            allVals.append(val)
        return allVals
    

    def connect(self):

        conn = psycopg2.connect(
                                database = self.database_config['database'],
                                user = self.database_config['user'],
                                password = self.database_config['password'],
                                host = self.database_config['host'],
                                port = self.database_config['port']
                                )
        return conn
    

    def insert(self):
        allVals = self.makeRecords()
        sqlQuery = """
                INSERT INTO user_logins (user_id, app_version, device_type, locale, create_date, masked_ip, masked_device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        conn = self.connect()
        cur = conn.cursor()
        cur.executemany(sqlQuery, allVals)
        conn.commit()
        cur.close()
        conn.close()
        return None

    