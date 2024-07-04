import psycopg2
import json 
from datetime import datetime
from getMessages import SQSReceiver

class postgresInserter:
    def __init__(self, data) -> None:
        self.data = data

    def makeRecords(self):
        allVals = []
        for d in self.data:
            # d['date'] = datetime.strptime(d['date'], '%Y-%m-%dT%H:%M:%S')

            val = tuple(d.values())
            allVals.append(val)
        return allVals
    
    def insert(self):
        allVals = self.makeRecords()
        sqlQuery = """
                INSERT INTO user_logins (user_id, app_version, device_type, locale, create_date, masked_ip, masked_device_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
        conn = psycopg2.connect(database = "postgres", user = "postgres", password = "postgres", host = "localhost", port = "5432")
        cur = conn.cursor()
        cur.executemany(sqlQuery, allVals)
        conn.commit()
        cur.close()
        conn.close()
        return None
    

if __name__ == '__main__':
    config_file = '.conf'
    sqs = SQSReceiver(config_file)
    allMessages = sqs.run()

    postgres = postgresInserter(allMessages)  # Wrap data in a list for consistency with makeRecords()
    postgres.insert()
    