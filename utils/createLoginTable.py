import psycopg2

def connect():
    conn = psycopg2.connect(database = "postgres", user = "postgres", password = "postgres", host = "localhost", port = "5432")
    return conn

def createTable(cur):
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
    
    return None

conn = connect()
cur = conn.cursor()
createTable(cur)
conn.commit()
conn.close()