# Fetch Data Engineering take home


## What do I need to do?
This challenge will focus on your ability to write a small application that can read from an AWS SQS Queue, transform that data, then write to a Postgres database.
- read JSON data containing user login behavior from an AWS SQS Queue, that is made
available via a custom localstack image that has the data pre loaded.
- Fetch wants to hide personal identifiable information (PII). The fields `device_id` and `ip`
should be masked, but in a way where it is easy for data analysts to identify duplicate
values in those fields.
- Once you have flattened the JSON data object and masked those two fields, write each
record to a Postgres database that is made available via a custom postgres image that has the tables pre created.


## Prerequesites 
- docker -- docker install guide
- docker-compose
- pip install awscli-local
- Psql - install


## How to run this project?

1. Clone this repo
    ```console
    git clone git@github.com:nitheeshkoushik/fetch-de.git
    ```
    ```console
    cd fetch-de
    ```
2. start the docker-compose file
    ```console
    docker-compose up -d
    ```
3. Create a .conf for credentials
   This is the tricky part if it was prod level string credentials in .conf file not recommended.
   ```console
    echo "[AWS]" > .conf
    echo "aws_access_key_id = test" >> .conf
    echo "aws_secret_access_key = test" >> .conf
    echo "region = us-east-1" >> .conf
    
    echo "endpoint_url = http://localhost:4566" >> .conf
    echo "queue_url = http://localhost:4566/000000000000/login-queue" >> .conf
    
    echo "[postgresql]" >> .conf
    echo "database = postgres" >> .conf
    echo "user = postgres" >> .conf
    echo "password = postgres" >> .conf
    echo "host = localhost" >> .conf
    echo "port = 5432" >> .conf
    ```
4. Install all the requirements
   ```console
    pip install -r requirements.text
    ```
5. Check the postgres database before
   ```console
    psql -d postgres -U postgres -p 5432 -h localhost -W
    ```
   ```console
    postgres=# select * from user_logins;
    ```
   This return a Table not found error as we did not create any tables yet.
6. Run app/main.py
   ```console
    python app/main.py
    ```
7. Check the postgres database after
    ```console
    psql -d postgres -U postgres -p 5432 -h localhost -W
    ```
   ```console
    postgres=# select * from user_logins LIMIT 10;
    ```
   This returns 10 rows from the DB as creation, message rerival and insertion are perfomed by main.py 


