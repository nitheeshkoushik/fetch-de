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


