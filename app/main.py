from createLoginTable import TableCreator
from getMessages import SQSReceiver
from insertPostgres import postgresInserter
import os 
def main():
    config_file = '.conf' # Configuration file path

    # Create the 'user_logins' table in PostgreSQL if it doesn't exist
    table_creator = TableCreator(config_file)
    table_creator.createTable()

    # Retrieve messages from SQS queue and process them
    sqs = SQSReceiver(config_file)
    allMessages = sqs.run()

    # Insert processed messages into PostgreSQL
    table_inserter = postgresInserter(allMessages, config_file)  
    table_inserter.insert()

if __name__ == "__main__":
    main() # Call main function when script is run directly



