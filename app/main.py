from createLoginTable import TableCreator
from getMessages import SQSReceiver
from insertPostgres import postgresInserter
import os 
def main():
    config_file = '.conf'

    table_creator = TableCreator(config_file)
    table_creator.createTable()

    sqs = SQSReceiver(config_file)
    allMessages = sqs.run()

    table_inserter = postgresInserter(allMessages, config_file)  
    table_inserter.insert()

if __name__ == "__main__":
    main()



