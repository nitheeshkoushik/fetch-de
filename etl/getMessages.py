import boto3 
from datetime import datetime
import json
from botocore.exceptions import ClientError
import configparser

class SQSReceiver:

    def __init__(self, config_file):

        config = configparser.ConfigParser()
        config.read(config_file)

        self.endpoint_url = config.get('AWS', 'endpoint_url')
        self.queue_url = config.get('AWS', 'queue_url')
        self.sqs = boto3.client('sqs', endpoint_url=self.endpoint_url)
    
    def recieveMessages(self):
        messagesToDelete = []
        batchMessages = []
        try:
            response = self.sqs.receive_message(QueueUrl = self.queue_url, 
                                        MaxNumberOfMessages=10)
            
            messages = response['Messages']

            date = response['ResponseMetadata']['HTTPHeaders']['date']
            for message in messages:
                    messageNew = json.loads(message['Body'])
                    messageNew['date'] = date
                    batchMessages.append(messageNew)
                    messagesToDelete.append(message['ReceiptHandle'])

            return batchMessages, messagesToDelete
        
        except (ClientError, KeyError) as e:
             return None, None
    
    def deleteMessages(self, toBeDeletedList):
        for elem in toBeDeletedList:
             self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=elem
            )
        return None
    
def main():
    config_file = '.conf'
    sqs_manager = SQSReceiver(config_file)

    allMessages = []
    while True:
        batchMessages, messagesToDelete = sqs_manager.recieveMessages()
        if messagesToDelete:
            sqs_manager.deleteMessages(messagesToDelete)
            allMessages.extend(batchMessages)
        else:
             break
    return allMessages


if __name__ == "__main__":
    allMessages = main()
    print(allMessages[0])