import boto3 
from datetime import datetime
import json
from botocore.exceptions import ClientError
import configparser
from encrypt import Encryptor

class SQSReceiver:

    def __init__(self, config_file):

        self.encryptor = Encryptor(config_file)

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

            dateStr = response['ResponseMetadata']['HTTPHeaders']['date']
            date = datetime.strptime(dateStr, '%a, %d %b %Y %H:%M:%S %Z')

            for message in messages:
                    messageNew = json.loads(message['Body'])
                    messageNew['date'] = date
                    messageNew['masked_device_id'] = self.encryptor.encrypt(messageNew['device_id'])
                    del messageNew['device_id']
                    messageNew['masked_ip'] = self.encryptor.encrypt(messageNew['ip'])
                    del messageNew['ip']

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