import boto3 
from datetime import datetime
import json
from botocore.exceptions import ClientError
import configparser
import hashlib

class SQSReceiver:

    def __init__(self, config_file):

        config = configparser.ConfigParser()
        config.read(config_file)

        self.endpoint_url = config.get('AWS', 'endpoint_url')
        self.queue_url = config.get('AWS', 'queue_url')
        self.sqs = boto3.client('sqs', endpoint_url=self.endpoint_url)
        
    def hashVal(self, string):
        sha256 = hashlib.sha256()
        sha256.update(string.encode('utf-8'))
        return sha256.hexdigest()
    
    def recieveMessages(self):
        messagesToDelete = []
        batchMessages = []
        try:
            response = self.sqs.receive_message(QueueUrl = self.queue_url, 
                                        MaxNumberOfMessages=10, 
                                         WaitTimeSeconds=1)
            
            messages = response['Messages']

            dateStr = response['ResponseMetadata']['HTTPHeaders']['date']
            date = datetime.strptime(dateStr, '%a, %d %b %Y %H:%M:%S %Z')

            for message in messages:
                messageNew = json.loads(message['Body'])
                messageNew['date'] = date
                if 'ip' in messageNew:
                    messageNew["masked_ip"] = self.hashVal(messageNew['ip'])
                    del messageNew['ip']
                if 'device_id' in messageNew:
                    messageNew["masked_device_id"] = self.hashVal(messageNew['device_id'])
                    del messageNew['device_id']

                batchMessages.append(messageNew)
                messagesToDelete.append(message['ReceiptHandle'])

            return batchMessages, messagesToDelete
        
        except (ClientError, KeyError) as e:
             return [], []
        

    
    def deleteMessages(self, toBeDeletedList):
        for elem in toBeDeletedList:
             self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=elem
            )
        return None
    
    def run(self):
        allMessages = []

        while True:
            batchMessages, messagesToDelete = self.recieveMessages()
            if messagesToDelete:
                self.deleteMessages(messagesToDelete)
                allMessages.extend(batchMessages)
            else:
                break
        return allMessages


if __name__ == "__main__":
    config_file = '.conf'
    sqs = SQSReceiver(config_file)
    allMessages = sqs.run()
    print(allMessages, len(allMessages))