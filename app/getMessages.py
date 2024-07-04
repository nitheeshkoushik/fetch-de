import boto3 
from datetime import datetime
import json
from botocore.exceptions import ClientError
import configparser


from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import base64

class SQSReceiver:

    def __init__(self, config_file):

        config = configparser.ConfigParser()
        config.read(config_file)

        self.endpoint_url = config.get('AWS', 'endpoint_url')
        self.queue_url = config.get('AWS', 'queue_url')

        self.sqs = boto3.client('sqs', aws_access_key_id = 'test', 
                                aws_secret_access_key = 'test', 
                                endpoint_url=self.endpoint_url, region_name='us-east-1')
        
    def encrypt_deterministic(self, string):
        key = b'Sixteen byte key'
        cipher = AES.new(key, AES.MODE_ECB)
        ct_bytes = cipher.encrypt(pad(string.encode('utf-8'), AES.block_size))
        return base64.b64encode(ct_bytes).decode('utf-8')
    
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
                    messageNew["masked_ip"] = self.encrypt_deterministic(messageNew['ip'])
                    del messageNew['ip']
                if 'device_id' in messageNew:
                    messageNew["masked_device_id"] = self.encrypt_deterministic(messageNew['device_id'])
                    del messageNew['device_id']
                if len(messageNew) == 7:
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
