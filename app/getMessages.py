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

        """
        Initialize the SQSReceiver with AWS credentials and queue configuration from a config file.

        Args:
        - config_file (str): Path to the configuration file containing AWS and queue settings.
        """
        # Read configuration from file
        config = configparser.ConfigParser()
        config.read(config_file)

        # Initialize SQS client with provided endpoint URL and region
        self.endpoint_url = config.get('AWS', 'endpoint_url')
        self.queue_url = config.get('AWS', 'queue_url')

        self.sqs = boto3.client('sqs', aws_access_key_id = 'test', 
                                aws_secret_access_key = 'test', 
                                endpoint_url=self.endpoint_url, region_name='us-east-1')
        

        
    def encrypt_deterministic(self, string):

        """
        Encrypts a string using AES encryption in ECB mode with a fixed key and base64 encodes the result.

        Args:
        - string (str): The string to encrypt.

        Returns:
        - str: The encrypted string encoded in base64.
        """


        key = b'Sixteen byte key'
        cipher = AES.new(key, AES.MODE_ECB) # AES encryption key (must be 16 bytes)
        ct_bytes = cipher.encrypt(pad(string.encode('utf-8'), AES.block_size))
        return base64.b64encode(ct_bytes).decode('utf-8')
    
    def recieveMessages(self):

        """
        Receives messages from the configured SQS queue, processes them by encrypting sensitive data,
        and prepares them for further processing.

        Returns:
        - tuple: A tuple containing two lists:
                 - batchMessages (list): List of processed messages with masked IP and device IDs.
                 - messagesToDelete (list): List of receipt handles for messages to be deleted from the queue.
        """

        messagesToDelete = []
        batchMessages = []
        try:
            # Receive up to 10 messages from the SQS queue with a short polling wait time
            response = self.sqs.receive_message(QueueUrl = self.queue_url, 
                                        MaxNumberOfMessages=10, 
                                         WaitTimeSeconds=1)
            
            messages = response['Messages'] # Extract received messages from the response

            dateStr = response['ResponseMetadata']['HTTPHeaders']['date']  # Timestamp of the SQS response
            date = datetime.strptime(dateStr, '%a, %d %b %Y %H:%M:%S %Z') # datetime object

            # Process each received message
            for message in messages:

                messageNew = json.loads(message['Body'])
                messageNew['date'] = date # Add received timestamp to the message


                # Encrypt and mask if present in the message
                if 'ip' in messageNew:
                    messageNew["masked_ip"] = self.encrypt_deterministic(messageNew['ip'])
                    del messageNew['ip']
                if 'device_id' in messageNew:
                    messageNew["masked_device_id"] = self.encrypt_deterministic(messageNew['device_id'])
                    del messageNew['device_id']

                # Check if the message has exactly 7 fields (customize based on actual message structure, should be changed for production)
                if len(messageNew) == 7:
                    batchMessages.append(messageNew) # Add processed message to batch list
                    messagesToDelete.append(message['ReceiptHandle']) # Collect receipt handle for message deletion

            return batchMessages, messagesToDelete # Return processed messages and deletion handles
        
        except (ClientError, KeyError) as e:
             return [], []
        

    
    def deleteMessages(self, toBeDeletedList):
        """
        Deletes messages from the SQS queue using their receipt handles.

        Args:
        - toBeDeletedList (list): List of receipt handles for messages to be deleted.

        Returns:
        - None
        """

        for elem in toBeDeletedList:
             self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=elem
            )
        return None
    
    def run(self):
        """
        Continuously runs the SQSReceiver to receive messages, process them, and delete them from the queue.

        Returns:
        - list: List of all processed messages received during the execution.
        """

        allMessages = []

        while True:
            batchMessages, messagesToDelete = self.recieveMessages() # Receive and process messages
            if messagesToDelete:
                self.deleteMessages(messagesToDelete) # Delete processed messages from the queue
                allMessages.extend(batchMessages) # Collect processed messages
            else:
                break
        return allMessages # Return all processed messages at the end
