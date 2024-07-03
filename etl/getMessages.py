import boto3 
from datetime import datetime
from botocore.exceptions import ClientError

def getMessages():
    sqs = boto3.client('sqs', endpoint_url="http://localhost:4566")
    allMessages = []
    while True:
        try:
            messagesToDelete = []
            response = sqs.receive_message(QueueUrl = 'http://localhost:4566/000000000000/login-queue', 
                                        MaxNumberOfMessages=10)
            messages = response['Messages']
            date = response['ResponseMetadata']['HTTPHeaders']['date']
            for message in messages:
                allMessages.append({'body': message['Body'], 
                                    'date': date})
                messagesToDelete.append(message['ReceiptHandle'])
            for msgToDel in messagesToDelete:
                sqs.delete_message(
                        QueueUrl= 'http://localhost:4566/000000000000/login-queue', 
                        ReceiptHandle=msgToDel)
        except (ClientError, KeyError) as e:
            break
    return allMessages




if __name__ == '__main__':
    allMessages = getMessages()
    print(allMessages)
    # print(messages)
    # print(len(messagesToDelete))