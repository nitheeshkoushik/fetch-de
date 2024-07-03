import boto3 

sqs = boto3.client('sqs', endpoint_url="http://localhost:4566")

response = sqs.receive_message(
            QueueUrl="http://localhost:4566/000000000000/login-queue"
        )

print(response)