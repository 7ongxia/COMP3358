from datetime import datetime
import boto3
import os


# Set up your AWS credentials and create a new S3 and SQS client object
account_id = '054717022140'
access_key_id = 'AKIAQZPLDP66DQQOF2EF'
secret_access_key = 'P+8AGVZwDfyqLObU0/iAfzaK5KLAE/YqWlLgvOrz'
region = 'ap-northeast-2'
s3 = boto3.client('s3', aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key, region_name=region)
inbox_sqs = boto3.client('sqs', aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key, region_name=region)
inbox_sqs_name = 'c3358_inbox.fifo'

# Create a new S3 bucket or use an existing one
bucket_name = 'hku-comp3358'
if s3.head_bucket(Bucket=bucket_name)['ResponseMetadata']['HTTPStatusCode'] != 200:
    s3.create_bucket(Bucket=bucket_name)

command = ""
original_img_path = './images/'
resized_img_path = './resized_images/'
while command != "exit":
    # Get a list of files in the directory
    file_list = os.listdir(original_img_path)
    # Print the list of files
    print(" > ", end="")
    for file in file_list:
        print(file.split('.')[0], end=", ")
    print("")
    img_name = input("Enter an image file name: ")

    # Upload the image to the S3 bucket
    now = datetime.now()
    key = now.strftime('%Y-%m-%d-%H-%M-%S_') + f"{img_name}.png"
    file_path = f"./images/{img_name}.png"
    s3.upload_file(file_path, bucket_name, key)

    # Send a message to the queue
    queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{inbox_sqs_name}'
    response = inbox_sqs.send_message(QueueUrl=queue_url, MessageBody=key, MessageDeduplicationId=key, MessageGroupId=key)

    # # Retrieve the key of the uploaded image
    # bucket_url = f'https://s3.{region}.amazonaws.com/{bucket_name}/'
    # image_url = bucket_url + key
    # print(f' > Image URL: {image_url}')
