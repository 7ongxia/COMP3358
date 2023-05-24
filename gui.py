import tkinter as tk
from tkinter import filedialog
from tkinter.filedialog import askopenfile
from PIL import Image, ImageTk
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
    
window = tk.Tk()
window.title("Image Resizer")
window.geometry("640x400+100+100")
window.resizable(False, False)

b1 = tk.Button(window, text='Upload File', 
   width=20,command = lambda:upload_file())
b1.grid(row=1,column=1) 

def upload_file():
    global img
    f_types = [('Jpg Files', '*.jpg')]
    img_path = filedialog.askopenfilename(filetypes=f_types) 
    
    # Upload img to S3 bucket
    now = datetime.now()
    key = now.strftime('%Y-%m-%d-%H-%M-%S')
    s3.upload_file(img_path, bucket_name, key)
    
    # Send a message to the queue
    queue_url = f'https://sqs.{region}.amazonaws.com/{account_id}/{inbox_sqs_name}'
    resp = inbox_sqs.send_message(QueueUrl=queue_url, MessageBody=key, MessageDeduplicationId=key, MessageGroupId=key)
    print(resp)
    


window.mainloop()