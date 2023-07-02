#This line imports the boto3 library, which is the official AWS SDK for Python.
import boto3



aws_access_key_id = 'hvkjihib'
aws_secret_access_key = 'jnljb'


# creates an S3 client object using the boto3.client() method. The client is initialized with your AWS credentials.
# You can use this client object to interact with S3 services.
# Create an S3 client
s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

# Specify the source file path
source_file_path = '/Users/gayathri1299/Desktop/archive/BTC.csv'

# Specify the S3 bucket name and destination file name
bucket_name = 'gayathritest'
destination_file_name = 'BTC.csv'

# Upload the file to S3
s3.upload_file(source_file_path, bucket_name, destination_file_name)

print(f'File "{source_file_path}" uploaded to S3 bucket "{bucket_name}" as "{destination_file_name}"')
