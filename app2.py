import boto3

s3=boto3.client('s3')
bucket_name="scrape-input"
key="input.xlsx"

response = s3.get_object(Bucket=bucket_name,Key=key)
file_content = response['Body'].read()

file_content_str = file_content.decode('utf-8')
print(file_content_str)
