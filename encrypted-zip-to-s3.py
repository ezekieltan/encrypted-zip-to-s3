import boto3
import zipfile
import io
import os
from botocore.exceptions import ClientError
from zipfile import BadZipFile

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context): 
    try:
    # Set the source bucket and file details
        source_bucket = "source-bucket-here"
        source_key = "attachment.zip"  # The name of the zip file
        destination_bucket = "destination-bucket-here"  # The destination bucket name
        password = "P@ssw0rd"  # The password for the zip file

        # Fetch the zip file from S3
        print(f"Downloading file from s3://{source_bucket}/{source_key}")
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        file_content = response['Body'].read()

        # Unzip the file in memory
        with zipfile.ZipFile(io.BytesIO(file_content)) as zf:
            # Check for password protection and extract using password
            zf.setpassword(password.encode())
            
            # Iterate through the files in the zip
            for file_name in zf.namelist():
                with zf.open(file_name) as file:
                    # Read the file and upload to the destination S3 bucket
                    print(f"Uploading {file_name} to s3://{destination_bucket}/{file_name}")
                    s3_client.upload_fileobj(file, destination_bucket, file_name)

        return {
            'statusCode': 200,
            'body': f"Successfully unzipped and uploaded files to {destination_bucket}."
        }
    except BadZipFile:
        return {
            'statusCode': 400,
            'body': 'The zip file is corrupted or invalid.'
        }
    except ClientError as e:
        print(f"ClientError: {e}")
        return {
            'statusCode': 500,
            'body': f"Error processing the file: {str(e)}"
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': f"Error processing the zip file: {str(e)}"
        }
