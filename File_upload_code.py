import json
import boto3
import base64
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        http_method = event['http-method']
        s3_bucket = "mownika-poc-s3"
        # Gets all the files in the s3 bucket
        if http_method == "GET":
            objects = s3.list_objects_v2(Bucket=s3_bucket)
            file_names = [obj['Key'] for obj in objects.get('Contents', [])]

            response = {
                "data": json.dumps({"data": file_names})
            }
            return response
        # POST Method
        elif http_method == "POST":
            json_data = event['body-json']
            # Checks if request body has file_based_64, file_type and filename keys, if yes uploads file to s3
            if 'file_based_64' in json_data and 'file_type' in json_data and 'filename' in json_data:
                filebased64 = json_data.get('file_based_64')
                filetype = json_data.get('file_type')
                filename = json_data.get('filename')

                filebased64_padded = filebased64 + '=' * (-len(filebased64) % 4)
                file_content = base64.b64decode(filebased64_padded)
                file_content_stream = BytesIO(file_content)
                s3_key = f"{filename}.{filetype}"

                s3.upload_fileobj(
                    file_content_stream,
                    s3_bucket,
                    s3_key
                )
                s3_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_key}"
                response = {
                    'data': {
                        'statusCode': 201,  
                        's3_url': s3_url
                    }
                }
                return response
            
            #Checks if request body has file key, if yes generates presigned url for the file to download it.
            elif 'file' in json_data:
                file_name = json_data.get('file')
                file_key = f"{file_name}"

                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': s3_bucket, 'Key': file_key, 'ResponseContentType': 'application/octet-stream' },
                    ExpiresIn=300  
                )
                response = {
                    'file-url': presigned_url
                }
                return response
            else:
                # Return an error message for unsupported payloads
                response = {
                    "statusCode": 400,
                    "body": "Invalid payload"
                }
                return response
        # Deletes the file from s3 bucket
        elif http_method == "DELETE":
            json_data = event['body-json']
            file_name_to_delete = json_data.get('file')
            s3.delete_object(Bucket=s3_bucket, Key=file_name_to_delete)

            response = {
                'statusCode': 201,
                'status': f"{file_name_to_delete} deleted successfully"
            }
            return response
        else:
            response = {
                "statusCode": 405,
                "body": "Method not allowed"
            }
            return response
    except Exception as e:
        # Return an error response if any exception occurs
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
        return response
