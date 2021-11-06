import json
import boto3
import base64
import os.path
import uuid
import requests
from datetime import datetime

from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account


def lambda_handler(event, context):
  metadata = {
	"comment": "metadata structure",
	"receive_utc_timestamp": "",
	"source": {
				"email": {
							"source_email": "",
							"email_utc_timestamp": "",
							"attachment_filename": "",
							"invoke_response": {"status_code": "",
                                                "content": "",
                                                "request": "",
                                                "text": "",
                                                "reason": ""}
							}
				},
	"original_filename": "",
	"s3_path": "",
	"invoke_utc_timestamp": "",
    "request_sent": ""}
    
    metadata["receive_utc_timestamp"] = datetime.now()
    
    sns = boto3.client('sns')
    s3 = boto3.resource("s3")
    
    post_msg = {'uuid':'','email':'','s3_uri':''}
    
    bucket_name = "test"
    uid = str(uuid.uuid4())
    post_msg['uuid'] = uid
    
    metadata["original_filename"] = uid

    SCOPES = ['https://mail.google.com/']
    creds = None
    creds = service_account.Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    delegated_credentials = creds.with_subject('test@gmail.com')

    service = build('gmail', 'v1', credentials=delegated_credentials)
    results = service.users().messages().list(userId='me',labelIds=['UNREAD']).execute()
    messages = results.get('messages', [])
    
    print(messages)
    
    try:
        if not messages:
            return {
            'statusCode': 200,
            'body': json.dumps('No new messages found!')
            }
    
    
        else:
    
            for message in messages:
                msg_id = message['id']
                msg = service.users().messages().get(userId='me', id=msg_id).execute()
                
    
            for sender in msg['payload']['headers']:
                if sender['name']  == 'From':
                
                    sender_email = sender['value']
                    sender_email = sender_email[ sender_email.find("<")+1:sender_email.find(">") ]
                    post_msg['email'] = sender_email
                    
                    metadata["source"]["email"]["email_utc_timestamp"] = datetime.now()
                    metadata["source"]["email"]["source_email"] = sender_email

            print(post_msg['email'])
    
            for part in msg['payload']['parts']:
    
                if 'data' in part['body']:    
                    data = part['body']['data']
                    service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds':['UNREAD']}).execute()
                    
                else:
    
                    if 'attachmentId' in part['body']:
                        att_id = part['body']['attachmentId']
                        att = service.users().messages().attachments().get(userId='me', messageId=msg_id,id=att_id).execute()
                
                        data = att['data']
                        file_data = base64.urlsafe_b64decode(data.encode('UTF-8'))
                        path = part['filename']
                        metadata["source"]["email"]["attachment_filename"] = path

                        s3_path =  uid + '/' + path
                        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=file_data)
                        
                        metadata["s3_path"] = bucket_name + '/' + s3_path

                        post_msg['s3_uri'] = "s3://" + bucket_name + '/' + s3_path
                        msg_json = {'test': [ post_msg ] }

                        metadata["request_sent"] = msg_json
                        
                        metadata["invoke_utc_timestamp"] = datetime.now()

                        r = requests.post('https://ttt.execute-api.REGION.amazonaws.com/API', json=msg_json)
                        
                        metadata["source"]["email"]["invoke_response"]["status_code"] = r.status_code
                        metadata["source"]["email"]["invoke_response"]["content"] = r.content
                        metadata["source"]["email"]["invoke_response"]["request"] = r.request
                        metadata["source"]["email"]["invoke_response"]["text"] = r.text
                        metadata["source"]["email"]["invoke_response"]["reason"] = r.reason
                        
                        w_metadata = json.dumps(metadata)
                        
                        file_data = base64.urlsafe_b64decode(w_metadata.encode('UTF-8'))
                        s3_path =  uid + '/' + uid + '.json'
                        s3.Bucket(bucket_name).put_object(Key=s3_path, Body=file_data)
                        
                        if r.status_code == 204:
                            message = {
                            "source": "test@gmail.com",
                            "destination": post_msg['email'],
                            "subject": "test",
                            "body": "test"
                                        }
                            print(message)
                            sns.publish(
                                        TopicArn="arn:aws:sns:::",
                                        Message=json.dumps(message)
                                        )
                            
                            # mark the message as read
                            service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds':['UNREAD']}).execute()

                        else:
                            message = {
                            "source": "test@gmail.com",
                            "destination": post_msg['email'],
                            "subject": "test",
                            "body": """test"""
                                        }
                            print(message)
                            sns.publish(
                                        TopicArn="arn:aws:sns:::",
                                        Message=json.dumps(message)
                                        )
                            
                            # mark the message as read
                            service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds':['UNREAD']}).execute()

    except:
        # TBD
        # mark the message as read
        # service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds':['UNREAD']}).execute()
        pass
        service.users().messages().modify(userId='me', id=msg_id, body={'removeLabelIds':['UNREAD']}).execute()
    return {
        'statusCode': 200,
        'body': json.dumps('getEmailAttachments Done!')
    }
