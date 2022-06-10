import pickle
import base64
import os.path
import datetime as dt
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']

def main(sender='vad@gpee.com.ua', target_folder='data/gmail_parsing/harpok/'):
    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('gmail', 'v1', credentials=creds)

    # Call the Gmail API
    results = service.users().messages().list(userId='me', q=f'is:unread from:({sender}) (has:attachment) has:attachment').execute()
    messages = results.get('messages', [])

    if not messages:
        print('No messages found.')
    else:
        print('Messages:')
        for message in messages:
            
            full_message = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
            parts = full_message['payload']['parts']
            subject = full_message['payload']['headers'][17]['value']
            name_starts = subject.replace('\"', '').find('ТОВ')
            entity = subject.replace('\"', '')[name_starts:]
            print(entity)
            period = ' '.join(full_message['snippet'].split(' ')[5:7])
            print(period)
            #site_name = full_message['payload']['headers'][0]['value'].split('-')[1].split('@')[0]

            print('Message id: ' + message['id'] + '---')
            #print(message)
            for part in parts:
                if part['filename'][-3:] in ['pdf', 'p7s']:
                    print('File: ' + part['filename'] + ' of ' + str(part['body']['size']) + ' kB')

                    filename = part['filename']

                    attachment_id = part['body']['attachmentId']
                    attachment = service.users().messages().attachments().get(id=attachment_id, messageId=message['id'], userId='me').execute()
                    
                    data = base64.urlsafe_b64decode(attachment['data'].encode('utf-8'))
                  
                    if not os.path.exists(target_folder):
                        os.makedirs(target_folder)

                    if filename[-3:] == 'p7s':
                        f = open(target_folder + ' '.join(['s1', entity, period, filename[:-4]]), 'wb')
                    
                    if filename[-3:] == 'pdf':
                        f = open(target_folder + ' '.join([entity, period, filename]), 'wb')


                    f.write(data)
                    f.close()
            service.users().messages().modify(userId='me', id=message['id'], body={'removeLabelIds': ['UNREAD'], 'addLabelIds': []}).execute()

if __name__ == '__main__':
    main()