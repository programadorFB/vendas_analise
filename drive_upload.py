import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.client import OAuth2Credentials

def get_drive_service():
    with open('mycreds.txt', 'r') as f:
        creds_data = json.load(f)

    creds = OAuth2Credentials(
        access_token=creds_data['access_token'],
        client_id=creds_data['client_id'],
        client_secret=creds_data['client_secret'],
        refresh_token=creds_data['refresh_token'],
        token_expiry=creds_data.get('token_expiry'),
        token_uri=creds_data['token_uri'],
        user_agent=creds_data.get('user_agent'),
        revoke_uri=creds_data.get('revoke_uri'),
        scopes=creds_data.get('scopes', ['https://www.googleapis.com/auth/drive'])
    )
    return build('drive', 'v3', credentials=creds)

def find_file_id_by_name(service, filename):
    response = service.files().list(q=f"name='{filename}' and trashed=false").execute()
    files = response.get('files', [])
    return files[0]['id'] if files else None

def upload_or_replace_file(filename):
    service = get_drive_service()
    file_id = find_file_id_by_name(service, filename)
    media = MediaFileUpload(filename, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', resumable=True)

    if file_id:
        updated = service.files().update(fileId=file_id, media_body=media).execute()
        print(f"✅ Arquivo atualizado no Drive: {updated.get('name')}")
    else:
        new = service.files().create(body={'name': filename}, media_body=media).execute()
        print(f"✅ Arquivo criado no Drive: {new.get('name')}")
