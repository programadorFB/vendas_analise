#!/usr/bin/env python3
"""
Script de configuração para integração com Google Drive
Valida credenciais e testa a conexão
"""

import json
import os
import sys
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from oauth2client.client import OAuth2Credentials

def check_credentials_file():
    """Verifica se o arquivo de credenciais existe e está válido"""
    creds_file = 'mycreds.txt'
    
    if not os.path.exists(creds_file):
        print("❌ Arquivo 'mycreds.txt' não encontrado!")
        print("   Certifique-se de que o arquivo está no diretório raiz do projeto.")
        return False
    
    try:
        with open(creds_file, 'r') as f:
            creds_data = json.load(f)
        
        # Verificar campos obrigatórios
        required_fields = [
            'access_token', 'client_id', 'client_secret', 
            'refresh_token', 'token_uri'
        ]
        
        missing_fields = [field for field in required_fields if field not in creds_data]
        
        if missing_fields:
            print(f"❌ Campos obrigatórios ausentes: {', '.join(missing_fields)}")
            return False
        
        print("✅ Arquivo de credenciais encontrado e válido")
        return True
        
    except json.JSONDecodeError:
        print("❌ Arquivo 'mycreds.txt' não é um JSON válido!")
        return False
    except Exception as e:
        print(f"❌ Erro ao ler credenciais: {e}")
        return False

def test_google_drive_connection():
    """Testa a conexão com o Google Drive"""
    try:
        print("🔄 Testando conexão com Google Drive...")
        
        # Carregar credenciais
        with open('mycreds.txt', 'r') as f:
            creds_data = json.load(f)
        
        # Criar objeto de credenciais
        credentials = OAuth2Credentials(
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
        
        # Construir serviço
        service = build('drive', 'v3', credentials=credentials)
        
        # Testar com uma operação simples
        results = service.files().list(pageSize=1).execute()
        
        print("✅ Conexão com Google Drive estabelecida com sucesso!")
        print(f"   Permissões ativas: {', '.join(creds_data.get('scopes', []))}")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao conectar com Google Drive: {e}")
        print("   Verifique se:")
        print("   - As credenciais estão corretas")
        print("   - O token não expirou")
        print("   - As permissões do Google Drive estão ativas")
        return False

def create_test_folder():
    """Cria uma pasta de teste no Google Drive"""
    try:
        print("🔄 Criando pasta de teste...")
        
        # Carregar credenciais
        with open('mycreds.txt', 'r') as f:
            creds_data = json.load(f)
        
        credentials = OAuth2Credentials(
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
        
        service = build('drive', 'v3', credentials=credentials)
        
        # Nome da pasta de teste
        test_folder_name = f"Teste Webhooks - {datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Criar pasta
        file_metadata = {
            'name': test_folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        folder = service.files().create(
            body=file_metadata,
            fields='id,name,webViewLink'
        ).execute()
        
        print(f"✅ Pasta de teste criada com sucesso!")
        print(f"   Nome: {folder.get('name')}")
        print(f"   ID: {folder.get('id')}")
        print(f"   Link: {folder.get('webViewLink')}")
        
        return folder.get('id')
        
    except Exception as e:
        print(f"❌ Erro ao criar pasta de teste: {e}")
        return None

def cleanup_test_folder(folder_id):
    """Remove a pasta de teste"""
    try:
        if not folder_id:
            return
        
        print("🔄 Removendo pasta de teste...")
        
        # Carregar credenciais
        with open('mycreds.txt', 'r') as f:
            creds_data = json.load(f)
        
        credentials = OAuth2Credentials(
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
        
        service = build('drive', 'v3', credentials=credentials)
        
        # Deletar pasta
        service.files().delete(fileId=folder_id).execute()
        
        print("✅ Pasta de teste removida com sucesso!")
        
    except Exception as e:
        print(f"⚠️  Aviso: Não foi possível remover a pasta de teste: {e}")
        print(f"   Você pode removê-la manualmente do Google Drive")

def check_dependencies():
    """Verifica se todas as dependências estão instaladas"""
    print("🔄 Verificando dependências...")
    
    required_packages = [
        'google-api-python-client',
        'oauth2client',
        'pandas',
        'flask'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ Pacotes ausentes: {', '.join(missing_packages)}")
        print("   Execute: pip install -r requirements.txt")
        return False
    
    print("✅ Todas as dependências estão instaladas")
    return True

def main():
    """Função principal de configuração"""
    print("=" * 60)
    print("🚀 CONFIGURAÇÃO GOOGLE DRIVE - WEBHOOK SYSTEM")
    print("=" * 60)
    
    # Verificar dependências
    if not check_dependencies():
        print("\n❌ Falha na verificação de dependências!")
        sys.exit(1)
    
    # Verificar credenciais
    if not check_credentials_file():
        print("\n❌ Falha na verificação de credenciais!")
        sys.exit(1)
    
    # Testar conexão
    if not test_google_drive_connection():
        print("\n❌ Falha no teste de conexão!")
        sys.exit(1)
    
    # Criar pasta de teste
    test_folder_id = create_test_folder()
    
    if test_folder_id:
        print("\n🎉 CONFIGURAÇÃO CONCLUÍDA COM SUCESSO!")
        print("\nPróximos passos:")
        print("1. Execute seu aplicativo Flask: python app.py")
        print("2. Teste os endpoints:")
        print("   - GET /export/drive/folders")
        print("   - GET /export/excel/drive")
        print("3. Configure cron jobs para exportações agendadas")
        
        # Perguntar se deve remover pasta de teste
        response = input("\nDeseja remover a pasta de teste? (s/N): ").lower()
        if response in ['s', 'sim', 'y', 'yes']:
            cleanup_test_folder(test_folder_id)
    else:
        print("\n⚠️  Configuração concluída com avisos")
        print("   A conexão funciona, mas houve problemas na criação de pastas")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    main()