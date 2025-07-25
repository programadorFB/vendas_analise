import json
import os
import logging
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from oauth2client.client import OAuth2Credentials
from googleapiclient.errors import HttpError
from io import BytesIO

# Configure logging
logger = logging.getLogger(__name__)

def get_drive_service():
    """
    Obter serviço do Google Drive com tratamento de erros aprimorado
    """
    try:
        # Verificar se o arquivo de credenciais existe
        creds_file = 'mycreds.txt'
        if not os.path.exists(creds_file):
            logger.error(f"❌ Arquivo de credenciais não encontrado: {creds_file}")
            raise FileNotFoundError(f"Credenciais do Google Drive não encontradas em {creds_file}")

        with open(creds_file, 'r') as f:
            creds_data = json.load(f)

        # Validar campos obrigatórios
        required_fields = ['access_token', 'client_id', 'client_secret', 'refresh_token', 'token_uri']
        missing_fields = [field for field in required_fields if not creds_data.get(field)]
        
        if missing_fields:
            logger.error(f"❌ Campos obrigatórios ausentes nas credenciais: {missing_fields}")
            raise ValueError(f"Credenciais incompletas. Campos ausentes: {missing_fields}")

        # Criar objeto de credenciais
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
        
        # Construir serviço
        service = build('drive', 'v3', credentials=creds)
        
        # Teste a conexão fazendo uma chamada simples
        test_response = service.files().list(pageSize=1).execute()
        logger.info("✅ Conexão com Google Drive estabelecida com sucesso")
        
        return service
        
    except FileNotFoundError as e:
        logger.error(f"❌ Arquivo mycreds.txt não encontrado: {e}")
        raise Exception("Credenciais do Google Drive não encontradas. Verifique se o arquivo mycreds.txt existe.")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erro ao decodificar mycreds.txt: {e}")
        raise Exception("Formato inválido das credenciais do Google Drive. Verifique o JSON em mycreds.txt.")
    except HttpError as e:
        logger.error(f"❌ Erro HTTP na API do Google Drive: {e}")
        if e.resp.status == 401:
            raise Exception("Credenciais do Google Drive expiradas ou inválidas. Renove o token de acesso.")
        elif e.resp.status == 403:
            raise Exception("Permissões insuficientes no Google Drive. Verifique os escopos de acesso.")
        else:
            raise Exception(f"Erro na API do Google Drive: {e}")
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao conectar com Google Drive: {e}")
        raise Exception(f"Falha na conexão com Google Drive: {str(e)}")

def find_file_id_by_name(service, filename, parent_folder_id=None):
    """
    Encontrar ID do arquivo pelo nome no Google Drive
    
    Args:
        service: Serviço do Google Drive
        filename: Nome do arquivo para buscar
        parent_folder_id: ID da pasta pai (opcional)
    
    Returns:
        str or None: ID do arquivo se encontrado
    """
    try:
        # Construir query de busca
        query = f"name='{filename}' and trashed=false"
        
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        
        response = service.files().list(
            q=query,
            fields="files(id, name, modifiedTime, parents)",
            orderBy="modifiedTime desc"
        ).execute()
        
        files = response.get('files', [])
        
        if files:
            file_info = files[0]
            logger.info(f"📁 Arquivo encontrado no Drive: {filename} (ID: {file_info['id']})")
            return file_info['id']
        else:
            logger.info(f"📁 Arquivo não encontrado no Drive: {filename}")
            return None
            
    except HttpError as e:
        logger.error(f"❌ Erro HTTP ao buscar arquivo no Drive: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao buscar arquivo: {e}")
        return None

def create_folder_if_not_exists(service, folder_name, parent_folder_id=None):
    """
    Criar pasta no Google Drive se não existir
    
    Args:
        service: Serviço do Google Drive
        folder_name: Nome da pasta
        parent_folder_id: ID da pasta pai (opcional)
    
    Returns:
        str or None: ID da pasta criada/encontrada
    """
    try:
        # Buscar pasta existente
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_folder_id:
            query += f" and '{parent_folder_id}' in parents"
        
        response = service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        
        folders = response.get('files', [])
        
        if folders:
            folder_id = folders[0]['id']
            logger.info(f"📁 Pasta já existe: {folder_name} (ID: {folder_id})")
            return folder_id
        
        # Criar nova pasta
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_folder_id:
            folder_metadata['parents'] = [parent_folder_id]
        
        folder = service.files().create(
            body=folder_metadata,
            fields="id, name"
        ).execute()
        
        folder_id = folder.get('id')
        logger.info(f"📁 Pasta criada: {folder_name} (ID: {folder_id})")
        return folder_id
        
    except HttpError as e:
        logger.error(f"❌ Erro HTTP ao criar pasta: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Erro inesperado ao criar pasta: {e}")
        return None

def upload_or_replace_file(filename, folder_name="Webhooks_Reports", custom_name=None):
    """
    Upload ou substituir arquivo no Google Drive com organização em pastas
    
    Args:
        filename: Caminho do arquivo local
        folder_name: Nome da pasta no Drive (padrão: Webhooks_Reports)
        custom_name: Nome customizado para o arquivo no Drive (opcional)
    
    Returns:
        dict: Resultado da operação com status e informações
    """
    try:
        service = get_drive_service()
        
        # Verificar se o arquivo existe localmente
        if not os.path.exists(filename):
            error_msg = f"Arquivo não encontrado: {filename}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        # Obter informações do arquivo
        file_size = os.path.getsize(filename)
        file_basename = custom_name or os.path.basename(filename)
        
        logger.info(f"📤 Iniciando upload: {file_basename} ({format_bytes(file_size)})")
        
        # Criar ou encontrar pasta de destino
        folder_id = create_folder_if_not_exists(service, folder_name)
        if not folder_id:
            error_msg = f"Não foi possível criar/encontrar a pasta: {folder_name}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        # Verificar se arquivo já existe na pasta
        existing_file_id = find_file_id_by_name(service, file_basename, folder_id)
        
        # Preparar upload
        media = MediaFileUpload(
            filename, 
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        if existing_file_id:
            # Atualizar arquivo existente
            logger.info(f"🔄 Atualizando arquivo existente: {file_basename}")
            
            updated_file = service.files().update(
                fileId=existing_file_id,
                media_body=media,
                fields="id, name, size, modifiedTime"
            ).execute()
            
            result = {
                "success": True,
                "action": "updated",
                "file_id": updated_file.get('id'),
                "file_name": updated_file.get('name'),
                "file_size": updated_file.get('size'),
                "modified_time": updated_file.get('modifiedTime'),
                "folder_name": folder_name
            }
            
            logger.info(f"✅ Arquivo atualizado no Drive: {updated_file.get('name')}")
            return result
            
        else:
            # Criar novo arquivo
            logger.info(f"📝 Criando novo arquivo: {file_basename}")
            
            file_metadata = {
                'name': file_basename,
                'parents': [folder_id]
            }
            
            new_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, name, size, modifiedTime"
            ).execute()
            
            result = {
                "success": True,
                "action": "created",
                "file_id": new_file.get('id'),
                "file_name": new_file.get('name'),
                "file_size": new_file.get('size'),
                "modified_time": new_file.get('modifiedTime'),
                "folder_name": folder_name
            }
            
            logger.info(f"✅ Arquivo criado no Drive: {new_file.get('name')}")
            return result
            
    except Exception as e:
        error_msg = f"Erro no upload para Google Drive: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def upload_buffer_to_drive(buffer, filename, folder_name="Webhooks_Reports"):
    """
    Upload de buffer BytesIO diretamente para Google Drive
    
    Args:
        buffer: BytesIO buffer com os dados
        filename: Nome do arquivo no Drive
        folder_name: Nome da pasta no Drive
    
    Returns:
        dict: Resultado da operação
    """
    try:
        service = get_drive_service()
        
        # Verificar se buffer tem conteúdo
        buffer_size = len(buffer.getvalue())
        if buffer_size == 0:
            error_msg = "Buffer está vazio"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        logger.info(f"📤 Iniciando upload de buffer: {filename} ({format_bytes(buffer_size)})")
        
        # Criar ou encontrar pasta de destino
        folder_id = create_folder_if_not_exists(service, folder_name)
        if not folder_id:
            error_msg = f"Não foi possível criar/encontrar a pasta: {folder_name}"
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
        
        # Verificar se arquivo já existe
        existing_file_id = find_file_id_by_name(service, filename, folder_id)
        
        # Preparar upload do buffer
        buffer.seek(0)  # Garantir que estamos no início do buffer
        media = MediaIoBaseUpload(
            buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        if existing_file_id:
            # Atualizar arquivo existente
            logger.info(f"🔄 Atualizando arquivo existente via buffer: {filename}")
            
            updated_file = service.files().update(
                fileId=existing_file_id,
                media_body=media,
                fields="id, name, size, modifiedTime"
            ).execute()
            
            result = {
                "success": True,
                "action": "updated",
                "file_id": updated_file.get('id'),
                "file_name": updated_file.get('name'),
                "file_size": updated_file.get('size'),
                "modified_time": updated_file.get('modifiedTime'),
                "folder_name": folder_name,
                "source": "buffer"
            }
            
            logger.info(f"✅ Buffer atualizado no Drive: {updated_file.get('name')}")
            return result
            
        else:
            # Criar novo arquivo
            logger.info(f"📝 Criando novo arquivo via buffer: {filename}")
            
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            new_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id, name, size, modifiedTime"
            ).execute()
            
            result = {
                "success": True,
                "action": "created",
                "file_id": new_file.get('id'),
                "file_name": new_file.get('name'),
                "file_size": new_file.get('size'),
                "modified_time": new_file.get('modifiedTime'),
                "folder_name": folder_name,
                "source": "buffer"
            }
            
            logger.info(f"✅ Buffer enviado para Drive: {new_file.get('name')}")
            return result
            
    except Exception as e:
        error_msg = f"Erro no upload de buffer para Google Drive: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def list_webhook_files(service=None, limit=50, folder_name="Webhooks_Reports"):
    """
    Listar arquivos de webhook no Google Drive
    
    Args:
        service: Serviço do Google Drive (opcional)
        limit: Número máximo de arquivos para retornar
        folder_name: Nome da pasta para buscar
    
    Returns:
        list: Lista de arquivos encontrados
    """
    try:
        if not service:
            service = get_drive_service()
        
        # Encontrar pasta específica se especificada
        folder_id = None
        if folder_name:
            folder_id = create_folder_if_not_exists(service, folder_name)
        
        # Construir query de busca
        if folder_id:
            query = f"'{folder_id}' in parents and trashed=false"
        else:
            query = "name contains 'webhook' and trashed=false"
        
        response = service.files().list(
            q=query,
            orderBy='modifiedTime desc',
            pageSize=limit,
            fields="files(id, name, modifiedTime, size, parents, mimeType)"
        ).execute()
        
        files = response.get('files', [])
        
        # Processar e formatar arquivos
        formatted_files = []
        for file in files:
            formatted_files.append({
                'id': file['id'],
                'name': file['name'],
                'modified': file['modifiedTime'],
                'size': file.get('size', 'N/A'),
                'size_formatted': format_bytes(int(file.get('size', 0))) if file.get('size', '').isdigit() else 'N/A',
                'mime_type': file.get('mimeType', 'unknown'),
                'is_excel': file.get('mimeType') == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            })
        
        logger.info(f"📋 Listados {len(formatted_files)} arquivos na pasta {folder_name or 'raiz'}")
        return formatted_files
        
    except Exception as e:
        logger.error(f"❌ Erro ao listar arquivos: {e}")
        return []

def delete_old_files(service=None, days_old=30, folder_name="Webhooks_Reports", dry_run=False):
    """
    Deletar arquivos antigos para economizar espaço
    
    Args:
        service: Serviço do Google Drive (opcional)
        days_old: Idade em dias para considerar arquivo como antigo
        folder_name: Nome da pasta para buscar
        dry_run: Se True, apenas simula a deleção sem executar
    
    Returns:
        dict: Resultado da operação
    """
    try:
        if not service:
            service = get_drive_service()
        
        # Calcular data de corte
        cutoff_date = datetime.now() - timedelta(days=days_old)
        cutoff_str = cutoff_date.isoformat() + 'Z'
        
        # Encontrar pasta específica
        folder_id = None
        if folder_name:
            folder_id = create_folder_if_not_exists(service, folder_name)
        
        # Construir query para arquivos antigos
        if folder_id:
            query = f"'{folder_id}' in parents and modifiedTime < '{cutoff_str}' and trashed=false"
        else:
            query = f"name contains 'webhook' and modifiedTime < '{cutoff_str}' and trashed=false"
        
        response = service.files().list(
            q=query,
            fields="files(id, name, modifiedTime, size)",
            orderBy="modifiedTime asc"
        ).execute()
        
        files = response.get('files', [])
        
        if dry_run:
            logger.info(f"🔍 DRY RUN: {len(files)} arquivos seriam deletados (mais antigos que {days_old} dias)")
            return {
                "success": True,
                "dry_run": True,
                "files_to_delete": len(files),
                "cutoff_date": cutoff_date.isoformat(),
                "files": [{"name": f['name'], "modified": f['modifiedTime']} for f in files]
            }
        
        # Deletar arquivos
        deleted_files = []
        failed_deletions = []
        
        for file in files:
            try:
                service.files().delete(fileId=file['id']).execute()
                deleted_files.append({
                    "name": file['name'],
                    "modified": file['modifiedTime'],
                    "size": file.get('size', 'N/A')
                })
                logger.info(f"🗑️ Arquivo deletado: {file['name']}")
            except Exception as delete_error:
                failed_deletions.append({
                    "name": file['name'],
                    "error": str(delete_error)
                })
                logger.error(f"❌ Erro ao deletar {file['name']}: {delete_error}")
        
        result = {
            "success": True,
            "deleted_count": len(deleted_files),
            "failed_count": len(failed_deletions),
            "cutoff_days": days_old,
            "cutoff_date": cutoff_date.isoformat(),
            "deleted_files": deleted_files,
            "failed_deletions": failed_deletions
        }
        
        logger.info(f"🧹 Limpeza concluída: {len(deleted_files)} arquivos deletados, {len(failed_deletions)} falhas")
        return result
        
    except Exception as e:
        error_msg = f"Erro na limpeza de arquivos: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

def get_drive_usage(service=None):
    """
    Obter informações de uso do Google Drive
    
    Args:
        service: Serviço do Google Drive (opcional)
    
    Returns:
        dict: Informações de uso do Drive
    """
    try:
        if not service:
            service = get_drive_service()
        
        about = service.about().get(fields="storageQuota, user").execute()
        quota = about.get('storageQuota', {})
        user = about.get('user', {})
        
        # Converter valores para inteiros
        limit = int(quota.get('limit', 0))
        usage = int(quota.get('usage', 0))
        usage_in_drive = int(quota.get('usageInDrive', 0))
        usage_in_drive_trash = int(quota.get('usageInDriveTrash', 0))
        
        # Calcular disponível
        available = limit - usage if limit > 0 else 0
        
        result = {
            'user_email': user.get('emailAddress', 'N/A'),
            'user_name': user.get('displayName', 'N/A'),
            'limit_bytes': limit,
            'usage_bytes': usage,
            'usage_in_drive_bytes': usage_in_drive,
            'usage_in_drive_trash_bytes': usage_in_drive_trash,
            'available_bytes': available,
            'limit_gb': round(limit / (1024**3), 2),
            'usage_gb': round(usage / (1024**3), 2),
            'usage_in_drive_gb': round(usage_in_drive / (1024**3), 2),
            'usage_in_drive_trash_gb': round(usage_in_drive_trash / (1024**3), 2),
            'available_gb': round(available / (1024**3), 2),
            'usage_percentage': round((usage / limit) * 100, 1) if limit > 0 else 0
        }
        
        logger.info(f"📊 Uso do Drive: {result['usage_gb']} GB / {result['limit_gb']} GB ({result['usage_percentage']}%)")
        return result
        
    except Exception as e:
        logger.error(f"❌ Erro ao obter uso do Drive: {e}")
        return {"error": str(e)}

def format_bytes(bytes_size):
    """
    Formatar tamanho em bytes para formato legível
    
    Args:
        bytes_size: Tamanho em bytes
    
    Returns:
        str: Tamanho formatado
    """
    if not isinstance(bytes_size, (int, float)):
        return "0 B"
    
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.1f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.1f} PB"

def create_shared_link(service, file_id, role='reader', type_='anyone'):
    """
    Criar link compartilhado para um arquivo
    
    Args:
        service: Serviço do Google Drive
        file_id: ID do arquivo
        role: Papel do compartilhamento ('reader', 'writer', 'commenter')
        type_: Tipo de permissão ('user', 'group', 'domain', 'anyone')
    
    Returns:
        dict: Resultado da operação
    """
    try:
        # Criar permissão
        permission = {
            'type': type_,
            'role': role
        }
        
        service.permissions().create(
            fileId=file_id,
            body=permission
        ).execute()
        
        # Obter link do arquivo
        file_info = service.files().get(
            fileId=file_id,
            fields="webViewLink, webContentLink, name"
        ).execute()
        
        result = {
            "success": True,
            "file_name": file_info.get('name'),
            "view_link": file_info.get('webViewLink'),
            "download_link": file_info.get('webContentLink'),
            "permission_role": role,
            "permission_type": type_
        }
        
        logger.info(f"🔗 Link compartilhado criado para: {file_info.get('name')}")
        return result
        
    except Exception as e:
        error_msg = f"Erro ao criar link compartilhado: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}

# Função de conveniência para manter compatibilidade
def upload_file_to_drive(local_path, drive_filename=None, folder_name="Webhooks_Reports"):
    """
    Função de conveniência para upload simples
    
    Args:
        local_path: Caminho do arquivo local
        drive_filename: Nome personalizado para o arquivo no Drive
        folder_name: Nome da pasta no Drive
    
    Returns:
        bool: True se sucesso, False se erro
    """
    result = upload_or_replace_file(local_path, folder_name, drive_filename)
    return result.get("success", False)

# Função para backup automático com rotação
def create_backup_with_rotation(buffer, base_filename, max_backups=5, folder_name="Webhooks_Backups"):
    """
    Criar backup com rotação automática (manter apenas os N mais recentes)
    
    Args:
        buffer: Buffer com os dados
        base_filename: Nome base do arquivo (sem timestamp)
        max_backups: Número máximo de backups para manter
        folder_name: Nome da pasta para backups
    
    Returns:
        dict: Resultado da operação
    """
    try:
        service = get_drive_service()
        
        # Criar nome com timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename_with_timestamp = f"{base_filename}_{timestamp}.xlsx"
        
        # Upload do novo backup
        upload_result = upload_buffer_to_drive(buffer, filename_with_timestamp, folder_name)
        
        if not upload_result.get("success"):
            return upload_result
        
        # Listar backups existentes com o mesmo nome base
        all_files = list_webhook_files(service, limit=100, folder_name=folder_name)
        backup_files = [f for f in all_files if f['name'].startswith(base_filename)]
        
        # Ordenar por data de modificação (mais recente primeiro)
        backup_files.sort(key=lambda x: x['modified'], reverse=True)
        
        # Deletar backups excedentes
        deleted_backups = []
        if len(backup_files) > max_backups:
            files_to_delete = backup_files[max_backups:]
            
            for file_to_delete in files_to_delete:
                try:
                    service.files().delete(fileId=file_to_delete['id']).execute()
                    deleted_backups.append(file_to_delete['name'])
                    logger.info(f"🗑️ Backup antigo deletado: {file_to_delete['name']}")
                except Exception as e:
                    logger.error(f"❌ Erro ao deletar backup {file_to_delete['name']}: {e}")
        
        result = {
            "success": True,
            "backup_created": filename_with_timestamp,
            "backups_deleted": deleted_backups,
            "total_backups": min(len(backup_files), max_backups),
            "max_backups": max_backups
        }
        
        logger.info(f"🔄 Backup criado com rotação: {filename_with_timestamp}")
        return result
        
    except Exception as e:
        error_msg = f"Erro no backup com rotação: {str(e)}"
        logger.error(f"❌ {error_msg}")
        return {"success": False, "error": error_msg}