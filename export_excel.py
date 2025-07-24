import os
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify, send_file
from io import BytesIO
import logging
from cachetools import cached, TTLCache
import json
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from oauth2client.client import OAuth2Credentials
import tempfile

# Configurações iniciais
load_dotenv()
export_bp = Blueprint('export', __name__)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache para estatísticas (1 hora de duração)
stats_cache = TTLCache(maxsize=100, ttl=3600)

def get_db_connection():
    """Estabelece conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        logger.info("Conexão com o banco de dados estabelecida")
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

def get_google_drive_service():
    """
    Cria e retorna um serviço do Google Drive usando as credenciais
    """
    try:
        # Carrega as credenciais do arquivo
        with open('mycreds.txt', 'r') as f:
            creds_data = json.load(f)
        
        # Cria o objeto de credenciais
        credentials = OAuth2Credentials(
            access_token=creds_data['access_token'],
            client_id=creds_data['client_id'],
            client_secret=creds_data['client_secret'],
            refresh_token=creds_data['refresh_token'],
            token_expiry=creds_data['token_expiry'],
            token_uri=creds_data['token_uri'],
            user_agent=creds_data.get('user_agent'),
            revoke_uri=creds_data.get('revoke_uri'),
            id_token=creds_data.get('id_token'),
            token_response=creds_data.get('token_response'),
            scopes=creds_data.get('scopes', ['https://www.googleapis.com/auth/drive'])
        )
        
        # Constrói o serviço do Drive
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Serviço do Google Drive inicializado com sucesso")
        return service
        
    except Exception as e:
        logger.error(f"Erro ao inicializar serviço do Google Drive: {e}")
        raise

def upload_to_drive(file_content, filename, folder_id=None):
    """
    Faz upload de um arquivo para o Google Drive
    
    Args:
        file_content: BytesIO object com o conteúdo do arquivo
        filename: Nome do arquivo
        folder_id: ID da pasta no Drive (opcional)
    
    Returns:
        dict: Informações do arquivo enviado
    """
    try:
        service = get_google_drive_service()
        
        # Metadados do arquivo
        file_metadata = {
            'name': filename,
            'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        # Se especificada uma pasta, adiciona aos metadados
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Cria o objeto de media para upload
        media = MediaIoBaseUpload(
            file_content,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            resumable=True
        )
        
        # Executa o upload
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,webViewLink,webContentLink'
        ).execute()
        
        logger.info(f"Arquivo {filename} enviado para o Google Drive com ID: {file.get('id')}")
        
        return {
            'file_id': file.get('id'),
            'name': file.get('name'),
            'web_view_link': file.get('webViewLink'),
            'download_link': file.get('webContentLink')
        }
        
    except Exception as e:
        logger.error(f"Erro ao enviar arquivo para o Google Drive: {e}")
        raise

def create_drive_folder(folder_name, parent_folder_id=None):
    """
    Cria uma pasta no Google Drive
    
    Args:
        folder_name: Nome da pasta
        parent_folder_id: ID da pasta pai (opcional)
    
    Returns:
        str: ID da pasta criada
    """
    try:
        service = get_google_drive_service()
        
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        
        folder = service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        logger.info(f"Pasta {folder_name} criada no Google Drive com ID: {folder.get('id')}")
        return folder.get('id')
        
    except Exception as e:
        logger.error(f"Erro ao criar pasta no Google Drive: {e}")
        raise

def find_or_create_folder(folder_name, parent_folder_id=None):
    """
    Busca uma pasta no Google Drive ou a cria se não existir
    
    Args:
        folder_name: Nome da pasta
        parent_folder_id: ID da pasta pai (opcional)
    
    Returns:
        str: ID da pasta
    """
    try:
        service = get_google_drive_service()
        
        # Busca pela pasta existente
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        if parent_folder_id:
            query += f" and parents in '{parent_folder_id}'"
        
        results = service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            # Pasta encontrada
            folder_id = items[0]['id']
            logger.info(f"Pasta {folder_name} encontrada com ID: {folder_id}")
            return folder_id
        else:
            # Pasta não encontrada, criar nova
            return create_drive_folder(folder_name, parent_folder_id)
            
    except Exception as e:
        logger.error(f"Erro ao buscar/criar pasta no Google Drive: {e}")
        raise

def fetch_webhook_data(start_date=None, end_date=None, platform=None, include_raw_data=False):
    """
    Busca dados de webhooks com filtros opcionais
    """
    # Seleciona colunas base
    base_columns = """
        platform, event_type, webhook_id, customer_email, customer_name,
        customer_document, product_name, product_id, transaction_id,
        amount, currency, payment_method, status, commission_amount,
        affiliate_email, utm_source, utm_medium, sales_link,
        attendant_name, attendant_email, created_at
    """
    
    if include_raw_data:
        base_columns += ", raw_data"
    
    base_query = f"SELECT {base_columns} FROM webhooks WHERE 1=1"
    
    conditions = []
    params = []
    
    if start_date:
        conditions.append("created_at >= %s")
        params.append(start_date)
    
    if end_date:
        conditions.append("created_at <= %s")
        params.append(end_date)
    
    if platform:
        conditions.append("platform = %s")
        params.append(platform)
    
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    base_query += " ORDER BY created_at DESC"
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(base_query, params)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            
            logger.info(f"Retornados {len(results)} registros do banco de dados")
            return columns, results
            
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_excel_report(data, columns, include_raw_data=False):
    try:
        df = pd.DataFrame(data, columns=columns)

        # Remove timezone de created_at para evitar erro no Excel
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)

        # Processar raw_data se incluído
        if include_raw_data and 'raw_data' in df.columns:
            df['raw_data'] = df['raw_data'].apply(lambda x: json.loads(x) if x else {})
            raw_data_df = pd.json_normalize(df['raw_data'])

            columns_to_add = [col for col in raw_data_df.columns if col not in df.columns]
            raw_data_df = raw_data_df[columns_to_add]
            df = pd.concat([df.drop('raw_data', axis=1), raw_data_df], axis=1)

        output = BytesIO()

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if 'platform' in df.columns:
                for platform_name, platform_df in df.groupby('platform'):
                    sheet_name = platform_name[:31] or 'Plataforma'  # Excel limita a 31 caracteres
                    platform_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    format_excel(writer, platform_df, sheet_name)
            else:
                df.to_excel(writer, sheet_name='Webhooks', index=False)
                format_excel(writer, df, 'Webhooks')

        output.seek(0)
        return output

    except Exception as e:
        logger.error(f"Erro ao criar relatório Excel: {e}")
        raise

def create_platform_summary(df):
    """Cria resumo por plataforma"""
    platform_summary = df.groupby('platform').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean'],
        'commission_amount': 'sum'
    }).round(2)
    
    platform_summary.columns = ['Total_Transactions', 'Total_Amount', 'Average_Amount', 'Total_Commission']
    return platform_summary.reset_index()

def create_daily_sales(df):
    """Cria análise de vendas diárias"""
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date'] = df['created_at'].dt.date
    
    daily_sales = df.groupby('date').agg({
        'transaction_id': 'count',
        'amount': 'sum',
        'commission_amount': 'sum'
    }).round(2)
    
    daily_sales.columns = ['Daily_Transactions', 'Daily_Revenue', 'Daily_Commission']
    return daily_sales.reset_index()

def create_top_products(df):
    """Cria ranking de produtos"""
    product_performance = df.groupby('product_name').agg({
        'transaction_id': 'count',
        'amount': 'sum'
    }).round(2)
    
    product_performance.columns = ['Sales_Count', 'Total_Revenue']
    return product_performance.sort_values('Total_Revenue', ascending=False).reset_index()

def create_affiliate_performance(df):
    """Cria performance de afiliados"""
    affiliate_data = df[df['affiliate_email'].notna()]
    if affiliate_data.empty:
        return None
        
    affiliate_performance = affiliate_data.groupby('affiliate_email').agg({
        'transaction_id': 'count',
        'amount': 'sum',
        'commission_amount': 'sum'
    }).round(2)
    
    affiliate_performance.columns = ['Sales_Count', 'Total_Sales', 'Total_Commission']
    return affiliate_performance.sort_values('Total_Commission', ascending=False).reset_index()

def format_excel(writer, df, sheet_name):
    """Aplica formatação profissional ao Excel por sheet"""
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]

    # Formatações
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#4472C4',
        'font_color': 'white',
        'border': 1
    })

    money_format = workbook.add_format({
        'num_format': 'R$ #,##0.00',
        'border': 1
    })

    date_format = workbook.add_format({
        'num_format': 'dd/mm/yyyy',
        'border': 1
    })

    # Formatar cabeçalho
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)

    # Auto-ajustar colunas
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).map(len).max(),
            len(str(col))
        ) + 2
        worksheet.set_column(i, i, max_len)

    # Formatar colunas especiais
    if 'amount' in df.columns:
        col_idx = df.columns.get_loc('amount')
        worksheet.set_column(col_idx, col_idx, 15, money_format)

    if 'created_at' in df.columns:
        col_idx = df.columns.get_loc('created_at')
        worksheet.set_column(col_idx, col_idx, 15, date_format)


@export_bp.route('/excel', methods=['GET'])
def export_to_excel():
    """
    Endpoint para exportação manual com filtros
    """
    # Obter parâmetros
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    platform = request.args.get('platform')
    include_raw = request.args.get('include_raw', 'false').lower() == 'true'
    
    # Validar datas
    try:
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        logger.warning("Formato de data inválido recebido")
        return jsonify({"error": "Formato de data inválido. Use YYYY-MM-DD"}), 400
    
    try:
        logger.info(f"Iniciando exportação para {platform} entre {start_date} e {end_date}")
        
        # Buscar dados
        columns, data = fetch_webhook_data(start_date, end_date, platform, include_raw)
        
        if not data:
            logger.info("Nenhum dado encontrado para os critérios especificados")
            return jsonify({"message": "Nenhum dado encontrado"}), 204
        
        # Criar relatório
        excel_file = create_excel_report(data, columns, include_raw)
        
        # Nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'relatorio_webhooks_{timestamp}.xlsx'
        
        logger.info(f"Relatório {filename} gerado com sucesso")
        
        # Enviar arquivo
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Falha na exportação: {str(e)}")
        return jsonify({"error": f"Falha ao gerar relatório: {str(e)}"}), 500

@export_bp.route('/excel/drive', methods=['GET'])
def export_to_drive():
    """
    Endpoint para exportação direta para Google Drive
    """
    # Obter parâmetros
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    platform = request.args.get('platform')
    include_raw = request.args.get('include_raw', 'false').lower() == 'true'
    folder_name = request.args.get('folder_name', 'Relatórios Webhooks')
    
    # Validar datas
    try:
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        logger.warning("Formato de data inválido recebido")
        return jsonify({"error": "Formato de data inválido. Use YYYY-MM-DD"}), 400
    
    try:
        logger.info(f"Iniciando exportação para Google Drive - {platform} entre {start_date} e {end_date}")
        
        # Buscar dados
        columns, data = fetch_webhook_data(start_date, end_date, platform, include_raw)
        
        if not data:
            logger.info("Nenhum dado encontrado para os critérios especificados")
            return jsonify({"message": "Nenhum dado encontrado"}), 204
        
        # Criar relatório
        excel_file = create_excel_report(data, columns, include_raw)
        
        # Nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'relatorio_webhooks_{timestamp}.xlsx'
        
        # Buscar ou criar pasta no Google Drive
        folder_id = find_or_create_folder(folder_name)
        
        # Fazer upload para o Drive
        drive_file = upload_to_drive(excel_file, filename, folder_id)
        
        logger.info(f"Relatório {filename} enviado para Google Drive com sucesso")
        
        return jsonify({
            "message": "Relatório enviado para Google Drive com sucesso",
            "file_info": drive_file,
            "records_processed": len(data),
            "folder_name": folder_name
        }), 200
        
    except Exception as e:
        logger.error(f"Falha na exportação para Google Drive: {str(e)}")
        return jsonify({"error": f"Falha ao enviar para Google Drive: {str(e)}"}), 500

@export_bp.route('/excel/scheduled', methods=['POST'])
def schedule_export():
    """
    Endpoint para exportações agendadas (atualização horária) com envio para Google Drive
    """
    data = request.json
    hours_back = data.get('hours_back', 1)  # Padrão: 1 hora atrás
    send_to_drive = data.get('send_to_drive', False)
    folder_name = data.get('folder_name', 'Relatórios Agendados')
    
    # Calcular período com base em horas
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=hours_back)
    
    try:
        logger.info(f"Iniciando exportação agendada para últimas {hours_back} horas")
        
        # Buscar dados
        columns, webhook_data = fetch_webhook_data(start_date, end_date)
        
        if not webhook_data:
            logger.info("Nenhum dado encontrado para o período especificado")
            return jsonify({"message": "Nenhum dado encontrado"}), 204
        
        # Criar relatório
        excel_file = create_excel_report(webhook_data, columns)
        
        response_data = {
            "message": "Exportação agendada concluída",
            "period": f"{start_date.strftime('%Y-%m-%d %H:%M:%S')} a {end_date.strftime('%Y-%m-%d %H:%M:%S')}",
            "records_processed": len(webhook_data),
            "time_window_hours": hours_back
        }
        
        # Se solicitado, enviar para Google Drive
        if send_to_drive:
            try:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'relatorio_agendado_{timestamp}.xlsx'
                
                # Buscar ou criar pasta no Google Drive
                folder_id = find_or_create_folder(folder_name)
                
                # Fazer upload para o Drive
                drive_file = upload_to_drive(excel_file, filename, folder_id)
                
                response_data["drive_upload"] = {
                    "status": "success",
                    "file_info": drive_file,
                    "folder_name": folder_name
                }
                
                logger.info(f"Relatório agendado enviado para Google Drive: {filename}")
                
            except Exception as drive_error:
                logger.error(f"Erro ao enviar para Google Drive: {drive_error}")
                response_data["drive_upload"] = {
                    "status": "error",
                    "error": str(drive_error)
                }
        
        logger.info(f"Exportação agendada concluída com {len(webhook_data)} registros processados")
        
        return jsonify(response_data), 200
        
    except Exception as e:
        logger.error(f"Falha na exportação agendada: {str(e)}")
        return jsonify({"error": f"Falha na exportação agendada: {str(e)}"}), 500

@export_bp.route('/stats', methods=['GET'])
@cached(stats_cache)
def get_quick_stats():
    """
    Endpoint para estatísticas rápidas (com cache)
    """
    try:
        logger.info("Gerando estatísticas rápidas")
        
        # Período padrão: últimas 24 horas
        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)
        
        columns, data = fetch_webhook_data(start_date, end_date)
        
        if not data:
            logger.info("Nenhum dado encontrado para geração de estatísticas")
            return jsonify({"message": "Nenhum dado encontrado"}), 204
        
        df = pd.DataFrame(data, columns=columns)
        
        stats = {
            "total_transactions": len(df),
            "total_revenue": float(df['amount'].sum()) if 'amount' in df.columns else 0,
            "total_commission": float(df['commission_amount'].sum()) if 'commission_amount' in df.columns else 0,
            "platforms": df['platform'].nunique() if 'platform' in df.columns else 0,
            "top_products": get_top_items(df, 'product_name', 'amount', 5),
            "top_affiliates": get_top_items(df, 'affiliate_email', 'commission_amount', 5),
            "time_window_hours": 24,
            "date_range": {
                "start": start_date.strftime('%Y-%m-%d %H:%M:%S'),
                "end": end_date.strftime('%Y-%m-%d %H:%M:%S')
            }
        }
        
        logger.info("Estatísticas geradas com sucesso")
        return jsonify(stats), 200
        
    except Exception as e:
        logger.error(f"Erro ao gerar estatísticas: {str(e)}")
        return jsonify({"error": f"Falha ao obter estatísticas: {str(e)}"}), 500

def get_top_items(df, group_col, value_col, top_n=5):
    """
    Helper para obter os itens mais relevantes
    """
    if group_col not in df.columns or value_col not in df.columns:
        return []
    
    try:
        top_items = df.groupby(group_col)[value_col].sum().nlargest(top_n)
        return [
            {"name": name, "value": float(value)}
            for name, value in top_items.items()
        ]
    except:
        return []

@export_bp.route('/drive/folders', methods=['GET'])
def list_drive_folders():
    """
    Endpoint para listar pastas no Google Drive
    """
    try:
        service = get_google_drive_service()
        
        # Buscar pastas
        results = service.files().list(
            q="mimeType='application/vnd.google-apps.folder'",
            fields="files(id, name, parents)"
        ).execute()
        
        folders = results.get('files', [])
        
        return jsonify({
            "folders": [
                {
                    "id": folder['id'],
                    "name": folder['name'],
                    "parents": folder.get('parents', [])
                }
                for folder in folders
            ]
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao listar pastas do Google Drive: {str(e)}")
        return jsonify({"error": f"Falha ao listar pastas: {str(e)}"}), 500

@export_bp.route('/drive/files', methods=['GET'])
def list_drive_files():
    """
    Endpoint para listar arquivos Excel no Google Drive
    """
    try:
        service = get_google_drive_service()
        folder_id = request.args.get('folder_id')
        
        # Construir query
        query = "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
        if folder_id:
            query += f" and parents in '{folder_id}'"
        
        # Buscar arquivos
        results = service.files().list(
            q=query,
            fields="files(id, name, createdTime, modifiedTime, webViewLink, size)",
            orderBy="modifiedTime desc"
        ).execute()
        
        files = results.get('files', [])
        
        return jsonify({
            "files": [
                {
                    "id": file['id'],
                    "name": file['name'],
                    "created_time": file['createdTime'],
                    "modified_time": file['modifiedTime'],
                    "web_view_link": file['webViewLink'],
                    "size": file.get('size', 0)
                }
                for file in files
            ]
        }), 200
        
    except Exception as e:
        logger.error(f"Erro ao listar arquivos do Google Drive: {str(e)}")
        return jsonify({"error": f"Falha ao listar arquivos: {str(e)}"}), 500