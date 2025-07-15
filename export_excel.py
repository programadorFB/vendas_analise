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

            # Filtra colunas que já existem para evitar duplicidade
            columns_to_add = [col for col in raw_data_df.columns if col not in df.columns]

            # Seleciona apenas as colunas novas
            raw_data_df = raw_data_df[columns_to_add]

            # Concatena com o df original (sem raw_data)
            df = pd.concat([df.drop('raw_data', axis=1), raw_data_df], axis=1)

        
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Sheet 1: Raw Data
            df.to_excel(writer, sheet_name='Raw Data', index=False)
            
            # Sheet 2: Platform Summary
            if not df.empty:
                platform_summary = create_platform_summary(df)
                platform_summary.to_excel(writer, sheet_name='Platform Summary', index=False)
            
            # Sheet 3: Daily Sales
            if not df.empty and 'created_at' in df.columns:
                daily_sales = create_daily_sales(df)
                daily_sales.to_excel(writer, sheet_name='Daily Sales', index=False)
            
            # Sheet 4: Top Products
            if not df.empty and 'product_name' in df.columns:
                top_products = create_top_products(df)
                top_products.to_excel(writer, sheet_name='Top Products', index=False)
            
            # Sheet 5: Affiliate Performance
            if not df.empty and 'affiliate_email' in df.columns:
                affiliate_perf = create_affiliate_performance(df)
                if affiliate_perf is not None:
                    affiliate_perf.to_excel(writer, sheet_name='Affiliate Performance', index=False)
            
            # Formatação do Excel
            format_excel(writer, df)
        
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

def format_excel(writer, df):
    """Aplica formatação profissional ao Excel"""
    workbook = writer.book
    
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
    
    # Aplicar formatação a cada planilha
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        
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

@export_bp.route('/excel/scheduled', methods=['POST'])
def schedule_export():
    """
    Endpoint para exportações agendadas (atualização horária)
    """
    data = request.json
    hours_back = data.get('hours_back', 1)  # Padrão: 1 hora atrás
    email_to = data.get('email_to')
    
    # Calcular período com base em horas
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=hours_back)
    
    try:
        logger.info(f"Iniciando exportação agendada para últimas {hours_back} horas")
        
        # Buscar dados
        columns, data = fetch_webhook_data(start_date, end_date)
        
        if not data:
            logger.info("Nenhum dado encontrado para o período especificado")
            return jsonify({"message": "Nenhum dado encontrado"}), 204
        
        # Criar relatório
        excel_file = create_excel_report(data, columns)
        
        logger.info(f"Exportação agendada concluída com {len(data)} registros processados")
        
        return jsonify({
            "message": "Exportação agendada concluída",
            "period": f"{start_date.strftime('%Y-%m-%d %H:%M:%S')} a {end_date.strftime('%Y-%m-%d %H:%M:%S')}",
            "records_processed": len(data),
            "time_window_hours": hours_back,
            "next_steps": "Relatório pronto para distribuição"
        }), 200
        
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