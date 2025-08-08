import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from io import StringIO, BytesIO
import csv
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from functools import wraps
import time

load_dotenv()
logger = logging.getLogger(__name__)

def retry_on_failure(max_retries=3, delay=1):
    """Decorator para retry em operações de banco"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise e
                    logger.warning(f"Tentativa {attempt + 1} falhou: {e}. Tentando novamente em {delay}s...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

@retry_on_failure()
def get_db_connection():
    """Obtém conexão com o banco de dados com retry automático"""
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def safe_float(value, default=None):
    """Converte valor para float com segurança"""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_str(value):
    """Converte valor para string com segurança"""
    if value is None:
        return None
    return str(value)

def safe_int(value, default=0):
    """Converte valor para int com segurança"""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

@retry_on_failure()
def salvar_evento(plataforma, tipo, payload):
    """
    Salva evento no banco de dados com tratamento adequado de tipos
    """
    insert_sql = sql.SQL("""
    INSERT INTO webhooks (
        platform, event_type, webhook_id, customer_email, customer_name,
        customer_document, product_name, product_id, transaction_id,
        amount, currency, payment_method, status, commission_amount,
        affiliate_email, utm_source, utm_medium, sales_link, 
        attendant_name, attendant_email, raw_data, created_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, NOW()
    )
    """)
    
    conn = None
    try:
        # Garante que raw_data seja sempre uma string
        if 'raw_data' in payload and isinstance(payload['raw_data'], str):
            raw_data = payload['raw_data']
        else:
            # Se raw_data não existir ou não for string, converte todo o payload
            raw_data = json.dumps(payload, ensure_ascii=False, default=str)
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (
                plataforma, 
                tipo,
                safe_str(payload.get('webhook_id')),
                safe_str(payload.get('customer_email')),
                safe_str(payload.get('customer_name')),
                safe_str(payload.get('customer_document')),
                safe_str(payload.get('product_name')),
                safe_str(payload.get('product_id')),
                safe_str(payload.get('transaction_id')),
                safe_float(payload.get('amount')),
                safe_str(payload.get('currency')),
                safe_str(payload.get('payment_method')),
                safe_str(payload.get('status')),
                safe_float(payload.get('commission_amount')),
                safe_str(payload.get('affiliate_email')),
                safe_str(payload.get('utm_source')),
                safe_str(payload.get('utm_medium')),
                safe_str(payload.get('sales_link')),
                safe_str(payload.get('attendant_name')),
                safe_str(payload.get('attendant_email')),
                raw_data
            ))
        conn.commit()
        logger.info(f"✅ Evento salvo: {plataforma} - {tipo}")
    except Exception as e:
        logger.error(f"❌ Erro ao inserir evento: {e}")
        logger.error(f"Payload problemático: {payload}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def obter_estatisticas_gerais(start_date=None, end_date=None, platform=None):
    """
    Obtém estatísticas gerais do sistema
    """
    query = """
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT platform) as platforms_count,
        COUNT(DISTINCT customer_email) as unique_customers,
        COUNT(DISTINCT product_name) as unique_products,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_revenue,
        AVG(CASE WHEN amount > 0 THEN amount ELSE NULL END) as avg_transaction,
        MIN(created_at) as first_event,
        MAX(created_at) as last_event
    FROM webhooks
    WHERE 1=1
    """
    
    params = []
    if start_date:
        query += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND created_at <= %s"
        params.append(end_date)
    if platform:
        query += " AND platform = %s"
        params.append(platform)
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if result:
                return {
                    'total_events': result[0],
                    'platforms_count': result[1],
                    'unique_customers': result[2],
                    'unique_products': result[3],
                    'total_revenue': float(result[4] or 0),
                    'avg_transaction': float(result[5] or 0),
                    'first_event': result[6],
                    'last_event': result[7]
                }
            return None
            
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas gerais: {e}")
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def analisar_performance_produtos(start_date=None, end_date=None, limit=20):
    """
    Análise detalhada de performance de produtos
    """
    query = """
    SELECT 
        product_name,
        COUNT(*) as total_events,
        COUNT(CASE WHEN event_type IN ('SALE_APPROVED', 'NewSale', 'purchase_approved') THEN 1 END) as sales,
        COUNT(CASE WHEN event_type IN ('ABANDONED_CART', 'CanceledSale', 'checkout_abandonment') THEN 1 END) as abandons,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as revenue,
        AVG(CASE WHEN amount > 0 THEN amount ELSE NULL END) as avg_price,
        COUNT(DISTINCT customer_email) as unique_customers,
        COUNT(DISTINCT platform) as platforms_used
    FROM webhooks
    WHERE product_name IS NOT NULL AND product_name != ''
    """
    
    params = []
    if start_date:
        query += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND created_at <= %s"
        params.append(end_date)
    
    query += """
    GROUP BY product_name
    ORDER BY revenue DESC
    LIMIT %s
    """
    params.append(limit)
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            products_analysis = []
            for row in results:
                conversion_rate = (row[2] / row[1] * 100) if row[1] > 0 else 0
                abandon_rate = (row[3] / row[1] * 100) if row[1] > 0 else 0
                
                products_analysis.append({
                    'product_name': row[0],
                    'total_events': row[1],
                    'sales': row[2],
                    'abandons': row[3],
                    'revenue': float(row[4] or 0),
                    'avg_price': float(row[5] or 0),
                    'unique_customers': row[6],
                    'platforms_used': row[7],
                    'conversion_rate': round(conversion_rate, 2),
                    'abandon_rate': round(abandon_rate, 2)
                })
            
            return products_analysis
            
    except Exception as e:
        logger.error(f"Erro na análise de performance de produtos: {e}")
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def analisar_cohort_clientes(start_date=None, end_date=None):
    """
    Análise de cohort de clientes para entender retenção
    """
    query = """
    WITH customer_first_purchase AS (
        SELECT 
            customer_email,
            MIN(DATE(created_at)) as first_purchase_date,
            COUNT(*) as total_purchases,
            SUM(amount) as total_spent
        FROM webhooks
        WHERE customer_email IS NOT NULL 
        AND event_type IN ('SALE_APPROVED', 'NewSale', 'purchase_approved')
        AND amount > 0
    """
    
    params = []
    if start_date:
        query += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND created_at <= %s"
        params.append(end_date)
    
    query += """
        GROUP BY customer_email
    ),
    cohort_data AS (
        SELECT 
            first_purchase_date,
            COUNT(*) as customers_count,
            SUM(total_purchases) as total_purchases,
            AVG(total_purchases) as avg_purchases_per_customer,
            SUM(total_spent) as total_revenue,
            AVG(total_spent) as avg_ltv
        FROM customer_first_purchase
        GROUP BY first_purchase_date
        ORDER BY first_purchase_date
    )
    SELECT * FROM cohort_data
    """
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            cohort_analysis = []
            for row in results:
                cohort_analysis.append({
                    'date': row[0].strftime('%Y-%m-%d'),
                    'customers_count': row[1],
                    'total_purchases': row[2],
                    'avg_purchases_per_customer': float(row[3] or 0),
                    'total_revenue': float(row[4] or 0),
                    'avg_ltv': float(row[5] or 0)
                })
            
            return cohort_analysis
            
    except Exception as e:
        logger.error(f"Erro na análise de cohort: {e}")
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def detectar_anomalias_vendas():
    """
    Detecta anomalias nas vendas usando estatísticas básicas
    """
    query = """
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as sales_count,
        SUM(amount) as daily_revenue
    FROM webhooks
    WHERE event_type IN ('SALE_APPROVED', 'NewSale', 'purchase_approved')
    AND created_at >= CURRENT_DATE - INTERVAL '90 days'
    AND amount > 0
    GROUP BY DATE(created_at)
    ORDER BY date
    """
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            
            if len(results) < 7:  # Precisa de pelo menos 7 dias de dados
                return []
            
            # Converter para DataFrame para análise
            df = pd.DataFrame(results, columns=['date', 'sales_count', 'daily_revenue'])
            df['daily_revenue'] = df['daily_revenue'].astype(float)
            
            # Calcular estatísticas
            revenue_mean = df['daily_revenue'].mean()
            revenue_std = df['daily_revenue'].std()
            sales_mean = df['sales_count'].mean()
            sales_std = df['sales_count'].std()
            
            # Detectar anomalias (valores > 2 desvios padrão)
            anomalies = []
            for _, row in df.iterrows():
                revenue_z_score = abs((row['daily_revenue'] - revenue_mean) / revenue_std) if revenue_std > 0 else 0
                sales_z_score = abs((row['sales_count'] - sales_mean) / sales_std) if sales_std > 0 else 0
                
                if revenue_z_score > 2 or sales_z_score > 2:
                    anomaly_type = []
                    if revenue_z_score > 2:
                        anomaly_type.append('revenue')
                    if sales_z_score > 2:
                        anomaly_type.append('sales')
                    
                    anomalies.append({
                        'date': row['date'].strftime('%Y-%m-%d'),
                        'sales_count': row['sales_count'],
                        'daily_revenue': row['daily_revenue'],
                        'revenue_z_score': round(revenue_z_score, 2),
                        'sales_z_score': round(sales_z_score, 2),
                        'anomaly_type': anomaly_type,
                        'severity': 'high' if max(revenue_z_score, sales_z_score) > 3 else 'medium'
                    })
            
            return anomalies
            
    except Exception as e:
        logger.error(f"Erro na detecção de anomalias: {e}")
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def exportar_csv(plataforma=None, start_date=None, end_date=None):
    """
    Exporta dados do banco para CSV com análises adicionais
    """
    query = """
    SELECT 
        platform, event_type, webhook_id, customer_email, customer_name,
        customer_document, product_name, product_id, transaction_id,
        amount, currency, payment_method, status, commission_amount,
        affiliate_email, utm_source, utm_medium, sales_link,
        attendant_name, attendant_email, created_at,
        EXTRACT(HOUR FROM created_at) as hour_of_day,
        EXTRACT(DOW FROM created_at) as day_of_week,
        CASE 
            WHEN event_type IN ('SALE_APPROVED', 'NewSale', 'purchase_approved') THEN 'conversion'
            WHEN event_type IN ('ABANDONED_CART', 'CanceledSale', 'checkout_abandonment') THEN 'abandonment'
            ELSE 'other'
        END as event_category
    FROM webhooks
    WHERE 1=1
    """
    
    params = []
    if plataforma:
        query += " AND platform = %s"
        params.append(plataforma)
    if start_date:
        query += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND created_at <= %s"
        params.append(end_date)
    
    query += " ORDER BY created_at DESC"
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            
            # Criar CSV
            output = StringIO()
            writer = csv.writer(output)
            
            # Header com colunas adicionais
            headers = [desc[0] for desc in cursor.description]
            writer.writerow(headers)
            
            # Dados
            writer.writerows(cursor.fetchall())
            
            return output.getvalue()
            
    except Exception as e:
        logger.error(f"Erro ao exportar CSV: {e}")
        raise
    finally:
        if conn:
            conn.close()

def exportar_xlsx(plataforma=None, start_date=None, end_date=None):
    """
    Exporta dados do banco para arquivo Excel (XLSX) com múltiplas abas e análises
    """
    query_main = """
    SELECT 
        id, platform, event_type, created_at, customer_email, customer_name,
        product_name, transaction_id, amount, currency, payment_method, 
        status, utm_source, utm_medium, raw_data
    FROM webhooks
    WHERE 1=1
    """
    
    params = []
    if plataforma:
        query_main += " AND platform = %s"
        params.append(plataforma)
    if start_date:
        query_main += " AND created_at >= %s"
        params.append(start_date)
    if end_date:
        query_main += " AND created_at <= %s"
        params.append(end_date)
    
    query_main += " ORDER BY created_at DESC"
    
    conn = None
    try:
        conn = get_db_connection()
        
        # Obter dados principais
        with conn.cursor() as cursor:
            cursor.execute(query_main, params)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        
        # Converter para DataFrame
        df_main = pd.DataFrame(rows, columns=columns)
        
        # Processar o raw_data (JSON) para colunas separadas
        if 'raw_data' in df_main.columns and len(df_main) > 0:
            df_main['raw_data'] = df_main['raw_data'].apply(
                lambda x: json.loads(x) if x and isinstance(x, str) else {}
            )
            raw_data_df = pd.json_normalize(df_main['raw_data'])
            df_main = pd.concat([df_main.drop('raw_data', axis=1), raw_data_df], axis=1)
        
        # Obter análises adicionais
        stats = obter_estatisticas_gerais(start_date, end_date, plataforma)
        products_analysis = analisar_performance_produtos(start_date, end_date)
        cohort_analysis = analisar_cohort_clientes(start_date, end_date)
        
        # Criar DataFrames para as análises
        df_stats = pd.DataFrame([stats]) if stats else pd.DataFrame()
        df_products = pd.DataFrame(products_analysis)
        df_cohort = pd.DataFrame(cohort_analysis)
        
        # Criar arquivo Excel em memória
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Aba principal com dados
            df_main.to_excel(writer, sheet_name='Dados Principais', index=False)
            
            # Aba com estatísticas gerais
            if not df_stats.empty:
                df_stats.to_excel(writer, sheet_name='Estatísticas Gerais', index=False)
            
            # Aba com análise de produtos
            if not df_products.empty:
                df_products.to_excel(writer, sheet_name='Análise de Produtos', index=False)
            
            # Aba com análise de cohort
            if not df_cohort.empty:
                df_cohort.to_excel(writer, sheet_name='Análise de Cohort', index=False)
            
            # Acessar workbook para formatação
            workbook = writer.book
            
            # Formatos
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
            
            percent_format = workbook.add_format({
                'num_format': '0.00%',
                'border': 1
            })
            
            # Formatação para cada aba
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                
                # Aplicar formato de cabeçalho
                for col_num in range(len(writer.sheets[sheet_name].table.columns)):
                    worksheet.write(0, col_num, writer.sheets[sheet_name].table.columns[col_num], header_format)
                
                # Auto-ajustar largura das colunas
                for i, col in enumerate(writer.sheets[sheet_name].table.columns):
                    max_len = max(
                        len(str(col)),
                        10  # Largura mínima
                    )
                    worksheet.set_column(i, i, min(max_len + 2, 50))  # Máximo de 50 caracteres
                
                # Adicionar filtros
                if len(writer.sheets[sheet_name].table.index) > 0:
                    worksheet.autofilter(0, 0, len(writer.sheets[sheet_name].table.index), 
                                       len(writer.sheets[sheet_name].table.columns) - 1)
            
            # Adicionar metadados
            workbook.set_properties({
                'title': f'Relatório Completo de Webhooks - {datetime.now().strftime("%Y-%m-%d")}',
                'subject': 'Análise Completa de Transações e Performance',
                'author': 'Sistema de Analytics Pro',
                'company': 'Analytics Dashboard',
                'comments': f'Período: {start_date} a {end_date}. Plataforma: {plataforma or "Todas"}'
            })
        
        output.seek(0)
        return output
        
    except Exception as e:
        logger.error(f"Erro ao exportar XLSX: {e}")
        raise
    finally:
        if conn:
            conn.close()

@retry_on_failure()
def limpar_dados_antigos(dias_retencao=365):
    """
    Remove dados antigos do banco para manter performance
    """
    cutoff_date = datetime.now() - timedelta(days=dias_retencao)
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Contar registros a serem removidos
            cursor.execute("SELECT COUNT(*) FROM webhooks WHERE created_at < %s", (cutoff_date,))
            count_to_delete = cursor.fetchone()[0]
            
            if count_to_delete > 0:
                # Fazer backup dos dados antes de remover (opcional)
                backup_query = """
                SELECT * FROM webhooks 
                WHERE created_at < %s 
                ORDER BY created_at
                """
                cursor.execute(backup_query, (cutoff_date,))
                
                # Remover dados antigos
                cursor.execute("DELETE FROM webhooks WHERE created_at < %s", (cutoff_date,))
                conn.commit()
                
                logger.info(f"✅ Removidos {count_to_delete} registros anteriores a {cutoff_date.strftime('%Y-%m-%d')}")
                return count_to_delete
            else:
                logger.info("ℹ️ Nenhum registro antigo encontrado para remoção")
                return 0
                
    except Exception as e:
        logger.error(f"Erro ao limpar dados antigos: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def otimizar_banco():
    """
    Executa operações de otimização no banco de dados
    """
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Atualizar estatísticas das tabelas
            cursor.execute("ANALYZE webhooks;")
            
            # Reindexar se necessário
            cursor.execute("REINDEX TABLE webhooks;")
            
            conn.commit()
            logger.info("✅ Otimização do banco concluída")
            
    except Exception as e:
        logger.error(f"Erro na otimização do banco: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()