import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
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
                raw_data  # Já convertido para string acima
            ))
        conn.commit()
        print(f"✅ Evento salvo: {plataforma} - {tipo}")
    except Exception as e:
        print(f"❌ Erro ao inserir evento: {e}")
        print(f"Payload problemático: {payload}")  # Debug
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def exportar_csv(plataforma=None, start_date=None, end_date=None):
    """
    Exporta dados do banco para CSV
    """
    import csv
    from io import StringIO
    
    query = """
    SELECT 
        platform, event_type, webhook_id, customer_email, customer_name,
        customer_document, product_name, product_id, transaction_id,
        amount, currency, payment_method, status, commission_amount,
        affiliate_email, utm_source, utm_medium, sales_link,
        attendant_name, attendant_email, created_at
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
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            
            # Criar CSV
            output = StringIO()
            writer = csv.writer(output)
            
            # Header
            writer.writerow([desc[0] for desc in cursor.description])
            
            # Dados
            writer.writerows(cursor.fetchall())
            
            return output.getvalue()
            
    except Exception as e:
        print(f"Erro ao exportar CSV: {e}")
        raise
    finally:
        if conn:
            conn.close()