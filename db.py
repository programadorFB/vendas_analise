import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def salvar_evento(plataforma, tipo, payload):
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
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (
                plataforma, tipo,
                payload.get('webhook_id'),
                payload.get('customer_email'),
                payload.get('customer_name'),
                payload.get('customer_document'),
                payload.get('product_name'),
                payload.get('product_id'),
                payload.get('transaction_id'),
                float(payload.get('amount', 0)) if payload.get('amount') else None,
                payload.get('currency'),
                payload.get('payment_method'),
                payload.get('status'),
                float(payload.get('commission_amount', 0)) if payload.get('commission_amount') else None,
                payload.get('affiliate_email'),
                payload.get('utm_source'),
                payload.get('utm_medium'),
                payload.get('sales_link'),
                payload.get('attendant_name'),
                payload.get('attendant_email'),
                json.dumps(payload)
            ))
        conn.commit()
    except Exception as e:
        print(f"Erro ao inserir evento: {e}")
        raise  # Re-lança a exceção para ser tratada no endpoint
    finally:
        if conn:
            conn.close()