import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from io import StringIO, BytesIO
import csv
import pandas as pd
from datetime import datetime

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
    
    conn = None
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

def exportar_xlsx(plataforma=None, start_date=None, end_date=None):
    """
    Exporta dados do banco para arquivo Excel (XLSX), focando nos dados do raw_data (JSON)
    Retorna um objeto BytesIO com o arquivo Excel em memória
    """
    query = """
    SELECT 
        id, platform, event_type, created_at,  -- colunas de controle
        raw_data  -- dados principais em JSON
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
        
        # Obter os dados do banco
        with conn.cursor() as cursor:
            cursor.execute(query, params if params else None)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
        
        # Converter para DataFrame
        df = pd.DataFrame(rows, columns=columns)
        
        # Processar o raw_data (JSON) para colunas separadas
        if 'raw_data' in df.columns:
            # Converter JSON string para dict
            df['raw_data'] = df['raw_data'].apply(lambda x: json.loads(x) if x else {})
            
            # Normalizar o JSON em colunas separadas
            raw_data_df = pd.json_normalize(df['raw_data'])
            
            # Combinar com as colunas de controle
            final_df = pd.concat([df.drop('raw_data', axis=1), raw_data_df], axis=1)
        else:
            final_df = df
        
        # Criar um arquivo Excel em memória
        output = BytesIO()
        
        # Criar um Excel writer usando pandas
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, sheet_name='Dados', index=False)
            
            # Acessar a workbook e worksheet para formatação
            workbook = writer.book
            worksheet = writer.sheets['Dados']
            
            # Formatar cabeçalho
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'fg_color': '#4472C4',
                'font_color': 'white',
                'border': 1
            })
            
            # Formatar células de dados
            data_format = workbook.add_format({
                'text_wrap': True,
                'valign': 'top',
                'border': 1
            })
            
            # Formatar células de valores monetários
            money_format = workbook.add_format({
                'num_format': 'R$ #,##0.00',
                'border': 1
            })
            
            # Aplicar formatos
            for col_num, value in enumerate(final_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Identificar colunas numéricas automaticamente
            numeric_cols = final_df.select_dtypes(include=['int64', 'float64']).columns
            for col in numeric_cols:
                if any(keyword in col.lower() for keyword in ['amount', 'value', 'total', 'price']):
                    col_idx = final_df.columns.get_loc(col)
                    worksheet.set_column(col_idx, col_idx, 15, money_format)
            
            # Auto-ajustar largura das colunas
            for i, col in enumerate(final_df.columns):
                # Converter todos os valores para string para calcular o comprimento máximo
                max_len = max(
                    final_df[col].astype(str).map(len).max(),  # maior valor na coluna
                    len(str(col))  # tamanho do cabeçalho
                ) + 2  # pequeno padding
                worksheet.set_column(i, i, max_len, data_format)
            
            # Adicionar filtros
            worksheet.autofilter(0, 0, len(final_df), len(final_df.columns) - 1)
            
            # Congelar painel superior (mantendo colunas de controle visíveis)
            worksheet.freeze_panes(1, 4)  # Congela após a 4ª coluna (created_at)
            
            # Adicionar título e metadados
            workbook.set_properties({
                'title': f'Relatório de Webhooks - {datetime.now().strftime("%Y-%m-%d")}',
                'subject': 'Dados de transações',
                'author': 'Sistema de Webhooks',
                'company': 'Sua Empresa'
            })
        
        output.seek(0)
        return output
        
    except Exception as e:
        print(f"Erro ao exportar XLSX: {e}")
        raise
    finally:
        if conn:
            conn.close()