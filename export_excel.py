import json
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import Blueprint, request, jsonify, send_file
from drive_upload import upload_or_replace_file
from db import get_db_connection
import logging
import os

# Configurar logging
logger = logging.getLogger(__name__)

export_bp = Blueprint("export", __name__)

def format_excel(writer, df, sheet_name):
    """
    Formatar planilha Excel com largura automática das colunas
    """
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Formatar colunas com largura automática
    for i, col in enumerate(df.columns):
        column_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, min(column_len, 50))  # Limitar largura máxima
    
    # Adicionar filtros automáticos
    worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

def create_excel_report(data, columns, filename="relatorio_webhooks.xlsx", upload_to_drive=True):
    """
    Criar relatório Excel e opcionalmente fazer upload para Google Drive
    
    Args:
        data: Dados para o relatório
        columns: Colunas do DataFrame
        filename: Nome do arquivo
        upload_to_drive: Se deve fazer upload para o Google Drive
    
    Returns:
        BytesIO: Buffer com o arquivo Excel gerado
    """
    try:
        df = pd.DataFrame(data, columns=columns)
        
        # Criar buffer em memória para o arquivo Excel
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            if 'platform' in df.columns and len(df['platform'].unique()) > 1:
                # Criar abas separadas por plataforma
                for platform_name, platform_df in df.groupby('platform'):
                    sheet_name = platform_name[:31]  # Limite do Excel para nomes de aba
                    platform_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    format_excel(writer, platform_df, sheet_name)
            else:
                # Criar aba única
                df.to_excel(writer, sheet_name='Webhooks', index=False)
                format_excel(writer, df, 'Webhooks')
        
        excel_buffer.seek(0)
        logger.info(f"✅ Arquivo Excel gerado em memória: {filename}")
        
        # Upload para Google Drive se solicitado
        if upload_to_drive:
            try:
                # Salvar temporariamente para upload
                temp_path = f"/tmp/{filename}"
                with open(temp_path, 'wb') as temp_file:
                    temp_file.write(excel_buffer.getvalue())
                
                # Fazer upload para Google Drive
                upload_or_replace_file(temp_path)
                
                # Limpar arquivo temporário
                os.remove(temp_path)
                logger.info(f"✅ Arquivo enviado para Google Drive: {filename}")
                
            except Exception as drive_error:
                logger.error(f"❌ Erro ao enviar para Google Drive: {drive_error}")
                # Continuar mesmo se o upload falhar
        
        excel_buffer.seek(0)
        return excel_buffer
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar relatório Excel: {e}")
        raise

@export_bp.route("/excel", methods=["GET"])
def export_excel():
    """
    Exportar webhooks para Excel com opções de filtro
    """
    try:
        # Parâmetros de filtro
        platform = request.args.get('platform')
        days = request.args.get('days', 30, type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        upload_drive = request.args.get('upload_drive', 'true').lower() == 'true'
        
        # Construir query SQL
        query = """
            SELECT 
                id, platform, event_type, webhook_id, transaction_id,
                customer_email, customer_name, customer_document, customer_phone,
                product_name, product_id, offer_name, offer_id,
                amount, currency, payment_method, status,
                commission_amount, affiliate_email,
                utm_source, utm_medium, utm_campaign,
                sales_link, attendant_name, attendant_email,
                created_at, paid_at, reason, refund_reason
            FROM webhooks 
            WHERE 1=1
        """
        params = []
        
        # Aplicar filtros
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        if start_date and end_date:
            query += " AND created_at BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        elif days:
            query += " AND created_at >= NOW() - INTERVAL %s DAY"
            params.append(days)
        
        query += " ORDER BY created_at DESC"
        
        # Executar query
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        if not data:
            return jsonify({"error": "Nenhum dado encontrado para os filtros especificados"}), 404
        
        # Gerar nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        platform_suffix = f"_{platform}" if platform else "_todas_plataformas"
        filename = f"webhooks_report{platform_suffix}_{timestamp}.xlsx"
        
        # Criar Excel
        excel_buffer = create_excel_report(
            data=data, 
            columns=columns, 
            filename=filename,
            upload_to_drive=upload_drive
        )
        
        # Retornar arquivo para download
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"❌ Erro na exportação Excel: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/excel/scheduled", methods=["POST"])
def scheduled_export():
    """
    Exportação agendada - apenas upload para Drive sem download
    """
    try:
        data = request.get_json() or {}
        
        # Parâmetros
        platform = data.get('platform')
        days = data.get('days', 7)
        
        # Query para dados recentes
        query = """
            SELECT 
                id, platform, event_type, webhook_id, transaction_id,
                customer_email, customer_name, customer_document,
                product_name, amount, currency, payment_method, status,
                commission_amount, affiliate_email, created_at
            FROM webhooks 
            WHERE created_at >= NOW() - INTERVAL %s DAY
        """
        params = [days]
        
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        query += " ORDER BY created_at DESC"
        
        # Executar query
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            webhook_data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        if not webhook_data:
            return jsonify({
                "status": "no_data",
                "message": "Nenhum webhook encontrado no período"
            })
        
        # Gerar arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        platform_suffix = f"_{platform}" if platform else "_backup"
        filename = f"webhooks_backup{platform_suffix}_{timestamp}.xlsx"
        
        # Criar Excel e enviar para Drive
        excel_buffer = create_excel_report(
            data=webhook_data,
            columns=columns,
            filename=filename,
            upload_to_drive=True
        )
        
        return jsonify({
            "status": "success",
            "message": f"Backup criado e enviado para Google Drive",
            "filename": filename,
            "records_exported": len(webhook_data),
            "period_days": days,
            "platform": platform or "todas"
        })
        
    except Exception as e:
        logger.error(f"❌ Erro na exportação agendada: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/stats", methods=["GET"])
def export_stats():
    """
    Exportar estatísticas detalhadas
    """
    try:
        days = request.args.get('days', 30, type=int)
        upload_drive = request.args.get('upload_drive', 'true').lower() == 'true'
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Estatísticas por plataforma
            cursor.execute("""
                SELECT 
                    platform,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT customer_email) as unique_customers,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_amount,
                    COUNT(CASE WHEN status LIKE '%paid%' OR status LIKE '%aprovado%' THEN 1 END) as paid_events,
                    COUNT(CASE WHEN status LIKE '%pending%' OR status LIKE '%pendente%' THEN 1 END) as pending_events,
                    COUNT(CASE WHEN status LIKE '%cancelled%' OR status LIKE '%cancelado%' THEN 1 END) as cancelled_events,
                    MIN(created_at) as first_event,
                    MAX(created_at) as last_event
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                GROUP BY platform
                ORDER BY total_revenue DESC
            """, [days])
            
            stats_data = cursor.fetchall()
            stats_columns = [desc[0] for desc in cursor.description]
            
            # Top produtos
            cursor.execute("""
                SELECT 
                    platform,
                    product_name,
                    COUNT(*) as sales_count,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_price
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                    AND product_name IS NOT NULL
                GROUP BY platform, product_name
                ORDER BY total_revenue DESC
                LIMIT 50
            """, [days])
            
            products_data = cursor.fetchall()
            products_columns = [desc[0] for desc in cursor.description]
            
        conn.close()
        
        # Criar Excel com múltiplas abas
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"webhooks_statistics_{days}days_{timestamp}.xlsx"
        
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            # Aba de estatísticas por plataforma
            if stats_data:
                stats_df = pd.DataFrame(stats_data, columns=stats_columns)
                stats_df.to_excel(writer, sheet_name='Estatísticas_Plataforma', index=False)
                format_excel(writer, stats_df, 'Estatísticas_Plataforma')
            
            # Aba de top produtos
            if products_data:
                products_df = pd.DataFrame(products_data, columns=products_columns)
                products_df.to_excel(writer, sheet_name='Top_Produtos', index=False)
                format_excel(writer, products_df, 'Top_Produtos')
        
        excel_buffer.seek(0)
        
        # Upload para Drive se solicitado
        if upload_drive:
            try:
                temp_path = f"/tmp/{filename}"
                with open(temp_path, 'wb') as temp_file:
                    temp_file.write(excel_buffer.getvalue())
                
                upload_or_replace_file(temp_path)
                os.remove(temp_path)
                logger.info(f"✅ Estatísticas enviadas para Google Drive: {filename}")
                
            except Exception as drive_error:
                logger.error(f"❌ Erro ao enviar estatísticas para Google Drive: {drive_error}")
        
        excel_buffer.seek(0)
        
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"❌ Erro na exportação de estatísticas: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/backup", methods=["POST"])
def create_backup():
    """
    Criar backup completo e enviar para Google Drive
    """
    try:
        # Backup apenas para Drive, sem download
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM webhooks 
                ORDER BY created_at DESC
            """)
            all_data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        if not all_data:
            return jsonify({"error": "Nenhum dado para backup"}), 404
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"webhooks_backup_completo_{timestamp}.xlsx"
        
        # Criar Excel e enviar para Drive
        create_excel_report(
            data=all_data,
            columns=columns,
            filename=filename,
            upload_to_drive=True
        )
        
        return jsonify({
            "status": "success",
            "message": "Backup completo criado e enviado para Google Drive",
            "filename": filename,
            "total_records": len(all_data),
            "timestamp": timestamp
        })
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar backup: {e}")
        return jsonify({"error": str(e)}), 500