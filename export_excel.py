import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, send_file
from drive_upload import upload_or_replace_file, upload_buffer_to_drive, create_backup_with_rotation
from db import get_db_connection
import logging
import os

# Configurar logging
logger = logging.getLogger(__name__)

export_bp = Blueprint("export", __name__)

def get_safe_columns():
    """
    Verificar quais colunas existem na tabela webhooks e retornar apenas as seguras
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'webhooks'
                ORDER BY ordinal_position
            """)
            existing_columns = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Colunas básicas que devem existir
        basic_columns = [
            'id', 'platform', 'event_type', 'webhook_id', 'transaction_id',
            'customer_email', 'customer_name', 'customer_document',
            'product_name', 'product_id', 'amount', 'currency', 
            'payment_method', 'status', 'created_at'
        ]
        
        # Colunas opcionais que podem existir
        optional_columns = [
            'customer_phone', 'offer_name', 'offer_id', 'commission_amount',
            'affiliate_email', 'utm_source', 'utm_medium', 'utm_campaign', 
            'utm_term', 'utm_content', 'sales_link', 'attendant_name', 
            'attendant_email', 'paid_at', 'reason', 'refund_reason',
            'base_amount', 'discount', 'payment_method_name', 'installments'
        ]
        
        # Retornar apenas colunas que realmente existem
        safe_columns = [col for col in basic_columns + optional_columns if col in existing_columns]
        
        logger.info(f"✅ Encontradas {len(safe_columns)} colunas seguras na tabela webhooks")
        return safe_columns
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar colunas: {e}")
        # Fallback para colunas mínimas essenciais
        return ['id', 'platform', 'event_type', 'customer_email', 'customer_name', 
                'product_name', 'amount', 'status', 'created_at']

def fix_timezone_columns(df):
    """
    Versão simples e robusta para corrigir problemas de timezone com Excel
    """
    logger.info("🕐 Iniciando correção de timezone para Excel...")
    
    try:
        # Identificar colunas de data
        date_columns = []
        for col in df.columns:
            # Verificar se é coluna de data pelo nome
            if any(keyword in col.lower() for keyword in ['date', 'created_at', 'paid_at', 'time', 'expires_at', 'data']):
                date_columns.append(col)
            # Verificar se é coluna de data pelo tipo
            elif str(df[col].dtype).startswith('datetime64'):
                date_columns.append(col)
        
        logger.info(f"📅 Encontradas {len(date_columns)} colunas de data: {date_columns}")
        
        # Corrigir cada coluna de data
        for col in date_columns:
            try:
                original_dtype = str(df[col].dtype)
                logger.info(f"🔧 Processando coluna: {col} (tipo: {original_dtype})")
                
                # Se já é datetime com timezone, remover
                if 'datetime64' in original_dtype and ('tz' in original_dtype or 'UTC' in original_dtype):
                    logger.info(f"   📍 Removendo timezone de {col}")
                    df[col] = df[col].dt.tz_localize(None)
                    continue
                
                # Se é string, limpar e converter
                if df[col].dtype == 'object':
                    logger.info(f"   🔄 Convertendo string para datetime: {col}")
                    
                    # Converter para string e limpar timezone
                    df[col] = df[col].astype(str)
                    
                    # Remover indicadores de timezone
                    timezone_patterns = ['+00:00', 'UTC', 'Z', '+0000', ' UTC', 'T00:00:00Z']
                    for pattern in timezone_patterns:
                        df[col] = df[col].str.replace(pattern, '', regex=False)
                    
                    # Converter para datetime sem timezone
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                
                logger.info(f"   ✅ {col} processado: {str(df[col].dtype)}")
                
            except Exception as col_error:
                logger.warning(f"   ⚠️ Erro ao processar {col}: {col_error}")
                # Fallback: manter como string
                try:
                    df[col] = df[col].astype(str)
                    logger.info(f"   📝 {col} convertido para string")
                except:
                    logger.error(f"   ❌ Falha total na coluna {col}")
        
        # Verificação final: garantir que não há timezone
        for col in df.columns:
            if str(df[col].dtype).startswith('datetime64'):
                if 'tz' in str(df[col].dtype) or 'UTC' in str(df[col].dtype):
                    logger.warning(f"🚨 Conversão final forçada para {col}")
                    try:
                        # Última tentativa: converter para string formatada e depois datetime
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        df[col] = df[col].astype(str)
        
        logger.info("✅ Correção de timezone concluída")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro crítico na correção de timezone: {e}")
        
        # Fallback de emergência: converter problemáticas para string
        for col in df.columns:
            if str(df[col].dtype).startswith('datetime64'):
                try:
                    df[col] = df[col].astype(str)
                    logger.warning(f"🆘 {col} convertido para string (emergência)")
                except:
                    pass
        
        return df

def format_excel(writer, df, sheet_name):
    """
    Formatar planilha Excel com largura automática das colunas e estilos
    
    Args:
        writer: Objeto ExcelWriter do pandas
        df: DataFrame para formatar
        sheet_name: Nome da aba
    """
    try:
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        
        # Definir formatos
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#4472C4',
            'font_color': 'white',
            'border': 1
        })
        
        currency_format = workbook.add_format({
            'num_format': 'R$ #,##0.00',
            'border': 1
        })
        
        date_format = workbook.add_format({
            'num_format': 'dd/mm/yyyy hh:mm',
            'border': 1
        })
        
        text_format = workbook.add_format({
            'text_wrap': True,
            'valign': 'top',
            'border': 1
        })
        
        # Formatar cabeçalhos
        for col_num, column in enumerate(df.columns):
            worksheet.write(0, col_num, column, header_format)
        
        # Formatar colunas com largura automática
        for i, col in enumerate(df.columns):
            # Calcular largura baseada no conteúdo
            max_len = max(
                df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                len(str(col))
            )
            
            # Limitar largura entre 10 e 50 caracteres
            column_width = min(max(max_len + 2, 10), 50)
            worksheet.set_column(i, i, column_width)
            
            # Aplicar formatação específica por tipo de coluna
            if 'amount' in col.lower() or 'valor' in col.lower() or 'preco' in col.lower():
                # Formatar colunas monetárias
                worksheet.set_column(i, i, column_width, currency_format)
            elif 'date' in col.lower() or 'data' in col.lower() or 'created_at' in col.lower():
                # Formatar colunas de data
                worksheet.set_column(i, i, column_width, date_format)
            else:
                # Formato padrão para texto
                worksheet.set_column(i, i, column_width, text_format)
        
        # Adicionar filtros automáticos
        if len(df) > 0:
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        # Congelar primeira linha (cabeçalhos)
        worksheet.freeze_panes(1, 0)
        
        logger.info(f"✅ Formatação aplicada na aba: {sheet_name}")
        
    except Exception as e:
        logger.error(f"❌ Erro ao formatar Excel: {e}")

def prepare_dataframe(data, columns):
    """
    Preparar DataFrame com limpeza e formatação dos dados
    
    Args:
        data: Dados brutos do banco
        columns: Nomes das colunas
    
    Returns:
        DataFrame: DataFrame preparado e limpo
    """
    try:
        # Criar DataFrame
        df = pd.DataFrame(data, columns=columns)
        
        if df.empty:
            logger.warning("⚠️ DataFrame vazio criado")
            return df
        
        # Renomear colunas para nomes mais amigáveis
        column_mapping = {
            'id': 'ID',
            'platform': 'Plataforma',
            'event_type': 'Tipo de Evento',
            'webhook_id': 'ID Webhook',
            'transaction_id': 'ID Transação',
            'customer_email': 'Email do Cliente',
            'customer_name': 'Nome do Cliente',
            'customer_document': 'Documento',
            'customer_phone': 'Telefone',
            'product_name': 'Nome do Produto',
            'product_id': 'ID Produto',
            'offer_name': 'Nome da Oferta',
            'offer_id': 'ID Oferta',
            'amount': 'Valor',
            'currency': 'Moeda',
            'payment_method': 'Método de Pagamento',
            'status': 'Status',
            'commission_amount': 'Comissão',
            'affiliate_email': 'Email Afiliado',
            'utm_source': 'UTM Source',
            'utm_medium': 'UTM Medium',
            'utm_campaign': 'UTM Campaign',
            'sales_link': 'Link de Vendas',
            'attendant_name': 'Nome do Atendente',
            'attendant_email': 'Email do Atendente',
            'created_at': 'Data de Criação',
            'paid_at': 'Data de Pagamento',
            'reason': 'Motivo',
            'refund_reason': 'Motivo do Reembolso'
        }
        
        # Aplicar renomeação apenas para colunas existentes
        existing_columns = {col: column_mapping.get(col, col) for col in df.columns if col in column_mapping}
        df = df.rename(columns=existing_columns)
        
        # Converter datas para formato adequado (remover timezone)
        date_columns = ['Data de Criação', 'Data de Pagamento', 'created_at', 'paid_at']
        for date_col in date_columns:
            if date_col in df.columns:
                try:
                    # Converter para datetime
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                    
                    # Verificar e remover timezone se presente
                    if hasattr(df[date_col].dtype, 'tz') and df[date_col].dtype.tz is not None:
                        logger.info(f"🕐 Removendo timezone da coluna: {date_col}")
                        df[date_col] = df[date_col].dt.tz_localize(None)
                    elif 'UTC' in str(df[date_col].dtype) or '+00:00' in str(df[date_col].dtype):
                        logger.info(f"🕐 Convertendo timezone na coluna: {date_col}")
                        df[date_col] = df[date_col].dt.tz_localize(None)
                        
                except Exception as date_error:
                    logger.warning(f"⚠️ Problema ao converter data na coluna {date_col}: {date_error}")
                    # Tentar conversão alternativa
                    try:
                        df[date_col] = pd.to_datetime(df[date_col].astype(str).str.replace(r'\+00:00|UTC', '', regex=True), errors='coerce')
                    except:
                        logger.warning(f"⚠️ Mantendo coluna {date_col} como texto")
                        df[date_col] = df[date_col].astype(str)
        
        # Converter valores monetários
        money_columns = ['Valor', 'Comissão', 'amount', 'commission_amount']
        for money_col in money_columns:
            if money_col in df.columns:
                df[money_col] = pd.to_numeric(df[money_col], errors='coerce')
        
        # Limpar campos de texto
        text_columns = df.select_dtypes(include=['object']).columns
        for text_col in text_columns:
            if text_col not in date_columns:  # Não aplicar em datas
                df[text_col] = df[text_col].astype(str).str.strip()
                df[text_col] = df[text_col].replace(['None', 'nan', 'NaN', ''], None)
        
        logger.info(f"✅ DataFrame preparado: {len(df)} registros, {len(df.columns)} colunas")
        return df
        
    except Exception as e:
        logger.error(f"❌ Erro ao preparar DataFrame: {e}")
        return pd.DataFrame()

def create_excel_report(data, columns, filename="relatorio_webhooks.xlsx", upload_to_drive=True):
    """
    Criar relatório Excel com formatação avançada e upload automático para Google Drive
    
    Args:
        data: Dados para o relatório
        columns: Colunas do DataFrame
        filename: Nome do arquivo
        upload_to_drive: Se deve fazer upload para o Google Drive
    
    Returns:
        BytesIO: Buffer com o arquivo Excel gerado
    """
    try:
        # Preparar DataFrame
        df = prepare_dataframe(data, columns)
        
        if df.empty:
            logger.warning("⚠️ Nenhum dado para gerar relatório")
            # Criar Excel vazio com cabeçalhos
            df = pd.DataFrame(columns=[columns[0]] if columns else ['Sem Dados'])
        
        # IMPORTANTE: Corrigir problemas de timezone antes de criar Excel
        df = fix_timezone_columns(df)
        
        # Criar buffer em memória para o arquivo Excel
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            # Verificar se deve criar abas separadas por plataforma
            if 'platform' in df.columns or 'Plataforma' in df.columns:
                platform_col = 'Plataforma' if 'Plataforma' in df.columns else 'platform'
                unique_platforms = df[platform_col].dropna().unique()
                
                if len(unique_platforms) > 1:
                    # Criar abas separadas por plataforma
                    for platform_name in unique_platforms:
                        platform_df = df[df[platform_col] == platform_name].copy()
                        sheet_name = str(platform_name)[:31]  # Limite do Excel para nomes de aba
                        
                        platform_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)
                        format_excel(writer, platform_df, sheet_name)
                        
                        logger.info(f"📊 Aba criada: {sheet_name} ({len(platform_df)} registros)")
                    
                    # Criar aba com resumo geral
                    if len(unique_platforms) <= 10:  # Evitar resumos muito grandes
                        summary_df = df.groupby(platform_col).agg({
                            'ID' if 'ID' in df.columns else df.columns[0]: 'count',
                            'Valor' if 'Valor' in df.columns else (
                                'amount' if 'amount' in df.columns else df.columns[0]
                            ): ['sum', 'mean', 'count'] if 'Valor' in df.columns or 'amount' in df.columns else 'count'
                        }).round(2)
                        
                        summary_df.to_excel(writer, sheet_name='Resumo', index=True)
                        logger.info(f"📈 Aba de resumo criada")
                else:
                    # Apenas uma plataforma, criar aba única
                    df.to_excel(writer, sheet_name='Webhooks', index=False)
                    format_excel(writer, df, 'Webhooks')
            else:
                # Sem coluna de plataforma, criar aba única
                df.to_excel(writer, sheet_name='Webhooks', index=False)
                format_excel(writer, df, 'Webhooks')
        
        excel_buffer.seek(0)
        file_size = len(excel_buffer.getvalue())
        logger.info(f"✅ Arquivo Excel gerado em memória: {filename} ({file_size:,} bytes)")
        
        # Upload para Google Drive se solicitado
        if upload_to_drive:
            try:
                excel_buffer.seek(0)  # Reset buffer position
                
                # Upload direto do buffer
                upload_result = upload_buffer_to_drive(
                    buffer=excel_buffer,
                    filename=filename,
                    folder_name="Webhooks_Reports"
                )
                
                if upload_result.get("success"):
                    logger.info(f"✅ Arquivo enviado para Google Drive: {filename}")
                else:
                    logger.error(f"❌ Falha no upload para Google Drive: {upload_result.get('error')}")
                
            except Exception as drive_error:
                logger.error(f"❌ Erro ao enviar para Google Drive: {drive_error}")
                # Continuar mesmo se o upload falhar
        
        excel_buffer.seek(0)
        return excel_buffer
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar relatório Excel: {e}")
        # Retornar buffer vazio em caso de erro
        error_buffer = BytesIO()
        with pd.ExcelWriter(error_buffer, engine='xlsxwriter') as writer:
            error_df = pd.DataFrame({'Erro': [f'Erro ao gerar relatório: {str(e)}']})
            error_df.to_excel(writer, sheet_name='Erro', index=False)
        error_buffer.seek(0)
        return error_buffer

@export_bp.route("/excel", methods=["GET"])
def export_excel():
    """
    Exportar webhooks para Excel com opções avançadas de filtro
    """
    try:
        # Parâmetros de filtro
        platform = request.args.get('platform')
        days = request.args.get('days', 30, type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        upload_drive = request.args.get('upload_drive', 'true').lower() == 'true'
        event_type = request.args.get('event_type')
        status_filter = request.args.get('status')
        min_amount = request.args.get('min_amount', type=float)
        max_amount = request.args.get('max_amount', type=float)
        
        logger.info(f"📊 Iniciando exportação Excel com filtros: platform={platform}, days={days}")
        
        # Obter colunas seguras da tabela
        safe_columns = get_safe_columns()
        columns_str = ", ".join(safe_columns)
        
        # Construir query SQL dinâmica com colunas seguras
        query = f"""
            SELECT 
                {columns_str}
            FROM webhooks 
            WHERE 1=1
        """
        params = []
        
        # Aplicar filtros dinamicamente
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        if event_type:
            query += " AND event_type = %s"
            params.append(event_type)
        
        if status_filter:
            query += " AND status ILIKE %s"
            params.append(f"%{status_filter}%")
        
        if min_amount is not None:
            query += " AND amount >= %s"
            params.append(min_amount)
        
        if max_amount is not None:
            query += " AND amount <= %s"
            params.append(max_amount)
        
        # Filtro de data
        if start_date and end_date:
            query += " AND created_at BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        elif days:
            query += " AND created_at >= NOW() - INTERVAL %s DAY"
            params.append(f'{days}')
        
        query += " ORDER BY created_at DESC"
        
        # Executar query
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()
            columns = safe_columns  # Usar as colunas seguras
        conn.close()
        
        if not data:
            logger.warning("⚠️ Nenhum dado encontrado para os filtros especificados")
            return jsonify({
                "error": "Nenhum dado encontrado para os filtros especificados",
                "filters_applied": {
                    "platform": platform,
                    "days": days,
                    "start_date": start_date,
                    "end_date": end_date,
                    "event_type": event_type,
                    "status": status_filter,
                    "min_amount": min_amount,
                    "max_amount": max_amount
                }
            }), 404
        
        # Gerar nome do arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filters_suffix = []
        
        if platform:
            filters_suffix.append(platform)
        if event_type:
            filters_suffix.append(event_type)
        if status_filter:
            filters_suffix.append(status_filter)
        
        filter_str = "_".join(filters_suffix) if filters_suffix else "todos"
        filename = f"webhooks_report_{filter_str}_{timestamp}.xlsx"
        
        # Criar Excel
        excel_buffer = create_excel_report(
            data=data, 
            columns=columns, 
            filename=filename,
            upload_to_drive=upload_drive
        )
        
        logger.info(f"✅ Exportação concluída: {len(data)} registros em {filename}")
        
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
    Ideal para automação e backups programados
    """
    try:
        data = request.get_json() or {}
        
        # Parâmetros com valores padrão
        platform = data.get('platform')
        days = data.get('days', 7)
        event_types = data.get('event_types', [])  # Lista de tipos de evento
        create_backup = data.get('create_backup', False)
        
        logger.info(f"⏰ Exportação agendada iniciada: platform={platform}, days={days}")
        
        # Obter colunas seguras
        safe_columns = get_safe_columns()
        
        # Colunas básicas para exportação agendada
        scheduled_columns = [col for col in [
            'id', 'platform', 'event_type', 'webhook_id', 'transaction_id',
            'customer_email', 'customer_name', 'customer_document',
            'product_name', 'product_id', 'amount', 'currency', 
            'payment_method', 'status', 'commission_amount', 
            'affiliate_email', 'created_at', 'utm_source', 'utm_medium'
        ] if col in safe_columns]
        
        columns_str = ", ".join(scheduled_columns)
        
        query = f"""
            SELECT 
                {columns_str}
            FROM webhooks 
            WHERE created_at >= NOW() - INTERVAL %s DAY
        """
        params = [f'{days}']
        
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        if event_types:
            placeholders = ','.join(['%s'] * len(event_types))
            query += f" AND event_type IN ({placeholders})"
            params.extend(event_types)
        
        query += " ORDER BY created_at DESC"
        
        # Executar query
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            webhook_data = cursor.fetchall()
            columns = scheduled_columns
        conn.close()
        
        if not webhook_data:
            logger.warning("⚠️ Nenhum webhook encontrado para exportação agendada")
            return jsonify({
                "status": "no_data",
                "message": "Nenhum webhook encontrado no período especificado",
                "period_days": days,
                "platform": platform,
                "event_types": event_types
            })
        
        # Gerar arquivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        platform_suffix = f"_{platform}" if platform else "_scheduled"
        filename = f"webhooks_scheduled{platform_suffix}_{timestamp}.xlsx"
        
        # Criar Excel e enviar para Drive
        excel_buffer = create_excel_report(
            data=webhook_data,
            columns=columns,
            filename=filename,
            upload_to_drive=True
        )
        
        result = {
            "status": "success",
            "message": "Exportação agendada concluída",
            "filename": filename,
            "records_exported": len(webhook_data),
            "period_days": days,
            "platform": platform or "todas",
            "event_types": event_types,
            "timestamp": timestamp
        }
        
        # Criar backup com rotação se solicitado
        if create_backup:
            try:
                excel_buffer.seek(0)
                backup_result = create_backup_with_rotation(
                    buffer=excel_buffer,
                    base_filename=f"webhook_backup_{platform or 'all'}",
                    max_backups=10,
                    folder_name="Webhooks_Backups"
                )
                
                if backup_result.get("success"):
                    result["backup"] = {
                        "created": backup_result.get("backup_created"),
                        "deleted_old": backup_result.get("backups_deleted", []),
                        "total_backups": backup_result.get("total_backups")
                    }
                    logger.info(f"🔄 Backup com rotação criado: {backup_result.get('backup_created')}")
                
            except Exception as backup_error:
                logger.error(f"❌ Erro no backup: {backup_error}")
                result["backup_error"] = str(backup_error)
        
        logger.info(f"✅ Exportação agendada concluída: {len(webhook_data)} registros")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Erro na exportação agendada: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/stats", methods=["GET"])
def export_stats():
    """
    Exportar estatísticas detalhadas com múltiplas abas
    """
    try:
        days = request.args.get('days', 30, type=int)
        upload_drive = request.args.get('upload_drive', 'true').lower() == 'true'
        
        logger.info(f"📈 Iniciando exportação de estatísticas para {days} dias")
        
        conn = get_db_connection()
        
        # Dados para múltiplas abas
        excel_data = {}
        
        with conn.cursor() as cursor:
            # 1. Estatísticas por plataforma
            cursor.execute("""
                SELECT 
                    platform,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT customer_email) as unique_customers,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_amount,
                    COUNT(CASE WHEN status ILIKE '%paid%' OR status ILIKE '%aprovado%' OR status ILIKE '%completed%' THEN 1 END) as paid_events,
                    COUNT(CASE WHEN status ILIKE '%pending%' OR status ILIKE '%pendente%' THEN 1 END) as pending_events,
                    COUNT(CASE WHEN status ILIKE '%cancelled%' OR status ILIKE '%cancelado%' OR status ILIKE '%failed%' THEN 1 END) as cancelled_events,
                    MIN(created_at) as first_event,
                    MAX(created_at) as last_event
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                GROUP BY platform
                ORDER BY total_revenue DESC
            """, [f'{days}'])
            
            platform_stats = cursor.fetchall()
            platform_columns = [desc[0] for desc in cursor.description]
            excel_data['Estatisticas_Plataforma'] = (platform_stats, platform_columns)
            
            # 2. Top produtos por receita
            cursor.execute("""
                SELECT 
                    platform,
                    product_name,
                    COUNT(*) as sales_count,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_price,
                    COUNT(DISTINCT customer_email) as unique_customers
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                    AND product_name IS NOT NULL
                    AND product_name != ''
                GROUP BY platform, product_name
                ORDER BY total_revenue DESC
                LIMIT 100
            """, [f'{days}'])
            
            products_data = cursor.fetchall()
            products_columns = [desc[0] for desc in cursor.description]
            excel_data['Top_Produtos'] = (products_data, products_columns)
            
            # 3. Estatísticas por método de pagamento
            cursor.execute("""
                SELECT 
                    platform,
                    payment_method,
                    COUNT(*) as transactions,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE NULL END) as avg_amount
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                    AND payment_method IS NOT NULL
                    AND payment_method != ''
                GROUP BY platform, payment_method
                ORDER BY total_revenue DESC
            """, [f'{days}'])
            
            payment_stats = cursor.fetchall()
            payment_columns = [desc[0] for desc in cursor.description]
            excel_data['Metodos_Pagamento'] = (payment_stats, payment_columns)
            
            # 4. Timeline diária (últimos 30 dias)
            if days <= 90:  # Evitar queries muito pesadas
                cursor.execute("""
                    SELECT 
                        DATE(created_at) as date,
                        platform,
                        COUNT(*) as events,
                        SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as revenue,
                        COUNT(DISTINCT customer_email) as unique_customers
                    FROM webhooks 
                    WHERE created_at >= NOW() - INTERVAL %s DAY
                    GROUP BY DATE(created_at), platform
                    ORDER BY date DESC, platform
                """, [f'{min(days, 90)}'])
                
                timeline_data = cursor.fetchall()
                timeline_columns = [desc[0] for desc in cursor.description]
                excel_data['Timeline_Diaria'] = (timeline_data, timeline_columns)
            
            # 5. Afiliados top (se houver dados de comissão)
            cursor.execute("""
                SELECT 
                    platform,
                    affiliate_email,
                    COUNT(*) as referrals,
                    SUM(CASE WHEN commission_amount IS NOT NULL THEN commission_amount ELSE 0 END) as total_commission,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as generated_revenue,
                    AVG(CASE WHEN commission_amount IS NOT NULL THEN commission_amount ELSE NULL END) as avg_commission
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s DAY
                    AND affiliate_email IS NOT NULL
                    AND affiliate_email != ''
                GROUP BY platform, affiliate_email
                ORDER BY total_commission DESC
                LIMIT 50
            """, [f'{days}'])
            
            affiliate_data = cursor.fetchall()
            if affiliate_data:
                affiliate_columns = [desc[0] for desc in cursor.description]
                excel_data['Top_Afiliados'] = (affiliate_data, affiliate_columns)
        
        conn.close()
        
        # Gerar Excel com múltiplas abas
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"webhooks_statistics_{days}days_{timestamp}.xlsx"
        
        excel_buffer = BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            for sheet_name, (data, columns) in excel_data.items():
                if data:  # Só criar aba se houver dados
                    df = prepare_dataframe(data, columns)
                    df = fix_timezone_columns(df)  # Corrigir timezone
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    format_excel(writer, df, sheet_name)
                    
                    logger.info(f"📊 Aba criada: {sheet_name} ({len(df)} registros)")
        
        excel_buffer.seek(0)
        
        # Upload para Drive se solicitado
        if upload_drive:
            try:
                excel_buffer.seek(0)
                upload_result = upload_buffer_to_drive(
                    buffer=excel_buffer,
                    filename=filename,
                    folder_name="Webhooks_Reports"
                )
                
                if upload_result.get("success"):
                    logger.info(f"✅ Estatísticas enviadas para Google Drive: {filename}")
                else:
                    logger.error(f"❌ Erro no upload: {upload_result.get('error')}")
                    
            except Exception as drive_error:
                logger.error(f"❌ Erro ao enviar estatísticas para Google Drive: {drive_error}")
        
        excel_buffer.seek(0)
        
        logger.info(f"✅ Exportação de estatísticas concluída: {filename}")
        
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
        data = request.get_json() or {}
        include_raw_data = data.get('include_raw_data', False)
        
        logger.info("🔄 Iniciando backup completo dos webhooks")
        
        # Obter colunas seguras
        safe_columns = get_safe_columns()
        
        # Se incluir raw_data, adicionar à lista se existir
        if include_raw_data and 'raw_data' not in safe_columns:
            # Verificar se raw_data existe na tabela
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'webhooks' AND column_name = 'raw_data'
                """)
                if cursor.fetchone():
                    safe_columns.append('raw_data')
            conn.close()
        
        columns_str = ", ".join(safe_columns)
        
        # Backup apenas para Drive, sem download
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = f"""
                SELECT {columns_str}
                FROM webhooks 
                ORDER BY created_at DESC
            """
            cursor.execute(query)
            all_data = cursor.fetchall()
            columns = safe_columns
        conn.close()
        
        if not all_data:
            logger.warning("⚠️ Nenhum dado encontrado para backup")
            return jsonify({"error": "Nenhum dado para backup"}), 404
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"webhooks_backup_completo_{timestamp}.xlsx"
        
        # Criar Excel e enviar para Drive com backup rotativo
        excel_buffer = create_excel_report(
            data=all_data,
            columns=columns,
            filename=filename,
            upload_to_drive=False  # Não usar upload simples
        )
        
        # Usar backup com rotação
        excel_buffer.seek(0)
        backup_result = create_backup_with_rotation(
            buffer=excel_buffer,
            base_filename="webhooks_backup_completo",
            max_backups=15,  # Manter 15 backups completos
            folder_name="Webhooks_Backups"
        )
        
        result = {
            "status": "success",
            "message": "Backup completo criado e enviado para Google Drive",
            "filename": filename,
            "total_records": len(all_data),
            "timestamp": timestamp,
            "include_raw_data": include_raw_data,
            "backup_rotation": backup_result if backup_result.get("success") else None
        }
        
        if backup_result.get("success"):
            result["backup_info"] = {
                "backup_created": backup_result.get("backup_created"),
                "old_backups_deleted": backup_result.get("backups_deleted", []),
                "total_backups_kept": backup_result.get("total_backups")
            }
        
        logger.info(f"✅ Backup completo concluído: {len(all_data)} registros")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Erro ao criar backup: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/quick", methods=["GET"])
def quick_export():
    """
    Exportação rápida com filtros básicos
    """
    try:
        platform = request.args.get('platform')
        hours = request.args.get('hours', 24, type=int)
        
        logger.info(f"⚡ Exportação rápida: platform={platform}, hours={hours}")
        
        # Obter colunas seguras
        safe_columns = get_safe_columns()
        
        # Colunas básicas para exportação rápida
        basic_columns = [col for col in [
            'platform', 'event_type', 'customer_email', 'customer_name',
            'product_name', 'amount', 'status', 'created_at'
        ] if col in safe_columns]
        
        columns_str = ", ".join(basic_columns)
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = f"""
                SELECT 
                    {columns_str}
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s HOUR
            """
            params = [f'{hours}']
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            query += " ORDER BY created_at DESC LIMIT 1000"
            
            cursor.execute(query, params)
            data = cursor.fetchall()
            columns = basic_columns
        conn.close()
        
        if not data:
            return jsonify({"error": "Nenhum dado recente encontrado"}), 404
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        platform_suffix = f"_{platform}" if platform else "_quick"
        filename = f"webhooks_quick{platform_suffix}_{timestamp}.xlsx"
        
        excel_buffer = create_excel_report(
            data=data,
            columns=columns,
            filename=filename,
            upload_to_drive=True
        )
        
        return send_file(
            excel_buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logger.error(f"❌ Erro na exportação rápida: {e}")
        return jsonify({"error": str(e)}), 500

@export_bp.route("/status", methods=["GET"])
def export_status():
    """
    Status do serviço de exportação
    """
    try:
        # Verificar últimas exportações
        from drive_upload import list_webhook_files, get_drive_usage
        
        recent_files = list_webhook_files(limit=10, folder_name="Webhooks_Reports")
        drive_usage = get_drive_usage()
        
        # Verificar colunas disponíveis
        safe_columns = get_safe_columns()
        
        # Estatísticas do banco
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_webhooks,
                    COUNT(DISTINCT platform) as platforms,
                    MIN(created_at) as oldest_record,
                    MAX(created_at) as newest_record
                FROM webhooks
            """)
            db_stats = cursor.fetchone()
        conn.close()
        
        return jsonify({
            "status": "operational",
            "timestamp": datetime.now().isoformat(),
            "database": {
                "total_webhooks": db_stats[0],
                "platforms": db_stats[1],
                "oldest_record": db_stats[2].isoformat() if db_stats[2] else None,
                "newest_record": db_stats[3].isoformat() if db_stats[3] else None,
                "available_columns": len(safe_columns),
                "columns": safe_columns
            },
            "google_drive": {
                "connected": bool(drive_usage and not drive_usage.get("error")),
                "usage": drive_usage,
                "recent_files_count": len(recent_files)
            },
            "recent_exports": [
                {
                    "name": f["name"],
                    "size": f["size_formatted"],
                    "modified": f["modified"]
                }
                for f in recent_files[:5]
            ],
            "available_endpoints": {
                "excel": "/export/excel - Exportação padrão com filtros",
                "stats": "/export/stats - Estatísticas detalhadas",
                "scheduled": "/export/excel/scheduled - Exportação agendada",
                "backup": "/export/backup - Backup completo",
                "quick": "/export/quick - Exportação rápida",
                "status": "/export/status - Status do serviço"
            }
        })
        
    except Exception as e:
        logger.error(f"❌ Erro no status de exportação: {e}")
        return jsonify({"error": str(e)}), 500