import io
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, send_file
from dotenv import load_dotenv

# Importações do ReportLab para geração de PDF
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT

# Módulos locais da aplicação
from db import get_db_connection
from webhooks.kirvano import kirvano_bp
from webhooks.hubla import hubla_bp
from webhooks.braip import braip_bp
from webhooks.cakto import cakto_bp

# ==================== ALTERAÇÃO 1: IMPORT DO BLUEPRINT DE EXPORTAÇÃO ====================
from export_excel import export_bp

# Configuração de Logging para registrar eventos e erros
load_dotenv()
stream_utf8 = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('webhook.log', encoding='utf-8'), logging.StreamHandler(stream_utf8)])
logger = logging.getLogger(__name__)

# Inicialização da Aplicação Flask
app = Flask(__name__, template_folder='templates', static_folder='static')
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')

# Registro dos Blueprints (rotas dos webhooks)
app.register_blueprint(kirvano_bp, url_prefix="/webhook/kirvano")
app.register_blueprint(hubla_bp, url_prefix="/webhook/hubla")
app.register_blueprint(braip_bp, url_prefix="/webhook/braip")
app.register_blueprint(cakto_bp, url_prefix="/webhook/cakto")

# ==================== ALTERAÇÃO 2: REGISTRO DO BLUEPRINT DE EXPORTAÇÃO ====================
app.register_blueprint(export_bp, url_prefix="/api/export")


@app.route("/")
@app.route("/dashboard")
def dashboard_page():
    """Serve a página principal do dashboard (o 'esqueleto' HTML)."""
    return render_template('dashboard.html')


@app.route("/api/dashboard-data")
def get_dashboard_data():
    """API endpoint que busca, processa e retorna todos os dados para o dashboard."""
    try:
        # 1. Coleta de filtros da requisição
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        top_n = request.args.get('top_n', 5, type=int)
        platform = request.args.get('platform')

        end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)

        # 2. Conexão e execução das queries no banco de dados
        conn = get_db_connection()
        with conn.cursor() as cursor:
            base_where_clause = "WHERE created_at BETWEEN %s AND %s"
            params = [start_date_str, end_date_dt]

            if platform:
                base_where_clause += " AND platform = %s"
                params.append(platform)

            # Definição dos filtros de eventos para reutilização nas queries
            profit_events_filter = "((platform = 'kirvano' AND event_type IN ('SALE_APPROVED', 'SUBSCRIPTION_RENEWED')) OR (platform = 'hubla' AND event_type = 'NewSale') OR (platform = 'cakto' AND event_type IN ('purchase_approved', 'subscription_renewed')) OR (platform = 'braip' AND event_type = 'SALE_COMPLETE'))"
            abandon_events_filter = "((platform = 'kirvano' AND event_type = 'ABANDONED_CART') OR (platform = 'hubla' AND event_type = 'CanceledSale') OR (platform = 'cakto' AND event_type = 'checkout_abandonment'))"
            refund_events_filter = """(
                (platform = 'kirvano' AND event_type = 'SALE_REFUNDED') OR
                (platform = 'hubla' AND event_type = 'refund_request.accepted') OR
                (platform = 'cakto' AND event_type IN ('refund', 'chargeback')) OR
                (platform = 'braip' AND event_type IN ('REFUND', 'CHARGEBACK'))
            )"""

            # Query para os KPIs principais
            cursor.execute(f"""
                SELECT
                    COALESCE(SUM(CASE WHEN {profit_events_filter} THEN amount ELSE 0 END), 0) as sales_value,
                    COALESCE(SUM(CASE WHEN {abandon_events_filter} THEN amount ELSE 0 END), 0) as abandoned_value,
                    COALESCE(SUM(CASE WHEN {refund_events_filter} THEN amount ELSE 0 END), 0) as refunds_value,
                    COUNT(CASE WHEN {profit_events_filter} THEN 1 END) as total_sales
                FROM webhooks {base_where_clause}
            """, tuple(params))
            sales_value, abandoned_value, refunds_value, total_sales = cursor.fetchone() or (0, 0, 0, 0)
            
            # Query para o gráfico de tendência diária
            cursor.execute(f"SELECT DATE(created_at) as date, SUM(amount) as daily_profit FROM webhooks {base_where_clause} AND {profit_events_filter} GROUP BY DATE(created_at) ORDER BY date", tuple(params))
            daily_trend_data = cursor.fetchall()

            # Query para a análise por plataforma
            cursor.execute(f"SELECT platform, COUNT(CASE WHEN {profit_events_filter} THEN 1 END) as sales_count, COALESCE(SUM(CASE WHEN {profit_events_filter} THEN amount ELSE 0 END), 0) as profit, COUNT(CASE WHEN {abandon_events_filter} THEN 1 END) as abandoned_count FROM webhooks {base_where_clause} GROUP BY platform ORDER BY profit DESC", tuple(params))
            platform_analysis_data = cursor.fetchall()

            # Query para o gráfico de reembolsos
            cursor.execute(f"SELECT platform, COUNT(*) as refund_count FROM webhooks {base_where_clause} AND {refund_events_filter} GROUP BY platform ORDER BY refund_count DESC", tuple(params))
            refund_analysis_data = cursor.fetchall()
            
            # Query para produtos mais vendidos
            cursor.execute(f"SELECT product_name, COUNT(*) as count FROM webhooks {base_where_clause} AND product_name IS NOT NULL AND product_name != '' AND {profit_events_filter} GROUP BY product_name ORDER BY count DESC LIMIT %s", tuple(params + [top_n]))
            top_selling_products = cursor.fetchall()
            
            # Query para produtos com mais abandonos
            cursor.execute(f"SELECT product_name, COUNT(*) as count FROM webhooks {base_where_clause} AND product_name IS NOT NULL AND product_name != '' AND {abandon_events_filter} GROUP BY product_name ORDER BY count DESC LIMIT %s", tuple(params + [top_n]))
            top_abandoned_products = cursor.fetchall()
            
        conn.close()

        # 3. Processamento e formatação dos dados para a resposta JSON
        date_range_days = (end_date_dt - datetime.strptime(start_date_str, '%Y-%m-%d')).days + 1
        date_dict = { (datetime.strptime(start_date_str, '%Y-%m-%d') + timedelta(days=i)).strftime('%d/%m') : 0 for i in range(date_range_days) }
        for date, daily_profit in daily_trend_data:
            date_dict[date.strftime('%d/%m')] = float(daily_profit)
        
        response_data = {
            "kpis": {
                "sales_value": f"R$ {sales_value:,.2f}",
                "abandoned_value": f"R$ {abandoned_value:,.2f}",
                "refunds_value": f"R$ {refunds_value:,.2f}",
                "total_sales": f"{total_sales:,}"
            },
            "daily_trend": { "labels": list(date_dict.keys()), "values": list(date_dict.values()) },
            "platform_analysis": {
                "chart_labels": [p[0].upper() for p in platform_analysis_data], "chart_values": [float(p[2]) for p in platform_analysis_data],
                "table_data": [{"platform": p[0].upper(), "sales": f"{p[1]:,}", "profit": f"R$ {p[2]:,.2f}", "ticket": f"R$ {(p[2]/p[1]) if p[1] > 0 else 0:,.2f}", "abandoned": f"{p[3]:,}"} for p in platform_analysis_data]
            },
            "refund_analysis": {
                "labels": [p[0].upper() for p in refund_analysis_data],
                "values": [int(p[1]) for p in refund_analysis_data]
            },
            "top_selling_products": { "labels": [p[0] for p in top_selling_products], "values": [p[1] for p in top_selling_products] },
            "top_abandoned_products": { "labels": [p[0] for p in top_abandoned_products], "values": [p[1] for p in top_abandoned_products] },
        }
        return jsonify(response_data)
    except Exception as e:
        logger.exception("API Error on /api/dashboard-data")
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500


@app.route("/api/hourly-analysis")
def get_hourly_analysis():
    """API para análise de vendas por hora."""
    return jsonify({ "labels": [], "values": [] }) # Placeholder


@app.route("/api/export-pdf", methods=['POST'])
def export_pdf_api():
    """Gera um PDF de resumo com os dados atuais do dashboard e lista de abandonos."""
    try:
        logger.info("🔄 Iniciando geração de PDF...")
        
        # 1. Validação dos dados recebidos
        data = request.get_json()
        if not data or not isinstance(data, dict):
            logger.error("❌ Dados JSON inválidos recebidos")
            return jsonify({"error": "Dados inválidos"}), 400

        # 2. Validação e processamento das datas
        try:
            start_date_str = request.args.get('start_date')
            end_date_str = request.args.get('end_date')
            
            if not start_date_str or not end_date_str:
                logger.warning("⚠️ Datas não fornecidas, usando padrão dos últimos 30 dias")
                today = datetime.now()
                start_date_obj = today - timedelta(days=29)
                end_date_obj = today
            else:
                start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
                end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')
                
        except (ValueError, TypeError) as date_error:
            logger.error(f"❌ Erro ao processar datas: {date_error}")
            today = datetime.now()
            start_date_obj = today - timedelta(days=29)
            end_date_obj = today
        
        end_date_obj_for_query = end_date_obj.replace(hour=23, minute=59, second=59)
        logger.info(f"📅 Período: {start_date_obj.strftime('%d/%m/%Y')} a {end_date_obj.strftime('%d/%m/%Y')}")

        # 3. Criação do buffer e documento PDF
        try:
            buffer = io.BytesIO()
            doc = SimpleDocTemplate(
                buffer, 
                pagesize=A4, 
                topMargin=2*cm, 
                bottomMargin=2*cm,
                leftMargin=2*cm,
                rightMargin=2*cm
            )
            logger.info("📄 Documento PDF criado")
        except Exception as pdf_error:
            logger.error(f"❌ Erro ao criar documento PDF: {pdf_error}")
            return jsonify({"error": f"Erro ao inicializar PDF: {str(pdf_error)}"}), 500

        # 4. Configuração de estilos
        try:
            styles = getSampleStyleSheet()
            
            # Criar estilos customizados
            title_style = ParagraphStyle(
                name='CustomTitle',
                parent=styles['h1'],
                fontSize=18,
                alignment=TA_CENTER,
                spaceAfter=12,
                textColor=colors.HexColor("#1e3a8a"),
                fontName='Helvetica-Bold'
            )
            
            h2_style = ParagraphStyle(
                name='CustomH2',
                parent=styles['h2'],
                fontSize=14,
                spaceAfter=10,
                textColor=colors.HexColor("#334155"),
                fontName='Helvetica-Bold'
            )
            
            date_style = ParagraphStyle(
                name='CustomDate',
                parent=styles['Normal'],
                fontSize=9,
                alignment=TA_CENTER,
                spaceAfter=20,
                textColor=colors.darkgrey
            )
            
            kpi_style = ParagraphStyle(
                name='CustomKPI',
                parent=styles['Normal'],
                fontSize=12,
                alignment=TA_CENTER,
                leading=16
            )
            
            logger.info("🎨 Estilos configurados")
            
        except Exception as style_error:
            logger.error(f"❌ Erro ao configurar estilos: {style_error}")
            return jsonify({"error": f"Erro nos estilos PDF: {str(style_error)}"}), 500

        # 5. Construção do conteúdo do PDF
        story = []
        
        try:
            # Cabeçalho
            story.append(Paragraph("Relatório de Performance do Dashboard", title_style))
            story.append(Paragraph(
                f"Período de Análise: {start_date_obj.strftime('%d/%m/%Y')} a {end_date_obj.strftime('%d/%m/%Y')}", 
                date_style
            ))
            
            # KPIs
            kpis = data.get('kpis', {})
            if kpis:
                kpi_data = [
                    [
                        Paragraph(f"<b>Valor em Vendas</b><br/>{kpis.get('sales_value', 'R$ 0,00')}", kpi_style), 
                        Paragraph(f"<b>Valor Perdido (Abandonos)</b><br/>{kpis.get('abandoned_value', 'R$ 0,00')}", kpi_style)
                    ],
                    [
                        Paragraph(f"<b>Valor Reembolsado</b><br/>{kpis.get('refunds_value', 'R$ 0,00')}", kpi_style), 
                        Paragraph(f"<b>Total de Vendas (Qtd)</b><br/>{kpis.get('total_sales', '0')}", kpi_style)
                    ]
                ]
                
                kpi_table = Table(kpi_data, colWidths=[9*cm, 9*cm])
                kpi_table.setStyle(TableStyle([
                    ('BOX', (0,0), (-1,-1), 1, colors.grey),
                    ('GRID', (0,0), (-1,-1), 1, colors.grey),
                    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                    ('TOPPADDING', (0,0), (-1,-1), 12),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 12)
                ]))
                story.append(kpi_table)
                story.append(Spacer(1, 1*cm))
            
            # Análise por Plataforma
            story.append(Paragraph("Desempenho por Plataforma", h2_style))
            platform_table_data = data.get('platform_analysis', {}).get('table_data', [])
            
            if platform_table_data:
                platform_data = [['Plataforma', 'Vendas', 'Lucro', 'Ticket Médio']]
                for row in platform_table_data:
                    platform_data.append([
                        str(row.get('platform', 'N/A')),
                        str(row.get('sales', '0')),
                        str(row.get('profit', 'R$ 0,00')),
                        str(row.get('ticket', 'R$ 0,00'))
                    ])
                
                platform_table = Table(platform_data, colWidths=[4*cm, 4*cm, 4*cm, 4*cm])
                platform_table.setStyle(TableStyle([
                    ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#1e3a8a")),
                    ('TEXTCOLOR', (0,0), (-1,0), colors.white),
                    ('GRID', (0,0), (-1,-1), 1, colors.grey),
                    ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                    ('FONTSIZE', (0,0), (-1,-1), 10),
                    ('VALIGN', (0,0), (-1,-1), 'MIDDLE')
                ]))
                story.append(platform_table)
            else:
                story.append(Paragraph("Nenhum dado de plataforma para exibir.", styles['Normal']))
            
            logger.info("📊 Primeira página do PDF construída")
            
        except Exception as content_error:
            logger.error(f"❌ Erro ao construir conteúdo principal: {content_error}")
            return jsonify({"error": f"Erro no conteúdo PDF: {str(content_error)}"}), 500

        # 6. Segunda página - Lista de Abandonos
        try:
            story.append(PageBreak())
            story.append(Paragraph("Lista Detalhada de Carrinhos Abandonados", title_style))
            
            # Buscar dados de abandonos no banco
            try:
                conn = get_db_connection()
                with conn.cursor() as cursor:
                    abandon_events_filter = """(
                        (platform = 'kirvano' AND event_type = 'ABANDONED_CART') OR 
                        (platform = 'hubla' AND event_type = 'CanceledSale') OR 
                        (platform = 'cakto' AND event_type = 'checkout_abandonment')
                    )"""
                    
                    query = f"""
                        SELECT platform, customer_email, customer_name, product_name, amount, created_at 
                        FROM webhooks 
                        WHERE created_at BETWEEN %s AND %s AND {abandon_events_filter} 
                        ORDER BY created_at DESC 
                        LIMIT 100
                    """
                    
                    cursor.execute(query, (start_date_obj, end_date_obj_for_query))
                    abandoned_details = cursor.fetchall()
                    
                conn.close()
                logger.info(f"📋 Encontrados {len(abandoned_details)} abandonos")
                
            except Exception as db_error:
                logger.error(f"❌ Erro ao buscar dados do banco: {db_error}")
                abandoned_details = []
                story.append(Paragraph(f"Erro ao acessar dados de abandonos: {str(db_error)}", styles['Normal']))

            if abandoned_details:
                abandoned_header = ['Plataforma', 'Email', 'Nome', 'Produto', 'Valor', 'Data']
                abandoned_table_data = [abandoned_header]
                
                for row in abandoned_details:
                    try:
                        platform = str(row[0]) if row[0] else 'N/A'
                        email = str(row[1]) if row[1] else 'N/A'
                        name = str(row[2]) if row[2] else 'N/A'
                        product = str(row[3]) if row[3] else 'N/A'
                        amount = f"R$ {float(row[4]):,.2f}" if row[4] else "R$ 0,00"
                        date = row[5].strftime('%d/%m/%Y %H:%M') if row[5] else 'N/A'
                        
                        abandoned_table_data.append([platform, email[:30], name[:25], product[:30], amount, date])
                    except Exception as row_error:
                        logger.warning(f"⚠️ Erro ao processar linha de abandono: {row_error}")
                        continue
                
                if len(abandoned_table_data) > 1:  # Se há dados além do cabeçalho
                    abandoned_table = Table(
                        abandoned_table_data, 
                        colWidths=[2.5*cm, 4.5*cm, 3.5*cm, 4*cm, 2*cm, 2.5*cm],
                        repeatRows=1
                    )
                    abandoned_table.setStyle(TableStyle([
                        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#1e3a8a")),
                        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
                        ('GRID', (0,0), (-1,-1), 1, colors.grey),
                        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0,0), (-1,-1), 8),
                        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                        ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.whitesmoke, colors.lightgrey])
                    ]))
                    story.append(abandoned_table)
                else:
                    story.append(Paragraph("Nenhum dado de abandono processado com sucesso.", styles['Normal']))
            else:
                story.append(Paragraph("Nenhum carrinho abandonado no período selecionado.", styles['Normal']))
            
            logger.info("📋 Segunda página do PDF construída")
            
        except Exception as abandon_error:
            logger.error(f"❌ Erro ao processar abandonos: {abandon_error}")
            story.append(Paragraph(f"Erro ao processar dados de abandono: {str(abandon_error)}", styles['Normal']))

        # 7. Construção final do PDF
        try:
            doc.build(story)
            buffer.seek(0)
            logger.info("✅ PDF construído com sucesso")
            
            # Gerar nome do arquivo
            filename = f"relatorio_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            
            return send_file(
                buffer, 
                as_attachment=True, 
                download_name=filename, 
                mimetype='application/pdf'
            )
            
        except Exception as build_error:
            logger.error(f"❌ Erro ao construir PDF final: {build_error}")
            return jsonify({"error": f"Erro ao finalizar PDF: {str(build_error)}"}), 500
            
    except Exception as e:
        logger.exception("❌ Erro crítico na geração de PDF")
        return jsonify({
            "error": "Falha interna ao gerar PDF", 
            "details": str(e),
            "type": type(e).__name__
        }), 500


# ==================== ALTERAÇÃO 3: NOVA ROTA DE COMPATIBILIDADE PARA EXCEL ====================
@app.route("/api/export-excel", methods=['POST'])
def export_excel_simple():
    """
    Rota de compatibilidade para exportação Excel simples
    Redireciona para o endpoint mais robusto
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Redirecionar para o endpoint robusto com os parâmetros
        from flask import redirect, url_for
        return redirect(url_for('export.export_excel', start_date=start_date, end_date=end_date, upload_drive='true'))
        
    except Exception as e:
        logger.error(f"Erro na exportação Excel simples: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)