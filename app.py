import os
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import all webhook handlers and utilities
from drive_upload import upload_or_replace_file, get_drive_service, find_file_id_by_name
from webhooks.kirvano import kirvano_bp
from webhooks.hubla import hubla_bp
from webhooks.braip import braip_bp
from webhooks.cakto import cakto_bp
from export_excel import export_bp
from db import get_db_connection
import sys
import io

# Força UTF-8 no console mesmo no Windows
stream_utf8 = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log', encoding='utf-8'),
        logging.StreamHandler(stream_utf8)
    ]
)

logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key')

# Register all webhook blueprints
app.register_blueprint(kirvano_bp, url_prefix="/webhook/kirvano")
app.register_blueprint(hubla_bp, url_prefix="/webhook/hubla")
app.register_blueprint(braip_bp, url_prefix="/webhook/braip")
app.register_blueprint(cakto_bp, url_prefix="/webhook/cakto")
app.register_blueprint(export_bp, url_prefix="/export")

def test_google_drive_connection():
    """
    Testa a conexão com o Google Drive
    """
    try:
        service = get_drive_service()
        # Fazer uma consulta simples para testar
        response = service.files().list(pageSize=1).execute()
        return True, "Conectado"
    except Exception as e:
        return False, str(e)

@app.route("/")
def health_check():
    """
    Main health check endpoint with comprehensive system status
    """
    try:
        # Test database connection
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM webhooks WHERE created_at >= NOW() - INTERVAL '1 hour'")
            recent_webhooks = cursor.fetchone()[0]
        conn.close()
        
        # Test Google Drive connection
        drive_connected, drive_status = test_google_drive_connection()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "message": "Webhook collection and export service is running",
            "stats": {
                "recent_webhooks_1h": recent_webhooks,
                "database": "connected",
                "google_drive": "connected" if drive_connected else "error"
            },
            "services": {
                "database": "✅ Conectado",
                "google_drive": f"✅ {drive_status}" if drive_connected else f"❌ {drive_status}",
                "webhook_receivers": "✅ Ativos",
                "export_service": "✅ Funcionando"
            },
            "available_endpoints": {
                "webhooks": {
                    "kirvano": "/webhook/kirvano",
                    "hubla": "/webhook/hubla", 
                    "braip": "/webhook/braip",
                    "cakto": "/webhook/cakto"
                },
                "exports": {
                    "excel": "/export/excel",
                    "excel_scheduled": "/export/excel/scheduled",
                    "stats": "/export/stats",
                    "backup": "/export/backup",
                    "quick_excel": "/export/quick-excel",
                    "abandoned_carts_pdf": "/export/abandoned-carts-pdf"
                },
                "analytics": {
                    "abandonment": "/analytics/abandonment"
                },
                "management": {
                    "status": "/status",
                    "dashboard": "/dashboard",
                    "recent": "/recent",
                    "drive_status": "/drive/status"
                }
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }, 500

@app.route("/export/abandoned-carts-pdf")
def export_abandoned_carts_pdf():
    """
    Exporta lista de carrinhos abandonados em PDF
    """
    try:
        days = request.args.get('days', 30, type=int)
        platform = request.args.get('platform')
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Query para carrinhos abandonados com filtros específicos
            query = """
                SELECT 
                    platform,
                    customer_email,
                    customer_name,
                    product_name,
                    amount,
                    created_at,
                    utm_source
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s
                AND (
                    (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                    OR (platform = 'hubla' AND event_type = 'CanceledSale')  
                    OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                )
            """
            params = [f"{days} days"]
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            query += " ORDER BY created_at DESC, platform, customer_email"
            
            cursor.execute(query, params)
            abandoned_data = cursor.fetchall()
            
        conn.close()
        
        if not abandoned_data:
            return jsonify({"error": "Nenhum carrinho abandonado encontrado"}), 404
        
        # Gerar PDF
        from reportlab.lib.pagesizes import letter, A4
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        import io
        
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=18)
        
        # Estilos
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], alignment=TA_CENTER, spaceAfter=30)
        subtitle_style = ParagraphStyle('CustomSubtitle', parent=styles['Heading2'], fontSize=14, spaceAfter=20)
        normal_style = styles['Normal']
        
        story = []
        
        # Título
        title = Paragraph(f"📊 Relatório de Carrinhos Abandonados", title_style)
        story.append(title)
        
        # Informações do relatório
        timestamp = datetime.now().strftime('%d/%m/%Y às %H:%M:%S')
        platform_filter = f" - Plataforma: {platform.upper()}" if platform else " - Todas as plataformas"
        info = Paragraph(f"Gerado em: {timestamp}{platform_filter}<br/>Período: Últimos {days} dias<br/>Total de registros: {len(abandoned_data)}", normal_style)
        story.append(info)
        story.append(Spacer(1, 20))
        
        # Estatísticas por plataforma
        platform_stats = {}
        total_value = 0
        
        for row in abandoned_data:
            platform_name = row[0]
            amount = row[4] if row[4] else 0
            
            if platform_name not in platform_stats:
                platform_stats[platform_name] = {'count': 0, 'value': 0, 'customers': set()}
            
            platform_stats[platform_name]['count'] += 1
            platform_stats[platform_name]['value'] += amount
            platform_stats[platform_name]['customers'].add(row[1])  # customer_email
            total_value += amount
        
        # Resumo por plataforma
        story.append(Paragraph("📈 Resumo por Plataforma", subtitle_style))
        
        summary_data = [['Plataforma', 'Abandonos', 'Clientes Únicos', 'Valor Perdido']]
        for platform_name, stats in platform_stats.items():
            summary_data.append([
                platform_name.upper(),
                f"{stats['count']:,}",
                f"{len(stats['customers']):,}",
                f"R$ {stats['value']:,.2f}"
            ])
        
        # Total
        unique_customers = len(set(row[1] for row in abandoned_data))
        summary_data.append(['TOTAL', f"{len(abandoned_data):,}", f"{unique_customers:,}", f"R$ {total_value:,.2f}"])
        
        summary_table = Table(summary_data)
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -2), colors.beige),
            ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 30))
        
        # Lista detalhada
        story.append(Paragraph("📋 Lista Detalhada de Carrinhos Abandonados", subtitle_style))
        
        # Preparar dados para tabela principal
        table_data = [['Plataforma', 'Email', 'Nome', 'Produto', 'Valor', 'Data', 'UTM Source']]
        
        for row in abandoned_data:
            platform_name, email, name, product, amount, date, utm = row
            table_data.append([
                platform_name.upper(),
                email or 'N/A',
                name or 'N/A',
                (product or 'N/A')[:30] + '...' if product and len(product) > 30 else (product or 'N/A'),
                f"R$ {amount:,.2f}" if amount else 'R$ 0,00',
                date.strftime('%d/%m/%Y %H:%M') if date else 'N/A',
                (utm or 'N/A')[:20] + '...' if utm and len(utm) > 20 else (utm or 'N/A')
            ])
        
        # Dividir em páginas se necessário (máximo 25 linhas por página)
        rows_per_page = 25
        
        for i in range(0, len(table_data) - 1, rows_per_page):
            # Cabeçalho em cada página
            page_data = [table_data[0]]  # header
            page_data.extend(table_data[i+1:i+rows_per_page+1])
            
            # Criar tabela
            table = Table(page_data, colWidths=[1*inch, 2.2*inch, 1.5*inch, 2*inch, 1*inch, 1.2*inch, 1.1*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 10),
                ('FONTSIZE', (0, 1), (-1, -1), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
            ]))
            
            story.append(table)
            
            # Page break se não for a última página
            if i + rows_per_page < len(table_data) - 1:
                story.append(PageBreak())
        
        # Adicionar insights finais
        story.append(Spacer(1, 30))
        story.append(Paragraph("💡 Insights e Oportunidades", subtitle_style))
        
        insights = [
            f"• Total de {total_value:,.2f} reais em vendas perdidas por abandono de carrinho",
            f"• {unique_customers:,} clientes únicos abandonaram seus carrinhos",
            f"• Ticket médio abandonado: R$ {(total_value/len(abandoned_data)):,.2f}" if len(abandoned_data) > 0 else "• Sem dados para calcular ticket médio",
            f"• Plataforma com mais abandonos: {max(platform_stats.keys(), key=lambda x: platform_stats[x]['count']).upper()}" if platform_stats else "• Sem dados de plataforma",
        ]
        
        # Recomendações específicas por plataforma
        if 'kirvano' in platform_stats:
            insights.append(f"• Kirvano: {platform_stats['kirvano']['count']} carrinhos abandonados - considere campanhas de retargeting")
        if 'hubla' in platform_stats:
            insights.append(f"• Hubla: {platform_stats['hubla']['count']} vendas canceladas - revisar processo de checkout")
        if 'cakto' in platform_stats:
            insights.append(f"• Cakto: {platform_stats['cakto']['count']} abandonos no checkout - otimizar experiência de pagamento")
        
        for insight in insights:
            story.append(Paragraph(insight, normal_style))
        
        # Rodapé
        story.append(Spacer(1, 20))
        footer = Paragraph(f"Relatório gerado automaticamente pelo Sistema de Análise de Webhooks em {timestamp}", 
                          ParagraphStyle('Footer', parent=styles['Normal'], fontSize=8, textColor=colors.grey))
        story.append(footer)
        
        # Gerar PDF
        doc.build(story)
        buffer.seek(0)
        
        filename = f"carrinhos_abandonados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )
        
    except Exception as e:
        logger.error(f"PDF export failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/analytics/abandonment")
def abandonment_analytics():
    """
    Endpoint JSON para estatísticas de abandono
    """
    try:
        days = request.args.get('days', 30, type=int)
        platform = request.args.get('platform')
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Estatísticas gerais de abandono
            query = """
                SELECT 
                    platform,
                    COUNT(*) as abandonment_count,
                    COUNT(DISTINCT customer_email) as unique_customers,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as lost_revenue,
                    AVG(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as avg_abandoned_value
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s
                AND (
                    (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                    OR (platform = 'hubla' AND event_type = 'CanceledSale')  
                    OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                )
            """
            params = [f"{days} days"]
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            query += " GROUP BY platform ORDER BY lost_revenue DESC"
            
            cursor.execute(query, params)
            platform_stats = cursor.fetchall()
            
            # Comparação com vendas efetivas (taxa de abandono)
            cursor.execute("""
                SELECT 
                    platform,
                    -- Vendas efetivas
                    COUNT(CASE WHEN (
                        (platform = 'kirvano' AND event_type IN ('SALE_APPROVED', 'SUBSCRIPTION_RENEWED'))
                        OR (platform = 'hubla' AND event_type = 'NewSale')
                        OR (platform = 'cakto' AND event_type IN ('purchase_approved', 'subscription_renewed'))
                    ) THEN 1 END) as successful_sales,
                    -- Abandonos
                    COUNT(CASE WHEN (
                        (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                        OR (platform = 'hubla' AND event_type = 'CanceledSale')
                        OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                    ) THEN 1 END) as abandonments
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s
                GROUP BY platform
                HAVING COUNT(*) > 0
            """, [f"{days} days"])
            
            conversion_vs_abandonment = cursor.fetchall()
            
            # Evolução temporal (últimos 7 dias)
            cursor.execute("""
                SELECT 
                    DATE(created_at) as date,
                    platform,
                    COUNT(*) as daily_abandonments,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as daily_lost_revenue
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '7 days'
                AND (
                    (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                    OR (platform = 'hubla' AND event_type = 'CanceledSale')  
                    OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                )
                GROUP BY DATE(created_at), platform
                ORDER BY date DESC, platform
            """)
            
            daily_trends = cursor.fetchall()
            
            # Top produtos abandonados
            cursor.execute("""
                SELECT 
                    product_name,
                    platform,
                    COUNT(*) as abandonment_count,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as lost_value
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL %s
                AND product_name IS NOT NULL
                AND product_name != ''
                AND (
                    (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                    OR (platform = 'hubla' AND event_type = 'CanceledSale')  
                    OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                )
                GROUP BY product_name, platform
                ORDER BY lost_value DESC
                LIMIT 10
            """, [f"{days} days"])
            
            top_abandoned_products = cursor.fetchall()
            
        conn.close()
        
        # Processar dados para resposta JSON
        platform_analysis = []
        total_abandonments = 0
        total_lost_revenue = 0
        
        for platform_name, count, customers, revenue, avg_value in platform_stats:
            total_abandonments += count
            total_lost_revenue += revenue
            
            platform_analysis.append({
                "platform": platform_name,
                "abandonment_count": count,
                "unique_customers": customers,
                "lost_revenue": float(revenue),
                "avg_abandoned_value": float(avg_value) if avg_value else 0
            })
        
        # Calcular taxas de abandono por plataforma
        abandonment_rates = {}
        for platform_name, successful, abandonments in conversion_vs_abandonment:
            total_attempts = successful + abandonments
            abandonment_rate = (abandonments / total_attempts * 100) if total_attempts > 0 else 0
            abandonment_rates[platform_name] = {
                "successful_sales": successful,
                "abandonments": abandonments,
                "total_attempts": total_attempts,
                "abandonment_rate": round(abandonment_rate, 2),
                "success_rate": round(100 - abandonment_rate, 2)
            }
        
        # Tendências diárias
        daily_data = {}
        for date, platform_name, count, revenue in daily_trends:
            date_str = date.strftime('%Y-%m-%d')
            if date_str not in daily_data:
                daily_data[date_str] = {}
            daily_data[date_str][platform_name] = {
                "abandonments": count,
                "lost_revenue": float(revenue)
            }
        
        # Top produtos
        top_products = [
            {
                "product_name": product,
                "platform": platform_name,
                "abandonment_count": count,
                "lost_value": float(value)
            }
            for product, platform_name, count, value in top_abandoned_products
        ]
        
        return jsonify({
            "summary": {
                "total_abandonments": total_abandonments,
                "total_lost_revenue": round(total_lost_revenue, 2),
                "avg_abandoned_value": round(total_lost_revenue / total_abandonments, 2) if total_abandonments > 0 else 0,
                "period_days": days,
                "platforms_analyzed": len(platform_analysis)
            },
            "platform_breakdown": platform_analysis,
            "abandonment_rates": abandonment_rates,
            "daily_trends": daily_data,
            "top_abandoned_products": top_products,
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Abandonment analytics failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/status")
def get_detailed_status():
    """
    Detailed system status with metrics by platform
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get stats by platform
            cursor.execute("""
                SELECT 
                    platform,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT customer_email) as unique_customers,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    MAX(created_at) as last_event
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY platform
                ORDER BY total_events DESC
            """)
            platform_stats = cursor.fetchall()
            
            # Get recent error rates
            cursor.execute("""
                SELECT 
                    platform,
                    COUNT(*) as total,
                    SUM(CASE WHEN status LIKE '%error%' OR status LIKE '%fail%' THEN 1 ELSE 0 END) as errors
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
                GROUP BY platform
            """)
            error_stats = cursor.fetchall()
            
        conn.close()
        
        # Test Google Drive
        drive_connected, drive_message = test_google_drive_connection()
        
        return {
            "status": "operational",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "webhook_receivers": "active",
                "database": "connected",
                "export_service": "ready",
                "google_drive": "connected" if drive_connected else "error",
                "google_drive_message": drive_message
            },
            "last_24h_stats": {
                platform: {
                    "total_events": total,
                    "unique_customers": customers,
                    "total_revenue": float(revenue) if revenue else 0,
                    "last_event": last_event.isoformat() if last_event else None
                }
                for platform, total, customers, revenue, last_event in platform_stats
            },
            "error_rates_1h": {
                platform: {
                    "total_requests": total,
                    "errors": errors,
                    "error_rate": f"{(errors/total*100):.1f}%" if total > 0 else "0%"
                }
                for platform, total, errors in error_stats
            }
        }
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.route("/recent")
def get_recent_webhooks():
    """
    Get recent webhook events for monitoring
    """
    try:
        limit = request.args.get('limit', 50, type=int)
        platform = request.args.get('platform')
        
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = """
                SELECT 
                    id, platform, event_type, customer_email, customer_name,
                    product_name, amount, status, created_at
                FROM webhooks 
                WHERE 1=1
            """
            params = []
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            events = cursor.fetchall()
            
        conn.close()
        
        return {
            "recent_events": [
                {
                    "id": event[0],
                    "platform": event[1],
                    "event_type": event[2],
                    "customer_email": event[3],
                    "customer_name": event[4],
                    "product_name": event[5],
                    "amount": float(event[6]) if event[6] else None,
                    "status": event[7],
                    "created_at": event[8].isoformat()
                }
                for event in events
            ],
            "total_returned": len(events),
            "filters": {
                "platform": platform,
                "limit": limit
            }
        }
    except Exception as e:
        logger.error(f"Recent webhooks fetch failed: {e}")
        return {"error": str(e)}, 500

@app.route("/dashboard")
def dashboard():
    """
    Enhanced dashboard with profit analysis and abandonment stats
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # === INÍCIO DAS QUERIES PARA O DASHBOARD ===

            # Step 1: Overall stats (last 30 days)
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT customer_email) as unique_customers
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '30 days'
            """)
            overall_stats = cursor.fetchone()
            overall_stats = tuple(val or 0 for val in overall_stats) if overall_stats else (0, 0)

            # Step 2: Real profit metrics (last 30 days)
            cursor.execute("""
                SELECT 
                    COUNT(*) as profit_events,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as actual_profit
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '30 days'
                AND (
                    (platform = 'kirvano' AND event_type IN ('SALE_APPROVED', 'SUBSCRIPTION_RENEWED'))
                    OR (platform = 'hubla' AND event_type = 'NewSale')
                    OR (platform = 'cakto' AND event_type IN ('purchase_approved', 'subscription_renewed'))
                )
            """)
            profit_stats_30d = cursor.fetchone()
            profit_stats_30d = tuple(val or 0 for val in profit_stats_30d) if profit_stats_30d else (0, 0)
            
            # Step 3: Today's profit metrics
            cursor.execute("""
                SELECT 
                    COUNT(*) as profit_events_today,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as profit_today
                FROM webhooks 
                WHERE DATE(created_at) = CURRENT_DATE
                AND (
                    (platform = 'kirvano' AND event_type IN ('SALE_APPROVED', 'SUBSCRIPTION_RENEWED'))
                    OR (platform = 'hubla' AND event_type = 'NewSale')
                    OR (platform = 'cakto' AND event_type IN ('purchase_approved', 'subscription_renewed'))
                )
            """)
            profit_today_stats = cursor.fetchone()
            profit_today_stats = tuple(val or 0 for val in profit_today_stats) if profit_today_stats else (0, 0)

            # Step 4: Abandoned cart statistics (last 30 days)
            cursor.execute("""
                SELECT
                    COUNT(*) as abandoned_count,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as lost_revenue
                FROM webhooks
                WHERE created_at >= NOW() - INTERVAL '30 days'
                AND (
                    (platform = 'kirvano' AND event_type = 'ABANDONED_CART')
                    OR (platform = 'hubla' AND event_type = 'CanceledSale')  
                    OR (platform = 'cakto' AND event_type = 'checkout_abandonment')
                )
            """)
            abandonment_stats = cursor.fetchone()
            abandonment_stats = tuple(val or 0 for val in abandonment_stats) if abandonment_stats else (0, 0)

            # Step 5: Profit breakdown by platform
            cursor.execute("""
                SELECT 
                    platform,
                    COUNT(*) as profit_events,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as platform_profit
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '30 days'
                AND (
                    (platform = 'kirvano' AND event_type IN ('SALE_APPROVED', 'SUBSCRIPTION_RENEWED'))
                    OR (platform = 'hubla' AND event_type = 'NewSale')
                    OR (platform = 'cakto' AND event_type IN ('purchase_approved', 'subscription_renewed'))
                )
                GROUP BY platform
                ORDER BY platform_profit DESC
            """)
            profit_by_platform = cursor.fetchall()
            
        conn.close()
        
        # Calculate key business ratios
        profit_conversion_rate = (profit_stats_30d[0] / overall_stats[0] * 100) if overall_stats[0] > 0 else 0
        
        # === INÍCIO DA GERAÇÃO DO HTML DO DASHBOARD ===
        
        # --- CORREÇÃO: INICIALIZAR E CRIAR A VARIÁVEL 'abandonment_html' ---
        abandonment_html = ""
        if abandonment_stats and abandonment_stats[0] > 0:
            abandonment_html = f"""
                <div class="stat-card abandoned">
                    <div class="stat-number">{abandonment_stats[0]:,}</div>
                    <div class="stat-label">Carrinhos Abandonados</div>
                    <div class="stat-sublabel">Oportunidades de recuperação</div>
                </div>
                <div class="stat-card abandoned">
                    <div class="stat-number">R$ {abandonment_stats[1]:,.2f}</div>
                    <div class="stat-label">Receita Potencial Perdida</div>
                    <div class="stat-sublabel">Valor total nos carrinhos</div>
                </div>
            """
        else:
            abandonment_html = """
                <div class="stat-card">
                    <div class="stat-number">🎉 0</div>
                    <div class="stat-label">Carrinhos Abandonados</div>
                    <div class="stat-sublabel">Nenhum abandono nos últimos 30 dias!</div>
                </div>
            """

        # Platform profit breakdown
        platform_breakdown_html = ""
        if profit_by_platform:
            for platform, events, profit in profit_by_platform:
                percentage = (profit / profit_stats_30d[1] * 100) if profit_stats_30d[1] > 0 else 0
                platform_breakdown_html += f"""
                    <div class="platform-card">
                        <strong>{platform.upper()}</strong><br>
                        <span class="platform-events">{events:,} vendas</span><br>
                        <span class="platform-profit">R$ {profit:,.2f}</span><br>
                        <span class="platform-percentage">{percentage:.1f}% do lucro total</span>
                    </div>
                """
        else:
            platform_breakdown_html = """
                <div class="alert alert-info" style="grid-column: 1/-1; text-align: center;">
                    <strong>ℹ️ Nenhuma venda registrada nos últimos 30 dias.</strong>
                </div>
            """

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard de Análise de Webhooks - Inteligência de Negócios</title>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 40px; background: #f5f7fa; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin: 20px 0; }}
                .stat-card {{ background: #fff; color: #333; padding: 25px; border-radius: 12px; text-align: center; transition: transform 0.3s; box-shadow: 0 2px 10px rgba(0,0,0,0.08); border-top: 4px solid #667eea; }}
                .stat-card:hover {{ transform: translateY(-5px); }}
                .stat-card.profit {{ border-top-color: #28a745; }}
                .stat-card.abandoned {{ border-top-color: #dc3545; }}
                .stat-number {{ font-size: 2.2em; font-weight: bold; margin-bottom: 5px; }}
                .stat-label {{ font-size: 0.95em; color: #555; }}
                .stat-sublabel {{ color: #777; font-size: 0.8em; margin-top: 5px; }}
                .section {{ margin: 35px 0; }}
                .platforms-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin: 20px 0; }}
                .platform-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; border-top: 3px solid #28a745; }}
                .platform-events {{ color: #6c757d; font-size: 0.9em; }}
                .platform-profit {{ color: #28a745; font-weight: bold; font-size: 1.1em; }}
                .platform-percentage {{ color: #6c757d; font-size: 0.8em; font-style: italic; }}
                h1 {{ color: #2c3e50; text-align: center; margin-bottom: 10px; }}
                h2 {{ color: #34495e; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                .healthy {{ color: #28a745; font-weight: bold; }}
                .action-buttons {{ margin: 20px 0; text-align: center; }}
                .btn {{ background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; margin: 5px; display: inline-block; font-weight: bold; transition: background 0.2s; }}
                .btn:hover {{ background: #0056b3; }}
                .btn-danger {{ background: #c82333; }}
                .btn-danger:hover {{ background: #a81d2a; }}
                .alert {{ padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .alert-info {{ background: #d1ecf1; border: 1px solid #bee5eb; color: #0c5460; }}
                .methodology-box {{ background: #e8f4f8; border: 1px solid #bee5eb; border-radius: 8px; padding: 20px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🎯 Dashboard de Inteligência de Negócios</h1>
                <p style="text-align: center; color: #6c757d;">Sistema de Análise de Webhooks com Filtros de Lucro e Abandono</p>
                
                <div class="methodology-box">
                    <h3>📋 Metodologia de Análise</h3>
                    <p>Este painel diferencia <strong>atividade total</strong> de <strong>eventos que geram lucro</strong> ou representam <strong>perdas potenciais</strong> (carrinhos abandonados).</p>
                    <p><strong>Filtros de lucro:</strong> Kirvano (`SALE_APPROVED`, `SUBSCRIPTION_RENEWED`), Hubla (`NewSale`), Cakto (`purchase_approved`, `subscription_renewed`).</p>
                    <p><strong>Filtros de abandono:</strong> Kirvano (`ABANDONED_CART`), Hubla (`CanceledSale`), Cakto (`checkout_abandonment`).</p>
                </div>
                
                <div class="section">
                    <h2>📅 Visão Geral (Últimos 30 Dias)</h2>
                    <div class="stat-grid">
                        <div class="stat-card profit">
                            <div class="stat-number">R$ {profit_stats_30d[1]:,.2f}</div>
                            <div class="stat-label">💰 Lucro Real</div>
                            <div class="stat-sublabel">{profit_stats_30d[0]:,} vendas efetivas</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[0]:,}</div>
                            <div class="stat-label">Total de Eventos</div>
                             <div class="stat-sublabel">{profit_conversion_rate:.1f}% de conversão</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[1]:,}</div>
                            <div class="stat-label">Clientes Únicos</div>
                            <div class="stat-sublabel">Emails distintos</div>
                        </div>
                        <div class="stat-card profit">
                            <div class="stat-number">R$ {profit_today_stats[1]:,.2f}</div>
                            <div class="stat-label">💸 Lucro Hoje</div>
                            <div class="stat-sublabel">{profit_today_stats[0]:,} vendas hoje</div>
                        </div>
                    </div>
                </div>

                <div class="section">
                    <h2>🛒 Análise de Recuperação (Últimos 30 Dias)</h2>
                    <div class="stat-grid">
                        {abandonment_html}
                    </div>
                    <div class="action-buttons">
                        <a href="/export/abandoned-carts-pdf" class="btn btn-danger">📄 Gerar Relatório PDF de Recuperação</a>
                    </div>
                </div>

                <div class="section">
                    <h2>🏢 Distribuição de Lucro por Plataforma</h2>
                    <div class="platforms-grid">
                        {platform_breakdown_html}
                    </div>
                </div>

                <div class="section">
                    <h2>⚡ Ações Rápidas e Relatórios</h2>
                    <div class="action-buttons">
                        <a href="/export/excel" class="btn">📊 Exportar Excel Completo</a>
                        <a href="/analytics/abandonment" class="btn">📈 Ver API de Abandono (JSON)</a>
                        <a href="/recent" class="btn">🕐 Eventos Recentes</a>
                    </div>
                </div>
            </div>
            
            <script>
                setTimeout(function(){{
                    location.reload();
                }}, 120000);
            </script>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Dashboard failed: {e}")
        return f"<h1>Dashboard Error</h1><p>{str(e)}</p>", 500

@app.route("/export/quick-excel")
def quick_excel_export():
    """
    Quick Excel export maintaining ALL data - no profit filters applied
    This preserves the complete audit trail as requested
    """
    try:
        platform = request.args.get('platform')
        days = request.args.get('days', 30, type=int)
        
        # Query for ALL data - this is intentionally comprehensive
        query = """
            SELECT 
                id, platform, event_type, webhook_id, transaction_id,
                customer_email, customer_name, customer_document,
                product_name, product_id, amount, currency, 
                payment_method, status, commission_amount,
                affiliate_email, created_at
            FROM webhooks 
            WHERE created_at >= NOW() - INTERVAL %s
        """
        params = [f"{days} days"]

        if platform:
            query += " AND platform = %s"
            params.append(platform)
        
        query += " ORDER BY created_at DESC"
        
        # Execute query
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        if not data:
            return jsonify({"error": "Nenhum dado encontrado"}), 404
        
        # Generate Excel with ALL data
        from export_excel import create_excel_report
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        platform_suffix = f"_{platform}" if platform else "_all_platforms"
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
        logger.error(f"Quick Excel export failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/drive/status", methods=["GET"])
def drive_status():
    """
    Check Google Drive integration status
    """
    try:
        service = get_drive_service()
        
        # List recent files to test connection
        response = service.files().list(
            q="name contains 'webhooks'",
            orderBy='modifiedTime desc',
            pageSize=10,
            fields="files(id, name, modifiedTime, size)"
        ).execute()
        
        files = response.get('files', [])
        
        # Get storage quota information if available
        try:
            about = service.about().get(fields="storageQuota").execute()
            quota = about.get('storageQuota', {})
            
            total_storage = int(quota.get('limit', 0))
            used_storage = int(quota.get('usage', 0))
            
            storage_info = {
                "total_gb": round(total_storage / (1024**3), 2),
                "used_gb": round(used_storage / (1024**3), 2),
                "available_gb": round((total_storage - used_storage) / (1024**3), 2),
                "usage_percentage": round((used_storage / total_storage) * 100, 1) if total_storage > 0 else 0
            }
        except:
            storage_info = {"error": "Unable to get storage info"}
        
        return jsonify({
            "status": "connected",
            "message": "Google Drive conectado com sucesso",
            "recent_files": [
                {
                    "name": file['name'],
                    "id": file['id'],
                    "modified": file['modifiedTime'],
                    "size": file.get('size', 'N/A')
                }
                for file in files
            ],
            "total_webhook_files": len(files),
            "storage": storage_info
        })
        
    except Exception as e:
        logger.error(f"Drive status check failed: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.errorhandler(404)
def not_found(error):
    return {
        "error": "Endpoint not found",
        "message": "Check available endpoints at /",
        "timestamp": datetime.now().isoformat()
    }, 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return {
        "error": "Internal server error",
        "message": "Please check logs for details",
        "timestamp": datetime.now().isoformat()
    }, 500

if __name__ == "__main__":
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    
    logger.info(f"🚀 Starting webhook application on port {port}")
    logger.info(f"🔧 Debug mode: {debug}")
    
    # Test Google Drive connection on startup
    drive_connected, drive_message = test_google_drive_connection()
    logger.info(f"📁 Google Drive: {'✅ Connected' if drive_connected else f'❌ Error - {drive_message}'}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug
    )