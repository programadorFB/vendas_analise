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
                    "quick_excel": "/export/quick-excel"
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
    Enhanced dashboard view with Google Drive integration
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # Get overall stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT customer_email) as unique_customers,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as total_revenue,
                    COUNT(DISTINCT platform) as active_platforms
                FROM webhooks 
                WHERE created_at >= NOW() - INTERVAL '30 days'
            """)
            overall_stats = cursor.fetchone()
            
            # Get today's stats
            cursor.execute("""
                SELECT 
                    COUNT(*) as events_today,
                    SUM(CASE WHEN amount IS NOT NULL THEN amount ELSE 0 END) as revenue_today
                FROM webhooks 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            today_stats = cursor.fetchone()
            
        conn.close()
        
        # Test Google Drive connection
        drive_connected, drive_status = test_google_drive_connection()
        drive_status_icon = "✅" if drive_connected else "❌"
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Webhook Dashboard</title>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 40px; background: #f5f7fa; }}
                .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.1); }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 20px; margin: 20px 0; }}
                .stat-card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 25px; border-radius: 12px; text-align: center; transition: transform 0.3s; }}
                .stat-card:hover {{ transform: translateY(-5px); }}
                .stat-number {{ font-size: 2.2em; font-weight: bold; margin-bottom: 5px; }}
                .stat-label {{ opacity: 0.9; font-size: 0.95em; }}
                .section {{ margin: 35px 0; }}
                .services-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
                .service-card {{ background: #f8f9fa; padding: 15px; border-radius: 8px; border-left: 4px solid #28a745; }}
                h1 {{ color: #2c3e50; text-align: center; margin-bottom: 10px; }}
                h2 {{ color: #34495e; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                .endpoint {{ background: #f8f9fa; padding: 12px; margin: 8px 0; border-left: 4px solid #007bff; border-radius: 4px; }}
                .endpoint:hover {{ background: #e9ecef; }}
                .healthy {{ color: #28a745; font-weight: bold; }}
                .status-indicator {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }}
                .status-ok {{ background-color: #28a745; }}
                .status-error {{ background-color: #dc3545; }}
                .action-buttons {{ margin: 20px 0; text-align: center; }}
                .btn {{ background: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; margin: 5px; display: inline-block; }}
                .btn:hover {{ background: #0056b3; }}
                .btn-success {{ background: #28a745; }}
                .btn-success:hover {{ background: #1e7e34; }}
                .alert {{ padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .alert-info {{ background: #d1ecf1; border: 1px solid #bee5eb; color: #0c5460; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔗 Sistema de Gestão de Webhooks</h1>
                
                <div class="section">
                    <p class="healthy">✅ Sistema Operacional</p>
                    <p>Última atualização: {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}</p>
                </div>
                
                <div class="section">
                    <h2>🔧 Status dos Serviços</h2>
                    <div class="services-grid">
                        <div class="service-card">
                            <div class="status-indicator status-ok"></div>
                            <strong>Banco de Dados</strong><br>
                            <small>Conectado e funcionando</small>
                        </div>
                        <div class="service-card">
                            <div class="status-indicator {'status-ok' if drive_connected else 'status-error'}"></div>
                            <strong>Google Drive</strong><br>
                            <small>{drive_status_icon} {drive_status}</small>
                        </div>
                        <div class="service-card">
                            <div class="status-indicator status-ok"></div>
                            <strong>Webhooks</strong><br>
                            <small>4 plataformas ativas</small>
                        </div>
                        <div class="service-card">
                            <div class="status-indicator status-ok"></div>
                            <strong>Exportação</strong><br>
                            <small>Excel + Drive integrado</small>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📊 Estatísticas (Últimos 30 Dias)</h2>
                    <div class="stat-grid">
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[0]:,}</div>
                            <div class="stat-label">Total de Eventos</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[1]:,}</div>
                            <div class="stat-label">Clientes Únicos</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">R$ {overall_stats[2]:,.2f}</div>
                            <div class="stat-label">Receita Total</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[3]}</div>
                            <div class="stat-label">Plataformas Ativas</div>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📅 Atividade de Hoje</h2>
                    <div class="stat-grid">
                        <div class="stat-card">
                            <div class="stat-number">{today_stats[0]:,}</div>
                            <div class="stat-label">Eventos Hoje</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">R$ {today_stats[1]:,.2f}</div>
                            <div class="stat-label">Receita Hoje</div>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>⚡ Ações Rápidas</h2>
                    <div class="action-buttons">
                        <a href="/export/excel" class="btn">📊 Exportar Excel</a>
                        <a href="/export/stats" class="btn">📈 Relatório Estatísticas</a>
                        <a href="/recent" class="btn">🕐 Eventos Recentes</a>
                        <a href="/status" class="btn">🔍 Status Detalhado</a>
                        <a href="/drive/status" class="btn">📁 Status Google Drive</a>
                    </div>
                    
                    <div class="alert alert-info">
                        <strong>💡 Dica:</strong> Todos os relatórios Excel são automaticamente salvos no Google Drive para backup e compartilhamento.
                    </div>
                </div>
                
                <div class="section">
                    <h2>🔗 Endpoints de Webhooks</h2>
                    <div class="endpoint">
                        <strong>POST /webhook/kirvano</strong> - Recebe webhooks da plataforma Kirvano
                    </div>
                    <div class="endpoint">
                        <strong>POST /webhook/hubla</strong> - Recebe webhooks da plataforma Hubla
                    </div>
                    <div class="endpoint">
                        <strong>POST /webhook/braip</strong> - Recebe webhooks da plataforma Braip
                    </div>
                    <div class="endpoint">
                        <strong>POST /webhook/cakto</strong> - Recebe webhooks da plataforma Cakto
                    </div>
                </div>
                
                <div class="section">
                    <h2>📁 Exportação e Relatórios</h2>
                    <div class="endpoint">
                        <strong>GET /export/excel</strong> - Baixar relatório Excel (com upload automático para Drive)
                        <br><small>Parâmetros: ?platform=nome&days=30&upload_drive=true</small>
                    </div>
                    <div class="endpoint">
                        <strong>GET /export/stats</strong> - Exportar estatísticas detalhadas
                        <br><small>Inclui métricas por plataforma e top produtos</small>
                    </div>
                    <div class="endpoint">
                        <strong>POST /export/excel/scheduled</strong> - Exportação agendada (apenas upload para Drive)
                        <br><small>Para automação e backups programados</small>
                    </div>
                    <div class="endpoint">
                        <strong>POST /export/backup</strong> - Criar backup completo no Google Drive
                        <br><small>Exporta todos os dados históricos</small>
                    </div>
                    <div class="endpoint">
                        <strong>GET /export/quick-excel</strong> - Exportação rápida com filtros
                        <br><small>Upload automático para Drive + download local</small>
                    </div>
                </div>
                
                <div class="section">
                    <h2>🔧 Gestão e Monitoramento</h2>
                    <div class="endpoint">
                        <strong>GET /status</strong> - Status detalhado do sistema com métricas
                    </div>
                    <div class="endpoint">
                        <strong>GET /recent</strong> - Eventos recentes para monitoramento
                        <br><small>Parâmetros: ?limit=50&platform=nome</small>
                    </div>
                    <div class="endpoint">
                        <strong>GET /dashboard</strong> - Esta página do dashboard
                    </div>
                    <div class="endpoint">
                        <strong>GET /drive/status</strong> - Status específico do Google Drive
                        <br><small>Lista arquivos recentes e informações de armazenamento</small>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📋 Informações Técnicas</h2>
                    <p><strong>Integração Google Drive:</strong> {'✅ Ativa' if drive_connected else '❌ Com problemas'}</p>
                    <p><strong>Formatos suportados:</strong> Excel (.xlsx) com múltiplas abas por plataforma</p>
                    <p><strong>Backup automático:</strong> Todos os exports são enviados para Google Drive</p>
                    <p><strong>Logs:</strong> Armazenados em webhook.log para auditoria</p>
                    <p><strong>Organização Drive:</strong> Arquivos salvos na pasta "Webhooks_Reports"</p>
                </div>
            </div>
            
            <script>
                // Auto-refresh a cada 30 segundos
                setTimeout(function(){{
                    location.reload();
                }}, 30000);
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
    Exportação rápida com upload automático para Google Drive
    """
    try:
        platform = request.args.get('platform')
        days = request.args.get('days', 30, type=int)
        
        # Query para dados
        query = """
            SELECT 
                id, platform, event_type, webhook_id, transaction_id,
                customer_email, customer_name, customer_document,
                product_name, product_id, amount, currency, 
                payment_method, status, commission_amount,
                affiliate_email, created_at
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
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
        conn.close()
        
        if not data:
            return jsonify({"error": "Nenhum dado encontrado"}), 404
        
        # Gerar Excel
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
    Verificar status específico do Google Drive
    """
    try:
        service = get_drive_service()
        
        # Listar arquivos recentes para testar
        response = service.files().list(
            q="name contains 'webhooks'",
            orderBy='modifiedTime desc',
            pageSize=10,
            fields="files(id, name, modifiedTime, size)"
        ).execute()
        
        files = response.get('files', [])
        
        # Obter informações de quota se disponível
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

@app.route("/drive/cleanup", methods=["POST"])
def drive_cleanup():
    """
    Limpar arquivos antigos no Google Drive
    """
    try:
        days_old = request.json.get('days_old', 30) if request.json else 30
        
        from drive_upload import delete_old_files
        deleted_count = delete_old_files(days_old=days_old)
        
        return jsonify({
            "status": "success",
            "message": f"Limpeza concluída: {deleted_count} arquivos deletados",
            "deleted_files": deleted_count,
            "cutoff_days": days_old
        })
        
    except Exception as e:
        logger.error(f"Drive cleanup failed: {e}")
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return {
        "error": "Endpoint not found",
        "message": "Check available endpoints at /dashboard",
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