import os
import json
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import all webhook handlers and utilities
from drive_upload import upload_or_replace_file
from webhooks.kirvano import kirvano_bp
from webhooks.hubla import hubla_bp
from webhooks.braip import braip_bp
from webhooks.cakto import cakto_bp
from export_excel import export_bp
from db import get_db_connection, exportar_xlsx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler()
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
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "message": "Webhook collection and export service is running",
            "stats": {
                "recent_webhooks_1h": recent_webhooks,
                "database": "connected"
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
                    "scheduled": "/export/excel/scheduled",
                    "stats": "/export/stats"
                },
                "management": {
                    "status": "/status",
                    "dashboard": "/dashboard",
                    "recent": "/recent"
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
        
        return {
            "status": "operational",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "webhook_receivers": "active",
                "database": "connected",
                "export_service": "ready"
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
    Simple dashboard view with basic statistics
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
        
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Webhook Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .stat-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
                .stat-card {{ background: #e3f2fd; padding: 20px; border-radius: 8px; text-align: center; }}
                .stat-number {{ font-size: 2em; font-weight: bold; color: #1976d2; }}
                .stat-label {{ color: #666; margin-top: 5px; }}
                .section {{ margin: 30px 0; }}
                h1 {{ color: #333; text-align: center; }}
                h2 {{ color: #555; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; }}
                .endpoint {{ background: #f8f9fa; padding: 10px; margin: 5px 0; border-left: 4px solid #007bff; }}
                .healthy {{ color: #28a745; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔗 Webhook Management Dashboard</h1>
                
                <div class="section">
                    <p class="healthy">✅ System Status: Operational</p>
                    <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="section">
                    <h2>📊 Statistics (Last 30 Days)</h2>
                    <div class="stat-grid">
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[0]:,}</div>
                            <div class="stat-label">Total Events</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[1]:,}</div>
                            <div class="stat-label">Unique Customers</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">R$ {overall_stats[2]:,.2f}</div>
                            <div class="stat-label">Total Revenue</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">{overall_stats[3]}</div>
                            <div class="stat-label">Active Platforms</div>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>📅 Today's Activity</h2>
                    <div class="stat-grid">
                        <div class="stat-card">
                            <div class="stat-number">{today_stats[0]:,}</div>
                            <div class="stat-label">Events Today</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-number">R$ {today_stats[1]:,.2f}</div>
                            <div class="stat-label">Revenue Today</div>
                        </div>
                    </div>
                </div>
                
                <div class="section">
                    <h2>🔗 Available Endpoints</h2>
                    <h3>Webhook Receivers:</h3>
                    <div class="endpoint">POST /webhook/kirvano - Kirvano platform webhooks</div>
                    <div class="endpoint">POST /webhook/hubla - Hubla platform webhooks</div>
                    <div class="endpoint">POST /webhook/braip - Braip platform webhooks</div>
                    <div class="endpoint">POST /webhook/cakto - Cakto platform webhooks</div>
                    
                    <h3>Data Export:</h3>
                    <div class="endpoint">GET /export/excel - Download Excel report</div>
                    <div class="endpoint">GET /export/stats - Export statistics</div>
                    
                    <h3>Management:</h3>
                    <div class="endpoint">GET /status - Detailed system status</div>
                    <div class="endpoint">GET /recent - Recent webhook events</div>
                </div>
            </div>
        </body>
        </html>
        """
    except Exception as e:
        logger.error(f"Dashboard failed: {e}")
        return f"<h1>Dashboard Error</h1><p>{str(e)}</p>", 500

@app.route("/export/quick-excel")
def quick_excel_export():
    try:
        platform = request.args.get('platform')
        days = request.args.get('days', 30, type=int)
        
        # Calculate date range
        start_date = datetime.now() - timedelta(days=days)
        
        # Generate Excel file
        excel_file = exportar_xlsx(
            plataforma=platform,
            start_date=start_date,
            end_date=datetime.now()
        )
        
        # Create filename
        platform_suffix = f"_{platform}" if platform else "_all_platforms"
        filename = f"webhooks_report{platform_suffix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Save to temporary file for upload
        temp_path = f"/tmp/{filename}"
        with open(temp_path, 'wb') as f:
            f.write(excel_file.getvalue())
        
        # Upload to Google Drive
        upload_or_replace_file(temp_path)
        
        # Return the file for download
        return send_file(
            excel_file,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.error(f"Quick Excel export failed: {e}")
        return {"error": str(e)}, 500
    
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
    
    logger.info(f"Starting webhook application on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(
        host='0.0.0.0',
        port=port,
        debug=debug
    )

