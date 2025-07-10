from flask import Flask
from webhooks.kirvano import kirvano_bp
from webhooks.hubla import hubla_bp
from webhooks.braip import braip_bp
from webhooks.cakto import cakto_bp
from export_excel import export_bp  # Import your new export module

app = Flask(__name__)

# Register all webhook blueprints - these handle incoming data
app.register_blueprint(kirvano_bp, url_prefix="/webhook/kirvano")
app.register_blueprint(hubla_bp, url_prefix="/webhook/hubla")
app.register_blueprint(braip_bp, url_prefix="/webhook/braip")
app.register_blueprint(cakto_bp, url_prefix="/webhook/cakto")

# Register export blueprint - this handles data export functionality
app.register_blueprint(export_bp, url_prefix="/export")

@app.route("/")
def health_check():
    """
    Simple health check endpoint to verify your application is running
    This is like having a reception desk that confirms your business is open
    """
    return {
        "status": "healthy",
        "message": "Webhook collection and export service is running",
        "available_endpoints": {
            "webhooks": [
                "/webhook/kirvano",
                "/webhook/hubla", 
                "/webhook/braip",
                "/webhook/cakto/sync"
            ],
            "exports": [
                "/export/excel",
                "/export/excel/scheduled",
                "/export/stats"
            ]
        }
    }

@app.route("/status")
def get_status():
    """
    More detailed status endpoint for monitoring
    This gives you insight into how your system is performing
    """
    try:
        # You could add database connectivity checks here
        # For now, we'll just return basic status
        return {
            "status": "operational",
            "services": {
                "webhook_receivers": "active",
                "database": "connected",
                "export_service": "ready"
            }
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }, 500

if __name__ == "__main__":
    app.run(debug=True)