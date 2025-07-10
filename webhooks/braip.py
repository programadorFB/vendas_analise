from flask import Blueprint, request, abort
from db import salvar_evento
import hmac
import hashlib
import os

braip_bp = Blueprint('braip', __name__)

@braip_bp.route("/", methods=["POST"])
def receber():
    payload = request.json
    signature = request.headers.get('X-Braip-Signature')
    
    # Validação de assinatura
    secret = os.getenv("BRAIP_WEBHOOK_SECRET")
    if secret:
        expected_signature = hmac.new(
            secret.encode(),
            request.data,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_signature):
            abort(403, description="Assinatura inválida")

    try:
        salvar_evento("braip", payload.get('event_type', 'unknown'), payload)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500