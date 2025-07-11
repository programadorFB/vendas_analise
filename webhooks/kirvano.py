
from flask import Blueprint, request
from db import salvar_evento
import hmac
import hashlib
import os

kirvano_bp = Blueprint('kirvano', __name__)

@kirvano_bp.route("/", methods=["POST"])
def receber():
    payload = request.json
    
    # Validação de assinatura (se a Kirvano usar)
    signature = request.headers.get('X-Kirvano-Signature')
    secret = os.getenv("KIRVANO_WEBHOOK_SECRET")
    
    if secret and signature:
        expected_signature = hmac.new(
            secret.encode(),
            request.data,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_signature):
            return {"error": "Assinatura inválida"}, 403

    try:
        # Mapear dados da Kirvano para formato padrão
        dados_padronizados = {
            'webhook_id': payload.get('webhook_id'),
            'customer_email': payload.get('customer_email'),
            'customer_name': payload.get('customer_name'),
            'customer_document': payload.get('customer_document'),
            'product_name': payload.get('product_name'),
            'product_id': payload.get('product_id'),
            'transaction_id': payload.get('transaction_id'),
            'amount': payload.get('amount'),
            'currency': payload.get('currency', 'BRL'),
            'payment_method': payload.get('payment_method'),
            'status': payload.get('status'),
            'commission_amount': payload.get('commission_amount'),
            'affiliate_email': payload.get('affiliate_email'),
            'utm_source': payload.get('utm_source'),
            'utm_medium': payload.get('utm_medium'),
            'sales_link': payload.get('sales_link'),
            **payload  # Adiciona todos os campos originais
        }
        
        salvar_evento("kirvano", payload.get('event_type', 'evento_tipo'), dados_padronizados)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500