from flask import Blueprint, request, jsonify
from db import salvar_evento
import hmac
import hashlib
import os

hubla_bp = Blueprint('hubla', __name__)

@hubla_bp.route("/", methods=["POST"])
def receber_webhook():
    # 1. Validar assinatura (se a Hubla enviar)
    signature = request.headers.get('X-Hubla-Signature')
    secret = os.getenv("HUBLA_WEBHOOK_SECRET")  # Adicione ao .env
    
    if secret and signature:
        expected_sign = hmac.new(
            secret.encode(),
            request.data,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_sign):
            return jsonify({"error": "Assinatura inválida"}), 403

    # 2. Processar payload
    payload = request.json
    evento = payload.get('event')  # Ex: 'sale_approved'
    dados = payload.get('data')    # Dados da venda/afiliado

    # 3. Salvar no banco (exemplo ajustado para estrutura Hubla)
    try:
        salvar_evento(
            plataforma="hubla",
            tipo=evento,
            payload={
                "transacao_id": dados.get('id'),
                "valor": dados.get('amount'),
                "afiliado": dados.get('affiliate_email'),
                "status": dados.get('status'),
                "raw_data": payload  # Dados completos
            }
        )
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500