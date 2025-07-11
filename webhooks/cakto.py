from flask import Blueprint, request, jsonify
from db import salvar_evento
from dotenv import load_dotenv
import json
import os
import logging

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logger = logging.getLogger(__name__)

# Criação do blueprint
cakto_bp = Blueprint("cakto", __name__)

# Token opcional para segurança do webhook
WEBHOOK_TOKEN = os.getenv("CAKTO_WEBHOOK_TOKEN")

def extract_cakto_data(payload):
    """
    Função para padronizar dados recebidos do webhook da Cakto.
    Mapeia os campos específicos da Cakto para o formato padrão do banco.
    """
    # Extrai dados do customer se existir
    customer = payload.get("customer", {})
    product = payload.get("product", {})
    
    return {
        # Campos padrão do banco
        "webhook_id": payload.get("id"),
        "customer_email": customer.get("email") or payload.get("email"),
        "customer_name": customer.get("name") or payload.get("customer_name"),
        "customer_document": customer.get("document") or payload.get("document"),
        "product_name": product.get("name") or payload.get("product_name"),
        "product_id": product.get("id") or payload.get("product_id"),
        "transaction_id": payload.get("transaction_id") or payload.get("id"),
        "amount": payload.get("total") or payload.get("amount"),
        "currency": payload.get("currency", "BRL"),
        "payment_method": payload.get("payment_method") or payload.get("payment_type"),
        "status": payload.get("status"),
        "commission_amount": payload.get("commission_amount") or payload.get("commission"),
        "affiliate_email": payload.get("affiliate_email"),
        "utm_source": payload.get("utm_source"),
        "utm_medium": payload.get("utm_medium"),
        "sales_link": payload.get("sales_link"),
        "attendant_name": payload.get("attendant_name"),
        "attendant_email": payload.get("attendant_email"),
        
        # Campos específicos da Cakto (mantidos para referência)
        "pedido_id": payload.get("id"),
        "cliente_nome": customer.get("name") or payload.get("customer_name"),
        "produto": product.get("name") or payload.get("product_name"),
        "valor": payload.get("total") or payload.get("amount"),
        "data": payload.get("created_at")
    }

@cakto_bp.route("/", methods=["POST"])
def receber_webhook():
    """
    Endpoint para receber webhooks da Cakto
    """
    # Validação do token (opcional)
    token = request.args.get("token")
    if WEBHOOK_TOKEN and token != WEBHOOK_TOKEN:
        logger.warning("Token inválido recebido no webhook da Cakto.")
        return jsonify({"status": "unauthorized"}), 401

    try:
        # Extrai payload JSON enviado pela Cakto
        payload = request.get_json(force=True)
        logger.info("📩 Webhook recebido da Cakto.")
        
        # Debug: imprimir estrutura do payload
        print(f"\n📥 Webhook Cakto recebido:")
        print(f"Event: {payload.get('event') or payload.get('type')}")
        print(f"Data keys: {list(payload.keys())}")

        # Determina o tipo de evento
        evento = payload.get('event') or payload.get('type') or payload.get('event_type') or 'webhook'

        # Padroniza e salva os dados
        dados_padronizados = extract_cakto_data(payload)
        dados_padronizados["raw_data"] = json.dumps(payload, ensure_ascii=False)
        
        # Debug: verificar se algum campo ainda é dict
        for key, value in dados_padronizados.items():
            if isinstance(value, dict):
                print(f"⚠️  Campo {key} ainda é dict: {value}")
                dados_padronizados[key] = json.dumps(value)

        salvar_evento("cakto", evento, dados_padronizados)

        return jsonify({"status": "success"}), 200

    except Exception as e:
        logger.exception("Erro ao processar webhook da Cakto.")
        print(f"Erro no webhook Cakto: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@cakto_bp.route("/test", methods=["GET"])
def test_webhook():
    """
    Endpoint de teste para verificar se o webhook está funcionando
    """
    return jsonify({
        "status": "ok",
        "message": "Cakto webhook is working",
        "endpoint": "/webhook/cakto"
    })