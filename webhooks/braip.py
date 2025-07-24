# webhooks/braip.py - Missing Braip webhook handler
import json
import hmac
import hashlib
import os
from flask import Blueprint, request, jsonify
from db import salvar_evento

braip_bp = Blueprint('braip', __name__)

def extract_braip_data(payload):
    """
    Extrai dados específicos da Braip baseado na estrutura dos webhooks
    """
    # Braip usa estrutura diferente dependendo do tipo de evento
    transaction = payload.get('transaction', {})
    product = payload.get('product', {})
    customer = payload.get('customer', {})
    affiliate = payload.get('affiliate', {})
    
    # Extrair valor monetário
    amount = None
    if 'value' in transaction:
        # Valor pode vir em centavos ou reais
        raw_value = transaction['value']
        if isinstance(raw_value, str):
            # Remove formatação monetária
            clean_value = raw_value.replace('R$', '').replace(' ', '').replace(',', '.')
            try:
                amount = float(clean_value)
            except ValueError:
                amount = None
        elif isinstance(raw_value, (int, float)):
            # Se for número, pode estar em centavos (> 1000) ou reais
            amount = raw_value / 100 if raw_value > 1000 else raw_value
    
    return {
        # IDs e controle
        'webhook_id': transaction.get('id') or payload.get('transaction_id'),
        'transaction_id': transaction.get('id') or payload.get('transaction_id'),
        'event_type': payload.get('event') or payload.get('type'),
        
        # Dados do cliente
        'customer_email': customer.get('email') or payload.get('customer_email'),
        'customer_name': customer.get('name') or payload.get('customer_name'),
        'customer_document': customer.get('document') or customer.get('cpf'),
        'customer_phone': customer.get('phone'),
        
        # Dados do produto
        'product_name': product.get('name') or payload.get('product_name'),
        'product_id': product.get('id') or payload.get('product_id'),
        'product_ucode': product.get('ucode'),
        
        # Dados financeiros
        'amount': amount,
        'currency': payload.get('currency', 'BRL'),
        'commission_amount': affiliate.get('commission_amount'),
        
        # Dados de pagamento
        'payment_method': transaction.get('payment_method') or payload.get('payment_method'),
        'status': transaction.get('status') or payload.get('status'),
        'installments': transaction.get('installments'),
        
        # Dados do afiliado
        'affiliate_email': affiliate.get('email'),
        'affiliate_name': affiliate.get('name'),
        'affiliate_code': affiliate.get('code'),
        
        # UTM e tracking
        'utm_source': payload.get('utm_source'),
        'utm_medium': payload.get('utm_medium'),
        'utm_campaign': payload.get('utm_campaign'),
        'sales_link': payload.get('sales_link'),
        
        # Datas
        'created_at': transaction.get('created_at') or payload.get('created_at'),
        'paid_at': transaction.get('paid_at'),
        
        # Dados específicos da Braip
        'subscription_id': payload.get('subscription_id'),
        'offer_code': payload.get('offer_code'),
        'producer_name': payload.get('producer_name'),
        'producer_document': payload.get('producer_document'),
        
        # Campos extras mantidos
        'attendant_name': payload.get('attendant_name'),
        'attendant_email': payload.get('attendant_email'),
    }

@braip_bp.route("/", methods=["POST"])
def receber_webhook():
    """
    Endpoint para receber webhooks da Braip
    """
    # Validar assinatura se configurada
    signature = request.headers.get('X-Braip-Signature')
    secret = os.getenv("BRAIP_WEBHOOK_SECRET")
    
    if secret and signature:
        expected_signature = hmac.new(
            secret.encode(),
            request.data,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_signature):
            return jsonify({"error": "Assinatura inválida"}), 403

    try:
        payload = request.get_json(force=True)
        
        # Debug: imprimir estrutura do payload
        print(f"\n📥 Webhook Braip recebido:")
        print(f"Event: {payload.get('event') or payload.get('type')}")
        
        # Determinar tipo do evento
        evento = payload.get('event') or payload.get('type') or 'webhook'
        
        # Extrair e padronizar dados
        dados_extraidos = extract_braip_data(payload)
        
        # Converter payload para JSON string
        dados_extraidos['raw_data'] = json.dumps(payload, ensure_ascii=False, default=str)
        
        # Debug: verificar se algum campo ainda é dict
        for key, value in dados_extraidos.items():
            if isinstance(value, dict):
                print(f"⚠️  Campo {key} ainda é dict: {value}")
                dados_extraidos[key] = json.dumps(value)
        
        # Salvar no banco
        salvar_evento("braip", evento, dados_extraidos)
        
        print(f"✅ Evento Braip {evento} salvo com sucesso!")
        
        return jsonify({
            "status": "success",
            "message": f"Evento {evento} processado com sucesso",
            "transaction_id": dados_extraidos.get("transaction_id"),
            "customer_name": dados_extraidos.get("customer_name")
        }), 200
        
    except Exception as e:
        print(f"❌ Erro no webhook Braip: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500

@braip_bp.route("/test", methods=["GET"])
def test_webhook():
    """
    Endpoint de teste para verificar se o webhook está funcionando
    """
    return jsonify({
        "status": "ok",
        "message": "Braip webhook is working",
        "endpoint": "/webhook/braip",
        "auth": {
            "secret_configured": bool(os.getenv("BRAIP_WEBHOOK_SECRET"))
        }
    })