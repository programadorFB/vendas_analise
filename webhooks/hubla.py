import json
import hmac
import hashlib
import os
from flask import Blueprint, request, jsonify
from db import salvar_evento

hubla_bp = Blueprint('hubla', __name__)

def extract_hubla_data(payload):
    """
    Extrai dados específicos da Hubla baseado na estrutura real dos webhooks
    """
    # Primeiro, vamos checar se é um webhook v2.0.0 ou v1.0.0
    version = payload.get('version', '1.0.0')
    event_data = payload.get('event', {})
    
    # Estrutura comum para armazenar dados extraídos
    extracted_data = {}
    
    if version == '2.0.0':
        # Webhooks v2.0.0 têm estrutura mais complexa
        # Extrair dados do usuário/pagador
        if 'user' in event_data:
            user = event_data['user']
            extracted_data['customer_email'] = user.get('email')
            extracted_data['customer_name'] = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip()
            extracted_data['customer_document'] = user.get('document')
        
        # Dados do pagador (payer) se existir
        if 'payer' in event_data:
            payer = event_data['payer']
            extracted_data['customer_email'] = payer.get('email')
            extracted_data['customer_name'] = f"{payer.get('firstName', '')} {payer.get('lastName', '')}".strip()
            extracted_data['customer_document'] = payer.get('document')
        
        # Dados da invoice/subscription
        if 'invoice' in event_data:
            invoice = event_data['invoice']
            extracted_data['transaction_id'] = invoice.get('id')
            extracted_data['payment_method'] = invoice.get('paymentMethod')
            extracted_data['status'] = invoice.get('status')
            extracted_data['currency'] = invoice.get('currency', 'BRL')
            
            # Valor em centavos, converter para reais
            if 'amount' in invoice and isinstance(invoice['amount'], dict):
                total_cents = invoice['amount'].get('totalCents', 0)
                extracted_data['amount'] = total_cents / 100 if total_cents else None
        
        # Dados do produto
        if 'product' in event_data:
            product = event_data['product']
            extracted_data['product_id'] = product.get('id')
            extracted_data['product_name'] = product.get('name')
        
        # Dados da subscription
        if 'subscription' in event_data:
            subscription = event_data['subscription']
            extracted_data['transaction_id'] = subscription.get('id')
            extracted_data['payment_method'] = subscription.get('paymentMethod')
            extracted_data['status'] = subscription.get('status')
            
            # Extrair pagador da subscription
            if 'payer' in subscription:
                payer = subscription['payer']
                extracted_data['customer_email'] = payer.get('email')
                extracted_data['customer_name'] = f"{payer.get('firstName', '')} {payer.get('lastName', '')}".strip()
    
    elif version == '1.0.0':
        # Webhooks v1.0.0 têm estrutura mais simples
        extracted_data['customer_email'] = event_data.get('userEmail')
        extracted_data['customer_name'] = event_data.get('userName')
        extracted_data['customer_document'] = event_data.get('userDocument')
        extracted_data['product_id'] = event_data.get('groupId')
        extracted_data['product_name'] = event_data.get('groupName')
        extracted_data['transaction_id'] = event_data.get('transactionId')
        extracted_data['amount'] = event_data.get('totalAmount')
        extracted_data['payment_method'] = event_data.get('paymentMethod')
        
        # UTM tracking
        if 'utm' in event_data:
            utm = event_data['utm']
            extracted_data['utm_source'] = utm.get('source')
            extracted_data['utm_medium'] = utm.get('medium')
    
    # Adicionar campos padrão se não existirem
    return {
        'webhook_id': payload.get('id'),
        'customer_email': extracted_data.get('customer_email'),
        'customer_name': extracted_data.get('customer_name'),
        'customer_document': extracted_data.get('customer_document'),
        'product_name': extracted_data.get('product_name'),
        'product_id': extracted_data.get('product_id'),
        'transaction_id': extracted_data.get('transaction_id'),
        'amount': extracted_data.get('amount'),
        'currency': extracted_data.get('currency', 'BRL'),
        'payment_method': extracted_data.get('payment_method'),
        'status': extracted_data.get('status'),
        'commission_amount': extracted_data.get('commission_amount'),
        'affiliate_email': extracted_data.get('affiliate_email'),
        'utm_source': extracted_data.get('utm_source'),
        'utm_medium': extracted_data.get('utm_medium'),
        'sales_link': extracted_data.get('sales_link'),
        'attendant_name': extracted_data.get('attendant_name'),
        'attendant_email': extracted_data.get('attendant_email'),
    }

@hubla_bp.route("/", methods=["POST"])
def receber_webhook():
    # Validar assinatura
    signature = request.headers.get('X-Hubla-Signature')
    secret = os.getenv("HUBLA_WEBHOOK_SECRET")
    
    if secret and signature:
        expected_sign = hmac.new(
            secret.encode(),
            request.data,
            hashlib.sha256
        ).hexdigest()
        
        if not hmac.compare_digest(signature, expected_sign):
            return jsonify({"error": "Assinatura inválida"}), 403

    try:
        payload = request.get_json()
        
        # Debug: imprimir estrutura do payload
        print(f"\n📥 Webhook Hubla recebido:")
        print(f"Type: {payload.get('type')}")
        print(f"Version: {payload.get('version')}")
        
        # Determina o tipo de evento
        evento = payload.get('type') or payload.get('event') or 'unknown'
        
        # Extrai e padroniza os dados
        dados_extraidos = extract_hubla_data(payload)
        
        # IMPORTANTE: Converter o payload para string JSON antes de adicionar
        dados_extraidos['raw_data'] = json.dumps(payload, ensure_ascii=False)
        
        # Debug: verificar se algum campo ainda é dict
        for key, value in dados_extraidos.items():
            if isinstance(value, dict):
                print(f"⚠️  Campo {key} ainda é dict: {value}")
                dados_extraidos[key] = json.dumps(value)
        
        # Salva no banco
        salvar_evento("hubla", evento, dados_extraidos)
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        print(f"Erro no webhook Hubla: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@hubla_bp.route("/test", methods=["GET"])
def test_webhook():
    """
    Endpoint de teste para verificar se o webhook está funcionando
    """
    return jsonify({
        "status": "ok",
        "message": "Hubla webhook is working",
        "timestamp": os.popen('date').read().strip()
    })