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
WEBHOOK_SECRET = os.getenv("CAKTO_WEBHOOK_SECRET")

def extract_cakto_data(payload):
    """
    Função para padronizar dados recebidos do webhook da Cakto.
    Mapeia os campos específicos da Cakto para o formato padrão do banco.
    """
    # Os dados principais estão dentro do objeto 'data'
    data = payload.get("data", {})
    
    # Se não há objeto 'data', usa o payload diretamente (compatibilidade)
    if not data:
        data = payload
    
    # Extrai dados do customer
    customer = data.get("customer", {})
    
    # Extrai dados do produto e oferta
    product = data.get("product", {})
    offer = data.get("offer", {})
    
    # Extrai informações de pagamento específicas
    card = data.get("card", {})
    boleto = data.get("boleto", {})
    pix = data.get("pix", {})
    picpay = data.get("picpay", {})
    
    # Extrai primeira comissão se existir
    commissions = data.get("commissions", [])
    first_commission = commissions[0] if commissions else {}
    
    # Calcula valores
    base_amount = data.get("baseAmount", 0)
    discount = data.get("discount", 0)
    final_amount = data.get("amount", base_amount - discount if base_amount else None)
    
    return {
        # Campos padrão do banco
        "webhook_id": data.get("id"),
        "customer_email": customer.get("email") or data.get("email"),
        "customer_name": customer.get("name") or data.get("customer_name"),
        "customer_document": customer.get("docNumber") or customer.get("document") or data.get("document"),
        "customer_phone": customer.get("phone"),
        
        "product_name": product.get("name") or data.get("product_name"),
        "product_id": product.get("id") or data.get("product_id"),
        "product_short_id": product.get("short_id"),
        "product_support_email": product.get("supportEmail"),
        "product_type": product.get("type"),
        "invoice_description": product.get("invoiceDescription"),
        
        "transaction_id": data.get("id") or data.get("transaction_id"),
        "ref_id": data.get("refId"),
        "parent_order": data.get("parent_order"),
        
        "amount": final_amount or data.get("total"),
        "base_amount": base_amount,
        "discount": discount,
        "currency": data.get("currency", "BRL"),
        
        "payment_method": data.get("paymentMethod") or data.get("payment_method") or data.get("payment_type"),
        "payment_method_name": data.get("paymentMethodName"),
        "installments": data.get("installments"),
        "status": data.get("status"),
        
        # Comissões e afiliados
        "commission_amount": first_commission.get("totalAmount") or data.get("commission_amount") or data.get("commission"),
        "commission_type": first_commission.get("type"),
        "commission_percentage": first_commission.get("percentage"),
        "affiliate_email": data.get("affiliate") or first_commission.get("user") or data.get("affiliate_email"),
        
        # Oferta
        "offer_id": offer.get("id"),
        "offer_name": offer.get("name"),
        "offer_price": offer.get("price"),
        "offer_type": data.get("offer_type"),
        
        # URLs e links
        "checkout_url": data.get("checkoutUrl"),
        "sales_link": data.get("checkoutUrl") or data.get("sales_link"),
        
        # Datas
        "created_at": data.get("createdAt") or data.get("created_at"),
        "paid_at": data.get("paidAt"),
        
        # Informações específicas por método de pagamento
        # Cartão de Crédito
        "card_last_digits": card.get("lastDigits"),
        "card_holder_name": card.get("holderName"),
        "card_brand": card.get("brand"),
        
        # Boleto
        "boleto_barcode": boleto.get("barcode"),
        "boleto_url": boleto.get("boletoUrl"),
        "boleto_expiration": boleto.get("expirationDate"),
        
        # PIX
        "pix_qr_code": pix.get("qrCode"),
        "pix_expiration": pix.get("expirationDate"),
        
        # PicPay
        "picpay_qr_code": picpay.get("qrCode"),
        "picpay_url": picpay.get("paymentURL"),
        "picpay_expiration": picpay.get("expirationDate"),
        
        # Motivos e observações
        "reason": data.get("reason"),
        "refund_reason": data.get("refund_reason"),
        
        # UTMs (para compatibilidade)
        "utm_source": data.get("utm_source"),
        "utm_medium": data.get("utm_medium"),
        "attendant_name": data.get("attendant_name"),
        "attendant_email": data.get("attendant_email"),
        
        # Campos específicos da Cakto (mantidos para compatibilidade)
        "pedido_id": data.get("id"),
        "cliente_nome": customer.get("name") or data.get("customer_name"),
        "produto": product.get("name") or data.get("product_name"),
        "valor": final_amount or data.get("total") or data.get("amount"),
        "data": data.get("createdAt") or data.get("created_at"),
        
        # Informações de comissões completas (serializado)
        "commissions_json": json.dumps(commissions, ensure_ascii=False) if commissions else None,
        "commissions_count": len(commissions),
        
        # Dados de evento
        "event_type": payload.get("event"),
        "secret": payload.get("secret")  # Para auditoria
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
        
        # Validação opcional do secret enviado no payload
        secret_from_payload = payload.get("secret")
        if WEBHOOK_SECRET and secret_from_payload != WEBHOOK_SECRET:
            logger.warning("Secret inválido recebido no webhook da Cakto.")
            return jsonify({"status": "unauthorized", "error": "Secret inválido"}), 401
        
        # Debug: imprimir estrutura do payload
        print(f"\n📥 Webhook Cakto recebido:")
        print(f"Event: {payload.get('event') or payload.get('type')}")
        print(f"Secret: {secret_from_payload[:8]}..." if secret_from_payload else "Sem secret")
        print(f"Data keys: {list(payload.get('data', {}).keys())}")
        
        # Se há dados dentro de 'data', mostrar informações principais
        data = payload.get('data', {})
        if data:
            customer = data.get('customer', {})
            product = data.get('product', {})
            print(f"Customer: {customer.get('name', 'N/A')}")
            print(f"Product: {product.get('name', 'N/A')}")
            print(f"Amount: {data.get('amount', 'N/A')}")
            print(f"Status: {data.get('status', 'N/A')}")

        # Determina o tipo de evento
        evento = payload.get('event') or payload.get('type') or payload.get('event_type') or 'webhook'

        # Padroniza e salva os dados
        dados_padronizados = extract_cakto_data(payload)
        dados_padronizados["raw_data"] = json.dumps(payload, ensure_ascii=False)
        
        # Debug: verificar se algum campo ainda é dict
        dict_fields = []
        for key, value in dados_padronizados.items():
            if isinstance(value, dict):
                dict_fields.append(f"{key}: {value}")
                dados_padronizados[key] = json.dumps(value)
        
        if dict_fields:
            print(f"⚠️  Campos convertidos de dict para JSON: {dict_fields}")

        salvar_evento("cakto", evento, dados_padronizados)
        
        print(f"✅ Evento {evento} salvo com sucesso!")

        return jsonify({
            "status": "success",
            "message": f"Evento {evento} processado com sucesso",
            "transaction_id": dados_padronizados.get("transaction_id"),
            "customer_name": dados_padronizados.get("customer_name")
        }), 200

    except Exception as e:
        logger.exception("Erro ao processar webhook da Cakto.")
        print(f"❌ Erro no webhook Cakto: {str(e)}")
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
        "endpoint": "/webhook/cakto",
        "auth": {
            "secret_configured": bool(WEBHOOK_SECRET),
            "token_configured": bool(WEBHOOK_TOKEN)
        }
    })