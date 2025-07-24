from flask import Blueprint, request
from db import salvar_evento
import hmac
import hashlib
import os
import json

kirvano_bp = Blueprint('kirvano', __name__)

def extrair_valor_monetario(valor_str):
    """
    Extrai valor numérico de strings monetárias como 'R$ 169,80'
    """
    if not valor_str:
        return None
    
    # Remove 'R$', espaços e converte vírgula para ponto
    valor_limpo = str(valor_str).replace('R$', '').replace(' ', '').replace(',', '.')
    
    try:
        return float(valor_limpo)
    except (ValueError, TypeError):
        return None

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
        # Extrair dados do customer
        customer = payload.get('customer', {})
        
        # Extrair dados do payment
        payment = payload.get('payment', {})
        
        # Extrair dados do utm
        utm = payload.get('utm', {})
        
        # Processar produtos - pegar o primeiro produto como principal
        products = payload.get('products', [])
        main_product = products[0] if products else {}
        
        # Calcular valor total e comissões se houver
        total_price_raw = payload.get('total_price')
        total_amount = extrair_valor_monetario(total_price_raw)
        
        # Mapear dados da Kirvano para formato padrão
        dados_padronizados = {
            # IDs e controle
            'webhook_id': payload.get('checkout_id'),
            'transaction_id': payload.get('sale_id'),
            'event_type': payload.get('event'),
            'event_description': payload.get('event_description'),
            
            # Dados do cliente
            'customer_email': customer.get('email'),
            'customer_name': customer.get('name'),
            'customer_document': customer.get('document'),
            'customer_phone': customer.get('phone_number'),
            
            # Dados do produto principal
            'product_name': main_product.get('name'),
            'product_id': main_product.get('id'),
            'offer_id': main_product.get('offer_id'),
            'offer_name': main_product.get('offer_name'),
            'product_description': main_product.get('description'),
            'product_photo': main_product.get('photo'),
            'is_order_bump': main_product.get('is_order_bump'),
            
            # Dados financeiros
            'amount': total_amount,
            'total_price_raw': total_price_raw,
            'currency': 'BRL',  # Assumindo BRL baseado no formato R$
            'product_price': extrair_valor_monetario(main_product.get('price')),
            'product_price_raw': main_product.get('price'),
            
            # Dados de pagamento
            'payment_method': payload.get('payment_method'),
            'status': payload.get('status'),
            'type': payload.get('type'),
            'created_at': payload.get('created_at'),
            
            # Dados específicos do boleto (se aplicável)
            'payment_link': payment.get('link'),
            'digitable_line': payment.get('digitable_line'),
            'barcode': payment.get('barcode'),
            'expires_at': payment.get('expires_at'),
            
            # Dados de UTM
            'utm_source': utm.get('utm_source'),
            'utm_medium': utm.get('utm_medium'),
            'utm_campaign': utm.get('utm_campaign'),
            'utm_term': utm.get('utm_term'),
            'utm_content': utm.get('utm_content'),
            'src': utm.get('src'),
            
            # Dados de afiliado (se disponível no futuro)
            'commission_amount': None,  # Não presente neste formato
            'affiliate_email': None,   # Não presente neste formato
            
            # Campo para URL de vendas (se disponível)
            'sales_link': None,  # Não presente neste formato
            
            # Dados dos produtos (array completo serializado)
            'products_json': json.dumps(products, ensure_ascii=False) if products else None,
            'products_count': len(products),
            
            # Campos extras específicos da Kirvano
            'checkout_id': payload.get('checkout_id'),
            'sale_id': payload.get('sale_id'),
            
            # Manter payload original para referência
            'raw_data': json.dumps(payload, ensure_ascii=False, default=str)
        }
        
        # Determinar tipo do evento para salvar
        event_type = payload.get('event', 'webhook_received')
        
        salvar_evento("kirvano", event_type, dados_padronizados)
        return {"status": "ok", "message": f"Evento {event_type} processado com sucesso"}
        
    except Exception as e:
        print(f"❌ Erro ao processar webhook Kirvano: {e}")
        print(f"Payload recebido: {json.dumps(payload, indent=2, ensure_ascii=False)}")
        return {"status": "error", "message": str(e)}, 500