from flask import Blueprint, request, abort, jsonify
from db import salvar_evento
import hmac
import hashlib
import os
import requests
import json
from datetime import datetime, timedelta

braip_bp = Blueprint('braip', __name__)

# Token da API Braip
BRAIP_API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6ImUxMzlhOTAyOGNjZDc3Y2ExZDNhNTdkNjViZGI3Y2YzMDc2YWMzOWRhOWI2Y2RjMjliMTcxNjdmYzNlZGM2MTdhZWMwYzZlYzg5YzA5Mzk2In0.eyJhdWQiOiI0Nzc1MyIsImp0aSI6ImUxMzlhOTAyOGNjZDc3Y2ExZDNhNTdkNjViZGI3Y2YzMDc2YWMzOWRhOWI2Y2RjMjliMTcxNjdmYzNlZGM2MTdhZWMwYzZlYzg5YzA5Mzk2IiwiaWF0IjoxNzUyMjYxMDUzLCJuYmYiOjE3NTIyNjEwNTMsImV4cCI6MTc4Mzc5NzA1Mywic3ViIjoiODExNTU1Iiwic2NvcGVzIjpbXX0.IGzizj3D0uQnmcmWSneeoudsAD1vWYBHOHfc3h28Nu8CCug88jEK2mO2-MnBpyiCKD0Xa4x_hBfcskl3uQUlQBdGpcYzr0dVzoy-1WxNZ4sP01GGdrtuRSLqSGOsJWwxq8uaFoD7xpLrWKhSZQ2TWJvnHatuDXXIJH0RR6T9c3wvG32F7AA4nGtk8Y_TWYTvUtyd3aZuBlSrJoxZnJdOOyVfBiyTtqUVMdJU4gPWfuH2J6QIh0JPgseep40HRlCzieQCzW6xNIsJSHkJkiuHYMACnAB877M8XN6CcRk3PbOBcHLZkfpUngyYDA1Cg8QGbUBp80qLMmMvIKw-EJTuRzA7RI549ghbRwMP58GQKloLbJ4MzdVylFzvpLMB4Csw8u-hm9QyrYFJIyf3MHTzAT1HB8D5HWcUqgUTwXrPEj4cEW4PBsiRkn6g6OsSnploPq9G7Ih6nhsDXtGsJBxFPvmMBbzfqugKAmp4KtYY5k2jn9Na--1LGrpfkFAb8UBzLCxMV_8C0vVH8o0sh3AL1UmRlOhaHx-tj3jh66BiSBQl0j4W1ttv9dshckKPnaNUtEfEdXC1iat8gkyvTxYsTo6bYeR51GAUNLHFT7up5t7fPFk3cptt7xB0b_016G7YZC6xVJGjj2bE0TuBIh8WxGDCZtb4Q8b7Q-hkzEyIeiE"

BRAIP_API_BASE_URL = "https://api.braip.com"

def get_headers():
    """Retorna os headers necessários para a API da Braip"""
    return {
        'Authorization': f'Bearer {BRAIP_API_TOKEN}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

def fazer_requisicao_api(endpoint, params=None):
    """Faz uma requisição para a API da Braip"""
    url = f"{BRAIP_API_BASE_URL}/{endpoint}"
    headers = get_headers()
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API Braip: {e}")
        raise

def mapear_dados_transacao(transacao):
    """Mapeia dados da transação Braip para formato padrão"""
    return {
        'webhook_id': transacao.get('id'),
        'customer_email': transacao.get('customer', {}).get('email'),
        'customer_name': transacao.get('customer', {}).get('name'),
        'customer_document': transacao.get('customer', {}).get('document'),
        'product_name': transacao.get('product', {}).get('name'),
        'product_id': transacao.get('product', {}).get('id'),
        'transaction_id': transacao.get('transaction_id'),
        'amount': transacao.get('amount'),
        'currency': transacao.get('currency', 'BRL'),
        'payment_method': transacao.get('payment_method'),
        'status': transacao.get('status'),
        'commission_amount': transacao.get('commission_amount'),
        'affiliate_email': transacao.get('affiliate', {}).get('email'),
        'utm_source': transacao.get('utm_source'),
        'utm_medium': transacao.get('utm_medium'),
        'sales_link': transacao.get('sales_link'),
        'raw_data': json.dumps(transacao, ensure_ascii=False, default=str)
    }

@braip_bp.route("/", methods=["POST"])
def receber_webhook():
    """Endpoint original para receber webhooks da Braip"""
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
        # Mapear dados da Braip para formato padrão
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
            'raw_data': json.dumps(payload, ensure_ascii=False, default=str)
        }
        
        salvar_evento("braip", payload.get('event_type', 'webhook'), dados_padronizados)
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500

@braip_bp.route("/sync", methods=["POST"])
def sincronizar_transacoes():
    """Sincroniza transações da API Braip"""
    try:
        # Parâmetros opcionais da requisição
        data = request.get_json() or {}
        
        # Parâmetros para a API
        params = {
            'limit': data.get('limit', 100),  # Limite de resultados
            'page': data.get('page', 1),      # Página
        }
        
        # Filtros opcionais
        if data.get('start_date'):
            params['start_date'] = data['start_date']
        if data.get('end_date'):
            params['end_date'] = data['end_date']
        if data.get('status'):
            params['status'] = data['status']
        
        # Buscar transações na API
        response = fazer_requisicao_api('v1/transactions', params)
        
        transacoes_processadas = 0
        erros = []
        
        # Processar cada transação
        for transacao in response.get('data', []):
            try:
                dados_padronizados = mapear_dados_transacao(transacao)
                salvar_evento("braip", "api_sync", dados_padronizados)
                transacoes_processadas += 1
            except Exception as e:
                erros.append({
                    'transaction_id': transacao.get('id'),
                    'error': str(e)
                })
        
        return {
            "status": "success",
            "transacoes_processadas": transacoes_processadas,
            "total_encontradas": len(response.get('data', [])),
            "erros": erros,
            "meta": response.get('meta', {})
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }, 500

@braip_bp.route("/sync/recent", methods=["POST"])
def sincronizar_recentes():
    """Sincroniza transações dos últimos 7 dias"""
    try:
        # Calcular data de 7 dias atrás
        data_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        data_fim = datetime.now().strftime('%Y-%m-%d')
        
        params = {
            'start_date': data_inicio,
            'end_date': data_fim,
            'limit': 500
        }
        
        response = fazer_requisicao_api('v1/transactions', params)
        
        transacoes_processadas = 0
        erros = []
        
        for transacao in response.get('data', []):
            try:
                dados_padronizados = mapear_dados_transacao(transacao)
                salvar_evento("braip", "api_sync_recent", dados_padronizados)
                transacoes_processadas += 1
            except Exception as e:
                erros.append({
                    'transaction_id': transacao.get('id'),
                    'error': str(e)
                })
        
        return {
            "status": "success",
            "periodo": f"{data_inicio} até {data_fim}",
            "transacoes_processadas": transacoes_processadas,
            "total_encontradas": len(response.get('data', [])),
            "erros": erros
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }, 500

@braip_bp.route("/produtos", methods=["GET"])
def listar_produtos():
    """Lista produtos da conta Braip"""
    try:
        response = fazer_requisicao_api('v1/products')
        return {
            "status": "success",
            "produtos": response.get('data', [])
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }, 500

@braip_bp.route("/afiliados", methods=["GET"])
def listar_afiliados():
    """Lista afiliados da conta Braip"""
    try:
        response = fazer_requisicao_api('v1/affiliates')
        return {
            "status": "success",
            "afiliados": response.get('data', [])
        }
    except Exception as e:
        return {
            "status": "error", 
            "message": str(e)
        }, 500

@braip_bp.route("/status", methods=["GET"])
def status_api():
    """Verifica o status da conexão com a API Braip"""
    try:
        # Tenta fazer uma requisição simples para verificar conectividade
        response = fazer_requisicao_api('v1/products', {'limit': 1})
        return {
            "status": "connected",
            "api_status": "ok",
            "message": "Conexão com API Braip funcionando"
        }
    except Exception as e:
        return {
            "status": "error",
            "api_status": "failed",
            "message": str(e)
        }, 500