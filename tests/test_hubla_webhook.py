# tests/test_hubla_webhook.py
import pytest
import hmac
import hashlib
import os
from unittest.mock import patch
from ..webhooks.hubla import hubla_bp

def test_webhook_with_valid_signature(client):
    """Testa requisição com assinatura HMAC válida"""
    secret = "test_secret"
    os.environ["HUBLA_WEBHOOK_SECRET"] = secret
    payload = {'event': 'sale_approved', 'data': {'id': '123', 'amount': 100}}
    data = str(payload).encode()
    
    # Gerar assinatura válida
    signature = hmac.new(
        secret.encode(),
        data,
        hashlib.sha256
    ).hexdigest()
    
    headers = {'X-Hubla-Signature': signature}
    
    with patch('your_module.salvar_evento') as mock_save:
        response = client.post(
            '/',
            json=payload,
            headers=headers
        )
        
        assert response.status_code == 200
        mock_save.assert_called_once_with(
            plataforma="hubla",
            tipo="sale_approved",
            payload={
                "transacao_id": "123",
                "valor": 100,
                "afiliado": None,
                "status": None,
                "raw_data": payload
            }
        )

def test_webhook_with_invalid_signature(client):
    """Testa requisição com assinatura inválida"""
    os.environ["HUBLA_WEBHOOK_SECRET"] = "test_secret"
    payload = {'event': 'sale_approved', 'data': {'id': '123'}}
    
    headers = {'X-Hubla-Signature': 'assinatura_invalida'}
    
    response = client.post(
        '/',
        json=payload,
        headers=headers
    )
    
    assert response.status_code == 403
    assert b"Assinatura inv" in response.data

def test_webhook_without_signature(client):
    """Testa requisição sem assinatura quando secret está configurado"""
    os.environ["HUBLA_WEBHOOK_SECRET"] = "test_secret"
    payload = {'event': 'sale_approved', 'data': {'id': '123'}}
    
    response = client.post('/', json=payload)
    
    assert response.status_code == 403
    assert b"Assinatura inv" in response.data

def test_webhook_with_missing_event(client):
    """Testa payload sem campo 'event'"""
    response = client.post('/', json={'data': {'id': '123'}})
    
    assert response.status_code == 200  # Ou ajuste para 400 se quiser validação mais rigorosa
    # Verifique o comportamento esperado quando evento está faltando

def test_webhook_database_error(client):
    """Testa tratamento de erro no banco de dados"""
    payload = {'event': 'sale_approved', 'data': {'id': '123'}}
    
    with patch('your_module.salvar_evento', side_effect=Exception("DB Error")):
        response = client.post('/', json=payload)
        
        assert response.status_code == 500
        assert b"DB Error" in response.data

def test_webhook_with_minimal_payload(client):
    """Testa payload mínimo necessário"""
    with patch('your_module.salvar_evento') as mock_save:
        payload = {'event': 'user_created', 'data': {}}
        response = client.post('/', json=payload)
        
        assert response.status_code == 200
        mock_save.assert_called_once_with(
            plataforma="hubla",
            tipo="user_created",
            payload={
                "transacao_id": None,
                "valor": None,
                "afiliado": None,
                "status": None,
                "raw_data": payload
            }
        )