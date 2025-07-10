from flask import Blueprint
import requests
from db import salvar_evento
from dotenv import load_dotenv
import os

load_dotenv()
cakto_bp = Blueprint('cakto', __name__)

API_KEY = os.getenv("CAKTO_API_KEY")
URL_BASE = "https://api.cakto.com.br/v1"

@cakto_bp.route("/sync", methods=["GET"])
def puxar_transacoes():
    headers = {"Authorization": f"Bearer {API_KEY}"}
    try:
        r = requests.get(f"{URL_BASE}/orders", headers=headers)
        if r.status_code == 200:
            for item in r.json().get("orders", []):
                salvar_evento("cakto", "transacao", item)
            return {"status": "success"}, 200
        return {"status": "error", "message": f"API retornou {r.status_code}"}, 400
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500