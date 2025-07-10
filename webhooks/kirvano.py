from flask import Blueprint, request
from db import salvar_evento

kirvano_bp = Blueprint('kirvano', __name__)

@kirvano_bp.route("/", methods=["POST"])
def receber():
    payload = request.json
    # Salvando os dados no banco de dados PostgreSQL
    salvar_evento("kirvano", "evento_tipo", payload)
    return {"status": "ok"}
