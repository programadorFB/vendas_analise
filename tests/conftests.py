# tests/conftest.py
import pytest
from flask import Flask
from unittest.mock import patch, MagicMock

@pytest.fixture
def app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    return app

@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def hubla_bp(app):
    from your_module import hubla_bp
    app.register_blueprint(hubla_bp)
    return hubla_bp