# Script 1: setup_server.sh - Configuração inicial do servidor
#!/bin/bash

echo "=== Configurando servidor EC2 ==="

# Detectar sistema operacional
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
fi

echo "Sistema detectado: $OS"

# Atualizar sistema
if [[ "$OS" == *"Amazon Linux"* ]]; then
    sudo yum update -y
    sudo yum install -y python3 python3-pip git nginx htop
elif [[ "$OS" == *"Ubuntu"* ]]; then
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y python3 python3-pip python3-venv git nginx htop
fi

# Instalar pip para usuário
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py --user
rm get-pip.py

# Adicionar ao PATH
echo 'export PATH=$PATH:~/.local/bin' >> ~/.bashrc
source ~/.bashrc

echo "Configuração inicial concluída!"

# =====================================

# Script 2: deploy_app.sh - Deploy da aplicação
#!/bin/bash

PROJECT_NAME="seu-projeto"
REPO_URL="https://github.com/seu-usuario/seu-repo.git"
DOMAIN="seu-dominio.com"

echo "=== Iniciando deploy da aplicação ==="

# Clonar repositório
cd ~/
if [ -d "$PROJECT_NAME" ]; then
    echo "Projeto já existe. Atualizando..."
    cd $PROJECT_NAME
    git pull origin main
else
    echo "Clonando repositório..."
    git clone $REPO_URL $PROJECT_NAME
    cd $PROJECT_NAME
fi

# Criar ambiente virtual
if [ ! -d "venv" ]; then
    echo "Criando ambiente virtual..."
    python3 -m venv venv
fi

# Ativar ambiente virtual e instalar dependências
source venv/bin/activate
pip install -r requirements.txt
pip install gunicorn

echo "Deploy da aplicação concluído!"

# =====================================

# Script 3: setup_nginx.sh - Configuração do Nginx
#!/bin/bash

PROJECT_NAME="seu-projeto"
DOMAIN="seu-dominio.com"
SERVER_IP=$(curl -s http://checkip.amazonaws.com)

echo "=== Configurando Nginx ==="

# Criar arquivo de configuração do Nginx
sudo tee /etc/nginx/sites-available/$PROJECT_NAME > /dev/null <<EOF
server {
    listen 80;
    server_name $DOMAIN $SERVER_IP;

    location /static/ {
        alias /home/\$USER/$PROJECT_NAME/staticfiles/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    location /media/ {
        alias /home/\$USER/$PROJECT_NAME/media/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_connect_timeout 300s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
    }
}
EOF

# Ativar site
sudo ln -sf /etc/nginx/sites-available/$PROJECT_NAME /etc/nginx/sites-enabled/

# Remover configuração padrão
sudo rm -f /etc/nginx/sites-enabled/default

# Testar configuração
sudo nginx -t

if [ $? -eq 0 ]; then
    echo "Configuração do Nginx OK!"
    sudo systemctl restart nginx
    sudo systemctl enable nginx
else
    echo "Erro na configuração do Nginx!"
    exit 1
fi

echo "Nginx configurado com sucesso!"

# =====================================

# Script 4: setup_gunicorn.sh - Configuração do Gunicorn
#!/bin/bash

PROJECT_NAME="seu-projeto"
USER=$(whoami)

echo "=== Configurando Gunicorn ==="

# Criar arquivo de serviço do Gunicorn
sudo tee /etc/systemd/system/gunicorn.service > /dev/null <<EOF
[Unit]
Description=Gunicorn instance to serve $PROJECT_NAME
After=network.target

[Service]
User=$USER
Group=nginx
WorkingDirectory=/home/$USER/$PROJECT_NAME
Environment="PATH=/home/$USER/$PROJECT_NAME/venv/bin"
ExecStart=/home/$USER/$PROJECT_NAME/venv/bin/gunicorn --workers 3 --bind 127.0.0.1:8000 app:app
ExecReload=/bin/kill -s HUP \$MAINPID
Restart=on-failure
KillMode=mixed
TimeoutStopSec=5
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

# Recarregar systemd
sudo systemctl daemon-reload

# Iniciar e habilitar serviço
sudo systemctl start gunicorn
sudo systemctl enable gunicorn

# Verificar status
sudo systemctl status gunicorn

echo "Gunicorn configurado com sucesso!"

# =====================================

# Script 5: setup_ssl.sh - Configuração SSL com Let's Encrypt
#!/bin/bash

DOMAIN="seu-dominio.com"

echo "=== Configurando SSL ==="

# Instalar Certbot
if [[ "$OS" == *"Amazon Linux"* ]]; then
    sudo yum install -y certbot python3-certbot-nginx
elif [[ "$OS" == *"Ubuntu"* ]]; then
    sudo apt install -y certbot python3-certbot-nginx
fi

# Obter certificado SSL
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos --email admin@$DOMAIN

# Configurar renovação automática
echo "0 12 * * * /usr/bin/certbot renew --quiet" | sudo crontab -

echo "SSL configurado com sucesso!"

# =====================================

# Script 6: monitor.sh - Script de monitoramento
#!/bin/bash

echo "=== Status do Sistema ==="
echo "Data: $(date)"
echo ""

echo "=== Serviços ==="
systemctl is-active nginx && echo "✓ Nginx: Ativo" || echo "✗ Nginx: Inativo"
systemctl is-active gunicorn && echo "✓ Gunicorn: Ativo" || echo "✗ Gunicorn: Inativo"
echo ""

echo "=== Recursos do Sistema ==="
echo "CPU:"
top -bn1 | grep "Cpu(s)" | awk '{print $2}' | cut -d'%' -f1 | awk '{print "Uso: " $1"%"}'

echo "Memória:"
free -h | awk 'NR==2{printf "Uso: %s/%s (%.2f%%)\n", $3,$2,$3*100/$2 }'

echo "Disco:"
df -h | awk '$NF=="/"{printf "Uso: %s/%s (%s)\n", $3,$2,$5}'

echo ""
echo "=== Logs Recentes ==="
echo "Nginx (últimas 5 linhas):"
sudo tail -5 /var/log/nginx/error.log

echo ""
echo "Gunicorn (últimas 5 linhas):"
sudo journalctl -u gunicorn --no-pager -n 5

# =====================================

# Script 7: backup.sh - Script de backup
#!/bin/bash

PROJECT_NAME="seu-projeto"
BACKUP_DIR="/home/$(whoami)/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/${PROJECT_NAME}_backup_$DATE.tar.gz"

echo "=== Iniciando Backup ==="

# Criar diretório de backup
mkdir -p $BACKUP_DIR

# Criar backup
tar -czf $BACKUP_FILE \
    --exclude='venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    /home/$(whoami)/$PROJECT_NAME

echo "Backup criado: $BACKUP_FILE"

# Manter apenas os últimos 7 backups
find $BACKUP_DIR -name "${PROJECT_NAME}_backup_*.tar.gz" -type f -mtime +7 -delete

echo "Backup concluído!"

# =====================================

# Script 8: restart_services.sh - Reiniciar serviços
#!/bin/bash

echo "=== Reiniciando Serviços ==="

echo "Reiniciando Gunicorn..."
sudo systemctl restart gunicorn

echo "Recarregando Nginx..."
sudo systemctl reload nginx

echo "Verificando status..."
sudo systemctl status gunicorn --no-pager -l
sudo systemctl status nginx --no-pager -l

echo "Serviços reiniciados!"

# =====================================

# Script 9: full_deploy.sh - Deploy completo
#!/bin/bash

echo "=== Deploy Completo ==="

# Executar scripts na ordem
echo "1. Configurando servidor..."
./setup_server.sh

echo "2. Fazendo deploy da aplicação..."
./deploy_app.sh

echo "3. Configurando Nginx..."
./setup_nginx.sh

echo "4. Configurando Gunicorn..."
./setup_gunicorn.sh

echo "5. Configurando SSL (opcional)..."
read -p "Deseja configurar SSL? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    ./setup_ssl.sh
fi

echo "6. Fazendo backup..."
./backup.sh

echo "=== Deploy Completo Finalizado ==="
echo "Acesse: http://$(curl -s http://checkip.amazonaws.com)"