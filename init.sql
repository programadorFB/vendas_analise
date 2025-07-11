# init.sql - Script de inicialização do banco
CREATE TABLE IF NOT EXISTS webhooks (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50) NOT NULL,
    event_type VARCHAR(100),
    webhook_id VARCHAR(255),
    customer_email VARCHAR(255),
    customer_name VARCHAR(255),
    customer_document VARCHAR(50),
    product_name VARCHAR(255),
    product_id VARCHAR(100),
    transaction_id VARCHAR(255),
    amount DECIMAL(10, 2),
    currency VARCHAR(10),
    payment_method VARCHAR(100),
    status VARCHAR(50),
    commission_amount DECIMAL(10, 2),
    affiliate_email VARCHAR(255),
    utm_source VARCHAR(255),
    utm_medium VARCHAR(255),
    sales_link TEXT,
    attendant_name VARCHAR(255),
    attendant_email VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Criar índices para melhor performance
CREATE INDEX IF NOT EXISTS idx_webhooks_platform ON webhooks(platform);
CREATE INDEX IF NOT EXISTS idx_webhooks_created_at ON webhooks(created_at);
CREATE INDEX IF NOT EXISTS idx_webhooks_transaction_id ON webhooks(transaction_id);
CREATE INDEX IF NOT EXISTS idx_webhooks_customer_email ON webhooks(customer_email);
CREATE INDEX IF NOT EXISTS idx_webhooks_affiliate_email ON webhooks(affiliate_email);

-- Inserir dados de exemplo (opcional)
INSERT INTO webhooks (
    platform, event_type, transaction_id, customer_email, customer_name,
    product_name, amount, currency, status, commission_amount, affiliate_email,
    raw_data
) VALUES 
(
    'braip', 'sale_approved', 'DEMO_001', 'demo@exemplo.com', 'Cliente Demo',
    'Produto Demo', 100.00, 'BRL', 'approved', 20.00, 'afiliado@exemplo.com',
    '{"demo": true}'
),
(
    'hubla', 'sale_approved', 'DEMO_002', 'demo2@exemplo.com', 'Cliente Demo 2',
    'Produto Demo 2', 200.00, 'BRL', 'approved', 40.00, 'afiliado2@exemplo.com',
    '{"demo": true}'
),
(
    'kirvano', 'purchase_completed', 'DEMO_003', 'demo3@exemplo.com', 'Cliente Demo 3',
    'Produto Demo 3', 300.00, 'BRL', 'completed', 60.00, 'afiliado3@exemplo.com',
    '{"demo": true}'
);
