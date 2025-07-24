import json
import pandas as pd
from io import BytesIO
from datetime import datetime
from export_excel import format_excel


def create_excel_report(data, columns, include_raw_data=True, output_filename="relatorio_teste_multiplas_abas.xlsx"):
    df = pd.DataFrame(data, columns=columns)
    with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
        if 'platform' in df.columns:
            for platform_name, platform_df in df.groupby('platform'):
                if not include_raw_data:
                    platform_df = platform_df.drop(columns=['raw_data'])
                sheet_name = platform_name[:31] or 'Plataforma'
                platform_df.to_excel(writer, sheet_name=sheet_name, index=False)
                format_excel(writer, platform_df, sheet_name)
        else:
            df.to_excel(writer, sheet_name='Webhooks', index=False)
            format_excel(writer, df, 'Webhooks')
    print(f"✅ Arquivo gerado: {output_filename}")


# Simulação de dados
columns = [
    "platform", "event_type", "webhook_id", "customer_email", "created_at",
    "amount", "commission_amount", "raw_data"
]

data = [
    ("Kirvano", "sale", "wh_001", "cliente1@email.com", datetime.now(), 150.0, 30.0, json.dumps({"ip": "192.168.0.1"})),
    ("Braip", "sale", "wh_002", "cliente2@email.com", datetime.now(), 200.0, 50.0, json.dumps({"ip": "192.168.0.2"})),
    ("Kirvano", "refund", "wh_003", "cliente3@email.com", datetime.now(), -150.0, -30.0, json.dumps({"ip": "192.168.0.3"})),
    ("Cakto", "sale", "wh_004", "cliente4@email.com", datetime.now(), 300.0, 60.0, json.dumps({"ip": "192.168.0.4"})),
]

# Executar
create_excel_report(data, columns)
