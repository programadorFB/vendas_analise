import json
import pandas as pd
from io import BytesIO
from datetime import datetime
from flask import Blueprint
from drive_upload import upload_or_replace_file

export_bp = Blueprint("export", __name__)

def format_excel(writer, df, sheet_name):
    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    for i, col in enumerate(df.columns):
        column_len = max(df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(i, i, column_len)

def create_excel_report(data, columns, filename="relatorio_webhooks.xlsx"):
    df = pd.DataFrame(data, columns=columns)
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        if 'platform' in df.columns:
            for platform_name, platform_df in df.groupby('platform'):
                platform_df.to_excel(writer, sheet_name=platform_name[:31], index=False)
                format_excel(writer, platform_df, platform_name[:31])
        else:
            df.to_excel(writer, sheet_name='Webhooks', index=False)
            format_excel(writer, df, 'Webhooks')
    print(f"✅ Arquivo Excel gerado: {filename}")
    upload_or_replace_file(filename)

