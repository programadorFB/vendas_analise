import os
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from flask import Blueprint, request, jsonify, send_file
from io import BytesIO

load_dotenv()
export_bp = Blueprint('export', __name__)

def get_db_connection():
    """Establish database connection - reusing your existing pattern"""
    return psycopg2.connect(os.getenv("DATABASE_URL"))

def fetch_webhook_data(start_date=None, end_date=None, platform=None):
    """
    Fetch webhook data from database with optional filters
    Think of this as your data retrieval engine - it can get all data
    or filter by date range and platform
    """
    
    # Base query to get all webhook data
    base_query = """
    SELECT 
        platform,
        event_type,
        webhook_id,
        customer_email,
        customer_name,
        customer_document,
        product_name,
        product_id,
        transaction_id,
        amount,
        currency,
        payment_method,
        status,
        commission_amount,
        affiliate_email,
        utm_source,
        utm_medium,
        sales_link,
        attendant_name,
        attendant_email,
        created_at
    FROM webhooks
    WHERE 1=1
    """
    
    # Build dynamic WHERE conditions
    conditions = []
    params = []
    
    if start_date:
        conditions.append("created_at >= %s")
        params.append(start_date)
    
    if end_date:
        conditions.append("created_at <= %s")
        params.append(end_date)
    
    if platform:
        conditions.append("platform = %s")
        params.append(platform)
    
    # Add conditions to query
    if conditions:
        base_query += " AND " + " AND ".join(conditions)
    
    # Add ordering for consistent results
    base_query += " ORDER BY created_at DESC"
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(base_query, params)
            
            # Get column names for pandas DataFrame
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all results
            results = cursor.fetchall()
            
            return columns, results
            
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_excel_report(data, columns):
    """
    Create Excel file with multiple sheets for different analyses
    This is like creating a comprehensive business report with different views
    """
    
    # Create DataFrame from database results
    df = pd.DataFrame(data, columns=columns)
    
    # Create Excel file in memory
    output = BytesIO()
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Sheet 1: Raw Data - all webhook data
        df.to_excel(writer, sheet_name='Raw Data', index=False)
        
        # Sheet 2: Summary by Platform - shows performance of each platform
        if not df.empty:
            platform_summary = df.groupby('platform').agg({
                'transaction_id': 'count',  # Number of transactions
                'amount': ['sum', 'mean'],  # Total and average amounts
                'commission_amount': 'sum'  # Total commissions
            }).round(2)
            
            # Flatten column names for better readability
            platform_summary.columns = ['Total_Transactions', 'Total_Amount', 'Average_Amount', 'Total_Commission']
            platform_summary.reset_index(inplace=True)
            
            platform_summary.to_excel(writer, sheet_name='Platform Summary', index=False)
        
        # Sheet 3: Daily Sales - shows trends over time
        if not df.empty and 'created_at' in df.columns:
            # Convert created_at to datetime if it's not already
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['date'] = df['created_at'].dt.date
            
            daily_sales = df.groupby('date').agg({
                'transaction_id': 'count',
                'amount': 'sum',
                'commission_amount': 'sum'
            }).round(2)
            
            daily_sales.columns = ['Daily_Transactions', 'Daily_Revenue', 'Daily_Commission']
            daily_sales.reset_index(inplace=True)
            
            daily_sales.to_excel(writer, sheet_name='Daily Sales', index=False)
        
        # Sheet 4: Top Products - identify best performers
        if not df.empty and 'product_name' in df.columns:
            product_performance = df.groupby('product_name').agg({
                'transaction_id': 'count',
                'amount': 'sum'
            }).round(2)
            
            product_performance.columns = ['Sales_Count', 'Total_Revenue']
            product_performance = product_performance.sort_values('Total_Revenue', ascending=False)
            product_performance.reset_index(inplace=True)
            
            product_performance.to_excel(writer, sheet_name='Top Products', index=False)
        
        # Sheet 5: Affiliate Performance - track affiliate success
        if not df.empty and 'affiliate_email' in df.columns:
            affiliate_data = df[df['affiliate_email'].notna()]
            if not affiliate_data.empty:
                affiliate_performance = affiliate_data.groupby('affiliate_email').agg({
                    'transaction_id': 'count',
                    'amount': 'sum',
                    'commission_amount': 'sum'
                }).round(2)
                
                affiliate_performance.columns = ['Sales_Count', 'Total_Sales', 'Total_Commission']
                affiliate_performance = affiliate_performance.sort_values('Total_Commission', ascending=False)
                affiliate_performance.reset_index(inplace=True)
                
                affiliate_performance.to_excel(writer, sheet_name='Affiliate Performance', index=False)
    
    output.seek(0)
    return output

@export_bp.route('/excel', methods=['GET'])
def export_to_excel():
    """
    API endpoint to generate and download Excel report
    This is your main export function that handles the web request
    """
    
    # Get query parameters for filtering
    start_date = request.args.get('start_date')  # Format: YYYY-MM-DD
    end_date = request.args.get('end_date')      # Format: YYYY-MM-DD
    platform = request.args.get('platform')      # braip, hubla, kirvano, cakto
    
    # Convert string dates to datetime objects
    try:
        if start_date:
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
    
    try:
        # Fetch data from database
        columns, data = fetch_webhook_data(start_date, end_date, platform)
        
        if not data:
            return jsonify({"message": "No data found for the specified criteria"}), 204
        
        # Create Excel report
        excel_file = create_excel_report(data, columns)
        
        # Generate filename with current timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'webhook_report_{timestamp}.xlsx'
        
        # Return file for download
        return send_file(
            excel_file,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({"error": f"Failed to generate report: {str(e)}"}), 500

@export_bp.route('/excel/scheduled', methods=['POST'])
def schedule_export():
    """
    Create a scheduled export for regular reporting
    This could be extended to email reports automatically
    """
    
    data = request.json
    days_back = data.get('days_back', 7)  # Default to last 7 days
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    try:
        # Fetch data
        columns, webhook_data = fetch_webhook_data(start_date, end_date)
        
        if not webhook_data:
            return jsonify({"message": "No data found for the specified period"}), 204
        
        # Create Excel report
        excel_file = create_excel_report(webhook_data, columns)
        
        # In a real application, you might:
        # 1. Save the file to a cloud storage service
        # 2. Email it to stakeholders
        # 3. Upload to a shared folder
        
        # For now, we'll just return success
        return jsonify({
            "message": "Scheduled export completed",
            "period": f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
            "records_processed": len(webhook_data)
        }), 200
        
    except Exception as e:
        return jsonify({"error": f"Scheduled export failed: {str(e)}"}), 500

@export_bp.route('/stats', methods=['GET'])
def get_quick_stats():
    """
    Get quick statistics without generating full Excel report
    This is useful for dashboard displays
    """
    
    try:
        # Get basic stats from last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        columns, data = fetch_webhook_data(start_date, end_date)
        
        if not data:
            return jsonify({"message": "No data found"}), 204
        
        df = pd.DataFrame(data, columns=columns)
        
        # Calculate key metrics
        stats = {
            "total_transactions": len(df),
            "total_revenue": float(df['amount'].sum()) if 'amount' in df.columns else 0,
            "total_commission": float(df['commission_amount'].sum()) if 'commission_amount' in df.columns else 0,
            "platforms": df['platform'].nunique() if 'platform' in df.columns else 0,
            "date_range": {
                "start": start_date.strftime('%Y-%m-%d'),
                "end": end_date.strftime('%Y-%m-%d')
            }
        }
        
        return jsonify(stats), 200
        
    except Exception as e:
        return jsonify({"error": f"Failed to get stats: {str(e)}"}), 500