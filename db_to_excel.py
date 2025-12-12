"""
Convert SQLite database (gpd_portal.db) to Excel format (new_data.xlsx)
Each table in the database becomes a separate sheet in the Excel file
"""

import sqlite3
import pandas as pd
import os
from datetime import datetime

def db_to_excel(db_path='database/gpd_portal.db', output_file='new_data.xlsx'):
    """
    Convert all tables from SQLite database to Excel file
    
    Args:
        db_path (str): Path to the SQLite database file
        output_file (str): Name of the output Excel file
    
    Returns:
        dict: Status of the conversion process
    """
    try:
        # Check if database exists
        if not os.path.exists(db_path):
            return {
                'success': False,
                'error': f'Database file not found: {db_path}'
            }
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables except sqlite internal tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if not tables:
            return {
                'success': False,
                'error': 'No tables found in database'
            }
        
        # Convert each table to a DataFrame and store in dictionary
        table_stats = {}
        dataframes = {}
        
        for table_name in tables:
            try:
                # Read table into DataFrame using SQL query
                df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
                dataframes[table_name] = df
                
                # Store stats
                table_stats[table_name] = {
                    'rows': len(df),
                    'columns': len(df.columns)
                }
                
                print(f"✓ Read table '{table_name}': {len(df)} rows, {len(df.columns)} columns")
            
            except Exception as e:
                table_stats[table_name] = {'error': str(e)}
                print(f"✗ Error reading table '{table_name}': {str(e)}")
        
        # Write all dataframes to Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as excel_writer:
            for table_name, df in dataframes.items():
                try:
                    df.to_excel(excel_writer, sheet_name=table_name, index=False)
                    print(f"✓ Wrote table '{table_name}' to Excel sheet")
                except Exception as e:
                    print(f"✗ Error writing table '{table_name}' to Excel: {str(e)}")
        conn.close()
        
        # Get file info
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
        
        return {
            'success': True,
            'message': f'Database successfully converted to Excel',
            'output_file': output_file,
            'file_size_mb': round(file_size, 2),
            'tables_converted': len(table_stats),
            'table_stats': table_stats,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': f'Conversion failed: {str(e)}'
        }


if __name__ == '__main__':
    print("=" * 60)
    print("Database to Excel Converter")
    print("=" * 60)
    print()
    
    # Run conversion
    result = db_to_excel()
    
    # Print results
    if result['success']:
        print(f"✅ Success!")
        print(f"Output file: {result['output_file']}")
        print(f"File size: {result['file_size_mb']} MB")
        print(f"Tables converted: {result['tables_converted']}")
        print()
        print("Table Details:")
        print("-" * 60)
        for table, stats in result['table_stats'].items():
            if 'error' in stats:
                print(f"  {table}: ERROR - {stats['error']}")
            else:
                print(f"  {table}: {stats['rows']} rows, {stats['columns']} columns")
        print()
        print(f"Conversion completed at: {result['timestamp']}")
    else:
        print(f"❌ Error: {result['error']}")
    
    print("=" * 60)