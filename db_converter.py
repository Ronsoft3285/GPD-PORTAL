import os
import sqlite3
import pandas as pd
from datetime import datetime

class DatabaseConverter:
    """Handles Excel to SQL database conversion"""
    
    def __init__(self, database_path, upload_folder):
        self.database_path = database_path
        self.upload_folder = upload_folder
        self.allowed_extensions = {'xlsx', 'xls', 'csv'}
        
        # Create folders if they don't exist
        os.makedirs(upload_folder, exist_ok=True)
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
    
    def init_db(self):
        """Initialize the database with tables"""
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        
        # Students table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id TEXT UNIQUE,
                full_name TEXT,
                email TEXT,
                department TEXT,
                year TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Pastors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pastors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pastor_id TEXT UNIQUE,
                full_name TEXT,
                email TEXT,
                phone TEXT,
                assignment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Campuses table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS campuses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campus_id TEXT UNIQUE,
                campus_name TEXT,
                location TEXT,
                coordinates TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Upload logs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS upload_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                category TEXT,
                record_count INTEGER,
                status TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def allowed_file(self, filename):
        """Check if file has allowed extension"""
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in self.allowed_extensions
    
    def map_columns(self, df, category):
        """Map Excel columns to database columns based on category"""
        column_mapping = {
            'students': {
                'student_id': ['student id', 'id', 'matric number'],
                'full_name': ['full name', 'name', 'student name'],
                'email': ['email', 'e-mail'],
                'department': ['department', 'dept'],
                'year': ['year', 'level', 'class']
            },
            'pastors': {
                'pastor_id': ['pastor id', 'id'],
                'full_name': ['full name', 'name', 'pastor name'],
                'email': ['email', 'e-mail'],
                'phone': ['phone', 'phone number', 'mobile'],
                'assignment': ['assignment', 'church', 'station']
            },
            'campuses': {
                'campus_id': ['campus id', 'id'],
                'campus_name': ['campus name', 'name', 'campus'],
                'location': ['location', 'address'],
                'coordinates': ['coordinates', 'coords', 'lat/long']
            }
        }
        
        mapping = column_mapping.get(category.lower(), {})
        new_df = pd.DataFrame()
        
        # Normalize column names to lowercase
        df.columns = [col.lower().strip() for col in df.columns]
        
        for db_col, excel_cols in mapping.items():
            for excel_col in excel_cols:
                if excel_col in df.columns:
                    new_df[db_col] = df[excel_col]
                    break
        
        return new_df
    
    def convert_excel_to_sql(self, filepath, category, description=''):
        """Convert Excel file to SQL database"""
        try:
            # Read Excel file
            if filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)
            
            # Clean data
            df = df.dropna(how='all')
            df = df.fillna('')
            
            # Map columns
            df_mapped = self.map_columns(df, category)

            # If mapping produced no useful columns, fall back to dynamic table
            dynamic_table = False
            if df_mapped.empty or len(df_mapped.columns) == 0:
                dynamic_table = True
                # use original dataframe (normalized column names)
                df_dynamic = df.copy()
                df_dynamic.columns = [str(c).lower().strip().replace(' ', '_') for c in df_dynamic.columns]

                # sanitize table name: prefer category (if provided), else filename base
                if category and category.lower() in ['students', 'pastors', 'campuses']:
                    table_name = category.lower()
                else:
                    base = os.path.splitext(os.path.basename(filepath))[0]
                    table_name = ''.join(ch if ch.isalnum() or ch == '_' else '_' for ch in base.lower())

                # create table dynamically with TEXT columns for each column in df_dynamic
                col_defs = ', '.join([f"{col} TEXT" for col in df_dynamic.columns])
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {col_defs}, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
                cursor = sqlite3.connect(self.database_path).cursor()
                cursor.execute(create_sql)
                cursor.connection.commit()
                cursor.connection.close()
            
            # Insert into database
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()

            success_count = 0
            error_count = 0
            errors = []

            if dynamic_table:
                # insert using df_dynamic and table_name determined above
                for idx, row in df_dynamic.iterrows():
                    try:
                        columns = list(df_dynamic.columns)
                        values = [str(row[col]) for col in columns]
                        placeholders = ','.join(['?' for _ in columns])
                        insert_query = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})'
                        cursor.execute(insert_query, values)
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        errors.append(f"Row {idx + 1}: {str(e)}")
            else:
                table_name = category
                for idx, row in df_mapped.iterrows():
                    try:
                        # Prepare data
                        columns = [col for col in df_mapped.columns if row[col] != '']
                        values = [row[col] for col in columns]

                        if not columns:
                            continue

                        placeholders = ','.join(['?' for _ in columns])
                        insert_query = f'INSERT INTO {table_name} ({",".join(columns)}) VALUES ({placeholders})'

                        cursor.execute(insert_query, values)
                        success_count += 1
                    except sqlite3.IntegrityError as e:
                        error_count += 1
                        errors.append(f"Row {idx + 1}: {str(e)}")
                    except Exception as e:
                        error_count += 1
                        errors.append(f"Row {idx + 1}: {str(e)}")
            
            conn.commit()
            
            # Log the upload
            filename = os.path.basename(filepath)
            cursor.execute('''
                INSERT INTO upload_logs (file_name, category, record_count, status, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (filename, category, success_count, 'success' if error_count == 0 else 'partial', description))
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'category': category,
                'records_inserted': success_count,
                'errors': error_count,
                'error_details': errors[:10] if errors else []
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Error converting Excel file: {str(e)}'
            }
    
    def get_data(self, category):
        """Retrieve data from database"""
        try:
            if category.lower() not in ['students', 'pastors', 'campuses']:
                return {
                    'success': False,
                    'error': 'Invalid category'
                }
            
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            query = f'SELECT * FROM {category.lower()} ORDER BY created_at DESC LIMIT 100'
            cursor.execute(query)
            
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            
            conn.close()
            
            return {
                'success': True,
                'category': category,
                'count': len(data),
                'data': data
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Error retrieving data: {str(e)}'
            }
    
    def get_upload_logs(self):
        """Get upload history"""
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM upload_logs ORDER BY created_at DESC LIMIT 50')
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            
            conn.close()
            
            return {
                'success': True,
                'logs': data
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Error retrieving logs: {str(e)}'
            }
    
    def get_stats(self):
        """Get database statistics"""
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) FROM students')
            students_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM pastors')
            pastors_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM campuses')
            campuses_count = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'success': True,
                'stats': {
                    'total_students': students_count,
                    'total_pastors': pastors_count,
                    'total_campuses': campuses_count,
                    'total_records': students_count + pastors_count + campuses_count
                }
            }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Error retrieving stats: {str(e)}'
            }
