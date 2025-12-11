import os
import sqlite3
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from werkzeug.utils import secure_filename
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
DATABASE_FOLDER = os.path.join(os.path.dirname(__file__), 'database')
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# Create folders if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATABASE_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Initialize database
DATABASE_PATH = os.path.join(DATABASE_FOLDER, 'gpd_portal.db')

def init_db():
    """Initialize the database with tables"""
    conn = sqlite3.connect(DATABASE_PATH)
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

def allowed_file(filename):
    """Check if file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def map_columns(df, category):
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

@app.route('/api/upload-dataset', methods=['POST'])
def upload_dataset():
    """Handle Excel file upload and convert to SQL database"""
    try:
        # Check if file is in request
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        category = request.form.get('category', '').lower()
        description = request.form.get('description', '')
        
        if not category:
            return jsonify({'error': 'Category is required'}), 400
        
        if category not in ['students', 'pastors', 'campuses']:
            return jsonify({'error': f'Invalid category: {category}'}), 400
        
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Use xlsx, xls, or csv'}), 400
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        filename = timestamp + filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Read Excel file
        try:
            if file.filename.endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)
        except Exception as e:
            return jsonify({'error': f'Error reading file: {str(e)}'}), 400
        
        # Clean data
        df = df.dropna(how='all')
        df = df.fillna('')
        
        # Map columns
        df_mapped = map_columns(df, category)
        
        if df_mapped.empty or len(df_mapped.columns) == 0:
            return jsonify({'error': 'No matching columns found in Excel file'}), 400
        
        # Insert into database
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        table_name = category
        success_count = 0
        error_count = 0
        errors = []
        
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
        cursor.execute('''
            INSERT INTO upload_logs (file_name, category, record_count, status, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (filename, category, success_count, 'success' if error_count == 0 else 'partial', description))
        
        conn.commit()
        conn.close()
        
        # Clean up uploaded file
        os.remove(filepath)
        
        return jsonify({
            'success': True,
            'message': f'Dataset uploaded successfully',
            'category': category,
            'records_inserted': success_count,
            'errors': error_count,
            'error_details': errors[:10] if errors else []  # Limit error details
        }), 200
    
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/api/get-data/<category>', methods=['GET'])
def get_data(category):
    """Retrieve data from database"""
    try:
        if category.lower() not in ['students', 'pastors', 'campuses']:
            return jsonify({'error': 'Invalid category'}), 400
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = f'SELECT * FROM {category.lower()} ORDER BY created_at DESC LIMIT 100'
        cursor.execute(query)
        
        rows = cursor.fetchall()
        data = [dict(row) for row in rows]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'category': category,
            'count': len(data),
            'data': data
        }), 200
    
    except Exception as e:
        return jsonify({'error': f'Error retrieving data: {str(e)}'}), 500

@app.route('/api/upload-logs', methods=['GET'])
def get_upload_logs():
    """Get upload history"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM upload_logs ORDER BY created_at DESC LIMIT 50')
        rows = cursor.fetchall()
        data = [dict(row) for row in rows]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'logs': data
        }), 200
    
    except Exception as e:
        return jsonify({'error': f'Error retrieving logs: {str(e)}'}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM students')
        students_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM pastors')
        pastors_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM campuses')
        campuses_count = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_students': students_count,
                'total_pastors': pastors_count,
                'total_campuses': campuses_count,
                'total_records': students_count + pastors_count + campuses_count
            }
        }), 200
    
    except Exception as e:
        return jsonify({'error': f'Error retrieving stats: {str(e)}'}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'Server is running'}), 200

if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
