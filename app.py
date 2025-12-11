import os
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime
from db_converter import DatabaseConverter

app = Flask(__name__, static_folder=os.path.dirname(__file__))
CORS(app)

# Folders
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
DATABASE_FOLDER = os.path.join(os.path.dirname(__file__), 'database')
MAX_FILE_SIZE = 5 * 1024 * 1024  # 50MB

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATABASE_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Initialize database converter
DATABASE_PATH = os.path.join(DATABASE_FOLDER, 'gpd_portal.db')
db = DatabaseConverter(DATABASE_PATH, UPLOAD_FOLDER)


@app.route('/')
def home():
    """Serve admin.html"""
    return send_from_directory(os.path.dirname(__file__), 'admin.html')


@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory(os.path.dirname(__file__), filename)


@app.route('/api/upload-dataset', methods=['POST'])
def upload_dataset():
    """Handle Excel upload → save file → insert into gpd_records"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']
        category = request.form.get('category', '').lower()
        description = request.form.get('description', '')

        if not category:
            return jsonify({'error': 'Category is required'}), 400

        # UPDATED ✔ Allowed categories
        allowed_categories = ['gpd_records', 'regional', 'group_regional', 'group regional', 'rzm']

        if category not in allowed_categories:
            return jsonify({
                'error': f'Invalid category: {category}. Allowed: {allowed_categories}'
            }), 400

        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not db.allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Use xlsx, xls, csv'}), 400

        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        saved_filename = timestamp + filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], saved_filename)
        file.save(filepath)

        # Convert Excel → SQL (always inserts into gpd_records)
        result = db.convert_excel_to_sql(filepath, category, description)

        if result['success']:
            return jsonify({
                'success': True,
                'message': 'Dataset uploaded successfully',
                'category': 'gpd_records',
                'records_inserted': result['records_inserted'],
                'errors': result['errors'],
                'error_details': result['error_details'],
                'saved_to': saved_filename
            }), 200

        else:
            return jsonify({'error': result['error']}), 400

    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/api/get-data/<category>', methods=['GET'])
def get_data(category):
    """Retrieve data from gpd_records"""
    result = db.get_data(category)

    if result['success']:
        return jsonify(result), 200
    return jsonify({'error': result['error']}), 400


@app.route('/api/tables', methods=['GET'])
def list_tables():
    """Return list of tables with sample rows"""
    try:
        import sqlite3

        db_path = db.database_path
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        )
        tables = [r[0] for r in cur.fetchall()]

        result = []
        for t in tables:
            try:
                cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                count = cur.fetchone()[0]

                cur.execute(f'SELECT * FROM "{t}" LIMIT 5')
                rows = cur.fetchall()

                result.append({
                    'table': t,
                    'count': count,
                    'sample': [list(r) for r in rows]
                })
            except Exception as e:
                result.append({'table': t, 'error': str(e)})

        conn.close()
        return jsonify({'success': True, 'tables': result}), 200

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/add-record', methods=['POST'])
def add_record():
    """Insert a single record into gpd_records and log it."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No JSON payload provided'}), 400

        typ = (data.get('type') or '').lower().strip()
        if typ not in ['regional', 'group_regional', 'group regional', 'rzm']:
            return jsonify({'success': False, 'error': f'Invalid category: {typ}'}), 400

        region = data.get('region', '')
        designation = data.get('designation', '')
        name = data.get('name', '')
        kc_id = data.get('kc_id', '') or data.get('kc id', '')
        blw_zone = data.get('blw_zone', '')

        import sqlite3
        conn = sqlite3.connect(db.database_path)
        cur = conn.cursor()
        cur.execute(
            '''INSERT INTO gpd_records(region, designation, name, kc_id, blw_zone)
               VALUES (?, ?, ?, ?, ?)''',
            (region, designation, name, kc_id, blw_zone)
        )
        conn.commit()

        cur.execute(
            '''INSERT INTO upload_logs(file_name, category, record_count, status, description)
               VALUES (?, ?, ?, ?, ?)''',
            ('individual', typ, 1, 'success', 'individual insert')
        )
        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Record added'}), 200
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/upload-logs', methods=['GET'])
def get_upload_logs():
    """Return upload logs"""
    result = db.get_upload_logs()

    if result['success']:
        return jsonify(result), 200
    return jsonify({'error': result['error']}), 400


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Database stats"""
    result = db.get_stats()

    if result['success']:
        return jsonify(result), 200
    return jsonify({'error': result['error']}), 400


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'Server is running'}), 200


if __name__ == '__main__':
    db.init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
