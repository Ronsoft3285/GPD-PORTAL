# app.py
import os
import sqlite3
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime
from db_converter import DatabaseConverter

app = Flask(__name__, static_folder=os.path.dirname(__file__))
CORS(app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
DATABASE_FOLDER = os.path.join(BASE_DIR, 'database')
IMAGES_FOLDER = os.path.join(BASE_DIR, 'images')
DATABASE_PATH = os.path.join(DATABASE_FOLDER, 'gpd_portal.db')

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DATABASE_FOLDER, exist_ok=True)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
print(f"Images folder ready: {IMAGES_FOLDER}")

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['IMAGES_FOLDER'] = IMAGES_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

db = DatabaseConverter(DATABASE_PATH, UPLOAD_FOLDER)


@app.route('/')
def home():
    return send_from_directory(BASE_DIR, 'admin.html')


@app.route('/<path:filename>')
def serve_static(filename):
    return send_from_directory(BASE_DIR, filename)


@app.route('/images/<filename>')
def serve_image(filename):
    return send_from_directory(IMAGES_FOLDER, filename)


@app.route('/api/upload-image', methods=['POST'])
def upload_image():
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'No image provided'}), 400

        file = request.files['image']
        name = request.form.get('name', '').strip()

        if not name:
            return jsonify({'success': False, 'error': 'Name is required'}), 400
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in {'.jpg', '.jpeg', '.png', '.gif', '.webp'}:
            return jsonify({'success': False, 'error': 'Invalid image format'}), 400

        timestamp = datetime.now().strftime('%Y%m%d')
        safe_name = secure_filename(name.replace(' ', '_'))
        filename = f"{safe_name}_{timestamp}{ext}"
        filepath = os.path.join(IMAGES_FOLDER, filename)
        file.save(filepath)

        image_url = f"/images/{filename}"

        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()
        cur.execute(
            "UPDATE gpd_records SET image_path = ? WHERE TRIM(LOWER(name)) = TRIM(LOWER(?))",
            (image_url, name)
        )
        affected = cur.rowcount
        conn.commit()
        conn.close()

        if affected == 0:
            os.remove(filepath)
            return jsonify({'success': False, 'error': f'No record found: "{name}"'}), 404

        return jsonify({'success': True, 'message': 'Photo linked successfully!', 'image_path': image_url})

    except Exception as e:
        print(f"Image upload error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/upload-dataset', methods=['POST'])
def upload_dataset():
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['file']
        category = request.form.get('category', 'regional')
        description = request.form.get('description', '')

        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        if not db.allowed_file(file.filename):
            return jsonify({'error': 'Only .xlsx, .xls, .csv allowed'}), 400

        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        saved_name = timestamp + "_" + filename
        filepath = os.path.join(UPLOAD_FOLDER, saved_name)
        file.save(filepath)

        result = db.convert_excel_to_sql(filepath, category, description)

        if result['success']:
            return jsonify({
                'success': True,
                'message': result['message'],
                'records_inserted': result['records_inserted'],
                'records_skipped': result.get('records_skipped', 0)
            }), 200
        else:
            return jsonify({'error': result['error']}), 400

    except Exception as e:
        print(f"Dataset upload error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/add-record', methods=['POST'])
def add_record():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data received'}), 400

        typ = data.get('type', '').strip().lower()
        name = data.get('name', '').strip()

        if typ not in ['regional', 'group_regional']:
            return jsonify({'success': False, 'error': 'Invalid category'}), 400
        if not name:
            return jsonify({'success': False, 'error': 'Name is required'}), 400

        conn = sqlite3.connect(DATABASE_PATH)
        cur = conn.cursor()

        # Insert with all fields — name is NOT NULL and UNIQUE
        cur.execute('''
            INSERT INTO gpd_records (region, designation, name, kc_id, blw_zone)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            data.get('region', '').strip(),
            data.get('designation', '').strip(),
            name,
            data.get('kc_id', '').strip(),
            data.get('blw_zone', '').strip()
        ))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'message': 'Record added successfully!'}), 200

    except sqlite3.IntegrityError as e:
        if 'UNIQUE constraint failed' in str(e):
            return jsonify({'success': False, 'error': f'Duplicate name: "{name}" already exists'}), 400
        else:
            return jsonify({'success': False, 'error': 'Database error'}), 500
    except Exception as e:
        print(f"Add record error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/health')
def health():
    return jsonify({'status': 'OK', 'database': DATABASE_PATH})


if __name__ == '__main__':
    db.init_db()
    db.add_image_path_column_if_missing()
    print("GPD Portal is running at http://127.0.0.1:5000")
    app.run(debug=True, host='0.0.0.0', port=5000)