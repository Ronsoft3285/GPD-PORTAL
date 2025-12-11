import os
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from werkzeug.utils import secure_filename
from datetime import datetime
from db_converter import DatabaseConverter

app = Flask(__name__, static_folder=os.path.dirname(__file__))
CORS(app)

# Configuration
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
DATABASE_FOLDER = os.path.join(os.path.dirname(__file__), 'database')
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

# Create folders if they don't exist
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
        
        if not db.allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed. Use xlsx, xls, or csv'}), 400
        
        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_')
        filename = timestamp + filename
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Convert Excel to SQL
        result = db.convert_excel_to_sql(filepath, category, description)
        
        # Clean up uploaded file
        try:
            os.remove(filepath)
        except:
            pass
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': f'Dataset uploaded successfully',
                'category': result['category'],
                'records_inserted': result['records_inserted'],
                'errors': result['errors'],
                'error_details': result['error_details']
            }), 200
        else:
            return jsonify({'error': result['error']}), 400
    
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/api/get-data/<category>', methods=['GET'])
def get_data(category):
    """Retrieve data from database"""
    result = db.get_data(category)
    
    if result['success']:
        return jsonify(result), 200
    else:
        return jsonify({'error': result['error']}), 400

@app.route('/api/upload-logs', methods=['GET'])
def get_upload_logs():
    """Get upload history"""
    result = db.get_upload_logs()
    
    if result['success']:
        return jsonify(result), 200
    else:
        return jsonify({'error': result['error']}), 400

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    result = db.get_stats()
    
    if result['success']:
        return jsonify(result), 200
    else:
        return jsonify({'error': result['error']}), 400

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'Server is running'}), 200

if __name__ == '__main__':
    db.init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
