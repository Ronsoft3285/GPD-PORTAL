# GPD Portal - Admin Dashboard

A comprehensive admin dashboard for managing student, pastor, and campus data with Excel to SQL conversion.

## Features

- **Three Admin Panels**: Upload dataset, upload individual records, and upload images
- **Excel to SQL Conversion**: Automatically converts Excel files to a SQL database
- **Drag & Drop**: Easy file upload with drag-and-drop support
- **Data Validation**: Automatic column mapping and data cleaning
- **API Endpoints**: RESTful API for data management
- **Responsive Design**: Works on desktop and mobile devices

## Project Structure

```
GPD-PORTAL/
├── app.py                    # Flask backend server
├── index.html               # Home page
├── admin.html              # Admin dashboard with 3 panels
├── upload_dataset.html     # Bulk dataset upload
├── upload_individual.html  # Individual record upload
├── upload_image.html       # Image upload
├── css_file/
│   └── style.css
├── uploads/                # Uploaded files (created automatically)
├── database/               # SQLite database (created automatically)
└── requirements.txt        # Python dependencies
```

## Setup Instructions

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the Backend Server

```bash
python app.py
```

The server will start at `http://localhost:5000`

### 3. Open the Admin Dashboard

Open `admin.html` in your web browser or serve it using a local web server:

```bash
# Using Python 3
python -m http.server 8000

# Using Node.js http-server
npx http-server
```

Then navigate to `http://localhost:8000/admin.html`

## API Endpoints

### Upload Dataset
**POST** `/api/upload-dataset`

Upload an Excel/CSV file to convert to database.

**Parameters:**
- `file` (form-data): Excel or CSV file
- `category` (form-data): students, pastors, or campuses
- `description` (form-data): Optional description

**Response:**
```json
{
    "success": true,
    "message": "Dataset uploaded successfully",
    "category": "students",
    "records_inserted": 150,
    "errors": 2,
    "error_details": []
}
```

### Get Data
**GET** `/api/get-data/<category>`

Retrieve data by category (students, pastors, or campuses).

**Response:**
```json
{
    "success": true,
    "category": "students",
    "count": 150,
    "data": [...]
}
```

### Upload Logs
**GET** `/api/upload-logs`

Get history of all uploads.

### Statistics
**GET** `/api/stats`

Get database statistics.

```json
{
    "success": true,
    "stats": {
        "total_students": 150,
        "total_pastors": 50,
        "total_campuses": 10,
        "total_records": 210
    }
}
```

### Health Check
**GET** `/health`

Check if server is running.

## Excel File Format

### Students
| Student ID | Full Name | Email | Department | Year |
|---|---|---|---|---|
| STU001 | John Doe | john@email.com | Computer Science | 200 Level |

### Pastors
| Pastor ID | Full Name | Email | Phone | Assignment |
|---|---|---|---|---|
| PST001 | Rev. Smith | smith@email.com | +234123456789 | Main Campus |

### Campuses
| Campus ID | Campus Name | Location | Coordinates |
|---|---|---|---|---|
| CMP001 | Main Campus | Lagos | 6.5244, 3.3792 |

## Database Schema

### Students Table
- `id` (INTEGER PRIMARY KEY)
- `student_id` (TEXT UNIQUE)
- `full_name` (TEXT)
- `email` (TEXT)
- `department` (TEXT)
- `year` (TEXT)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

### Pastors Table
- `id` (INTEGER PRIMARY KEY)
- `pastor_id` (TEXT UNIQUE)
- `full_name` (TEXT)
- `email` (TEXT)
- `phone` (TEXT)
- `assignment` (TEXT)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

### Campuses Table
- `id` (INTEGER PRIMARY KEY)
- `campus_id` (TEXT UNIQUE)
- `campus_name` (TEXT)
- `location` (TEXT)
- `coordinates` (TEXT)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

## Column Mapping

The system automatically maps common Excel column names to database fields:

**Students:**
- Student ID: "student id", "id", "matric number"
- Full Name: "full name", "name", "student name"
- Email: "email", "e-mail"
- Department: "department", "dept"
- Year: "year", "level", "class"

**Pastors:**
- Pastor ID: "pastor id", "id"
- Full Name: "full name", "name", "pastor name"
- Email: "email", "e-mail"
- Phone: "phone", "phone number", "mobile"
- Assignment: "assignment", "church", "station"

**Campuses:**
- Campus ID: "campus id", "id"
- Campus Name: "campus name", "name", "campus"
- Location: "location", "address"
- Coordinates: "coordinates", "coords", "lat/long"

## Features in Detail

### 1. Upload Dataset Panel
- Drag & drop Excel/CSV files
- Automatic column mapping
- Bulk insert to database
- Error handling and reporting

### 2. Upload Individual Panel
- Dynamic forms based on data type
- One-record-at-a-time entry
- Supports Students, Pastors, and Campuses

### 3. Upload Image Panel
- Image drag & drop
- Preview before upload
- Image metadata display

## Error Handling

The system handles various error scenarios:
- Invalid file formats
- Missing required columns
- Duplicate entries
- Data type mismatches
- Database connection errors

All errors are logged and reported back to the user with detailed messages.

## Security Considerations

- File upload size limit: 50MB
- Only allowed file types accepted
- SQL injection protection via parameterized queries
- CORS enabled for cross-origin requests
- Uploaded files are cleaned up after processing

## Future Enhancements

- User authentication and authorization
- Data export to Excel/CSV
- Advanced search and filtering
- Data validation rules
- Batch operations
- Webhook notifications
- Image hosting and storage
- API rate limiting

## Support

For issues or questions, please refer to the project documentation or contact the development team.

## License

This project is licensed under the MIT License.
