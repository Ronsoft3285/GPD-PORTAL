import os
import sqlite3
import pandas as pd
from datetime import datetime


class DatabaseConverter:
    """Handles Excel to SQL database conversion.

    This class provides methods expected by `app.py`:
    - init_db()
    - allowed_file(filename)
    - convert_excel_to_sql(filepath, category, description='')
    - get_data(category)
    - get_upload_logs()
    - get_stats()

    The converter enforces a canonical `gpd_records` table with columns:
    region, designation, name, kc_id, blw_zone
    """

    def __init__(self, database_path, upload_folder):
        self.database_path = database_path
        self.upload_folder = upload_folder
        self.allowed_extensions = {"xlsx", "xls", "csv"}

        os.makedirs(upload_folder, exist_ok=True)
        os.makedirs(os.path.dirname(database_path), exist_ok=True)

    def init_db(self):
        """Reset user tables and create canonical tables."""
        conn = sqlite3.connect(self.database_path)
        cur = conn.cursor()

        # Drop existing non-system tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{t}"')

        # Create canonical table
        cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS gpd_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT,
                designation TEXT,
                name TEXT,
                kc_id TEXT,
                blw_zone TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        # upload logs
        cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS upload_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT,
                category TEXT,
                record_count INTEGER,
                status TEXT,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
        )

        conn.commit()
        conn.close()

    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in self.allowed_extensions

    def map_columns(self, df, category=None):
        """Map dataframe columns to the canonical gpd_records columns."""
        # normalize headers
        df = df.copy()
        df.columns = [str(c).lower().strip() for c in df.columns]

        candidates = {
            'region': ['region', 'state', 'area', 'zone', 'region_name'],
            'designation': ['designation', 'role', 'position', 'title'],
            'name': ['name', 'full name', 'person', 'person name'],
            'kc_id': ['kc id', 'kc_id', 'kcid', 'id', 'identifier'],
            'blw_zone': ['blw zone', 'blw_zone', 'blwzone', 'zone']
        }

        mapped = pd.DataFrame()
        for target, keys in candidates.items():
            for k in keys:
                if k in df.columns:
                    mapped[target] = df[k]
                    break
        # ensure all target columns exist
        for col in ['region', 'designation', 'name', 'kc_id', 'blw_zone']:
            if col not in mapped.columns:
                mapped[col] = ''

        return mapped

    def convert_excel_to_sql(self, filepath, category, description=''):
        """Read spreadsheet, map columns, and insert into `gpd_records`."""
        try:
            if filepath.lower().endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            df = df.dropna(how='all').fillna('')
            df_mapped = self.map_columns(df, category)

            conn = sqlite3.connect(self.database_path)
            cur = conn.cursor()

            inserted = 0
            errors = []

            for idx, row in df_mapped.iterrows():
                try:
                    vals = [str(row[c]).strip() for c in ['region', 'designation', 'name', 'kc_id', 'blw_zone']]
                    cur.execute(
                        'INSERT INTO gpd_records(region, designation, name, kc_id, blw_zone) VALUES (?, ?, ?, ?, ?)',
                        vals
                    )
                    inserted += 1
                except Exception as e:
                    errors.append(f'Row {idx+1}: {str(e)}')

            conn.commit()

            # log upload
            fname = os.path.basename(filepath)
            status = 'success' if not errors else 'partial'
            cur.execute(
                'INSERT INTO upload_logs(file_name, category, record_count, status, description) VALUES (?, ?, ?, ?, ?)',
                (fname, category or 'gpd_records', inserted, status, description)
            )
            conn.commit()
            conn.close()

            return {
                'success': True,
                'category': category,
                'records_inserted': inserted,
                'errors': len(errors),
                'error_details': errors[:10]
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_data(self, category):
        try:
            table = 'gpd_records'
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(f'SELECT * FROM {table} ORDER BY created_at DESC LIMIT 100')
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return {'success': True, 'category': table, 'count': len(rows), 'data': rows}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_upload_logs(self):
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM upload_logs ORDER BY created_at DESC LIMIT 50')
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return {'success': True, 'logs': rows}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_stats(self):
        try:
            conn = sqlite3.connect(self.database_path)
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM gpd_records')
            records = cur.fetchone()[0]
            cur.execute('SELECT COUNT(*) FROM upload_logs')
            uploads = cur.fetchone()[0]
            conn.close()
            return {'success': True, 'stats': {'total_records': records, 'upload_logs': uploads}}
        except Exception as e:
            return {'success': False, 'error': str(e)}
def convert_excel_to_sql(self, filepath, category, description=""):
    try:
        # Load Excel/CSV
        if filepath.endswith(".csv"):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)

        df = df.dropna(how="all").fillna("")

        # Map to required columns
        df_mapped = self.map_columns(df)

        target_cols = ["region", "designation", "name", "kc_id", "blw_zone"]

        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        insert_count = 0
        error_count = 0
        errors = []

        # Insert each row into table
        for idx, row in df_mapped.iterrows():
            try:
                values = [str(row[col]).strip() for col in target_cols]
                cursor.execute(
                    """
                    INSERT INTO gpd_records(region, designation, name, kc_id, blw_zone)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    values
                )
                insert_count += 1
            except Exception as e:
                error_count += 1
                errors.append(f"Row {idx+1}: {str(e)}")

        conn.commit()

        # Store upload log
        filename = os.path.basename(filepath)
        status = "success" if error_count == 0 else "partial"

        cursor.execute("""
            INSERT INTO upload_logs(file_name, category, record_count, status, description)
            VALUES (?, ?, ?, ?, ?)
        """, (filename, category, insert_count, status, description))

        conn.commit()
        conn.close()

        return {
            "success": True,
            "records_inserted": insert_count,
            "errors": error_count,
            "error_details": errors[:10],
            "category": category
        }

    except Exception as e:
        return {"success": False, "error": str(e)}
