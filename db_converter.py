# db_converter.py
import os
import sqlite3
import pandas as pd
from datetime import datetime


class DatabaseConverter:
    def __init__(self, database_path, upload_folder):
        self.database_path = database_path
        self.upload_folder = upload_folder
        self.allowed_extensions = {"xlsx", "xls", "csv"}

        os.makedirs(upload_folder, exist_ok=True)
        os.makedirs(os.path.dirname(database_path), exist_ok=True)

    def init_db(self):
        """Create tables with UNIQUE constraint on 'name'"""
        conn = sqlite3.connect(self.database_path)
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS gpd_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                region TEXT,
                designation TEXT,
                name TEXT UNIQUE NOT NULL,      -- Prevents duplicates forever
                kc_id TEXT,
                blw_zone TEXT,
                image_path TEXT DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cur.execute('''
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
        print("Database initialized with NO DUPLICATES allowed")

    def add_image_path_column_if_missing(self):
        conn = sqlite3.connect(self.database_path)
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(gpd_records)")
        columns = [info[1] for info in cur.fetchall()]
        if 'image_path' not in columns:
            cur.execute("ALTER TABLE gpd_records ADD COLUMN image_path TEXT")
            print("Added image_path column")
        conn.commit()
        conn.close()

    def allowed_file(self, filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in self.allowed_extensions

    def map_columns(self, df, category=None):
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
        for col in ['region', 'designation', 'name', 'kc_id', 'blw_zone']:
            if col not in mapped.columns:
                mapped[col] = ''
        return mapped

    def convert_excel_to_sql(self, filepath, category, description=''):
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
            skipped = 0
            errors = []

            for idx, row in df_mapped.iterrows():
                name = str(row['name']).strip()
                if not name:
                    errors.append(f"Row {idx+2}: Empty name")
                    continue

                try:
                    cur.execute(
                        '''INSERT INTO gpd_records(region, designation, name, kc_id, blw_zone)
                           VALUES (?, ?, ?, ?, ?)''',
                        (str(row['region']).strip(),
                         str(row['designation']).strip(),
                         name,
                         str(row['kc_id']).strip(),
                         str(row['blw_zone']).strip())
                    )
                    inserted += 1
                except sqlite3.IntegrityError as e:
                    if 'UNIQUE constraint failed' in str(e):
                        skipped += 1
                    else:
                        errors.append(f"Row {idx+2}: {str(e)}")
                except Exception as e:
                    errors.append(f"Row {idx+2}: {str(e)}")

            conn.commit()

            fname = os.path.basename(filepath)
            log_desc = f"{inserted} added, {skipped} duplicates skipped"
            cur.execute(
                '''INSERT INTO upload_logs(file_name, category, record_count, status, description)
                   VALUES (?, ?, ?, ?, ?)''',
                (fname, category, inserted + skipped, 'success' if skipped == 0 else 'partial', log_desc)
            )
            conn.commit()
            conn.close()

            return {
                'success': True,
                'records_inserted': inserted,
                'records_skipped': skipped,
                'message': log_desc
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_data(self, category):
        try:
            conn = sqlite3.connect(self.database_path)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute('SELECT * FROM gpd_records ORDER BY created_at DESC')
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            return {'success': True, 'data': rows}
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
            total = cur.fetchone()[0]
            conn.close()
            return {'success': True, 'stats': {'total_records': total}}
        except Exception as e:
            return {'success': False, 'error': str(e)}