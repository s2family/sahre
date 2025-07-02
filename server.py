#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Storage & Sharing Service
Complete rewrite with working banner management and performance settings
"""

import sqlite3
from datetime import datetime, timedelta, timezone
import os
import sys

# Fix SQLite datetime warning
sqlite3.register_adapter(datetime, lambda dt: dt.isoformat())
sqlite3.register_converter("DATETIME", lambda dt: datetime.fromisoformat(dt.decode()))

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, session, abort, Response
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
import uuid
import hashlib
import time
import threading
import secrets
import mimetypes
import qrcode
import io
import base64
from functools import wraps
import logging
import shutil
import mmap
import concurrent.futures
import json

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# ===== CONFIGURATION =====
UPLOAD_FOLDER = os.path.join('static', 'uploads', 'banners')
STORAGE_FOLDER = os.path.join('storage', 'files')  
TEMP_FOLDER = os.path.join(STORAGE_FOLDER, 'temp')

# Create directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(STORAGE_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

BANNER_MAX_SIZE = 16 * 1024 * 1024  # 16MB for banners
ALLOWED_BANNER_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}

# Performance settings
DEFAULT_CHUNK_SIZE_MB = 32
DEFAULT_MAX_CONCURRENT = 8
DEFAULT_MAX_FILE_SIZE_GB = 25
DEFAULT_BUFFER_SIZE_KB = 2048

# ===== DATABASE SETUP =====
def init_db():
    """Initialize database with all required tables"""
    conn = sqlite3.connect('file_storage.db')
    cursor = conn.cursor()
    
    # Files table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            original_name TEXT NOT NULL,
            stored_name TEXT NOT NULL,
            file_type TEXT,
            file_size INTEGER DEFAULT 0,
            mime_type TEXT,
            share_code TEXT UNIQUE NOT NULL,
            password TEXT,
            download_limit INTEGER DEFAULT 100,
            download_count INTEGER DEFAULT 0,
            uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME,
            last_accessed DATETIME,
            uploader_ip TEXT,
            description TEXT,
            is_public BOOLEAN DEFAULT 1
        )
    ''')
    
    # Visitors table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS visitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE,
            ip_address TEXT,
            user_agent TEXT,
            first_visit DATETIME,
            last_activity DATETIME,
            page_views INTEGER DEFAULT 1,
            is_active BOOLEAN DEFAULT 1
        )
    ''')
    
    # Banners table - Enhanced for full functionality
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS banners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            image_path TEXT,
            link_url TEXT,
            position TEXT CHECK(position IN ('left', 'right')) DEFAULT 'left',
            clicks INTEGER DEFAULT 0,
            status BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Settings table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            description TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Download stats table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS download_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id TEXT,
            file_name TEXT,
            download_ip TEXT,
            download_time DATETIME DEFAULT CURRENT_TIMESTAMP,
            user_agent TEXT,
            FOREIGN KEY (file_id) REFERENCES files (id)
        )
    ''')
    
    # Insert default settings
    default_settings = [
        ('admin_username', 'admin', 'Admin username'),
        ('admin_password_hash', generate_password_hash('admin123'), 'Admin password hash'),
        ('site_title', 'File Storage & Sharing', 'Site title'),
        ('maintenance_mode', 'false', 'Maintenance mode'),
        ('auto_cleanup_enabled', 'false', 'Auto cleanup enabled'),
        ('cleanup_interval_minutes', '60', 'Cleanup interval in minutes'),
        ('default_expire_days', '30', 'Default file expiration days'),
        ('max_file_size_gb', str(DEFAULT_MAX_FILE_SIZE_GB), 'Maximum file size in GB'),
        ('max_download_limit', '100', 'Default max downloads per file'),
        ('chunk_size_mb', str(DEFAULT_CHUNK_SIZE_MB), 'Upload chunk size in MB'),
        ('max_concurrent_chunks', str(DEFAULT_MAX_CONCURRENT), 'Maximum concurrent chunks'),
        ('buffer_size_kb', str(DEFAULT_BUFFER_SIZE_KB), 'I/O buffer size in KB'),
        ('connection_timeout', '300', 'Connection timeout in seconds'),
        ('enable_chunked_upload', 'true', 'Enable chunked upload'),
        ('enable_compression', 'true', 'Enable compression'),
        ('enable_caching', 'true', 'Enable caching'),
    ]
    
    for key, value, desc in default_settings:
        cursor.execute('INSERT OR IGNORE INTO settings (key, value, description) VALUES (?, ?, ?)', 
                      (key, value, desc))
    
    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")

# Initialize database
init_db()

# ===== UTILITY FUNCTIONS =====
def get_setting(key, default_value=None):
    """Get a setting value from database"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0]
        return default_value
    except Exception as e:
        logger.error(f"Error getting setting {key}: {e}")
        return default_value

def set_setting(key, value, description=None):
    """Set a setting value in database"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        if description:
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value, description, updated_at) 
                VALUES (?, ?, ?, ?)
            ''', (key, value, description, datetime.now(timezone.utc)))
        else:
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value, updated_at) 
                VALUES (?, ?, ?)
            ''', (key, value, datetime.now(timezone.utc)))
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error setting {key}: {e}")
        return False

def get_admin_settings():
    """Get admin-controlled settings"""
    try:
        return {
            'expire_days': int(get_setting('default_expire_days', 30)),
            'max_size_gb': int(get_setting('max_file_size_gb', DEFAULT_MAX_FILE_SIZE_GB)),
            'download_limit': int(get_setting('max_download_limit', 100))
        }
    except:
        return {'expire_days': 30, 'max_size_gb': DEFAULT_MAX_FILE_SIZE_GB, 'download_limit': 100}

def get_performance_settings():
    """Get performance-related settings"""
    try:
        return {
            'chunk_size_mb': int(get_setting('chunk_size_mb', DEFAULT_CHUNK_SIZE_MB)),
            'max_concurrent_chunks': int(get_setting('max_concurrent_chunks', DEFAULT_MAX_CONCURRENT)),
            'buffer_size_kb': int(get_setting('buffer_size_kb', DEFAULT_BUFFER_SIZE_KB)),
            'connection_timeout': int(get_setting('connection_timeout', 300)),
            'enable_chunked_upload': get_setting('enable_chunked_upload', 'true') == 'true',
            'enable_compression': get_setting('enable_compression', 'true') == 'true',
            'enable_caching': get_setting('enable_caching', 'true') == 'true',
        }
    except:
        return {
            'chunk_size_mb': DEFAULT_CHUNK_SIZE_MB,
            'max_concurrent_chunks': DEFAULT_MAX_CONCURRENT,
            'buffer_size_kb': DEFAULT_BUFFER_SIZE_KB,
            'connection_timeout': 300,
            'enable_chunked_upload': True,
            'enable_compression': True,
            'enable_caching': True,
        }

def get_banners(position=None):
    """Get banners from database"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        if position:
            cursor.execute('SELECT * FROM banners WHERE position = ? AND status = 1 ORDER BY id DESC', (position,))
        else:
            cursor.execute('SELECT * FROM banners WHERE status = 1 ORDER BY id DESC')
        
        banners = cursor.fetchall()
        conn.close()
        
        banner_list = []
        for banner in banners:
            banner_list.append({
                'id': banner[0],
                'title': banner[1],
                'description': banner[2],
                'image_path': banner[3],
                'link_url': banner[4],
                'position': banner[5],
                'clicks': banner[6],
                'status': banner[7],
                'created_at': banner[8]
            })
        
        return banner_list
        
    except Exception as e:
        logger.error(f"Error getting banners: {e}")
        return []

def generate_share_code():
    """Generate unique share code"""
    return hashlib.md5(str(uuid.uuid4()).encode()).hexdigest()[:12]

def get_file_type(filename):
    """Determine file type based on extension"""
    if not filename or '.' not in filename:
        return 'other'
    
    ext = filename.rsplit('.', 1)[1].lower()
    
    file_types = {
        'image': ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'svg'],
        'video': ['mp4', 'avi', 'mov', 'mkv', 'flv', 'webm', 'wmv'],
        'audio': ['mp3', 'wav', 'flac', 'aac', 'ogg', 'wma'],
        'document': ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt'],
        'archive': ['zip', 'rar', '7z', 'tar', 'gz']
    }
    
    for file_type, extensions in file_types.items():
        if ext in extensions:
            return file_type
    
    return 'other'

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f}{size_names[i]}"

def generate_qr_code(url):
    """Generate QR code for URL"""
    try:
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(url)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        img_buffer = io.BytesIO()
        img.save(img_buffer, format='PNG')
        img_buffer.seek(0)
        
        return base64.b64encode(img_buffer.getvalue()).decode()
    except Exception as e:
        logger.error(f"Error generating QR code: {e}")
        return None

def admin_required(f):
    """Decorator for admin authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    """Check if file extension is allowed for banners"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_BANNER_EXTENSIONS

# ===== VISITOR TRACKING =====
class VisitorTracker:
    def __init__(self):
        self.active_visitors = {}
        self.cleanup_thread = threading.Thread(target=self.cleanup_inactive_visitors, daemon=True)
        self.cleanup_thread.start()
    
    def track_visitor(self, request):
        try:
            session_id = session.get('session_id')
            if not session_id:
                session_id = str(uuid.uuid4())
                session['session_id'] = session_id
            
            ip_address = request.remote_addr
            user_agent = request.headers.get('User-Agent', '')
            current_time = datetime.now(timezone.utc)
            
            self.active_visitors[session_id] = current_time
            
            conn = sqlite3.connect('file_storage.db')
            cursor = conn.cursor()
            
            cursor.execute('SELECT id, page_views FROM visitors WHERE session_id = ?', (session_id,))
            visitor = cursor.fetchone()
            
            if visitor:
                cursor.execute('''
                    UPDATE visitors 
                    SET last_activity = ?, page_views = page_views + 1, is_active = 1
                    WHERE session_id = ?
                ''', (current_time, session_id))
            else:
                cursor.execute('''
                    INSERT INTO visitors (session_id, ip_address, user_agent, first_visit, last_activity)
                    VALUES (?, ?, ?, ?, ?)
                ''', (session_id, ip_address, user_agent, current_time, current_time))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"Visitor tracking error: {e}")
        
        return session_id
    
    def get_active_count(self):
        current_time = datetime.now(timezone.utc)
        cutoff_time = current_time - timedelta(minutes=5)
        
        self.active_visitors = {
            sid: last_activity for sid, last_activity in self.active_visitors.items()
            if last_activity > cutoff_time
        }
        
        return len(self.active_visitors)
    
    def cleanup_inactive_visitors(self):
        while True:
            try:
                time.sleep(300)  # 5 minutes
                current_time = datetime.now(timezone.utc)
                cutoff_time = current_time - timedelta(minutes=10)
                
                conn = sqlite3.connect('file_storage.db')
                cursor = conn.cursor()
                cursor.execute('UPDATE visitors SET is_active = 0 WHERE last_activity < ?', (cutoff_time,))
                conn.commit()
                conn.close()
                
            except Exception as e:
                logger.error(f"Visitor cleanup error: {e}")

visitor_tracker = VisitorTracker()

# ===== MIDDLEWARE =====
@app.before_request
def track_visitors():
    """Track visitor activity"""
    if request.endpoint and not request.endpoint.startswith('static'):
        visitor_tracker.track_visitor(request)

@app.before_request 
def update_max_content_length():
    """Update max content length before upload requests"""
    if request.endpoint in ['upload_file', 'upload_chunked']:
        max_size_gb = int(get_setting('max_file_size_gb', DEFAULT_MAX_FILE_SIZE_GB))
        app.config['MAX_CONTENT_LENGTH'] = max_size_gb * 1024 * 1024 * 1024

# ===== MAIN ROUTES =====
@app.route('/')
def index():
    """Main page"""
    try:
        left_banners = get_banners('left')
        right_banners = get_banners('right')
        admin_settings = get_admin_settings()
        
        return render_template('index.html', 
                             left_banners=left_banners, 
                             right_banners=right_banners,
                             recent_files=[],
                             stats=None,
                             admin_settings=admin_settings)
                             
    except Exception as e:
        logger.error(f"Homepage error: {e}")
        admin_settings = {'expire_days': 30, 'max_size_gb': DEFAULT_MAX_FILE_SIZE_GB, 'download_limit': 100}
        return render_template('index.html', 
                             left_banners=[], 
                             right_banners=[],
                             recent_files=[],
                             stats=None,
                             admin_settings=admin_settings)

@app.route('/api/performance-config')
def get_performance_config():
    """API to get performance settings for JavaScript"""
    try:
        settings = get_performance_settings()
        return jsonify({
            'success': True,
            'config': {
                'chunkSizeMB': settings['chunk_size_mb'],
                'maxConcurrentChunks': settings['max_concurrent_chunks'],
                'enableChunkedUpload': settings['enable_chunked_upload'],
                'bufferSizeKB': settings['buffer_size_kb'],
                'connectionTimeout': settings['connection_timeout']
            }
        })
    except Exception as e:
        logger.error(f"Performance config error: {e}")
        return jsonify({
            'success': True,
            'config': {
                'chunkSizeMB': DEFAULT_CHUNK_SIZE_MB,
                'maxConcurrentChunks': DEFAULT_MAX_CONCURRENT,
                'enableChunkedUpload': True,
                'bufferSizeKB': DEFAULT_BUFFER_SIZE_KB,
                'connectionTimeout': 300
            }
        })

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle regular file upload"""
    try:
        logger.info("Regular upload request received")
        
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'Không có file được chọn'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Không có file được chọn'}), 400
        
        admin_settings = get_admin_settings()
        
        # Check file size
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0)
        
        max_size = admin_settings['max_size_gb'] * 1024 * 1024 * 1024
        if file_size > max_size:
            return jsonify({
                'success': False, 
                'error': f'File quá lớn! Tối đa {admin_settings["max_size_gb"]}GB'
            }), 413
        
        description = request.form.get('description', '')
        expire_days = admin_settings['expire_days']
        download_limit = admin_settings['download_limit']
        
        # Generate file info
        file_id = str(uuid.uuid4())
        share_code = generate_share_code()
        original_name = secure_filename(file.filename)
        file_ext = original_name.rsplit('.', 1)[1].lower() if '.' in original_name else ''
        stored_name = f"{file_id}.{file_ext}" if file_ext else file_id
        file_path = os.path.join(STORAGE_FOLDER, stored_name)
        
        file_type = get_file_type(original_name)
        expires_at = datetime.now(timezone.utc) + timedelta(days=expire_days)
        
        # Save file
        buffer_size = get_performance_settings()['buffer_size_kb'] * 1024
        
        with open(file_path, 'wb') as f:
            while True:
                chunk = file.stream.read(buffer_size)
                if not chunk:
                    break
                f.write(chunk)
        
        # Create database record
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO files (
                id, original_name, stored_name, file_type, file_size, 
                mime_type, share_code, password, download_limit, expires_at, 
                uploader_ip, description, is_public
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_id, original_name, stored_name, file_type, file_size,
            file.mimetype or 'application/octet-stream', share_code, None, 
            download_limit, expires_at, request.remote_addr, description, True
        ))
        
        conn.commit()
        conn.close()
        
        # Generate response
        share_url = request.url_root + f"f/{share_code}"
        qr_code = generate_qr_code(share_url)
        
        logger.info(f"File uploaded successfully: {original_name} ({format_file_size(file_size)})")
        
        return jsonify({
            'success': True,
            'file_id': file_id,
            'share_code': share_code,  
            'share_url': share_url,
            'qr_code': qr_code,
            'expires_at': expires_at.isoformat(),
            'expire_days': expire_days,
            'file_size': format_file_size(file_size),
            'file_type': file_type
        })
        
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'success': False, 'error': f'Lỗi tải lên: {str(e)}'}), 500

@app.route('/upload/chunked', methods=['POST'])
def upload_chunked():
    """Handle chunked file upload"""
    try:
        # Log request details for debugging
        logger.info(f"Chunked upload request - Content-Type: {request.content_type}")
        
        # Validate required fields
        if 'chunk' not in request.files:
            logger.error("Missing 'chunk' in request.files")
            return jsonify({'success': False, 'error': 'Missing chunk data'}), 400
        
        # Get form data with validation
        try:
            chunk_number = int(request.form.get('chunkNumber', 0))
            total_chunks = int(request.form.get('totalChunks', 1))
            file_id = request.form.get('fileId', '').strip()
            original_filename = request.form.get('filename', '').strip()
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid form data: {e}")
            return jsonify({'success': False, 'error': 'Invalid chunk parameters'}), 400
        
        if not file_id:
            file_id = str(uuid.uuid4())
            logger.info(f"Generated new file_id: {file_id}")
        
        if not original_filename:
            logger.error("Missing filename")
            return jsonify({'success': False, 'error': 'Missing filename'}), 400
        
        logger.info(f"Processing chunk {chunk_number + 1}/{total_chunks} for file: {original_filename}")
        
        chunk_data = request.files['chunk']
        perf_settings = get_performance_settings()
        
        # Create temp directory
        temp_dir = os.path.join(TEMP_FOLDER, file_id)
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save chunk
        chunk_path = os.path.join(temp_dir, f'chunk_{chunk_number:06d}')
        buffer_size = perf_settings['buffer_size_kb'] * 1024
        
        with open(chunk_path, 'wb') as f:
            while True:
                data = chunk_data.stream.read(buffer_size)
                if not data:
                    break
                f.write(data)
        
        logger.info(f"Saved chunk {chunk_number} to {chunk_path}")
        
        # Check completion
        uploaded_chunks = len([f for f in os.listdir(temp_dir) if f.startswith('chunk_')])
        
        if uploaded_chunks == total_chunks:
            logger.info(f"All chunks received, merging file: {original_filename}")
            
            # Merge chunks
            admin_settings = get_admin_settings()
            final_filename = secure_filename(original_filename)
            file_ext = final_filename.rsplit('.', 1)[1].lower() if '.' in final_filename else ''
            stored_name = f"{file_id}.{file_ext}" if file_ext else file_id
            final_path = os.path.join(STORAGE_FOLDER, stored_name)
            
            # High-speed chunk merging
            with open(final_path, 'wb') as final_file:
                for i in range(total_chunks):
                    chunk_path = os.path.join(temp_dir, f'chunk_{i:06d}')
                    with open(chunk_path, 'rb') as chunk_file:
                        final_file.write(chunk_file.read())
            
            # Cleanup temp directory
            shutil.rmtree(temp_dir)
            
            # Get file info
            file_size = os.path.getsize(final_path)
            share_code = generate_share_code()
            file_type = get_file_type(original_filename)
            expires_at = datetime.now(timezone.utc) + timedelta(days=admin_settings['expire_days'])
            
            # Create database record
            conn = sqlite3.connect('file_storage.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO files (
                    id, original_name, stored_name, file_type, file_size, 
                    mime_type, share_code, password, download_limit, expires_at, 
                    uploader_ip, description, is_public
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id, original_filename, stored_name, file_type, file_size,
                'application/octet-stream', share_code, None, 
                admin_settings['download_limit'], expires_at, request.remote_addr, '', True
            ))
            
            conn.commit()
            conn.close()
            
            share_url = request.url_root + f"f/{share_code}"
            qr_code = generate_qr_code(share_url)
            
            logger.info(f"Chunked upload completed: {original_filename} ({format_file_size(file_size)})")
            
            return jsonify({
                'success': True,
                'completed': True,
                'file_id': file_id,
                'share_code': share_code,
                'share_url': share_url,
                'qr_code': qr_code,
                'file_size': format_file_size(file_size),
                'file_type': file_type
            })
        
        # Return progress
        progress = (uploaded_chunks / total_chunks) * 100
        logger.info(f"Chunk progress: {uploaded_chunks}/{total_chunks} ({progress:.1f}%)")
        
        return jsonify({
            'success': True,
            'completed': False,
            'uploaded_chunks': uploaded_chunks,
            'total_chunks': total_chunks,
            'progress': progress
        })
        
    except Exception as e:
        logger.error(f"Chunked upload error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/f/<share_code>')
def share_page(share_code):
    """Display shared file page"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM files WHERE share_code = ?', (share_code,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            abort(404)
        
        file_data = {
            'id': result[0],
            'original_name': result[1],
            'file_type': result[3],
            'file_size': format_file_size(result[4]),
            'share_code': result[6],
            'has_password': False,
            'download_limit': result[8],
            'download_count': result[9],
            'expires_at': result[11],
            'description': result[14]
        }
        
        # Check if expired
        if file_data['expires_at']:
            expires_at = datetime.fromisoformat(file_data['expires_at'].replace('Z', '+00:00'))
            if datetime.now(timezone.utc) > expires_at:
                conn.close()
                return render_template('index.html', error='File đã hết hạn')
        
        # Check download limit
        if file_data['download_count'] >= file_data['download_limit']:
            conn.close()
            return render_template('index.html', error='File đã đạt giới hạn tải xuống')
        
        # Update last accessed
        cursor.execute('UPDATE files SET last_accessed = ? WHERE share_code = ?', 
                      (datetime.now(timezone.utc), share_code))
        conn.commit()
        conn.close()
        
        # Get banners for display
        left_banners = get_banners('left')
        right_banners = get_banners('right')
        admin_settings = get_admin_settings()
        
        return render_template('index.html', 
                             shared_file=file_data, 
                             show_download=True,
                             admin_settings=admin_settings,
                             left_banners=left_banners,
                             right_banners=right_banners)
        
    except Exception as e:
        logger.error(f"Share page error: {e}")
        abort(500)

@app.route('/download/<share_code>')
def download_file(share_code):
    """Handle file download"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM files WHERE share_code = ?', (share_code,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            abort(404)
        
        file_data = {
            'id': result[0],
            'original_name': result[1],
            'stored_name': result[2],
            'download_limit': result[8],
            'download_count': result[9],
            'expires_at': result[11]
        }
        
        # Check if expired
        if file_data['expires_at']:
            expires_at = datetime.fromisoformat(file_data['expires_at'].replace('Z', '+00:00'))
            if datetime.now(timezone.utc) > expires_at:
                conn.close()
                return jsonify({'error': 'File đã hết hạn'}), 410
        
        # Check download limit
        if file_data['download_count'] >= file_data['download_limit']:
            conn.close()
            return jsonify({'error': 'File đã đạt giới hạn tải xuống'}), 403
        
        # Check if file exists
        file_path = os.path.join(STORAGE_FOLDER, file_data['stored_name'])
        if not os.path.exists(file_path):
            conn.close()
            return jsonify({'error': 'File không tồn tại'}), 404
        
        # Update download count and log
        cursor.execute('UPDATE files SET download_count = download_count + 1, last_accessed = ? WHERE share_code = ?', 
                      (datetime.now(timezone.utc), share_code))
        
        cursor.execute('''
            INSERT INTO download_stats (file_id, file_name, download_ip, user_agent)
            VALUES (?, ?, ?, ?)
        ''', (file_data['id'], file_data['original_name'], 
              request.remote_addr, request.headers.get('User-Agent', '')))
        
        conn.commit()
        conn.close()
        
        logger.info(f"File download: {file_data['original_name']} by {request.remote_addr}")
        
        # Stream file download
        return send_file(
            file_path,
            as_attachment=True,
            download_name=file_data['original_name'],
            mimetype='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"Download error: {e}")
        return jsonify({'error': str(e)}), 500

# ===== BANNER ROUTES =====
@app.route('/banner/click/<int:banner_id>')
def banner_click(banner_id):
    """Track banner click and redirect"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        # Update click count
        cursor.execute('UPDATE banners SET clicks = clicks + 1 WHERE id = ?', (banner_id,))
        
        # Get banner URL
        cursor.execute('SELECT link_url FROM banners WHERE id = ?', (banner_id,))
        result = cursor.fetchone()
        
        conn.commit()
        conn.close()
        
        if result and result[0]:
            logger.info(f"Banner {banner_id} clicked, redirecting to: {result[0]}")
            return redirect(result[0])
        else:
            return redirect(url_for('index'))
            
    except Exception as e:
        logger.error(f"Banner click error: {e}")
        return redirect(url_for('index'))

# ===== ADMIN ROUTES =====
@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    """Admin login page"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        db_username = get_setting('admin_username', 'admin')
        db_password_hash = get_setting('admin_password_hash')
        
        if username == db_username and check_password_hash(db_password_hash, password):
            session['admin_logged_in'] = True
            session['admin_username'] = username
            logger.info(f"Admin login successful: {username}")
            return redirect(url_for('admin_dashboard'))
        else:
            logger.warning(f"Admin login failed: {username}")
            return render_template('admin_login.html', error='Sai tên đăng nhập hoặc mật khẩu')
    
    return render_template('admin_login.html')

@app.route('/admin/logout')
def admin_logout():
    """Admin logout"""
    session.pop('admin_logged_in', None)
    session.pop('admin_username', None)
    logger.info("Admin logout")
    return redirect(url_for('admin_login'))

@app.route('/admin')
@admin_required
def admin_dashboard():
    """Admin dashboard"""
    return render_template('admin.html')

# ===== ADMIN API ROUTES =====
@app.route('/admin/api/stats')
@admin_required
def admin_stats():
    """Get admin statistics"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        # Active visitors
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        cursor.execute('SELECT COUNT(*) FROM visitors WHERE is_active = 1 AND last_activity > ?', (cutoff_time,))
        active_visitors = cursor.fetchone()[0]
        
        # Total files
        cursor.execute('SELECT COUNT(*) FROM files WHERE expires_at > ?', (datetime.now(timezone.utc),))
        total_files = cursor.fetchone()[0]
        
        # Total downloads
        cursor.execute('SELECT SUM(download_count) FROM files')
        total_downloads = cursor.fetchone()[0] or 0
        
        # Active banners
        cursor.execute('SELECT COUNT(*) FROM banners WHERE status = 1')
        active_banners = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            'success': True,
            'stats': {
                'visitors': {'active_now': active_visitors},
                'files': {'total_files': total_files, 'total_downloads': total_downloads},
                'banners': {'active_banners': active_banners}
            }
        })
        
    except Exception as e:
        logger.error(f"Stats API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/api/files')
@admin_required
def admin_files():
    """Get recent files for admin"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, original_name, file_type, file_size, download_count, 
                   uploaded_at, uploader_ip
            FROM files 
            ORDER BY uploaded_at DESC 
            LIMIT 50
        ''')
        
        files = []
        for row in cursor.fetchall():
            files.append({
                'id': row[0][:8] + '...',
                'name': row[1],
                'type': row[2],
                'size': format_file_size(row[3]),
                'downloads': row[4],
                'uploaded_at': row[5],
                'uploader_ip': row[6]
            })
        
        conn.close()
        
        return jsonify({'success': True, 'files': files})
        
    except Exception as e:
        logger.error(f"Files API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/api/performance', methods=['GET', 'POST'])
@admin_required
def admin_performance():
    """Get/update performance settings"""
    try:
        if request.method == 'GET':
            settings = []
            performance_keys = [
                'chunk_size_mb', 'max_concurrent_chunks', 'buffer_size_kb',
                'connection_timeout', 'enable_chunked_upload', 'enable_compression', 'enable_caching'
            ]
            
            for key in performance_keys:
                value = get_setting(key)
                if value:
                    settings.append({
                        'key': key,
                        'value': value,
                        'description': f'Performance setting: {key}'
                    })
            
            return jsonify({'success': True, 'settings': settings})
            
        elif request.method == 'POST':
            data = request.get_json()
            settings_to_update = data.get('settings', {})
            
            # Validate performance settings
            validation_errors = []
            
            for key, value in settings_to_update.items():
                if key == 'chunk_size_mb':
                    if not (1 <= int(value) <= 100):
                        validation_errors.append('Chunk size phải từ 1-100 MB')
                elif key == 'max_concurrent_chunks':
                    if not (1 <= int(value) <= 20):
                        validation_errors.append('Max concurrent chunks phải từ 1-20')
                elif key == 'buffer_size_kb':
                    if not (64 <= int(value) <= 8192):
                        validation_errors.append('Buffer size phải từ 64-8192 KB')
                elif key == 'connection_timeout':
                    if not (30 <= int(value) <= 3600):
                        validation_errors.append('Connection timeout phải từ 30-3600 giây')
            
            if validation_errors:
                return jsonify({'success': False, 'error': '; '.join(validation_errors)})
            
            # Update settings
            updated_count = 0
            for key, value in settings_to_update.items():
                if set_setting(key, value, f'Performance setting: {key}'):
                    updated_count += 1
            
            logger.info(f"Performance settings updated: {updated_count} settings")
            return jsonify({'success': True, 'message': f'Đã cập nhật {updated_count} cài đặt hiệu suất'})
            
    except Exception as e:
        logger.error(f"Performance API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/api/cache', methods=['GET', 'POST'])
@admin_required
def admin_cache():
    """Get cache info and cleanup"""
    try:
        if request.method == 'GET':
            total_files = 0
            total_size = 0
            
            if os.path.exists(STORAGE_FOLDER):
                for filename in os.listdir(STORAGE_FOLDER):
                    file_path = os.path.join(STORAGE_FOLDER, filename)
                    if os.path.isfile(file_path):
                        total_files += 1
                        total_size += os.path.getsize(file_path)
            
            cache_info = {
                'total_files': total_files,
                'total_size_mb': round(total_size / (1024 * 1024), 2)
            }
            
            return jsonify({'success': True, 'cache_info': cache_info})
            
        elif request.method == 'POST':
            data = request.get_json()
            action = data.get('action')
            
            if action == 'cleanup_old':
                deleted_count = cleanup_expired_files()
                message = f'Đã xóa {deleted_count} file hết hạn'
                
            elif action == 'clear_all':
                deleted_count = 0
                try:
                    conn = sqlite3.connect('file_storage.db')
                    cursor = conn.cursor()
                    
                    cursor.execute('SELECT stored_name FROM files')
                    all_files = cursor.fetchall()
                    
                    for (stored_name,) in all_files:
                        file_path = os.path.join(STORAGE_FOLDER, stored_name)
                        if os.path.exists(file_path):
                            os.remove(file_path)
                            deleted_count += 1
                    
                    cursor.execute('DELETE FROM files')
                    cursor.execute('DELETE FROM download_stats')
                    
                    conn.commit()
                    conn.close()
                    
                    message = f'Đã xóa tất cả {deleted_count} file'
                    
                except Exception as e:
                    logger.error(f"Clear all cache error: {e}")
                    return jsonify({'success': False, 'error': str(e)})
                
            else:
                return jsonify({'success': False, 'error': 'Invalid action'})
            
            logger.info(f"Cache cleanup: {message}")
            return jsonify({
                'success': True,
                'message': message,
                'deleted_count': deleted_count
            })
            
    except Exception as e:
        logger.error(f"Cache API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/api/banners', methods=['GET', 'POST', 'PUT', 'DELETE'])
@admin_required
def admin_banners():
    """Manage banners - Full CRUD operations"""
    try:
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        if request.method == 'GET':
            cursor.execute('SELECT * FROM banners ORDER BY id DESC')
            
            banners = []
            for row in cursor.fetchall():
                banners.append({
                    'id': row[0],
                    'title': row[1],
                    'description': row[2],
                    'image_path': row[3],
                    'link_url': row[4],
                    'position': row[5],
                    'clicks': row[6],
                    'status': bool(row[7]),
                    'created_at': row[8]
                })
            
            conn.close()
            return jsonify({'success': True, 'banners': banners})
            
        elif request.method == 'POST':
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No data received'})
            
            # Validate required fields
            if not data.get('title', '').strip():
                return jsonify({'success': False, 'error': 'Tiêu đề là bắt buộc'})
            
            cursor.execute('''
                INSERT INTO banners (title, description, image_path, link_url, position, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                data.get('title', '').strip(),
                data.get('description', '').strip(),
                data.get('image_path', '').strip(),
                data.get('link_url', '').strip(),
                data.get('position', 'left'),
                1 if data.get('status', True) else 0,
                datetime.now(timezone.utc),
                datetime.now(timezone.utc)
            ))
            
            banner_id = cursor.lastrowid
            conn.commit()
            conn.close()
            
            logger.info(f"Banner created: ID {banner_id}, Title: {data.get('title')}")
            return jsonify({
                'success': True, 
                'banner_id': banner_id, 
                'message': 'Banner đã được tạo thành công'
            })
            
        elif request.method == 'PUT':
            data = request.get_json()
            if not data or not data.get('id'):
                return jsonify({'success': False, 'error': 'Missing banner ID'})
            
            banner_id = data.get('id')
            
            # Check if banner exists
            cursor.execute('SELECT id FROM banners WHERE id = ?', (banner_id,))
            if not cursor.fetchone():
                conn.close()
                return jsonify({'success': False, 'error': 'Banner không tồn tại'})
            
            # Validate required fields
            if not data.get('title', '').strip():
                return jsonify({'success': False, 'error': 'Tiêu đề là bắt buộc'})
            
            cursor.execute('''
                UPDATE banners 
                SET title = ?, description = ?, image_path = ?, link_url = ?, 
                    position = ?, status = ?, updated_at = ?
                WHERE id = ?
            ''', (
                data.get('title', '').strip(),
                data.get('description', '').strip(),
                data.get('image_path', '').strip(),
                data.get('link_url', '').strip(),
                data.get('position', 'left'),
                1 if data.get('status', True) else 0,
                datetime.now(timezone.utc),
                banner_id
            ))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Banner updated: ID {banner_id}, Title: {data.get('title')}")
            return jsonify({
                'success': True, 
                'message': 'Banner đã được cập nhật thành công'
            })
            
        elif request.method == 'DELETE':
            data = request.get_json()
            if not data or not data.get('id'):
                return jsonify({'success': False, 'error': 'Missing banner ID'})
            
            banner_id = data.get('id')
            
            # Get banner info before deletion
            cursor.execute('SELECT image_path FROM banners WHERE id = ?', (banner_id,))
            result = cursor.fetchone()
            
            if not result:
                conn.close()
                return jsonify({'success': False, 'error': 'Banner không tồn tại'})
            
            # Delete image file if exists
            if result[0]:
                image_path = result[0]
                if image_path.startswith('/static/'):
                    full_path = os.path.join(app.root_path, image_path.lstrip('/'))
                else:
                    full_path = os.path.join(UPLOAD_FOLDER, os.path.basename(image_path))
                
                if os.path.exists(full_path):
                    try:
                        os.remove(full_path)
                        logger.info(f"Banner image deleted: {full_path}")
                    except Exception as e:
                        logger.error(f"Error deleting banner image: {e}")
            
            # Delete banner from database
            cursor.execute('DELETE FROM banners WHERE id = ?', (banner_id,))
            conn.commit()
            conn.close()
            
            logger.info(f"Banner deleted: ID {banner_id}")
            return jsonify({
                'success': True, 
                'message': 'Banner đã được xóa thành công'
            })
            
    except Exception as e:
        logger.error(f"Banner API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/upload', methods=['POST'])
@admin_required  
def admin_upload():
    """Handle banner image upload"""
    try:
        if 'image' not in request.files:
            return jsonify({'success': False, 'error': 'Không có file hình ảnh'})
        
        file = request.files['image']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'Chưa chọn file'})
        
        # Validate file
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'error': 'Định dạng file không được hỗ trợ'})
        
        # Check file size
        file.seek(0, os.SEEK_END)
        file_size = file.tell()
        file.seek(0)
        
        if file_size > BANNER_MAX_SIZE:
            return jsonify({'success': False, 'error': f'File quá lớn. Tối đa {BANNER_MAX_SIZE // (1024*1024)}MB'})
        
        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = secure_filename(file.filename)
        name, ext = os.path.splitext(filename)
        unique_filename = f"{timestamp}_{name}_{secrets.token_hex(4)}{ext}"
        
        file_path = os.path.join(UPLOAD_FOLDER, unique_filename)
        file.save(file_path)
        
        # Generate web path
        web_path = f"/static/uploads/banners/{unique_filename}"
        
        logger.info(f"Banner image uploaded: {unique_filename} ({format_file_size(file_size)})")
        
        return jsonify({
            'success': True,
            'image_path': web_path,
            'filename': unique_filename,
            'file_size': format_file_size(file_size),
            'message': 'Hình ảnh đã được upload thành công'
        })
        
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/admin/api/settings', methods=['GET', 'POST'])
@admin_required
def admin_settings():
    """Get/update admin settings"""
    try:
        if request.method == 'GET':
            settings_keys = [
                'admin_username', 'max_file_size_gb', 'default_expire_days',
                'max_download_limit', 'maintenance_mode', 'auto_cleanup_enabled'
            ]
            
            settings = []
            for key in settings_keys:
                value = get_setting(key)
                if value is not None:
                    settings.append({
                        'key': key,
                        'value': value,
                        'description': f'Setting: {key}'
                    })
            
            return jsonify({'success': True, 'settings': settings})
            
        elif request.method == 'POST':
            data = request.get_json()
            settings_to_update = data.get('settings', {})
            
            # Validate settings
            validation_errors = []
            
            for key, value in settings_to_update.items():
                if key == 'max_file_size_gb':
                    if not (1 <= int(value) <= 100):
                        validation_errors.append('Giới hạn file phải từ 1-100 GB')
                elif key == 'default_expire_days':
                    if not (1 <= int(value) <= 365):
                        validation_errors.append('Thời hạn lưu trữ phải từ 1-365 ngày')
                elif key == 'max_download_limit':
                    if not (1 <= int(value) <= 1000):
                        validation_errors.append('Giới hạn download phải từ 1-1000')
            
            if validation_errors:
                return jsonify({'success': False, 'error': '; '.join(validation_errors)})
            
            # Update settings
            updated_count = 0
            for key, value in settings_to_update.items():
                if key == 'admin_password':
                    # Hash password
                    password_hash = generate_password_hash(value)
                    if set_setting('admin_password_hash', password_hash, 'Admin password hash'):
                        updated_count += 1
                        logger.info("Admin password updated")
                else:
                    if set_setting(key, value, f'Setting: {key}'):
                        updated_count += 1
            
            logger.info(f"Admin settings updated: {updated_count} settings")
            return jsonify({'success': True, 'message': f'Đã cập nhật {updated_count} cài đặt'})
            
    except Exception as e:
        logger.error(f"Settings API error: {e}")
        return jsonify({'success': False, 'error': str(e)})

# ===== CLEANUP FUNCTIONS =====
def cleanup_expired_files():
    """Cleanup expired files"""
    try:
        current_time = datetime.now(timezone.utc)
        deleted_count = 0
        
        conn = sqlite3.connect('file_storage.db')
        cursor = conn.cursor()
        
        cursor.execute('SELECT id, stored_name FROM files WHERE expires_at < ?', (current_time.isoformat(),))
        expired_files = cursor.fetchall()
        
        for file_id, stored_name in expired_files:
            try:
                file_path = os.path.join(STORAGE_FOLDER, stored_name)
                if os.path.exists(file_path):
                    os.remove(file_path)
                
                cursor.execute('DELETE FROM files WHERE id = ?', (file_id,))
                cursor.execute('DELETE FROM download_stats WHERE file_id = ?', (file_id,))
                deleted_count += 1
                
            except Exception as e:
                logger.error(f"Error deleting file {file_id}: {e}")
        
        conn.commit()
        conn.close()
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        return 0

# ===== CLEANUP SCHEDULER =====
class CleanupScheduler:
    def __init__(self):
        self.scheduler_thread = threading.Thread(target=self.run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
    def run_scheduler(self):
        while True:
            try:
                time.sleep(3600)  # Check every hour
                
                enabled = get_setting('auto_cleanup_enabled', 'false') == 'true'
                if not enabled:
                    continue
                
                deleted_count = cleanup_expired_files()
                if deleted_count > 0:
                    logger.info(f"Scheduled cleanup completed: {deleted_count} files deleted")
                
            except Exception as e:
                logger.error(f"Scheduler error: {e}")

# Initialize cleanup scheduler
cleanup_scheduler = CleanupScheduler()

# ===== STATIC FILE ROUTES =====
@app.route('/favicon.ico')
def favicon():
    """Serve favicon"""
    return '', 204

@app.route('/robots.txt')
def robots():
    """Serve robots.txt"""
    return 'User-agent: *\nDisallow: /admin\nDisallow: /storage\n', 200, {'Content-Type': 'text/plain'}

# ===== ERROR HANDLERS =====
@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return render_template('index.html', error='Trang không tồn tại'), 404

@app.errorhandler(413)
def file_too_large(error):
    """Handle file too large errors"""
    max_size_gb = int(get_setting('max_file_size_gb', DEFAULT_MAX_FILE_SIZE_GB))
    return jsonify({
        'success': False, 
        'error': f'File quá lớn! Tối đa {max_size_gb}GB'
    }), 413

@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    logger.error(f"Internal server error: {error}")
    return render_template('index.html', error='Lỗi hệ thống'), 500

# ===== MAIN APPLICATION =====
if __name__ == '__main__':
    # Ensure directories exist
    for directory in [UPLOAD_FOLDER, STORAGE_FOLDER, TEMP_FOLDER]:
        os.makedirs(directory, exist_ok=True)
    
    logger.info("Starting File Storage & Sharing Service...")
    logger.info(f"Storage folder: {STORAGE_FOLDER}")
    logger.info(f"Banner upload folder: {UPLOAD_FOLDER}")
    logger.info(f"Temp folder: {TEMP_FOLDER}")
    logger.info(f"Max file size: {get_setting('max_file_size_gb', DEFAULT_MAX_FILE_SIZE_GB)}GB")
    
    # Start the application
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
        threaded=True
    )
