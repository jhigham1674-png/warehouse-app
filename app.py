from flask import Flask, request, redirect, url_for, render_template_string, session
from datetime import datetime
import json
import html
import csv
import io
import os
from functools import wraps

import psycopg2
import psycopg2.extras
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-secret-key")
DATABASE_URL = os.environ.get("DATABASE_URL")


BASE_HTML = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{{ title or "Warehouse Logging 2" }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, sans-serif;
            background: #f3f4f6;
            color: #111827;
        }
        .topbar {
            background: #163b7a;
            color: white;
            padding: 14px 20px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.10);
        }
        .topbar-inner {
            max-width: 1180px;
            margin: 0 auto;
        }
        .topbar h1 {
            margin: 0;
            font-size: 22px;
        }
        .nav {
            margin-top: 8px;
            display: flex;
            gap: 14px;
            flex-wrap: wrap;
            align-items: center;
        }
        .nav a {
            color: white;
            text-decoration: none;
            font-weight: bold;
            font-size: 14px;
        }
        .nav .user-badge {
            margin-left: auto;
            font-size: 13px;
            opacity: 0.95;
        }
        .container {
            max-width: 1180px;
            margin: 24px auto;
            padding: 0 16px;
        }
        .card {
            background: white;
            border-radius: 14px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        }
        h2, h3 { margin-top: 0; }
        .btn {
            display: inline-block;
            background: #1f3c88;
            color: white;
            padding: 10px 16px;
            border-radius: 9px;
            text-decoration: none;
            border: none;
            cursor: pointer;
            font-size: 14px;
            margin-right: 8px;
        }
        .btn.secondary { background: #6b7280; }
        .btn.print { background: #0f766e; }
        .btn.danger { background: #b91c1c; }
        .btn.gold { background: #a16207; }

        .tiny-btn {
            border: none;
            cursor: pointer;
            border-radius: 10px;
            padding: 12px 16px;
            font-size: 22px;
            font-weight: bold;
            color: white;
            margin-right: 6px;
        }
        .tiny-btn.ok { background: #15803d; }
        .tiny-btn.delete { background: #b91c1c; }

        .grid {
            display: grid;
            gap: 16px;
        }
        .grid-2 {
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        }

        label {
            display: block;
            font-weight: bold;
            margin-bottom: 6px;
        }
        input, select {
            width: 100%;
            padding: 10px;
            border: 1px solid #d1d5db;
            border-radius: 8px;
            font-size: 14px;
        }
        .row {
            display: grid;
            grid-template-columns: 1.2fr 2fr 0.8fr;
            gap: 12px;
            margin-bottom: 12px;
            align-items: end;
        }
        .readonly { background: #f9fafb; }

        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            text-align: left;
            padding: 12px;
            border-bottom: 1px solid #e5e7eb;
            vertical-align: top;
        }
        th { background: #f9fafb; }

        .muted { color: #6b7280; }
        .small-text {
            font-size: 13px;
            color: #6b7280;
        }

        .pill {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 999px;
            background: #e0e7ff;
            color: #1e3a8a;
            font-size: 12px;
            font-weight: bold;
        }
        .pill.level-1 {
            background: #e5e7eb;
            color: #374151;
        }
        .pill.level-2 {
            background: #dbeafe;
            color: #1d4ed8;
        }
        .pill.level-3 {
            background: #ede9fe;
            color: #6d28d9;
        }

        .search-box {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
        }
        .search-box input {
            flex: 1;
            min-width: 220px;
        }

        .notice {
            background: #eff6ff;
            color: #1e3a8a;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
        }
        .warning {
            background: #fef3c7;
            color: #92400e;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
        }
        .success {
            background: #ecfdf5;
            color: #065f46;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 16px;
        }

        .sku-row {
            border: 1px solid #e5e7eb;
            padding: 12px;
            border-radius: 10px;
            margin-bottom: 12px;
            background: #fafafa;
        }

        .actions-inline {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
        }

        .login-wrap {
            max-width: 460px;
            margin: 80px auto;
        }

        .label-toolbar {
            margin-bottom: 16px;
        }

        .label-sheet {
            page-break-after: always;
            page-break-inside: avoid;
            background: white;
            border: 2px solid #111827;
            border-radius: 12px;
            padding: 18px;
            margin: 0 auto 20px auto;
            max-width: 794px;
        }

        .label-brand {
            font-size: 18px;
            color: #374151;
            font-weight: bold;
            letter-spacing: 0.8px;
            text-align: center;
            margin-bottom: 10px;
        }

        .label-number-wrap {
            height: 420px;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 4px solid #111827;
            border-radius: 14px;
            margin-bottom: 12px;
            padding: 10px;
        }

        .label-number {
            font-size: 380px;
            line-height: 0.9;
            font-weight: 900;
            letter-spacing: 2px;
            text-align: center;
        }

        .label-name {
            font-size: 28px;
            font-weight: 800;
            margin: 0 0 12px 0;
            text-align: center;
        }

        .label-meta {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 12px;
        }

        .meta-box {
            background: #f9fafb;
            border: 1px solid #d1d5db;
            border-radius: 10px;
            padding: 10px;
        }

        .meta-box strong {
            display: block;
            font-size: 11px;
            color: #6b7280;
            margin-bottom: 3px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
        }

        .meta-box span {
            font-size: 16px;
            font-weight: 700;
        }

        .label-items table {
            margin-top: 6px;
            font-size: 16px;
        }

        .label-items th,
        .label-items td {
            padding: 8px;
        }

        .audit-mini {
            background: #f9fafb;
            border-radius: 10px;
            padding: 14px;
            margin-bottom: 10px;
        }

        .audit-row {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            align-items: center;
            padding: 14px 0;
            border-bottom: 1px solid #e5e7eb;
        }
        .audit-row:last-child { border-bottom: none; }
        .audit-main { flex: 1; }

        pre {
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        @media print {
            .topbar, .nav, .container > :not(.print-zone), .no-print {
                display: none !important;
            }
            body { background: white; }
            .print-zone { display: block !important; }
            .label-sheet {
                border: 1px solid #000;
                box-shadow: none;
                margin: 0 0 12px 0;
                max-width: 100%;
                border-radius: 0;
                padding: 12mm;
            }
            .label-number-wrap {
                height: 430px;
                border: 4px solid #000;
            }
            .label-number {
                font-size: 400px;
            }
            .label-name {
                font-size: 28px;
            }
            .label-items table {
                font-size: 16px;
            }
        }
    </style>
</head>
<body>
    {% if session.get("user_id") %}
    <div class="topbar">
        <div class="topbar-inner">
            <h1>Warehouse Logging 2</h1>
            <div class="nav">
                <a href="{{ url_for('home') }}">Home</a>
                <a href="{{ url_for('create_pallet') }}">Create Pallet</a>
                <a href="{{ url_for('view_all_pallets') }}">View Pallets</a>
                <a href="{{ url_for('search_sku') }}">Search SKU</a>

                {% if session.get("access_level", 1) >= 2 %}
                <a href="{{ url_for('pallet_audit') }}">Pallet Audit</a>
                <a href="{{ url_for('print_all_labels') }}">Print Labels</a>
                {% endif %}

                {% if session.get("access_level", 1) >= 3 %}
                <a href="{{ url_for('products_list') }}">Products</a>
                <a href="{{ url_for('import_products') }}">Import CSV</a>
                <a href="{{ url_for('user_management') }}">User Management</a>
                {% endif %}

                <a href="{{ url_for('logout') }}">Logout</a>
                <span class="user-badge">{{ session.get("username") }} | Level {{ session.get("access_level", 1) }}</span>
            </div>
        </div>
    </div>
    {% endif %}
    <div class="container">
        {{ content|safe }}
    </div>
</body>
</html>
"""


def esc(value):
    if value is None:
        return ""
    return html.escape(str(value))


def now_str():
    return datetime.now().strftime("%d/%m/%Y %H:%M")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def execute(query, params=None, fetchone=False, fetchall=False, commit=False):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params or ())
        result = None
        if fetchone:
            result = cur.fetchone()
        elif fetchall:
            result = cur.fetchall()
        if commit:
            conn.commit()
        cur.close()
        return result
    finally:
        conn.close()


def init_db():
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                access_level INTEGER NOT NULL DEFAULT 1
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                sku TEXT PRIMARY KEY,
                description TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pallets (
                pallet_number INTEGER PRIMARY KEY,
                pallet_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pallet_items (
                id SERIAL PRIMARY KEY,
                pallet_number INTEGER NOT NULL,
                sku TEXT NOT NULL,
                description TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                FOREIGN KEY (pallet_number) REFERENCES pallets (pallet_number) ON DELETE CASCADE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS pallet_audit (
                id SERIAL PRIMARY KEY,
                pallet_number INTEGER,
                action_type TEXT NOT NULL,
                username TEXT NOT NULL,
                details TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_runs (
                id SERIAL PRIMARY KEY,
                audit_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                created_by TEXT NOT NULL,
                is_closed BOOLEAN NOT NULL DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS audit_run_items (
                id SERIAL PRIMARY KEY,
                audit_run_id INTEGER NOT NULL,
                pallet_number INTEGER NOT NULL,
                confirmed_at TEXT NOT NULL,
                confirmed_by TEXT NOT NULL,
                UNIQUE(audit_run_id, pallet_number),
                FOREIGN KEY (audit_run_id) REFERENCES audit_runs(id) ON DELETE CASCADE
            )
        """)

        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS access_level INTEGER NOT NULL DEFAULT 1")
        cur.execute("ALTER TABLE pallets ADD COLUMN IF NOT EXISTS created_by TEXT")

        cur.execute("UPDATE users SET access_level = 1 WHERE access_level IS NULL")
        cur.execute("UPDATE users SET access_level = 3 WHERE username = 'admin'")
        cur.execute("UPDATE pallets SET created_by = 'Unknown' WHERE created_by IS NULL")

        seed_users = [
            ("admin", generate_password_hash("admin123"), "Owner", 3),
            ("warehouse", generate_password_hash("warehouse123"), "Warehouse User", 1),
        ]

        for username, password_hash, full_name, access_level in seed_users:
            cur.execute("SELECT id FROM users WHERE username = %s", (username,))
            existing = cur.fetchone()
            if not existing:
                cur.execute("""
                    INSERT INTO users (username, password_hash, full_name, access_level)
                    VALUES (%s, %s, %s, %s)
                """, (username, password_hash, full_name, access_level))

        conn.commit()
        cur.close()
    finally:
        conn.close()


def log_audit(action_type, details, pallet_number=None):
    if not session.get("username"):
        return
    execute("""
        INSERT INTO pallet_audit (pallet_number, action_type, username, details, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """, (pallet_number, action_type, session["username"], details, now_str()), commit=True)


def render_page(content, title="Warehouse Logging 2"):
    return render_template_string(BASE_HTML, content=content, title=title)


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def level_required(required_level):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not session.get("user_id"):
                return redirect(url_for("login"))
            if session.get("access_level", 1) < required_level:
                return render_page("""
                <div class="card">
                    <h2>Access denied</h2>
                    <p class="muted">You do not have permission to view this page.</p>
                    <a class="btn secondary" href="/">Back</a>
                </div>
                """, "Access Denied")
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def get_product_map():
    rows = execute("SELECT sku, description FROM products ORDER BY sku", fetchall=True)
    return {row["sku"]: {"description": row["description"]} for row in rows}


def get_used_pallet_numbers():
    rows = execute("SELECT pallet_number FROM pallets ORDER BY pallet_number", fetchall=True)
    return {row["pallet_number"] for row in rows}


def get_available_pallet_numbers(include_number=None):
    used = get_used_pallet_numbers()
    available = []
    for n in range(1, 201):
        if n not in used or n == include_number:
            available.append(n)
    return available


def pallet_exists(pallet_number, exclude_number=None):
    if exclude_number is None:
        row = execute("SELECT 1 FROM pallets WHERE pallet_number = %s", (pallet_number,), fetchone=True)
    else:
        row = execute(
            "SELECT 1 FROM pallets WHERE pallet_number = %s AND pallet_number != %s",
            (pallet_number, exclude_number),
            fetchone=True
        )
    return row is not None


def parse_items_from_form(product_map):
    skus = request.form.getlist("sku")
    quantities = request.form.getlist("quantity")
    stacked = {}

    for sku, quantity in zip(skus, quantities):
        sku = sku.strip()
        quantity = quantity.strip()

        if sku and quantity and sku in product_map:
            try:
                qty_num = int(quantity)
            except ValueError:
                continue

            if qty_num > 0:
                if sku not in stacked:
                    stacked[sku] = {
                        "sku": sku,
                        "description": product_map[sku]["description"],
                        "quantity": 0
                    }
                stacked[sku]["quantity"] += qty_num

    return list(stacked.values())


def build_item_rows(existing_items=None, row_count=1):
    if existing_items is None:
        existing_items = []

    rows_html = ""
    total_rows = max(row_count, len(existing_items) + 1)

    for i in range(total_rows):
        sku_value = ""
        desc_value = ""
        qty_value = ""

        if i < len(existing_items):
            sku_value = esc(existing_items[i]["sku"])
            desc_value = esc(existing_items[i]["description"])
            qty_value = esc(existing_items[i]["quantity"])

        rows_html += f"""
        <div class="sku-row">
            <div class="row">
                <div>
                    <label>SKU</label>
                    <input name="sku" class="sku-input" data-index="{i}" value="{sku_value}" placeholder="Enter SKU">
                </div>
                <div>
                    <label>Description</label>
                    <input class="desc-input readonly" data-index="{i}" value="{desc_value}" readonly>
                </div>
                <div>
                    <label>Qty</label>
                    <input type="number" name="quantity" class="qty-input" data-index="{i}" min="1" value="{qty_value}" placeholder="0">
                </div>
            </div>
        </div>
        """
    return rows_html


def fetch_pallet(num):
    pallet = execute("""
        SELECT pallet_number, pallet_name, created_at, created_by
        FROM pallets
        WHERE pallet_number = %s
    """, (num,), fetchone=True)

    items = execute("""
        SELECT sku, description, quantity
        FROM pallet_items
        WHERE pallet_number = %s
        ORDER BY sku
    """, (num,), fetchall=True)

    return pallet, items


def get_current_audit_run():
    return execute("""
        SELECT id, audit_name, created_at, created_by, is_closed
        FROM audit_runs
        WHERE is_closed = FALSE
        ORDER BY id DESC
        LIMIT 1
    """, fetchone=True)


def start_new_audit_run():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE audit_runs SET is_closed = TRUE WHERE is_closed = FALSE")
        audit_name = f"Weekly Audit - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        cur.execute("""
            INSERT INTO audit_runs (audit_name, created_at, created_by, is_closed)
            VALUES (%s, %s, %s, FALSE)
        """, (audit_name, now_str(), session["username"]))
        conn.commit()
        cur.close()
    finally:
        conn.close()


def render_label_html(pallet, items):
    item_rows = ""
    for item in items:
        item_rows += f"""
        <tr>
            <td>{esc(item['sku'])}</td>
            <td>{esc(item['description'])}</td>
            <td>{esc(item['quantity'])}</td>
        </tr>
        """

    return f"""
    <div class="label-sheet">
        <div class="label-brand">WAREHOUSE LOGGING 2</div>

        <div class="label-number-wrap">
            <div class="label-number">{pallet['pallet_number']}</div>
        </div>

        <p class="label-name">{esc(pallet['pallet_name'])}</p>

        <div class="label-meta">
            <div class="meta-box">
                <strong>Created</strong>
                <span>{esc(pallet['created_at'])}</span>
            </div>
            <div class="meta-box">
                <strong>Created By</strong>
                <span>{esc(pallet['created_by'])}</span>
            </div>
        </div>

        <div class="label-items">
            <table>
                <tr>
                    <th>SKU</th>
                    <th>Description</th>
                    <th>Qty</th>
                </tr>
                {item_rows}
            </table>
        </div>
    </div>
    """


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id"):
        return redirect(url_for("home"))

    error_message = ""
    success_message = ""

    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        if not full_name:
            error_message = "Please enter your full name."
        elif not username:
            error_message = "Please choose a username."
        elif len(password) < 6:
            error_message = "Password must be at least 6 characters."
        elif password != confirm_password:
            error_message = "Passwords do not match."
        else:
            existing = execute("SELECT id FROM users WHERE username = %s", (username,), fetchone=True)

            if existing:
                error_message = "That username is already taken."
            else:
                execute("""
                    INSERT INTO users (username, password_hash, full_name, access_level)
                    VALUES (%s, %s, %s, %s)
                """, (username, generate_password_hash(password), full_name, 1), commit=True)
                success_message = "Registration successful. You can now log in."

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    success_html = f'<div class="success">{esc(success_message)}</div>' if success_message else ""

    content = f"""
    <div class="login-wrap">
        <div class="card">
            <h2>Register</h2>
            {error_html}
            {success_html}
            <form method="post">
                <div style="margin-bottom:16px;">
                    <label>Full Name</label>
                    <input type="text" name="full_name" required>
                </div>
                <div style="margin-bottom:16px;">
                    <label>Username</label>
                    <input type="text" name="username" required>
                </div>
                <div style="margin-bottom:16px;">
                    <label>Password</label>
                    <input type="password" name="password" required>
                </div>
                <div style="margin-bottom:16px;">
                    <label>Confirm Password</label>
                    <input type="password" name="confirm_password" required>
                </div>
                <button class="btn" type="submit">Create Account</button>
                <a class="btn secondary" href="{url_for('login')}">Back to Login</a>
            </form>
        </div>
    </div>
    """
    return render_page(content, "Register")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("home"))

    error_message = ""

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = execute("""
            SELECT id, username, password_hash, full_name, access_level
            FROM users
            WHERE username = %s
        """, (username,), fetchone=True)

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["full_name"] = user["full_name"]
            session["access_level"] = user["access_level"]
            log_audit("LOGIN", "Signed in")
            return redirect(url_for("home"))
        else:
            error_message = "Invalid username or password."

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    content = f"""
    <div class="login-wrap">
        <div class="card">
            <h2>Login</h2>
            {error_html}
            <form method="post">
                <div style="margin-bottom:16px;">
                    <label>Username</label>
                    <input type="text" name="username" required>
                </div>
                <div style="margin-bottom:16px;">
                    <label>Password</label>
                    <input type="password" name="password" required>
                </div>
                <button class="btn" type="submit">Login</button>
                <a class="btn secondary" href="{url_for('register')}">Register</a>
            </form>

            <div class="notice" style="margin-top:16px;">
                Demo login:<br>
                <strong>admin</strong> / <strong>admin123</strong> (Level 3)<br>
                <strong>warehouse</strong> / <strong>warehouse123</strong> (Level 1)
            </div>
        </div>
    </div>
    """
    return render_page(content, "Login")


@app.route("/logout")
@login_required
def logout():
    username = session.get("username", "")
    log_audit("LOGOUT", f"{username} signed out")
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def home():
    content = f"""
    <div class="grid grid-2">
        <div class="card">
            <h2>Quick SKU Search</h2>
            <form method="get" action="{url_for('search_sku')}">
                <div class="search-box">
                    <input type="text" name="sku" placeholder="Enter SKU">
                    <button class="btn" type="submit">Search</button>
                </div>
            </form>
        </div>

        <div class="card">
            <h2>Quick Actions</h2>
            <div class="actions-inline">
                <a class="btn" href="{url_for('create_pallet')}">Create Pallet</a>
                <a class="btn secondary" href="{url_for('view_all_pallets')}">View Pallets</a>
                <a class="btn secondary" href="{url_for('search_sku')}">Search SKU</a>
                {"<a class='btn gold' href='" + url_for('pallet_audit') + "'>Pallet Audit</a>" if session.get("access_level", 1) >= 2 else ""}
            </div>
        </div>
    </div>
    """
    return render_page(content, "Home")


@app.route("/pallets")
@login_required
def view_all_pallets():
    pallets = execute("""
        SELECT pallet_number, pallet_name, created_at, created_by
        FROM pallets
        ORDER BY pallet_number ASC
    """, fetchall=True)

    rows = ""
    for pallet in pallets:
        rows += f"""
        <tr>
            <td><a href="{url_for('view_pallet', num=pallet['pallet_number'])}">Pallet {pallet['pallet_number']}</a></td>
            <td>{esc(pallet['pallet_name'])}</td>
            <td>{esc(pallet['created_at'])}</td>
            <td>{esc(pallet['created_by'])}</td>
        </tr>
        """

    content = f"""
    <div class="card">
        <h2>All Pallets</h2>
        <table>
            <tr>
                <th>Pallet</th>
                <th>Name</th>
                <th>Created</th>
                <th>Created By</th>
            </tr>
            {rows if rows else '<tr><td colspan="4" class="muted">No pallets created yet.</td></tr>'}
        </table>
    </div>
    """
    return render_page(content, "View Pallets")


@app.route("/users", methods=["GET", "POST"])
@login_required
@level_required(3)
def user_management():
    error_message = ""
    success_message = ""

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "add_user":
            full_name = request.form.get("full_name", "").strip()
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "").strip()
            access_level_raw = request.form.get("access_level", "1").strip()

            try:
                access_level = int(access_level_raw)
            except ValueError:
                access_level = 1

            existing = execute("SELECT id FROM users WHERE username = %s", (username,), fetchone=True)

            if not full_name or not username or not password:
                error_message = "Please complete all user fields."
            elif len(password) < 6:
                error_message = "Password must be at least 6 characters."
            elif access_level not in [1, 2, 3]:
                error_message = "Invalid access level."
            elif existing:
                error_message = "That username already exists."
            else:
                execute("""
                    INSERT INTO users (username, password_hash, full_name, access_level)
                    VALUES (%s, %s, %s, %s)
                """, (username, generate_password_hash(password), full_name, access_level), commit=True)
                success_message = f"User {username} created."
                log_audit("USER", f"Created user {username}")

        elif action == "reset_password":
            user_id = request.form.get("user_id", "").strip()
            new_password = request.form.get("new_password", "").strip()

            if not new_password or len(new_password) < 6:
                error_message = "New password must be at least 6 characters."
            else:
                user_row = execute("SELECT username FROM users WHERE id = %s", (user_id,), fetchone=True)
                if user_row:
                    execute("""
                        UPDATE users
                        SET password_hash = %s
                        WHERE id = %s
                    """, (generate_password_hash(new_password), user_id), commit=True)
                    success_message = f"Password reset for {user_row['username']}."
                    log_audit("USER", f"Reset password for {user_row['username']}")

        elif action == "change_level":
            user_id = request.form.get("user_id", "").strip()
            access_level_raw = request.form.get("access_level", "1").strip()

            try:
                access_level = int(access_level_raw)
            except ValueError:
                access_level = 1

            if access_level not in [1, 2, 3]:
                error_message = "Invalid access level."
            else:
                user_row = execute("SELECT username FROM users WHERE id = %s", (user_id,), fetchone=True)
                if user_row:
                    execute("""
                        UPDATE users
                        SET access_level = %s
                        WHERE id = %s
                    """, (access_level, user_id), commit=True)
                    success_message = f"Access level updated for {user_row['username']}."
                    log_audit("USER", f"Changed level for {user_row['username']} to {access_level}")

        elif action == "delete_user":
            user_id = request.form.get("user_id", "").strip()
            user_row = execute("SELECT username FROM users WHERE id = %s", (user_id,), fetchone=True)
            if user_row:
                if str(session.get("user_id")) == str(user_id):
                    error_message = "You cannot delete your own user while logged in."
                else:
                    execute("DELETE FROM users WHERE id = %s", (user_id,), commit=True)
                    success_message = f"Deleted user {user_row['username']}."
                    log_audit("USER", f"Deleted user {user_row['username']}")

    users = execute("""
        SELECT id, username, full_name, access_level
        FROM users
        ORDER BY username ASC
    """, fetchall=True)

    users_rows = ""
    for user in users:
        level_class = f"level-{user['access_level']}"
        users_rows += f"""
        <tr>
            <td>{esc(user['full_name'])}</td>
            <td>{esc(user['username'])}</td>
            <td><span class="pill {level_class}">Level {user['access_level']}</span></td>
            <td>
                <form method="post" style="display:inline-block; margin-right:8px;">
                    <input type="hidden" name="action" value="change_level">
                    <input type="hidden" name="user_id" value="{user['id']}">
                    <select name="access_level" style="width:110px; display:inline-block;">
                        <option value="1" {"selected" if user["access_level"] == 1 else ""}>Level 1</option>
                        <option value="2" {"selected" if user["access_level"] == 2 else ""}>Level 2</option>
                        <option value="3" {"selected" if user["access_level"] == 3 else ""}>Level 3</option>
                    </select>
                    <button class="btn secondary" type="submit">Save Level</button>
                </form>

                <form method="post" style="display:inline-block; margin-right:8px;">
                    <input type="hidden" name="action" value="reset_password">
                    <input type="hidden" name="user_id" value="{user['id']}">
                    <input type="password" name="new_password" placeholder="New password" style="width:160px; display:inline-block;">
                    <button class="btn secondary" type="submit">Reset Password</button>
                </form>

                <form method="post" style="display:inline-block;" onsubmit="return confirm('Delete user {esc(user['username'])}?');">
                    <input type="hidden" name="action" value="delete_user">
                    <input type="hidden" name="user_id" value="{user['id']}">
                    <button class="btn danger" type="submit">Delete</button>
                </form>
            </td>
        </tr>
        """

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    success_html = f'<div class="success">{esc(success_message)}</div>' if success_message else ""

    content = f"""
    <div class="card">
        <h2>User Management</h2>
        {error_html}
        {success_html}

        <div class="grid grid-2">
            <div>
                <h3>Add User</h3>
                <form method="post">
                    <input type="hidden" name="action" value="add_user">
                    <div style="margin-bottom:12px;">
                        <label>Full Name</label>
                        <input type="text" name="full_name" required>
                    </div>
                    <div style="margin-bottom:12px;">
                        <label>Username</label>
                        <input type="text" name="username" required>
                    </div>
                    <div style="margin-bottom:12px;">
                        <label>Password</label>
                        <input type="password" name="password" required>
                    </div>
                    <div style="margin-bottom:12px;">
                        <label>Access Level</label>
                        <select name="access_level">
                            <option value="1">Level 1</option>
                            <option value="2">Level 2</option>
                            <option value="3">Level 3</option>
                        </select>
                    </div>
                    <button class="btn" type="submit">Add User</button>
                </form>
            </div>

            <div>
                <h3>Access Levels</h3>
                <div class="notice">
                    <strong>Level 1:</strong> Create pallet, search SKU, view pallets<br>
                    <strong>Level 2:</strong> Level 1 + pallet audit + print all labels<br>
                    <strong>Level 3:</strong> everything
                </div>
            </div>
        </div>
    </div>

    <div class="card">
        <h3>Existing Users</h3>
        <table>
            <tr>
                <th>Full Name</th>
                <th>Username</th>
                <th>Level</th>
                <th>Actions</th>
            </tr>
            {users_rows}
        </table>
    </div>
    """
    return render_page(content, "User Management")


@app.route("/products", methods=["GET", "POST"])
@login_required
@level_required(3)
def products_list():
    error_message = ""
    success_message = ""

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "add_product":
            sku = request.form.get("sku", "").strip()
            description = request.form.get("description", "").strip()

            if not sku or not description:
                error_message = "Please enter both SKU and description."
            else:
                execute("""
                    INSERT INTO products (sku, description)
                    VALUES (%s, %s)
                    ON CONFLICT (sku) DO UPDATE SET
                        description = EXCLUDED.description
                """, (sku, description), commit=True)
                success_message = f"Product {sku} saved."
                log_audit("PRODUCT", f"Added or updated product {sku}")

        elif action == "delete_all_products":
            execute("DELETE FROM products", commit=True)
            success_message = "All products deleted."
            log_audit("PRODUCT", "Deleted all products")

    products = execute("""
        SELECT sku, description
        FROM products
        ORDER BY sku
    """, fetchall=True)

    rows = ""
    for product in products:
        rows += f"""
        <tr>
            <td>{esc(product['sku'])}</td>
            <td>{esc(product['description'])}</td>
        </tr>
        """

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    success_html = f'<div class="success">{esc(success_message)}</div>' if success_message else ""

    content = f"""
    <div class="card">
        <h2>Products</h2>
        {error_html}
        {success_html}

        <div class="grid grid-2">
            <div>
                <h3>Manual Product Add</h3>
                <form method="post">
                    <input type="hidden" name="action" value="add_product">
                    <div style="margin-bottom:12px;">
                        <label>SKU</label>
                        <input type="text" name="sku" required>
                    </div>
                    <div style="margin-bottom:12px;">
                        <label>Description</label>
                        <input type="text" name="description" required>
                    </div>
                    <button class="btn" type="submit">Save Product</button>
                </form>
            </div>

            <div>
                <h3>Danger Zone</h3>
                <div class="warning">This will remove all products from the products list.</div>
                <form method="post" onsubmit="return confirm('Delete all products?');">
                    <input type="hidden" name="action" value="delete_all_products">
                    <button class="btn danger" type="submit">Delete All Products</button>
                </form>
            </div>
        </div>
    </div>

    <div class="card">
        <h3>Current Products</h3>
        <table>
            <tr>
                <th>SKU</th>
                <th>Description</th>
            </tr>
            {rows if rows else '<tr><td colspan="2" class="muted">No products saved yet.</td></tr>'}
        </table>
    </div>
    """
    return render_page(content, "Products")


@app.route("/products/import", methods=["GET", "POST"])
@login_required
@level_required(3)
def import_products():
    error_message = ""
    success_message = ""

    if request.method == "POST":
        uploaded = request.files.get("csv_file")

        if not uploaded or uploaded.filename == "":
            error_message = "Please choose a CSV file."
        else:
            try:
                content = uploaded.read().decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(content))

                required = {"sku", "description"}
                if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                    error_message = "CSV must contain headers: sku, description"
                else:
                    conn = get_db_connection()
                    try:
                        cur = conn.cursor()
                        imported_count = 0

                        for row in reader:
                            sku = (row.get("sku") or "").strip()
                            description = (row.get("description") or "").strip()

                            if sku and description:
                                cur.execute("""
                                    INSERT INTO products (sku, description)
                                    VALUES (%s, %s)
                                    ON CONFLICT (sku) DO UPDATE SET
                                        description = EXCLUDED.description
                                """, (sku, description))
                                imported_count += 1

                        conn.commit()
                        cur.close()

                        success_message = f"Imported or updated {imported_count} products."
                        log_audit("PRODUCT IMPORT", f"Imported {imported_count} products from CSV")
                    finally:
                        conn.close()
            except Exception as e:
                error_message = f"Import failed: {esc(str(e))}"

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    success_html = f'<div class="success">{esc(success_message)}</div>' if success_message else ""

    content = f"""
    <div class="card">
        <h2>Import Products CSV</h2>
        {error_html}
        {success_html}

        <p class="muted">CSV must contain these headers: <strong>sku, description</strong></p>

        <form method="post" enctype="multipart/form-data">
            <div style="margin-bottom:16px;">
                <label>Choose CSV file</label>
                <input type="file" name="csv_file" accept=".csv" required>
            </div>
            <button class="btn" type="submit">Import CSV</button>
        </form>
    </div>

    <div class="card">
        <h3>Example CSV Format</h3>
        <pre>sku,description
100001,18mm Plywood Sheet
100002,Trade Matt White 10L
100003,Cement 25kg</pre>
    </div>
    """
    return render_page(content, "Import Products CSV")


@app.route("/create", methods=["GET", "POST"])
@login_required
@level_required(1)
def create_pallet():
    product_map = get_product_map()
    available_numbers = get_available_pallet_numbers()
    error_message = ""

    if request.method == "POST":
        pallet_name = request.form.get("pallet_name", "").strip()
        pallet_number_raw = request.form.get("pallet_number", "").strip()

        try:
            pallet_number = int(pallet_number_raw)
        except ValueError:
            pallet_number = None

        items = parse_items_from_form(product_map)

        if pallet_number is None or pallet_number < 1 or pallet_number > 200:
            error_message = "Pallet number must be between 1 and 200."
        elif pallet_exists(pallet_number):
            error_message = "That pallet number is already in use."
        elif not pallet_name:
            error_message = "Please enter a pallet name."
        elif not items:
            error_message = "Please add at least one valid SKU and quantity."
        else:
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                created_at = now_str()

                cur.execute("""
                    INSERT INTO pallets (pallet_number, pallet_name, created_at, created_by)
                    VALUES (%s, %s, %s, %s)
                """, (pallet_number, pallet_name, created_at, session["username"]))

                for item in items:
                    cur.execute("""
                        INSERT INTO pallet_items (pallet_number, sku, description, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (pallet_number, item["sku"], item["description"], item["quantity"]))

                conn.commit()
                cur.close()
            finally:
                conn.close()

            log_audit("CREATE", f"Pallet {pallet_number} created", pallet_number)
            return redirect(url_for("view_pallet", num=pallet_number))

    options_html = "".join(f'<option value="{n}">{n}</option>' for n in available_numbers)
    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    product_json = json.dumps(product_map)
    item_rows = build_item_rows(row_count=1)

    content = f"""
    <div class="card">
        <h2>Create Pallet</h2>
        {error_html}

        <form method="post">
            <div class="grid grid-2">
                <div>
                    <label>Pallet Number</label>
                    <select name="pallet_number" required>
                        <option value="">Choose a number</option>
                        {options_html}
                    </select>
                </div>
                <div>
                    <label>Pallet Name</label>
                    <input type="text" name="pallet_name" required placeholder="e.g. Bathroom Tiles">
                </div>
            </div>

            <div class="card" style="margin-top:16px;">
                <h3>Items</h3>
                <div id="item-rows">
                    {item_rows}
                </div>
            </div>

            <button class="btn" type="submit">Create Pallet</button>
        </form>
    </div>

    <div class="card">
        <h3>Available Products</h3>
        <table>
            <tr>
                <th>SKU</th>
                <th>Description</th>
            </tr>
            {"".join(
                f"<tr><td>{esc(sku)}</td><td>{esc(data['description'])}</td></tr>"
                for sku, data in product_map.items()
            ) if product_map else '<tr><td colspan="2" class="muted">No products imported yet.</td></tr>'}
        </table>
    </div>

    <script>
        const products = {product_json};

        function wireInputs() {{
            document.querySelectorAll(".sku-input").forEach(input => {{
                input.removeEventListener("input", handleSkuInput);
                input.removeEventListener("keydown", handleSkuKeydown);
                input.addEventListener("input", handleSkuInput);
                input.addEventListener("keydown", handleSkuKeydown);
            }});

            document.querySelectorAll(".qty-input").forEach(input => {{
                input.removeEventListener("keydown", handleQtyKeydown);
                input.addEventListener("keydown", handleQtyKeydown);
            }});
        }}

        function handleSkuInput() {{
            const idx = this.dataset.index;
            const descInput = document.querySelector('.desc-input[data-index="' + idx + '"]');
            const sku = this.value.trim();

            if (products[sku]) {{
                descInput.value = products[sku].description;
            }} else {{
                descInput.value = "";
            }}

            ensureTrailingBlankRow();
        }}

        function handleSkuKeydown(e) {{
            if (e.key === "Enter") {{
                e.preventDefault();
                const idx = this.dataset.index;
                const qtyInput = document.querySelector('.qty-input[data-index="' + idx + '"]');
                if (qtyInput) {{
                    qtyInput.focus();
                    qtyInput.select();
                }}
            }}
        }}

        function handleQtyKeydown(e) {{
            if (e.key === "Enter") {{
                e.preventDefault();
                const idx = parseInt(this.dataset.index, 10);
                ensureTrailingBlankRow();
                focusNextSku(idx + 1);
            }}
        }}

        function focusNextSku(index) {{
            let nextSku = document.querySelector('.sku-input[data-index="' + index + '"]');
            if (!nextSku) {{
                addRow();
                nextSku = document.querySelector('.sku-input[data-index="' + index + '"]');
            }}
            if (nextSku) {{
                nextSku.focus();
                nextSku.select();
            }}
        }}

        function ensureTrailingBlankRow() {{
            const rows = document.querySelectorAll("#item-rows .sku-row");
            const lastRow = rows[rows.length - 1];
            if (!lastRow) return;

            const lastSku = lastRow.querySelector(".sku-input").value.trim();
            const lastQty = lastRow.querySelector('.qty-input').value.trim();

            if (lastSku !== "" || lastQty !== "") {{
                addRow();
            }}
        }}

        function addRow() {{
            const container = document.getElementById("item-rows");
            const index = container.querySelectorAll(".sku-row").length;

            const row = document.createElement("div");
            row.className = "sku-row";
            row.innerHTML = `
                <div class="row">
                    <div>
                        <label>SKU</label>
                        <input name="sku" class="sku-input" data-index="${{index}}" placeholder="Enter SKU">
                    </div>
                    <div>
                        <label>Description</label>
                        <input class="desc-input readonly" data-index="${{index}}" readonly>
                    </div>
                    <div>
                        <label>Qty</label>
                        <input type="number" name="quantity" class="qty-input" data-index="${{index}}" min="1" placeholder="0">
                    </div>
                </div>
            `;
            container.appendChild(row);
            wireInputs();
        }}

        wireInputs();
    </script>
    """
    return render_page(content, "Create Pallet")


@app.route("/edit/<int:num>", methods=["GET", "POST"])
@login_required
@level_required(1)
def edit_pallet(num):
    product_map = get_product_map()

    pallet = execute("""
        SELECT pallet_number, pallet_name, created_at, created_by
        FROM pallets
        WHERE pallet_number = %s
    """, (num,), fetchone=True)

    items_rows = execute("""
        SELECT sku, description, quantity
        FROM pallet_items
        WHERE pallet_number = %s
        ORDER BY sku
    """, (num,), fetchall=True)

    if not pallet:
        return render_page("""
        <div class="card">
            <h2>Pallet not found</h2>
            <a class="btn secondary" href="/">Back</a>
        </div>
        """, "Not Found")

    existing_items = [
        {"sku": row["sku"], "description": row["description"], "quantity": row["quantity"]}
        for row in items_rows
    ]

    error_message = ""

    if request.method == "POST":
        pallet_name = request.form.get("pallet_name", "").strip()
        pallet_number_raw = request.form.get("pallet_number", "").strip()

        try:
            new_pallet_number = int(pallet_number_raw)
        except ValueError:
            new_pallet_number = None

        items = parse_items_from_form(product_map)

        if new_pallet_number is None or new_pallet_number < 1 or new_pallet_number > 200:
            error_message = "Pallet number must be between 1 and 200."
        elif pallet_exists(new_pallet_number, exclude_number=num):
            error_message = "That pallet number is already in use."
        elif not pallet_name:
            error_message = "Please enter a pallet name."
        elif not items:
            error_message = "Please add at least one valid SKU and quantity."
        else:
            conn = get_db_connection()
            try:
                cur = conn.cursor()

                if new_pallet_number != num:
                    cur.execute("""
                        UPDATE pallets
                        SET pallet_number = %s, pallet_name = %s
                        WHERE pallet_number = %s
                    """, (new_pallet_number, pallet_name, num))

                    cur.execute("""
                        UPDATE pallet_items
                        SET pallet_number = %s
                        WHERE pallet_number = %s
                    """, (new_pallet_number, num))
                else:
                    cur.execute("""
                        UPDATE pallets
                        SET pallet_name = %s
                        WHERE pallet_number = %s
                    """, (pallet_name, num))

                cur.execute("DELETE FROM pallet_items WHERE pallet_number = %s", (new_pallet_number,))

                for item in items:
                    cur.execute("""
                        INSERT INTO pallet_items (pallet_number, sku, description, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (new_pallet_number, item["sku"], item["description"], item["quantity"]))

                conn.commit()
                cur.close()
            finally:
                conn.close()

            log_audit("EDIT", f"Pallet {num} updated", new_pallet_number)
            return redirect(url_for("view_pallet", num=new_pallet_number))

        existing_items = items

    available_numbers = get_available_pallet_numbers(include_number=num)
    options_html = "".join(
        f'<option value="{n}" {"selected" if n == pallet["pallet_number"] else ""}>{n}</option>'
        for n in available_numbers
    )

    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""
    product_json = json.dumps(product_map)
    item_rows = build_item_rows(existing_items=existing_items, row_count=1)

    content = f"""
    <div class="card">
        <h2>Edit Pallet {num}</h2>
        {error_html}

        <form method="post">
            <div class="grid grid-2">
                <div>
                    <label>Pallet Number</label>
                    <select name="pallet_number" required>
                        {options_html}
                    </select>
                </div>
                <div>
                    <label>Pallet Name</label>
                    <input type="text" name="pallet_name" required value="{esc(pallet['pallet_name'])}">
                </div>
            </div>

            <div class="card" style="margin-top:16px;">
                <h3>Items</h3>
                <div id="item-rows">
                    {item_rows}
                </div>
            </div>

            <button class="btn" type="submit">Save Changes</button>
            <a class="btn secondary" href="{url_for('view_pallet', num=num)}">Cancel</a>
        </form>
    </div>

    <script>
        const products = {product_json};

        function wireInputs() {{
            document.querySelectorAll(".sku-input").forEach(input => {{
                input.removeEventListener("input", handleSkuInput);
                input.removeEventListener("keydown", handleSkuKeydown);
                input.addEventListener("input", handleSkuInput);
                input.addEventListener("keydown", handleSkuKeydown);
            }});

            document.querySelectorAll(".qty-input").forEach(input => {{
                input.removeEventListener("keydown", handleQtyKeydown);
                input.addEventListener("keydown", handleQtyKeydown);
            }});
        }}

        function handleSkuInput() {{
            const idx = this.dataset.index;
            const descInput = document.querySelector('.desc-input[data-index="' + idx + '"]');
            const sku = this.value.trim();

            if (products[sku]) {{
                descInput.value = products[sku].description;
            }} else {{
                descInput.value = "";
            }}

            ensureTrailingBlankRow();
        }}

        function handleSkuKeydown(e) {{
            if (e.key === "Enter") {{
                e.preventDefault();
                const idx = this.dataset.index;
                const qtyInput = document.querySelector('.qty-input[data-index="' + idx + '"]');
                if (qtyInput) {{
                    qtyInput.focus();
                    qtyInput.select();
                }}
            }}
        }}

        function handleQtyKeydown(e) {{
            if (e.key === "Enter") {{
                e.preventDefault();
                const idx = parseInt(this.dataset.index, 10);
                ensureTrailingBlankRow();
                focusNextSku(idx + 1);
            }}
        }}

        function focusNextSku(index) {{
            let nextSku = document.querySelector('.sku-input[data-index="' + index + '"]');
            if (!nextSku) {{
                addRow();
                nextSku = document.querySelector('.sku-input[data-index="' + index + '"]');
            }}
            if (nextSku) {{
                nextSku.focus();
                nextSku.select();
            }}
        }}

        function ensureTrailingBlankRow() {{
            const rows = document.querySelectorAll("#item-rows .sku-row");
            const lastRow = rows[rows.length - 1];
            if (!lastRow) return;

            const lastSku = lastRow.querySelector(".sku-input").value.trim();
            const lastQty = lastRow.querySelector('.qty-input').value.trim();

            if (lastSku !== "" || lastQty !== "") {{
                addRow();
            }}
        }}

        function addRow() {{
            const container = document.getElementById("item-rows");
            const index = container.querySelectorAll(".sku-row").length;

            const row = document.createElement("div");
            row.className = "sku-row";
            row.innerHTML = `
                <div class="row">
                    <div>
                        <label>SKU</label>
                        <input name="sku" class="sku-input" data-index="${{index}}" placeholder="Enter SKU">
                    </div>
                    <div>
                        <label>Description</label>
                        <input class="desc-input readonly" data-index="${{index}}" readonly>
                    </div>
                    <div>
                        <label>Qty</label>
                        <input type="number" name="quantity" class="qty-input" data-index="${{index}}" min="1" placeholder="0">
                    </div>
                </div>
            `;
            container.appendChild(row);
            wireInputs();
        }}

        wireInputs();
    </script>
    """
    return render_page(content, f"Edit Pallet {num}")


@app.route("/duplicate/<int:num>", methods=["GET", "POST"])
@login_required
@level_required(1)
def duplicate_pallet(num):
    pallet, items = fetch_pallet(num)

    if not pallet:
        return render_page("""
        <div class="card">
            <h2>Pallet not found</h2>
            <a class="btn secondary" href="/">Back</a>
        </div>
        """, "Not Found")

    available_numbers = get_available_pallet_numbers()
    error_message = ""

    if request.method == "POST":
        pallet_name = request.form.get("pallet_name", "").strip()
        pallet_number_raw = request.form.get("pallet_number", "").strip()

        try:
            new_pallet_number = int(pallet_number_raw)
        except ValueError:
            new_pallet_number = None

        if new_pallet_number is None or new_pallet_number < 1 or new_pallet_number > 200:
            error_message = "Pallet number must be between 1 and 200."
        elif pallet_exists(new_pallet_number):
            error_message = "That pallet number is already in use."
        elif not pallet_name:
            error_message = "Please enter a pallet name."
        else:
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO pallets (pallet_number, pallet_name, created_at, created_by)
                    VALUES (%s, %s, %s, %s)
                """, (new_pallet_number, pallet_name, now_str(), session["username"]))

                for item in items:
                    cur.execute("""
                        INSERT INTO pallet_items (pallet_number, sku, description, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (new_pallet_number, item["sku"], item["description"], item["quantity"]))

                conn.commit()
                cur.close()
            finally:
                conn.close()

            log_audit("DUPLICATE", f"Pallet {num} duplicated to {new_pallet_number}", new_pallet_number)
            return redirect(url_for("view_pallet", num=new_pallet_number))

    options_html = "".join(f'<option value="{n}">{n}</option>' for n in available_numbers)
    error_html = f'<div class="warning">{esc(error_message)}</div>' if error_message else ""

    content = f"""
    <div class="card">
        <h2>Duplicate Pallet {pallet['pallet_number']}</h2>
        {error_html}
        <form method="post">
            <div class="grid grid-2">
                <div>
                    <label>New Pallet Number</label>
                    <select name="pallet_number" required>
                        <option value="">Choose a number</option>
                        {options_html}
                    </select>
                </div>
                <div>
                    <label>New Pallet Name</label>
                    <input type="text" name="pallet_name" value="{esc(pallet['pallet_name'])} Copy" required>
                </div>
            </div>
            <div style="margin-top:16px;">
                <button class="btn gold" type="submit">Duplicate Pallet</button>
                <a class="btn secondary" href="{url_for('view_pallet', num=num)}">Cancel</a>
            </div>
        </form>
    </div>

    <div class="card">
        <h3>Items to duplicate</h3>
        <table>
            <tr>
                <th>SKU</th>
                <th>Description</th>
                <th>Qty</th>
            </tr>
            {"".join(f"<tr><td>{esc(i['sku'])}</td><td>{esc(i['description'])}</td><td>{esc(i['quantity'])}</td></tr>" for i in items)}
        </table>
    </div>
    """
    return render_page(content, f"Duplicate Pallet {num}")


@app.route("/delete/<int:num>", methods=["POST"])
@login_required
@level_required(1)
def delete_pallet(num):
    pallet, _ = fetch_pallet(num)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pallet_items WHERE pallet_number = %s", (num,))
        cur.execute("DELETE FROM audit_run_items WHERE pallet_number = %s", (num,))
        cur.execute("DELETE FROM pallets WHERE pallet_number = %s", (num,))
        conn.commit()
        cur.close()
    finally:
        conn.close()

    if pallet:
        log_audit("DELETE", f"Pallet {num} deleted", num)

    return redirect(url_for("home"))


@app.route("/pallet/<int:num>")
@login_required
@level_required(1)
def view_pallet(num):
    pallet = execute("""
        SELECT pallet_number, pallet_name, created_at, created_by
        FROM pallets
        WHERE pallet_number = %s
    """, (num,), fetchone=True)

    items = execute("""
        SELECT sku, description, quantity
        FROM pallet_items
        WHERE pallet_number = %s
        ORDER BY sku
    """, (num,), fetchall=True)

    audit_rows = execute("""
        SELECT action_type, username, details, created_at
        FROM pallet_audit
        WHERE pallet_number = %s
        ORDER BY id DESC
        LIMIT 12
    """, (num,), fetchall=True)

    if not pallet:
        return render_page("""
        <div class="card">
            <h2>Pallet not found</h2>
            <a class="btn secondary" href="/">Back</a>
        </div>
        """, "Not Found")

    item_rows = ""
    for item in items:
        item_rows += f"""
        <tr>
            <td>{esc(item['sku'])}</td>
            <td>{esc(item['description'])}</td>
            <td>{esc(item['quantity'])}</td>
        </tr>
        """

    audit_html = ""
    if audit_rows:
        audit_html += """
        <div class="card">
            <h3>Pallet Activity</h3>
        """
        for row in audit_rows:
            audit_html += f"""
            <div class="audit-mini">
                <strong>{esc(row['action_type'])}</strong> — {esc(row['details'])}<br>
                <span class="small-text">{esc(row['username'])} • {esc(row['created_at'])}</span>
            </div>
            """
        audit_html += "</div>"

    content = f"""
    <div class="card">
        <h2>Pallet {pallet['pallet_number']}</h2>
        <p><strong>Name:</strong> {esc(pallet['pallet_name'])}</p>
        <p><strong>Date:</strong> {esc(pallet['created_at'])}</p>
        <p><strong>Created By:</strong> {esc(pallet['created_by'])}</p>

        <p>
            <a class="btn print" href="{url_for('label', num=pallet['pallet_number'])}">Print Label</a>
            <a class="btn secondary" href="{url_for('edit_pallet', num=pallet['pallet_number'])}">Edit</a>
            <a class="btn gold" href="{url_for('duplicate_pallet', num=pallet['pallet_number'])}">Duplicate</a>
            <a class="btn secondary" href="{url_for('view_all_pallets')}">View Pallets</a>
            <a class="btn secondary" href="{url_for('home')}">Back</a>
        </p>

        <form method="post" action="{url_for('delete_pallet', num=pallet['pallet_number'])}" onsubmit="return confirm('Delete pallet {pallet['pallet_number']}?');">
            <button class="btn danger" type="submit">Delete Pallet</button>
        </form>
    </div>

    <div class="card">
        <h3>Items on this pallet</h3>
        <table>
            <tr>
                <th>SKU</th>
                <th>Description</th>
                <th>Quantity</th>
            </tr>
            {item_rows}
        </table>
    </div>

    {audit_html}
    """
    return render_page(content, f"Pallet {num}")


@app.route("/pallet-audit", methods=["GET", "POST"])
@login_required
@level_required(2)
def pallet_audit():
    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "new_audit":
            start_new_audit_run()
            log_audit("AUDIT", "Started new weekly pallet audit")
            return redirect(url_for("pallet_audit"))

        pallet_number_raw = request.form.get("pallet_number", "").strip()

        try:
            pallet_number = int(pallet_number_raw)
        except ValueError:
            return redirect(url_for("pallet_audit"))

        current_audit = get_current_audit_run()
        if not current_audit:
            start_new_audit_run()
            current_audit = get_current_audit_run()

        if action == "confirm":
            execute("""
                INSERT INTO audit_run_items (audit_run_id, pallet_number, confirmed_at, confirmed_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (audit_run_id, pallet_number) DO NOTHING
            """, (current_audit["id"], pallet_number, now_str(), session["username"]), commit=True)
            log_audit("AUDIT", f"Pallet {pallet_number} confirmed in audit {current_audit['audit_name']}", pallet_number)

        elif action == "delete":
            pallet, _ = fetch_pallet(pallet_number)
            conn = get_db_connection()
            try:
                cur = conn.cursor()
                cur.execute("DELETE FROM pallet_items WHERE pallet_number = %s", (pallet_number,))
                cur.execute("DELETE FROM audit_run_items WHERE pallet_number = %s", (pallet_number,))
                cur.execute("DELETE FROM pallets WHERE pallet_number = %s", (pallet_number,))
                conn.commit()
                cur.close()
            finally:
                conn.close()

            if pallet:
                log_audit("AUDIT DELETE", f"Pallet {pallet_number} removed during audit", pallet_number)

        return redirect(url_for("pallet_audit"))

    current_audit = get_current_audit_run()

    if not current_audit:
        start_new_audit_run()
        current_audit = get_current_audit_run()

    pallets = execute("""
        SELECT p.pallet_number, p.pallet_name, p.created_at
        FROM pallets p
        LEFT JOIN audit_run_items ari
            ON ari.pallet_number = p.pallet_number
            AND ari.audit_run_id = %s
        WHERE ari.id IS NULL
        ORDER BY p.pallet_number ASC
    """, (current_audit["id"],), fetchall=True)

    recent_audits = execute("""
        SELECT id, audit_name, created_at, created_by, is_closed
        FROM audit_runs
        ORDER BY id DESC
        LIMIT 6
    """, fetchall=True)

    if pallets:
        rows = ""
        for pallet in pallets:
            rows += f"""
            <div class="audit-row">
                <div class="audit-main">
                    <strong>Pallet {pallet['pallet_number']}</strong> — {esc(pallet['pallet_name'])}<br>
                    <span class="small-text">Created: {esc(pallet['created_at'])}</span>
                </div>
                <div>
                    <form method="post" style="display:inline-block;">
                        <input type="hidden" name="action" value="confirm">
                        <input type="hidden" name="pallet_number" value="{pallet['pallet_number']}">
                        <button class="tiny-btn ok" type="submit" title="Confirm pallet is present">✓</button>
                    </form>
                    <form method="post" style="display:inline-block;" onsubmit="return confirm('Delete pallet {pallet['pallet_number']}?');">
                        <input type="hidden" name="action" value="delete">
                        <input type="hidden" name="pallet_number" value="{pallet['pallet_number']}">
                        <button class="tiny-btn delete" type="submit" title="Delete pallet">✗</button>
                    </form>
                </div>
            </div>
            """
    else:
        rows = '<p class="muted">All pallets have been confirmed for this audit.</p>'

    audit_history_rows = ""
    for audit in recent_audits:
        status_text = "Open" if audit["is_closed"] is False else "Closed"
        audit_history_rows += f"""
        <tr>
            <td>{esc(audit['audit_name'])}</td>
            <td>{esc(audit['created_at'])}</td>
            <td>{esc(audit['created_by'])}</td>
            <td>{esc(status_text)}</td>
        </tr>
        """

    content = f"""
    <div class="card">
        <h2>Pallet Audit</h2>
        <p><strong>Current audit:</strong> {esc(current_audit['audit_name'])}</p>
        <p class="muted">Tick confirms pallet in this audit. Cross deletes the pallet completely.</p>

        <form method="post" class="no-print" style="margin-bottom:18px;">
            <input type="hidden" name="action" value="new_audit">
            <button class="btn gold" type="submit">Start New Weekly Audit</button>
        </form>

        {rows}
    </div>

    <div class="card">
        <h3>Recent Audit Runs</h3>
        <table>
            <tr>
                <th>Audit</th>
                <th>Created</th>
                <th>By</th>
                <th>Status</th>
            </tr>
            {audit_history_rows}
        </table>
    </div>
    """
    return render_page(content, "Pallet Audit")


@app.route("/search", methods=["GET", "POST"])
@login_required
@level_required(1)
def search_sku():
    sku_value = request.values.get("sku", "").strip()
    results_html = ""

    if sku_value:
        matches = execute("""
            SELECT p.pallet_number, p.pallet_name, p.created_at, p.created_by, i.sku, i.description, i.quantity
            FROM pallet_items i
            JOIN pallets p ON p.pallet_number = i.pallet_number
            WHERE i.sku = %s
            ORDER BY p.pallet_number ASC
        """, (sku_value,), fetchall=True)

        if matches:
            result_rows = ""
            for match in matches:
                result_rows += f"""
                <tr>
                    <td>{esc(match['sku'])}</td>
                    <td>{esc(match['description'])}</td>
                    <td><a href="{url_for('view_pallet', num=match['pallet_number'])}">Pallet {match['pallet_number']}</a></td>
                    <td>{esc(match['pallet_name'])}</td>
                    <td>{esc(match['quantity'])}</td>
                    <td>{esc(match['created_at'])}</td>
                    <td>{esc(match['created_by'])}</td>
                </tr>
                """

            results_html = f"""
            <div class="card">
                <h3>Results for SKU {esc(sku_value)}</h3>
                <table>
                    <tr>
                        <th>SKU</th>
                        <th>Description</th>
                        <th>Pallet</th>
                        <th>Pallet Name</th>
                        <th>Qty</th>
                        <th>Date</th>
                        <th>Created By</th>
                    </tr>
                    {result_rows}
                </table>
            </div>
            """
        else:
            results_html = f"""
            <div class="card">
                <h3>Results for SKU {esc(sku_value)}</h3>
                <p class="muted">No pallets found for that SKU.</p>
            </div>
            """

    content = f"""
    <div class="card">
        <h2>Search by SKU</h2>
        <form method="get">
            <div class="search-box">
                <input type="text" name="sku" placeholder="Enter SKU" value="{esc(sku_value)}">
                <button class="btn" type="submit">Search</button>
            </div>
        </form>
    </div>

    {results_html}
    """
    return render_page(content, "Search SKU")


@app.route("/label/<int:num>")
@login_required
@level_required(1)
def label(num):
    pallet, items = fetch_pallet(num)

    if not pallet:
        return render_page("""
        <div class="card">
            <h2>Label not found</h2>
            <a class="btn secondary" href="/">Back</a>
        </div>
        """, "Not Found")

    content = f"""
    <div class="no-print label-toolbar">
        <a class="btn secondary" href="{url_for('view_pallet', num=num)}">Back</a>
        <button class="btn print" onclick="window.print()">Print Label</button>
    </div>

    <div class="print-zone">
        {render_label_html(pallet, items)}
    </div>
    """
    return render_page(content, f"Label {num}")


@app.route("/labels/print-all")
@login_required
@level_required(2)
def print_all_labels():
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT pallet_number, pallet_name, created_at, created_by
            FROM pallets
            ORDER BY pallet_number ASC
        """)
        pallets = cur.fetchall()

        content_labels = ""
        for pallet in pallets:
            cur.execute("""
                SELECT sku, description, quantity
                FROM pallet_items
                WHERE pallet_number = %s
                ORDER BY sku
            """, (pallet["pallet_number"],))
            items = cur.fetchall()
            content_labels += render_label_html(pallet, items)

        cur.close()
    finally:
        conn.close()

    content = f"""
    <div class="no-print label-toolbar">
        <a class="btn secondary" href="{url_for('home')}">Back</a>
        <button class="btn print" onclick="window.print()">Print All Labels</button>
    </div>

    <div class="print-zone">
        {content_labels if content_labels else '<div class="card"><h2>No pallets to print</h2></div>'}
    </div>
    """
    return render_page(content, "Print All Labels")


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
