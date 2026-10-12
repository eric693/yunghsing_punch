"""
db.py — 資料庫連線、密碼雜湊、完整建表邏輯
"""
import hashlib
import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row

from config import DATABASE_URL, ADMIN_PASSWORD


# ── 連線 ──────────────────────────────────────────────────────────

@contextmanager
def get_db():
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        yield conn


# ── 密碼雜湊 ──────────────────────────────────────────────────────
# 新密碼一律用 werkzeug pbkdf2（加鹽）；舊資料為無鹽 SHA-256 hex，
# verify_password 兩者皆可驗證，登入成功後由呼叫端呼叫 upgrade 換新格式。

from werkzeug.security import generate_password_hash, check_password_hash


def _hash_pw(pw: str) -> str:
    """僅供驗證舊 SHA-256 資料用，勿用於產生新密碼"""
    return hashlib.sha256(pw.encode()).hexdigest()


def hash_password(pw: str) -> str:
    return generate_password_hash(pw)


def is_legacy_hash(stored: str) -> bool:
    return bool(stored) and ':' not in stored


def verify_password(pw: str, stored: str) -> bool:
    if not stored:
        return False
    if is_legacy_hash(stored):
        import hmac as _hmac
        return _hmac.compare_digest(stored, _hash_pw(pw))
    return check_password_hash(stored, pw)


# ── 完整建表（atomic，不拆分） ────────────────────────────────────

def init_db():
    # --- 主要資料表 ---
    tables = [
        """CREATE TABLE IF NOT EXISTS punch_staff (
            id             SERIAL PRIMARY KEY,
            name           TEXT NOT NULL,
            username       TEXT UNIQUE,
            password_hash  TEXT,
            role           TEXT DEFAULT 'employee',
            department     TEXT DEFAULT '',
            position_title TEXT DEFAULT '',
            employee_code  TEXT DEFAULT '',
            hire_date      DATE,
            birth_date     DATE,
            base_salary    NUMERIC(12,2) DEFAULT 0,
            insured_salary NUMERIC(12,2) DEFAULT 0,
            hourly_rate    NUMERIC(12,2) DEFAULT 0,
            daily_hours    NUMERIC(5,2) DEFAULT 8,
            salary_type    TEXT DEFAULT 'monthly',
            ot_rate1       NUMERIC(5,2) DEFAULT 1.33,
            ot_rate2       NUMERIC(5,2) DEFAULT 1.67,
            ot_rate3       NUMERIC(5,2) DEFAULT 2.00,
            vacation_quota INT,
            line_user_id   TEXT,
            active         BOOLEAN DEFAULT TRUE,
            sort_order     INT DEFAULT 0,
            store_id       INT,
            finance_synced BOOLEAN DEFAULT FALSE,
            terminated_at  DATE,
            termination_reason TEXT DEFAULT '',
            created_at     TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS punch_records (
            id            SERIAL PRIMARY KEY,
            staff_id      INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            punch_type    TEXT NOT NULL,
            punched_at    TIMESTAMPTZ DEFAULT NOW(),
            note          TEXT DEFAULT '',
            latitude      DOUBLE PRECISION,
            longitude     DOUBLE PRECISION,
            gps_distance  INT,
            location_name TEXT DEFAULT '',
            is_manual     BOOLEAN DEFAULT FALSE
        )""",
        """CREATE TABLE IF NOT EXISTS punch_locations (
            id            SERIAL PRIMARY KEY,
            location_name TEXT NOT NULL,
            lat           DOUBLE PRECISION NOT NULL,
            lng           DOUBLE PRECISION NOT NULL,
            radius_m      INT DEFAULT 100,
            active        BOOLEAN DEFAULT TRUE
        )""",
        """CREATE TABLE IF NOT EXISTS punch_config (
            id           INT PRIMARY KEY DEFAULT 1,
            gps_required BOOLEAN DEFAULT FALSE,
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS line_punch_config (
            id                   INT PRIMARY KEY DEFAULT 1,
            enabled              BOOLEAN DEFAULT FALSE,
            channel_access_token TEXT DEFAULT '',
            channel_secret       TEXT DEFAULT '',
            updated_at           TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS schedule_config (
            id              SERIAL PRIMARY KEY,
            month           TEXT NOT NULL,
            max_off_per_day INT DEFAULT 2,
            vacation_quota  INT DEFAULT 8,
            notes           TEXT DEFAULT '',
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(month)
        )""",
        """CREATE TABLE IF NOT EXISTS schedule_requests (
            id           SERIAL PRIMARY KEY,
            staff_id     INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            month        TEXT NOT NULL,
            dates        JSONB DEFAULT '[]',
            submit_note  TEXT DEFAULT '',
            status       TEXT DEFAULT 'pending',
            reviewed_by  TEXT DEFAULT '',
            reviewed_at  TIMESTAMPTZ,
            updated_at   TIMESTAMPTZ DEFAULT NOW(),
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(staff_id, month)
        )""",
        """CREATE TABLE IF NOT EXISTS punch_requests (
            id           SERIAL PRIMARY KEY,
            staff_id     INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            punch_type   TEXT NOT NULL,
            requested_at TIMESTAMPTZ NOT NULL,
            reason       TEXT DEFAULT '',
            status       TEXT DEFAULT 'pending',
            reviewed_by  TEXT DEFAULT '',
            reviewed_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS shift_types (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            start_time TIME,
            end_time   TIME,
            color      TEXT DEFAULT '#4A90D9',
            active     BOOLEAN DEFAULT TRUE,
            sort_order INT DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS shift_assignments (
            id            SERIAL PRIMARY KEY,
            staff_id      INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            shift_type_id INT REFERENCES shift_types(id) ON DELETE CASCADE,
            shift_date    DATE NOT NULL,
            note          TEXT DEFAULT '',
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(staff_id, shift_date)
        )""",
        """CREATE TABLE IF NOT EXISTS overtime_requests (
            id           SERIAL PRIMARY KEY,
            staff_id     INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            request_date DATE NOT NULL,
            start_time   TEXT DEFAULT '',
            end_time     TEXT DEFAULT '',
            ot_hours     NUMERIC(5,2) DEFAULT 0,
            reason       TEXT DEFAULT '',
            status       TEXT DEFAULT 'pending',
            pay_type     TEXT DEFAULT 'normal',
            day_type     TEXT DEFAULT 'weekday',
            ot_pay       NUMERIC(12,2) DEFAULT 0,
            reviewed_by  TEXT DEFAULT '',
            reviewed_at  TIMESTAMPTZ,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS shift_staffing_requirements (
            id             SERIAL PRIMARY KEY,
            shift_type_id  INT REFERENCES shift_types(id) ON DELETE CASCADE,
            day_of_week    INT NOT NULL,
            min_staff      INT DEFAULT 1,
            UNIQUE(shift_type_id, day_of_week)
        )""",
        """CREATE TABLE IF NOT EXISTS admin_accounts (
            id             SERIAL PRIMARY KEY,
            username       TEXT NOT NULL UNIQUE,
            password_hash  TEXT NOT NULL,
            display_name   TEXT DEFAULT '',
            is_super       BOOLEAN DEFAULT FALSE,
            permissions    JSONB DEFAULT '[]',
            active         BOOLEAN DEFAULT TRUE,
            last_login_at  TIMESTAMPTZ,
            created_at     TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS stores (
            id         SERIAL PRIMARY KEY,
            name       TEXT NOT NULL,
            address    TEXT DEFAULT '',
            phone      TEXT DEFAULT '',
            active     BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
    ]

    for sql in tables:
        try:
            with get_db() as conn:
                conn.execute(sql)
        except Exception as e:
            print(f'[init_db table] {e}')

    # --- Schema migrations (各用獨立連線，避免 transaction abort 汙染) ---
    migrations = [
        # punch_staff
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS department TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS position_title TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS employee_code TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hire_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS birth_date DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS base_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS insured_salary NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS hourly_rate NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS daily_hours NUMERIC(5,2) DEFAULT 8",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_type TEXT DEFAULT 'monthly'",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate1 NUMERIC(5,2) DEFAULT 1.33",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate2 NUMERIC(5,2) DEFAULT 1.67",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS ot_rate3 NUMERIC(5,2) DEFAULT 2.00",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS vacation_quota INT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS line_user_id TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS sort_order INT DEFAULT 0",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS store_id INT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS finance_synced BOOLEAN DEFAULT FALSE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS terminated_at DATE",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS termination_reason TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS pending_punch_type TEXT",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS pending_punch_at TIMESTAMPTZ",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS bank_code TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS bank_name TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS bank_branch TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS bank_account TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS account_holder TEXT DEFAULT ''",
        "ALTER TABLE shift_types ADD COLUMN IF NOT EXISTS departments TEXT DEFAULT ''",
        # punch_config — 固定上下班時間（無排班公司用，批次補登等功能的依據）
        "ALTER TABLE punch_config ADD COLUMN IF NOT EXISTS work_start_time TEXT DEFAULT '08:00'",
        "ALTER TABLE punch_config ADD COLUMN IF NOT EXISTS work_end_time TEXT DEFAULT '17:00'",
        "ALTER TABLE punch_config ADD COLUMN IF NOT EXISTS break_start_time TEXT DEFAULT '12:00'",
        "ALTER TABLE punch_config ADD COLUMN IF NOT EXISTS break_end_time TEXT DEFAULT '13:00'",
        # punch_records
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS gps_distance INT",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS location_name TEXT DEFAULT ''",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS is_manual BOOLEAN DEFAULT FALSE",
        "ALTER TABLE punch_records ADD COLUMN IF NOT EXISTS manual_by TEXT DEFAULT ''",
        # overtime_requests
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS start_time TEXT DEFAULT ''",
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS end_time TEXT DEFAULT ''",
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS pay_type TEXT DEFAULT 'normal'",
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS day_type TEXT DEFAULT 'weekday'",
        "ALTER TABLE overtime_requests ADD COLUMN IF NOT EXISTS ot_pay NUMERIC(12,2) DEFAULT 0",
        # schedule_config — rename old columns, add new
        "ALTER TABLE schedule_config ADD COLUMN IF NOT EXISTS max_off_per_day INT DEFAULT 2",
        "ALTER TABLE schedule_config ADD COLUMN IF NOT EXISTS vacation_quota INT DEFAULT 8",
        "ALTER TABLE schedule_config ADD COLUMN IF NOT EXISTS notes TEXT DEFAULT ''",
        "UPDATE schedule_config SET max_off_per_day=off_days_per_week WHERE max_off_per_day=2",
        "UPDATE schedule_config SET notes=note WHERE notes=''",
        # schedule_requests — rename old columns, add new, add UNIQUE constraint
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS dates JSONB DEFAULT '[]'",
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS submit_note TEXT DEFAULT ''",
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()",
        "UPDATE schedule_requests SET dates=preferred_off WHERE preferred_off IS NOT NULL AND preferred_off!='[]'::jsonb",
        "UPDATE schedule_requests SET submit_note=note WHERE note!=''",
        """DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conname='schedule_requests_staff_id_month_key'
              AND conrelid='schedule_requests'::regclass
          ) THEN
            ALTER TABLE schedule_requests ADD CONSTRAINT schedule_requests_staff_id_month_key UNIQUE (staff_id, month);
          END IF;
        END $$""",
        # schedule_requests status/reviewed columns
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pending'",
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS reviewed_by TEXT DEFAULT ''",
        "ALTER TABLE schedule_requests ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ",
        # admin_accounts
        "ALTER TABLE admin_accounts ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ",
        # 明文密碼欄位一律移除（安全性）
        "ALTER TABLE admin_accounts DROP COLUMN IF EXISTS password_plain",
        "ALTER TABLE punch_staff DROP COLUMN IF EXISTS password_plain",
        # leave_requests
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS leave_start_time TEXT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS leave_end_time TEXT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS total_days NUMERIC(7,4) DEFAULT 0",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS document_id INT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS force_reviewed BOOLEAN DEFAULT FALSE",
        # salary_records
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS finance_synced BOOLEAN DEFAULT FALSE",
        # finance_documents
        "ALTER TABLE finance_documents ADD COLUMN IF NOT EXISTS image_data TEXT",
        "ALTER TABLE finance_documents ADD COLUMN IF NOT EXISTS upload_date DATE",
    ]

    for sql in migrations:
        try:
            with get_db() as conn:
                conn.execute(sql)
        except Exception:
            pass

    # --- Seed default admin account ---
    try:
        with get_db() as conn:
            exists = conn.execute(
                "SELECT 1 FROM admin_accounts WHERE username='admin' LIMIT 1"
            ).fetchone()
            if not exists:
                conn.execute(
                    """INSERT INTO admin_accounts (username, password_hash, display_name, is_super)
                       VALUES ('admin', %s, '超級管理員', TRUE)""",
                    (hash_password(ADMIN_PASSWORD),)
                )
    except Exception as e:
        print(f'[init_db seed admin] {e}')

    # --- Seed default store ---
    try:
        with get_db() as conn:
            exists = conn.execute("SELECT 1 FROM stores LIMIT 1").fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO stores (name) VALUES ('總店')"
                )
    except Exception as e:
        print(f'[init_db seed store] {e}')

    # --- Seed default punch_config row ---
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO punch_config (id) VALUES (1) ON CONFLICT DO NOTHING"
            )
    except Exception as e:
        print(f'[init_db seed config] {e}')

    # --- Seed default line_punch_config row ---
    try:
        with get_db() as conn:
            conn.execute(
                "INSERT INTO line_punch_config (id) VALUES (1) ON CONFLICT DO NOTHING"
            )
    except Exception as e:
        print(f'[init_db seed line_config] {e}')
