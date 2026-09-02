"""
blueprints/punch.py — 員工打卡頁面、打卡記錄、補打卡申請、月出勤統計
"""
import math
from datetime import datetime as _dt, timezone as _tz, timedelta as _td
from datetime import date

import psycopg
from flask import Blueprint, session, request, jsonify, render_template

from auth import (login_required, require_module,
                  login_blocked, record_login_failure, clear_login_failures, LOGIN_BLOCKED_MSG)
from config import TW_TZ
from db import get_db, hash_password, verify_password, is_legacy_hash
from blueprints.notifications import _notify_review_result

bp = Blueprint('punch', __name__)


# ─── Row helpers ─────────────────────────────────────────────────────────────

def _gps_distance(lat1, lng1, lat2, lng2):
    R = 6371000
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2 +
         math.cos(lat1 * p) * math.cos(lat2 * p) *
         math.sin((lng2 - lng1) * p / 2) ** 2)
    return int(2 * R * math.asin(math.sqrt(a)))


def punch_staff_row(row):
    if not row: return None
    d = dict(row)
    d.pop('password_hash', None)
    d.pop('password_plain', None)
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    if d.get('hire_date'):  d['hire_date']  = d['hire_date'].isoformat()
    if d.get('birth_date'): d['birth_date'] = d['birth_date'].isoformat()
    return d


def _parse_tw_datetime(s):
    if not s:
        return None
    dt = _dt.fromisoformat(str(s).replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TW_TZ)
    return dt


def punch_record_row(row):
    if not row: return None
    d = dict(row)
    for f in ['latitude', 'longitude']:
        if d.get(f) is not None: d[f] = float(d[f])
    for f in ['punched_at', 'created_at']:
        if d.get(f):
            dt = d[f]
            if dt.tzinfo is None:
                from datetime import timezone as _utz
                dt = dt.replace(tzinfo=_utz.utc)
            d[f] = dt.astimezone(TW_TZ).isoformat()
    return d


def loc_row(row):
    if not row: return None
    d = dict(row)
    for f in ['lat', 'lng']:
        if d.get(f) is not None: d[f] = float(d[f])
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    return d


def punch_req_row(row):
    if not row: return None
    d = dict(row)
    if d.get('requested_at'): d['requested_at'] = d['requested_at'].isoformat()
    if d.get('reviewed_at'):  d['reviewed_at']  = d['reviewed_at'].isoformat()
    if d.get('created_at'):   d['created_at']   = d['created_at'].isoformat()
    return d


def ot_req_row(row):
    if not row: return None
    d = dict(row)
    if d.get('request_date'): d['request_date'] = d['request_date'].isoformat()
    if d.get('start_time'):   d['start_time']   = str(d['start_time'])[:5]
    if d.get('end_time'):     d['end_time']      = str(d['end_time'])[:5]
    if d.get('ot_pay'):       d['ot_pay']        = float(d['ot_pay'])
    if d.get('ot_hours'):     d['ot_hours']      = float(d['ot_hours'])
    if d.get('reviewed_at'):  d['reviewed_at']   = d['reviewed_at'].isoformat()
    if d.get('created_at'):   d['created_at']    = d['created_at'].isoformat()
    return d


def shift_type_row(row):
    if not row: return None
    d = dict(row)
    if d.get('start_time'): d['start_time'] = str(d['start_time'])[:5]
    if d.get('end_time'):   d['end_time']   = str(d['end_time'])[:5]
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d


def shift_assign_row(row):
    if not row: return None
    d = dict(row)
    if d.get('shift_date'): d['shift_date'] = d['shift_date'].isoformat()
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d


def sched_req_row(row):
    import json as _json
    if not row: return None
    d = dict(row)
    if isinstance(d.get('dates'), str):
        try: d['dates'] = _json.loads(d['dates'])
        except: d['dates'] = []
    if d.get('reviewed_at'): d['reviewed_at'] = d['reviewed_at'].isoformat()
    if d.get('created_at'):  d['created_at']  = d['created_at'].isoformat()
    if d.get('updated_at'):  d['updated_at']  = d['updated_at'].isoformat()
    return d


# ─── 員工打卡頁面 ────────────────────────────────────────────────────────────

@bp.route('/punch')
@bp.route('/staff')
def punch_page():
    return render_template('staff.html')


# ─── Employee Session ────────────────────────────────────────────────────────

@bp.route('/api/punch/login', methods=['POST'])
def api_punch_login():
    b = request.get_json(force=True)
    username = b.get('username', '').strip()
    password = b.get('password', '').strip()
    if not username or not password:
        return jsonify({'error': '請輸入帳號及密碼'}), 400
    if login_blocked(username):
        return jsonify({'error': LOGIN_BLOCKED_MSG}), 429
    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE username=%s AND active=TRUE", (username,)
        ).fetchone()
    if not staff or not verify_password(password, staff['password_hash']):
        record_login_failure(username)
        return jsonify({'error': '帳號或密碼錯誤'}), 401
    clear_login_failures(username)
    if is_legacy_hash(staff['password_hash']):
        with get_db() as conn:
            conn.execute("UPDATE punch_staff SET password_hash=%s WHERE id=%s",
                         (hash_password(password), staff['id']))
    session['punch_staff_id']   = staff['id']
    session['punch_staff_name'] = staff['name']
    return jsonify({'id': staff['id'], 'name': staff['name'], 'role': staff['role']})


@bp.route('/api/punch/logout', methods=['POST'])
def api_punch_logout():
    session.pop('punch_staff_id', None)
    session.pop('punch_staff_name', None)
    return jsonify({'ok': True})


@bp.route('/api/punch/me', methods=['GET'])
def api_punch_me():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        staff = conn.execute(
            "SELECT id,name,role FROM punch_staff WHERE id=%s AND active=TRUE", (sid,)
        ).fetchone()
    if not staff:
        session.pop('punch_staff_id', None)
        return jsonify({'error': 'not logged in'}), 401
    return jsonify(dict(staff))


# ─── GPS Settings ────────────────────────────────────────────────────────────

@bp.route('/api/punch/settings', methods=['GET'])
def api_punch_settings_get():
    with get_db() as conn:
        cfg  = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
        locs = conn.execute(
            "SELECT * FROM punch_locations WHERE active=TRUE ORDER BY id"
        ).fetchall()
    return jsonify({
        'gps_required': cfg['gps_required'] if cfg else False,
        'work_start_time': (cfg['work_start_time'] if cfg else None) or '08:00',
        'work_end_time':   (cfg['work_end_time']   if cfg else None) or '17:00',
        'locations': [loc_row(r) for r in locs]
    })


@bp.route('/api/punch/config', methods=['PUT'])
@require_module('punch')
def api_punch_config_update():
    import re as _re
    b = request.get_json(force=True)
    sets, params = [], []
    if 'gps_required' in b:
        sets.append("gps_required=%s"); params.append(bool(b['gps_required']))
    for key in ('work_start_time', 'work_end_time'):
        if key in b:
            val = str(b[key] or '').strip()
            if not _re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', val):
                return jsonify({'error': '時間格式須為 HH:MM'}), 400
            sets.append(f"{key}=%s"); params.append(val)
    if not sets:
        return jsonify({'error': '沒有可更新的欄位'}), 400
    with get_db() as conn:
        row = conn.execute(
            f"UPDATE punch_config SET {', '.join(sets)}, updated_at=NOW() WHERE id=1 RETURNING *",
            params
        ).fetchone()
    return jsonify({'gps_required': row['gps_required'],
                    'work_start_time': row['work_start_time'],
                    'work_end_time': row['work_end_time']})


@bp.route('/api/punch/locations', methods=['GET'])
@require_module('punch')
def api_punch_locations_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM punch_locations ORDER BY id").fetchall()
    return jsonify([loc_row(r) for r in rows])


@bp.route('/api/punch/locations', methods=['POST'])
@require_module('punch')
def api_punch_locations_create():
    b = request.get_json(force=True)
    name = b.get('location_name', '').strip() or '打卡地點'
    try:
        lat = float(b['lat']); lng = float(b['lng'])
    except Exception:
        return jsonify({'error': '請填入有效的緯度和經度'}), 400
    radius_m = int(b.get('radius_m') or 100)
    with get_db() as conn:
        row = conn.execute(
            "INSERT INTO punch_locations (location_name, lat, lng, radius_m) VALUES (%s,%s,%s,%s) RETURNING *",
            (name, lat, lng, radius_m)
        ).fetchone()
    return jsonify(loc_row(row)), 201


@bp.route('/api/punch/locations/<int:lid>', methods=['PUT'])
@require_module('punch')
def api_punch_locations_update(lid):
    b = request.get_json(force=True)
    name = b.get('location_name', '').strip() or '打卡地點'
    try:
        lat = float(b['lat']); lng = float(b['lng'])
    except Exception:
        return jsonify({'error': '請填入有效的緯度和經度'}), 400
    radius_m = int(b.get('radius_m') or 100)
    active   = bool(b.get('active', True))
    with get_db() as conn:
        row = conn.execute(
            "UPDATE punch_locations SET location_name=%s,lat=%s,lng=%s,radius_m=%s,active=%s,updated_at=NOW() WHERE id=%s RETURNING *",
            (name, lat, lng, radius_m, active, lid)
        ).fetchone()
    return jsonify(loc_row(row)) if row else ('', 404)


@bp.route('/api/punch/locations/<int:lid>', methods=['DELETE'])
@require_module('punch')
def api_punch_locations_delete(lid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_locations WHERE id=%s", (lid,))
    return jsonify({'deleted': lid})


# ─── Clock In/Out ────────────────────────────────────────────────────────────

@bp.route('/api/punch/clock', methods=['POST'])
def api_punch_clock():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': '請先登入'}), 401

    b          = request.get_json(force=True)
    punch_type = b.get('punch_type')
    lat        = b.get('lat')
    lng        = b.get('lng')

    if punch_type not in ('in', 'out', 'break_out', 'break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400

    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE id=%s AND active=TRUE", (sid,)
        ).fetchone()
        if not staff:
            return jsonify({'error': '員工不存在'}), 404
        cfg  = conn.execute("SELECT * FROM punch_config WHERE id=1").fetchone()
        locs = conn.execute("SELECT * FROM punch_locations WHERE active=TRUE").fetchall()

    gps_required = cfg['gps_required'] if cfg else False
    gps_distance = None
    matched_loc  = None

    if lat is not None and lng is not None and locs:
        for loc in locs:
            d = _gps_distance(lat, lng, float(loc['lat']), float(loc['lng']))
            if gps_distance is None or d < gps_distance:
                gps_distance = d
                matched_loc  = loc

    if gps_required:
        if lat is None or lng is None:
            return jsonify({'error': '無法取得 GPS，請允許定位權限後重試'}), 403
        if not locs:
            return jsonify({'error': '管理員尚未設定任何打卡地點'}), 403
        if gps_distance is None or gps_distance > int(matched_loc['radius_m']):
            return jsonify({
                'error': f'距離最近地點「{matched_loc["location_name"]}」{gps_distance} 公尺，超出允許範圍（{matched_loc["radius_m"]} 公尺）',
                'distance': gps_distance,
                'radius': int(matched_loc['radius_m'])
            }), 403

    with get_db() as conn:
        recent = conn.execute("""
            SELECT id FROM punch_records
            WHERE staff_id=%s AND punch_type=%s
              AND punched_at > NOW() - INTERVAL '1 minute'
        """, (sid, punch_type)).fetchone()
        if recent:
            return jsonify({'error': '1 分鐘內已打過卡'}), 429

        matched_name = matched_loc['location_name'] if matched_loc else ''
        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, latitude, longitude, gps_distance, location_name)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING *
        """, (sid, punch_type, lat, lng, gps_distance, matched_name)).fetchone()

    d = punch_record_row(row)
    d['staff_name']   = staff['name']
    d['gps_distance'] = gps_distance
    return jsonify(d), 201


@bp.route('/api/punch/today', methods=['GET'])
def api_punch_today():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify([])
    with get_db() as conn:
        rows = conn.execute("""
            SELECT pr.*, ps.name as staff_name
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE pr.staff_id=%s
              AND (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
                = (NOW() AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY pr.punched_at ASC
        """, (sid,)).fetchall()
    return jsonify([punch_record_row(r) for r in rows])


@bp.route('/api/punch/my-records', methods=['GET'])
def api_punch_my_records():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': 'not logged in'}), 401
    month = request.args.get('month', '')
    if not month:
        month = _dt.now(TW_TZ).strftime('%Y-%m')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT punch_type, punched_at, gps_distance, location_name, is_manual
            FROM punch_records
            WHERE staff_id=%s
              AND to_char(punched_at AT TIME ZONE 'Asia/Taipei', 'YYYY-MM') = %s
            ORDER BY punched_at ASC
        """, (sid, month)).fetchall()
    TW = TW_TZ
    LABEL = {'in': '上班', 'out': '下班', 'break_out': '休息開始', 'break_in': '休息結束'}
    result = {}
    for r in rows:
        pa = r['punched_at']
        if pa.tzinfo is None:
            from datetime import timezone as _utz
            pa = pa.replace(tzinfo=_utz.utc)
        pa_tw    = pa.astimezone(TW)
        date_str = pa_tw.strftime('%Y-%m-%d')
        time_str = pa_tw.strftime('%H:%M')
        if date_str not in result:
            result[date_str] = []
        result[date_str].append({
            'type':          r['punch_type'],
            'label':         LABEL.get(r['punch_type'], r['punch_type']),
            'time':          time_str,
            'gps_distance':  r['gps_distance'],
            'location_name': r['location_name'] or '',
            'is_manual':     bool(r['is_manual']),
        })
    return jsonify({'month': month, 'records': result})


# ─── Admin: Staff CRUD ───────────────────────────────────────────────────────

@bp.route('/api/punch/staff', methods=['GET'])
@login_required
def api_punch_staff_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM punch_staff ORDER BY sort_order, name").fetchall()
    result = [punch_staff_row(r) for r in rows]
    # 員工下拉選單全後台共用，但薪資/銀行/生日等敏感欄位只給有 punch 或 salary 模組權限的管理員
    perms = session.get('admin_permissions') or []
    if not (session.get('admin_is_super') or 'punch' in perms or 'salary' in perms):
        SENSITIVE = ('base_salary', 'insured_salary', 'hourly_rate', 'daily_hours',
                     'salary_type', 'ot_rate1', 'ot_rate2', 'ot_rate3', 'vacation_quota',
                     'bank_code', 'bank_name', 'bank_branch', 'bank_account',
                     'account_holder', 'birth_date', 'termination_reason')
        for d in result:
            for k in SENSITIVE:
                d.pop(k, None)
    return jsonify(result)


@bp.route('/api/punch/staff/reorder', methods=['POST'])
@require_module('punch')
def api_punch_staff_reorder():
    items = request.get_json(force=True) or []
    if not isinstance(items, list):
        return jsonify({'error': '格式錯誤'}), 400
    with get_db() as conn:
        for item in items:
            conn.execute(
                "UPDATE punch_staff SET sort_order=%s WHERE id=%s",
                (int(item.get('sort_order', 0)), int(item['id']))
            )
    return jsonify({'ok': True})


@bp.route('/api/punch/staff', methods=['POST'])
@require_module('punch')
def api_punch_staff_create():
    b        = request.get_json(force=True)
    name     = b.get('name', '').strip()
    username = b.get('username', '').strip()
    password = b.get('password', '').strip()
    if not name:     return jsonify({'error': '姓名為必填'}), 400
    if not username: return jsonify({'error': '帳號為必填'}), 400
    if not password: return jsonify({'error': '請設定密碼'}), 400
    if len(password) < 8: return jsonify({'error': '密碼至少 8 個字元'}), 400
    employee_code  = (b.get('employee_code') or '').strip() or None
    department     = (b.get('department') or '').strip()
    role           = b.get('role', '').strip()
    hire_date      = b.get('hire_date') or None
    birth_date     = b.get('birth_date') or None
    bank_code      = (b.get('bank_code') or '').strip()
    bank_name      = (b.get('bank_name') or '').strip()
    bank_branch    = (b.get('bank_branch') or '').strip()
    bank_account   = (b.get('bank_account') or '').strip()
    account_holder = (b.get('account_holder') or '').strip()
    try:
        with get_db() as conn:
            row = conn.execute("""
                INSERT INTO punch_staff
                  (name, username, password_hash, role, position_title, employee_code,
                   department, hire_date, birth_date,
                   bank_code, bank_name, bank_branch, bank_account, account_holder)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
            """, (name, username, hash_password(password), role, role, employee_code,
                  department, hire_date, birth_date,
                  bank_code, bank_name, bank_branch, bank_account, account_holder)).fetchone()
        return jsonify(punch_staff_row(row)), 201
    except psycopg.errors.UniqueViolation:
        return jsonify({'error': '姓名或帳號已存在，請換一個'}), 409
    except Exception as e:
        if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
            return jsonify({'error': '姓名或帳號已存在，請換一個'}), 409
        return jsonify({'error': f'新增失敗：{str(e)}'}), 500


@bp.route('/api/punch/staff/<int:sid>', methods=['PUT'])
@require_module('punch')
def api_punch_staff_update(sid):
    b             = request.get_json(force=True)
    name          = b.get('name', '').strip()
    username      = b.get('username', '').strip()
    password      = b.get('password', '').strip()
    role          = b.get('role', '').strip()
    active        = bool(b.get('active', True))
    employee_code = (b.get('employee_code') or '').strip() or None
    bank_code      = (b.get('bank_code') or '').strip()
    bank_name      = (b.get('bank_name') or '').strip()
    bank_branch    = (b.get('bank_branch') or '').strip()
    bank_account   = (b.get('bank_account') or '').strip()
    account_holder = (b.get('account_holder') or '').strip()
    department     = (b.get('department') or '').strip()
    hire_date      = b.get('hire_date') or None
    birth_date     = b.get('birth_date') or None
    if not name or not username:
        return jsonify({'error': '姓名和帳號為必填'}), 400
    if password and len(password) < 8:
        return jsonify({'error': '密碼至少 8 個字元'}), 400
    with get_db() as conn:
        if password:
            row = conn.execute("""
                UPDATE punch_staff
                SET name=%s,username=%s,password_hash=%s,role=%s,position_title=%s,active=%s,employee_code=%s,
                    department=%s,hire_date=%s,birth_date=%s,
                    bank_code=%s,bank_name=%s,bank_branch=%s,bank_account=%s,account_holder=%s
                WHERE id=%s RETURNING *
            """, (name, username, hash_password(password), role, role, active, employee_code,
                  department, hire_date, birth_date,
                  bank_code, bank_name, bank_branch, bank_account, account_holder, sid)).fetchone()
        else:
            row = conn.execute("""
                UPDATE punch_staff
                SET name=%s,username=%s,role=%s,position_title=%s,active=%s,employee_code=%s,
                    department=%s,hire_date=%s,birth_date=%s,
                    bank_code=%s,bank_name=%s,bank_branch=%s,bank_account=%s,account_holder=%s
                WHERE id=%s RETURNING *
            """, (name, username, role, role, active, employee_code,
                  department, hire_date, birth_date,
                  bank_code, bank_name, bank_branch, bank_account, account_holder, sid)).fetchone()
    return jsonify(punch_staff_row(row)) if row else ('', 404)


@bp.route('/api/punch/staff/<int:sid>', methods=['DELETE'])
@require_module('punch')
def api_punch_staff_delete(sid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_staff WHERE id=%s", (sid,))
    return jsonify({'deleted': sid})


# ─── Admin: Punch Records ────────────────────────────────────────────────────

@bp.route('/api/punch/records', methods=['GET'])
@require_module('punch')
def api_punch_records():
    staff_id  = request.args.get('staff_id')
    date_from = request.args.get('date_from')
    date_to   = request.args.get('date_to')
    month     = request.args.get('month')

    conds, params = ["TRUE"], []
    if staff_id: conds.append("pr.staff_id=%s"); params.append(int(staff_id))
    if month:
        conds.append("TO_CHAR(pr.punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s"); params.append(month)
    elif date_from:
        conds.append("(pr.punched_at AT TIME ZONE 'Asia/Taipei')::date>=%s"); params.append(date_from)
        if date_to:
            conds.append("(pr.punched_at AT TIME ZONE 'Asia/Taipei')::date<=%s"); params.append(date_to)

    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, ps.name as staff_name, ps.role as staff_role
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE {' AND '.join(conds)}
            ORDER BY pr.punched_at DESC LIMIT 500
        """, params).fetchall()
    return jsonify([punch_record_row(r) for r in rows])


@bp.route('/api/punch/records', methods=['POST'])
@require_module('punch')
def api_punch_record_manual():
    b          = request.get_json(force=True)
    staff_id   = b.get('staff_id')
    punch_type = b.get('punch_type')
    punched_at = b.get('punched_at')
    note       = b.get('note', '').strip()
    manual_by  = b.get('manual_by', '').strip()
    if not all([staff_id, punch_type, punched_at]):
        return jsonify({'error': '缺少必要欄位'}), 400
    if punch_type not in ('in', 'out', 'break_out', 'break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400
    punched_at_parsed = _parse_tw_datetime(punched_at)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO punch_records
              (staff_id, punch_type, punched_at, note, is_manual, manual_by)
            VALUES (%s,%s,%s,%s,TRUE,%s) RETURNING *
        """, (staff_id, punch_type, punched_at_parsed, note, manual_by)).fetchone()
        staff = conn.execute("SELECT name FROM punch_staff WHERE id=%s", (staff_id,)).fetchone()
    d = punch_record_row(row)
    if staff: d['staff_name'] = staff['name']
    return jsonify(d), 201


@bp.route('/api/punch/records/<int:rid>', methods=['PUT'])
@require_module('punch')
def api_punch_record_update(rid):
    b = request.get_json(force=True)
    if b.get('punch_type') not in ('in', 'out', 'break_out', 'break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400
    punched_at_parsed = _parse_tw_datetime(b.get('punched_at'))
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_records
            SET punch_type=%s, punched_at=%s, note=%s, is_manual=TRUE, manual_by=%s
            WHERE id=%s RETURNING *
        """, (b.get('punch_type'), punched_at_parsed,
              b.get('note', ''), b.get('manual_by', ''), rid)).fetchone()
    return jsonify(punch_record_row(row)) if row else ('', 404)


@bp.route('/api/punch/records/<int:rid>', methods=['DELETE'])
@require_module('punch')
def api_punch_record_delete(rid):
    with get_db() as conn:
        pr = conn.execute("SELECT staff_id, punched_at FROM punch_records WHERE id=%s", (rid,)).fetchone()
        conn.execute("DELETE FROM punch_records WHERE id=%s", (rid,))
        if pr and pr['staff_id']:
            punch_month = (pr['punched_at'].strftime('%Y-%m')
                           if hasattr(pr['punched_at'], 'strftime')
                           else str(pr['punched_at'])[:7])
            conn.execute("""
                DELETE FROM salary_records
                WHERE staff_id=%s AND month=%s AND status='draft'
            """, (pr['staff_id'], punch_month))
    return jsonify({'deleted': rid})


@bp.route('/api/punch/summary', methods=['GET'])
@require_module('punch')
def api_punch_summary():
    month = request.args.get('month') or _dt.now(TW_TZ).strftime('%Y-%m')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ps.id as staff_id, ps.name as staff_name,
                   (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date as work_date,
                   MIN(CASE WHEN pr.punch_type='in'  THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_out,
                   COUNT(*) as punch_count,
                   BOOL_OR(pr.is_manual) as has_manual
            FROM punch_records pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE TO_CHAR(pr.punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
            GROUP BY ps.id, ps.name, (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date DESC, ps.name
        """, (month,)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['work_date']  = d['work_date'].isoformat()  if d['work_date']  else None
        d['clock_in']   = d['clock_in'].isoformat()   if d['clock_in']   else None
        d['clock_out']  = d['clock_out'].isoformat()  if d['clock_out']  else None
        if d['clock_in'] and d['clock_out']:
            from datetime import datetime as _dt2
            ci = _dt2.fromisoformat(d['clock_in'].replace('Z', ''))
            co = _dt2.fromisoformat(d['clock_out'].replace('Z', ''))
            d['duration_min'] = max(0, int((co - ci).total_seconds() / 60))
        else:
            d['duration_min'] = None
        result.append(d)

    # Merge cross-midnight pairs
    from datetime import date as _date2, timedelta as _td2, datetime as _dt2m
    result.sort(key=lambda x: (x.get('staff_id', 0), x.get('work_date', '')))
    merged = []
    skip_idx = set()
    for i, d in enumerate(result):
        if i in skip_idx:
            continue
        if d['clock_in'] and not d['clock_out'] and i + 1 < len(result):
            nd = result[i + 1]
            if (nd['staff_id'] == d['staff_id']
                    and d['work_date'] and nd['work_date']
                    and nd['work_date'] == (
                        _date2.fromisoformat(d['work_date']) + _td2(days=1)
                    ).isoformat()
                    and nd['clock_out'] and not nd['clock_in']):
                d = dict(d)
                d['clock_out']   = nd['clock_out']
                ci = _dt2m.fromisoformat(d['clock_in'].replace('Z', ''))
                co = _dt2m.fromisoformat(d['clock_out'].replace('Z', ''))
                d['duration_min'] = max(0, int((co - ci).total_seconds() / 60))
                d['punch_count']  = d.get('punch_count', 0) + nd.get('punch_count', 0)
                d['has_manual']   = bool(d.get('has_manual')) or bool(nd.get('has_manual'))
                skip_idx.add(i + 1)
        merged.append(d)

    return jsonify(merged)


@bp.route('/api/attendance/monthly-stats', methods=['GET'])
@require_module('punch')
def api_attendance_monthly_stats():
    month = request.args.get('month') or _dt.now(TW_TZ).strftime('%Y-%m')
    import calendar as _cal_ms
    _month_last_ms = f"{month}-{_cal_ms.monthrange(int(month[:4]), int(month[5:7]))[1]:02d}"
    with get_db() as conn:
        rows = conn.execute("""
            SELECT ps.id as staff_id, ps.name as staff_name,
                   ps.department, ps.role,
                   (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date as work_date,
                   MIN(CASE WHEN pr.punch_type='in'  THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN pr.punched_at AT TIME ZONE 'Asia/Taipei' END) as clock_out,
                   BOOL_OR(pr.punch_type='in')  as has_in,
                   BOOL_OR(pr.punch_type='out') as has_out
            FROM punch_records pr
            JOIN punch_staff ps ON ps.id = pr.staff_id AND ps.active = TRUE
            WHERE TO_CHAR(pr.punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM') = %s
            GROUP BY ps.id, ps.name, ps.department, ps.role,
                     (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY ps.name, work_date
        """, (month,)).fetchall()

        shift_rows = conn.execute("""
            SELECT sa.staff_id, sa.shift_date as date, st.start_time, st.end_time
            FROM shift_assignments sa
            JOIN shift_types st ON st.id = sa.shift_type_id
            WHERE TO_CHAR(sa.shift_date,'YYYY-MM') = %s
        """, (month,)).fetchall()
        shift_map = {(r['staff_id'], str(r['date'])): r for r in shift_rows}

        # 無排班（固定工時公司）用打卡設定的固定上下班時間判遲到／早退
        _cfg_ms = conn.execute(
            "SELECT work_start_time, work_end_time FROM punch_config WHERE id=1"
        ).fetchone()
        fixed_start_ms = (_cfg_ms and _cfg_ms.get('work_start_time')) or '08:00'
        fixed_end_ms   = (_cfg_ms and _cfg_ms.get('work_end_time'))   or '17:00'
        # 已核准請假的日期不計遲到／早退／缺卡
        _lv_ms = conn.execute("""
            SELECT staff_id, start_date, end_date FROM leave_requests
            WHERE status='approved'
              AND start_date <= %s::date AND end_date >= %s::date
        """, (_month_last_ms, f'{month}-01')).fetchall()
        # 休息時段（break_out / break_in）用來扣除工時
        _brk_ms = conn.execute("""
            SELECT staff_id, punch_type, punched_at AT TIME ZONE 'Asia/Taipei' AS pt
            FROM punch_records
            WHERE punch_type IN ('break_out','break_in')
              AND TO_CHAR(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM') = %s
            ORDER BY punched_at
        """, (month,)).fetchall()

    rows = [dict(r) for r in rows]
    from datetime import timedelta as _td_cm, date as _date_cm

    on_leave_ms = set()
    for _lv in _lv_ms:
        _c = _lv['start_date']
        while _c <= _lv['end_date']:
            on_leave_ms.add((_lv['staff_id'], _c.isoformat()))
            _c += _td_cm(days=1)

    from collections import defaultdict as _dd_ms
    break_map = _dd_ms(list)   # (staff_id, date) -> [(type, dt)]
    for _b in _brk_ms:
        break_map[(_b['staff_id'], _b['pt'].date().isoformat())].append((_b['punch_type'], _b['pt']))

    rows.sort(key=lambda x: (x['staff_id'], x['work_date']))
    merged_rows = []
    skip_cm = set()
    for _i, _r in enumerate(rows):
        if _i in skip_cm:
            continue
        if _r['has_in'] and not _r['has_out'] and _i + 1 < len(rows):
            _nr = rows[_i + 1]
            _nxt_date = _r['work_date'] + _td_cm(days=1) if _r['work_date'] else None
            if (_nr['staff_id'] == _r['staff_id']
                    and _nxt_date and _nr['work_date'] == _nxt_date
                    and _nr['has_out'] and not _nr['has_in']):
                _r = dict(_r)
                _r['has_out']   = True
                _r['clock_out'] = _nr['clock_out']
                skip_cm.add(_i + 1)
        merged_rows.append(_r)
    rows = merged_rows

    from collections import defaultdict
    stats = defaultdict(lambda: {
        'staff_id': None, 'staff_name': '', 'department': '', 'role': '',
        'days_worked': 0, 'total_minutes': 0,
        'late_count': 0, 'early_count': 0, 'missing_in_count': 0, 'missing_out_count': 0,
        'anomaly_dates': [],
    })

    for r in rows:
        sid = r['staff_id']
        ds  = str(r['work_date'])
        s   = stats[sid]
        s['staff_id']   = sid
        s['staff_name'] = r['staff_name']
        s['department'] = r['department'] or ''
        s['role']       = r['role']       or ''

        has_in  = bool(r['has_in'])
        has_out = bool(r['has_out'])

        if has_in or has_out:
            s['days_worked'] += 1

        if r['clock_in'] and r['clock_out']:
            diff = (r['clock_out'] - r['clock_in']).total_seconds() / 60
            # 扣除休息時段，與薪資工時計算一致
            _bs = sorted(break_map.get((sid, ds), []), key=lambda x: x[1])
            _open = None
            for _bt, _bdt in _bs:
                if _bt == 'break_out' and _open is None:
                    _open = _bdt
                elif _bt == 'break_in' and _open is not None:
                    diff -= (_bdt - _open).total_seconds() / 60
                    _open = None
            if diff > 0:
                s['total_minutes'] += int(diff)

        if (sid, ds) in on_leave_ms:      # 當日已請假，不列異常
            continue

        if has_in and not has_out:
            s['missing_out_count'] += 1
            s['anomaly_dates'].append({'date': ds, 'type': 'missing_out', 'label': '缺下班卡'})
        if not has_in and has_out:
            s['missing_in_count'] += 1
            s['anomaly_dates'].append({'date': ds, 'type': 'missing_in', 'label': '缺上班卡'})

        if has_in and r['clock_in']:
            shift = shift_map.get((sid, ds))
            _exp_start = str(shift['start_time'])[:5] if (shift and shift['start_time']) else fixed_start_ms
            if _exp_start:
                try:
                    sh, sm = map(int, _exp_start.split(':'))
                    ci_local = r['clock_in']
                    ih, im   = ci_local.hour, ci_local.minute
                    late_mins = (ih * 60 + im) - (sh * 60 + sm)
                    if late_mins > 10:
                        s['late_count'] += 1
                        s['anomaly_dates'].append({'date': ds, 'type': 'late',
                                                   'label': f'遲到 {late_mins} 分鐘'})
                except Exception:
                    pass

        if has_out and r['clock_out']:
            shift = shift_map.get((sid, ds))
            _exp_end = str(shift['end_time'])[:5] if (shift and shift['end_time']) else fixed_end_ms
            if _exp_end:
                try:
                    eh, em = map(int, _exp_end.split(':'))
                    co_local = r['clock_out']
                    oh, om   = co_local.hour, co_local.minute
                    early_mins = (eh * 60 + em) - (oh * 60 + om)
                    if early_mins > 15:
                        s['early_count'] += 1
                        s['anomaly_dates'].append({'date': ds, 'type': 'early',
                                                   'label': f'早退 {early_mins} 分鐘'})
                except Exception:
                    pass

    result = []
    for s in sorted(stats.values(), key=lambda x: (x['department'], x['staff_name'])):
        h   = s['total_minutes'] // 60
        m   = s['total_minutes'] % 60
        avg = round(s['total_minutes'] / s['days_worked'] / 60, 1) if s['days_worked'] else 0
        s['total_hours']   = round(s['total_minutes'] / 60, 1)
        s['avg_hours_day'] = avg
        s['total_hm']      = f"{h}h {m:02d}m"
        s['name']          = s['staff_name']   # 前端月統計表格與 Excel 匯出讀 name
        result.append(s)
    return jsonify({'month': month, 'stats': result})


# ─── Punch Requests (補打卡申請) ─────────────────────────────────────────────

@bp.route('/api/punch/request', methods=['POST'])
def api_punch_req_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b            = request.get_json(force=True)
    punch_type   = b.get('punch_type')
    requested_at = b.get('requested_at')
    reason       = b.get('reason', '').strip()
    if punch_type not in ('in', 'out', 'break_out', 'break_in'):
        return jsonify({'error': '無效的打卡類型'}), 400
    if not requested_at:
        return jsonify({'error': '請選擇補打時間'}), 400
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO punch_requests (staff_id, punch_type, requested_at, reason)
            VALUES (%s,%s,%s,%s) RETURNING *
        """, (sid, punch_type, requested_at, reason)).fetchone()
    return jsonify(punch_req_row(row)), 201


@bp.route('/api/punch/request/my', methods=['GET'])
def api_punch_req_my():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM punch_requests WHERE staff_id=%s ORDER BY requested_at DESC LIMIT 20",
            (sid,)
        ).fetchall()
    return jsonify([punch_req_row(r) for r in rows])


@bp.route('/api/punch/requests', methods=['GET'])
@require_module('punch')
def api_punch_reqs_list():
    status = request.args.get('status', '')
    month  = request.args.get('month', '')
    conds, params = ['TRUE'], []
    if status: conds.append('pr.status=%s'); params.append(status)
    if month:  conds.append("TO_CHAR(pr.requested_at,'YYYY-MM')=%s"); params.append(month)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT pr.*, ps.name as staff_name, ps.role as staff_role
            FROM punch_requests pr JOIN punch_staff ps ON ps.id=pr.staff_id
            WHERE {' AND '.join(conds)}
            ORDER BY pr.created_at DESC LIMIT 500
        """, params).fetchall()
    return jsonify([punch_req_row(r) for r in rows])


@bp.route('/api/punch/requests/<int:rid>', methods=['DELETE'])
@require_module('punch')
def api_punch_req_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM punch_requests WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})


@bp.route('/api/punch/requests/<int:rid>', methods=['PUT'])
@require_module('punch')
def api_punch_req_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')
    review_note = b.get('review_note', '').strip()
    reviewed_by = session.get('admin_display_name', '管理員')
    if action not in ('approve', 'reject'):
        return jsonify({'error': 'action 必須為 approve 或 reject'}), 400
    new_status = 'approved' if action == 'approve' else 'rejected'
    with get_db() as conn:
        old = conn.execute("SELECT * FROM punch_requests WHERE id=%s", (rid,)).fetchone()
        if not old:
            return ('', 404)
        old_status = old['status']
        row = conn.execute("""
            UPDATE punch_requests
            SET status=%s, reviewed_by=%s, review_note=%s, reviewed_at=NOW()
            WHERE id=%s RETURNING *
        """, (new_status, reviewed_by, review_note, rid)).fetchone()
        # 依 old_status 判斷，避免重複核准重複插入打卡；核准後改駁回要移除已插入的記錄
        month = (row['requested_at'].astimezone(TW_TZ).strftime('%Y-%m')
                 if hasattr(row['requested_at'], 'astimezone')
                 else str(row['requested_at'])[:7])
        if action == 'approve' and old_status != 'approved':
            conn.execute("""
                INSERT INTO punch_records
                  (staff_id, punch_type, punched_at, note, is_manual, manual_by)
                VALUES (%s,%s,%s,%s,TRUE,%s)
            """, (row['staff_id'], row['punch_type'], row['requested_at'],
                  f'補打卡申請 #{rid}', reviewed_by))
        elif action == 'reject' and old_status == 'approved':
            conn.execute("""
                DELETE FROM punch_records
                WHERE staff_id=%s AND is_manual=TRUE AND note=%s
            """, (row['staff_id'], f'補打卡申請 #{rid}'))
        if old_status != new_status:
            conn.execute("""
                DELETE FROM salary_records
                WHERE staff_id=%s AND month=%s AND status='draft'
            """, (row['staff_id'], month))
    LABEL = {'in': '上班打卡', 'out': '下班打卡', 'break_out': '休息開始', 'break_in': '休息結束'}
    dt_str = row['requested_at'].isoformat()[:16].replace('T', ' ')
    extra  = f"{LABEL.get(row['punch_type'], '')} {dt_str}"
    if review_note: extra += f"\n審核意見：{review_note}"
    _notify_review_result(row['staff_id'], '補打卡申請', action, extra)
    return jsonify(punch_req_row(row))


# ─── Staff terminate / reinstate ─────────────────────────────────────────────

@bp.route('/api/punch/staff/<int:sid>/terminate', methods=['POST'])
@require_module('punch')
def api_punch_staff_terminate(sid):
    b = request.get_json(force=True) or {}
    # 前端送的是 termination_date，兩種鍵都收，否則管理員選的離職日會被忽略、
    # 一律被記成「今天」。
    terminated_at = (b.get('terminated_at') or b.get('termination_date')
                     or _dt.now(TW_TZ).strftime('%Y-%m-%d'))
    reason        = b.get('reason', '').strip()
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_staff
            SET active=FALSE, terminated_at=%s, termination_reason=%s
            WHERE id=%s RETURNING id, name, active, terminated_at, termination_reason
        """, (terminated_at, reason, sid)).fetchone()
    return jsonify(dict(row)) if row else ('', 404)


@bp.route('/api/punch/staff/<int:sid>/reinstate', methods=['POST'])
@require_module('punch')
def api_punch_staff_reinstate(sid):
    with get_db() as conn:
        row = conn.execute("""
            UPDATE punch_staff
            SET active=TRUE, terminated_at=NULL, termination_reason=''
            WHERE id=%s RETURNING id, name, active
        """, (sid,)).fetchone()
    return jsonify(dict(row)) if row else ('', 404)


@bp.route('/api/punch/staff/terminated', methods=['GET'])
@require_module('punch')
def api_punch_staff_terminated():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, name, employee_code, department, role,
                   terminated_at, termination_reason
            FROM punch_staff WHERE active=FALSE
            ORDER BY terminated_at DESC NULLS LAST
        """).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        if d.get('terminated_at'): d['terminated_at'] = str(d['terminated_at'])
        result.append(d)
    return jsonify(result)


# ─── Batch punch request review ──────────────────────────────────────────────

@bp.route('/api/punch/requests/batch', methods=['POST'])
@require_module('punch')
def api_punch_requests_batch():
    b           = request.get_json(force=True)
    ids         = b.get('ids', [])
    action      = b.get('action')
    review_note = b.get('review_note', '').strip()
    reviewed_by = session.get('admin_display_name', '管理員')
    if action not in ('approve', 'reject'):
        return jsonify({'error': 'invalid action'}), 400
    new_status = 'approved' if action == 'approve' else 'rejected'
    results = []
    with get_db() as conn:
        for rid in ids:
            row = conn.execute("""
                UPDATE punch_requests
                SET status=%s, reviewed_by=%s, review_note=%s, reviewed_at=NOW()
                WHERE id=%s RETURNING *
            """, (new_status, reviewed_by, review_note, rid)).fetchone()
            if row and action == 'approve':
                conn.execute("""
                    INSERT INTO punch_records
                      (staff_id, punch_type, punched_at, note, is_manual, manual_by)
                    VALUES (%s,%s,%s,%s,TRUE,%s)
                """, (row['staff_id'], row['punch_type'], row['requested_at'],
                      f'補打卡申請 #{rid}', reviewed_by))
                month = (row['requested_at'].strftime('%Y-%m')
                         if hasattr(row['requested_at'], 'strftime')
                         else str(row['requested_at'])[:7])
                conn.execute("""
                    DELETE FROM salary_records
                    WHERE staff_id=%s AND month=%s AND status='draft'
                """, (row['staff_id'], month))
            if row:
                results.append(punch_req_row(row))
    return jsonify({'updated': len(results), 'rows': results})
