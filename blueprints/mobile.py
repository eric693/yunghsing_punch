"""
blueprints/mobile.py — Mobile App JWT API (/api/mobile/*)
"""
import json as _json
import math
from collections import defaultdict
from datetime import datetime as _dt, date, timedelta

import jwt as _pyjwt
from flask import Blueprint, request, jsonify, g

from config import TW_TZ, MOBILE_JWT_SECRET, JWT_EXPIRE_HOURS
from db import get_db, hash_password, verify_password, is_legacy_hash

bp = Blueprint('mobile', __name__)

# ── JWT helpers ────────────────────────────────────────────────────

def _make_jwt(payload: dict) -> str:
    from datetime import timezone, timedelta
    payload['exp'] = _dt.now(timezone.utc) + timedelta(hours=JWT_EXPIRE_HOURS)
    secret = MOBILE_JWT_SECRET or _get_app_secret()
    return _pyjwt.encode(payload, secret, algorithm='HS256')


def _decode_jwt(token: str):
    secret = MOBILE_JWT_SECRET or _get_app_secret()
    return _pyjwt.decode(token, secret, algorithms=['HS256'])


def _get_app_secret():
    from flask import current_app
    return current_app.secret_key


def mobile_jwt_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer '):
            return jsonify({'error': '未授權'}), 401
        token = auth[7:]
        try:
            payload = _decode_jwt(token)
        except _pyjwt.ExpiredSignatureError:
            return jsonify({'error': 'token 已過期，請重新登入'}), 401
        except Exception:
            return jsonify({'error': 'token 無效'}), 401
        g.mobile_user = payload
        return f(*args, **kwargs)
    return decorated


def mobile_admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer '):
            return jsonify({'error': '未授權'}), 401
        token = auth[7:]
        try:
            payload = _decode_jwt(token)
        except Exception:
            return jsonify({'error': 'token 無效'}), 401
        if payload.get('role') != 'admin':
            return jsonify({'error': '需要管理員權限'}), 403
        g.mobile_user = payload
        return f(*args, **kwargs)
    return decorated


# ── Login ──────────────────────────────────────────────────────────

@bp.route('/api/mobile/login', methods=['POST'])
def mobile_login():
    b = request.get_json(force=True) or {}
    username = b.get('username', '').strip()
    password = b.get('password', '').strip()
    if not username or not password:
        return jsonify({'error': '請輸入帳號及密碼'}), 400
    from auth import login_blocked, record_login_failure, clear_login_failures, LOGIN_BLOCKED_MSG
    if login_blocked(username):
        return jsonify({'error': LOGIN_BLOCKED_MSG}), 429

    with get_db() as conn:
        admin = conn.execute(
            "SELECT * FROM admin_accounts WHERE username=%s AND active=TRUE", (username,)
        ).fetchone()
    if admin and verify_password(password, admin['password_hash']):
        clear_login_failures(username)
        if is_legacy_hash(admin['password_hash']):
            with get_db() as conn:
                conn.execute("UPDATE admin_accounts SET password_hash=%s WHERE id=%s",
                             (hash_password(password), admin['id']))
        perms = admin['permissions']
        if isinstance(perms, str):
            try: perms = _json.loads(perms)
            except: perms = []
        token = _make_jwt({
            'sub': str(admin['id']), 'role': 'admin',
            'username': admin['username'],
            'display_name': admin['display_name'] or admin['username'],
            'is_super': bool(admin['is_super']),
            'permissions': perms,
        })
        with get_db() as conn:
            conn.execute("UPDATE admin_accounts SET last_login_at=NOW() WHERE id=%s", (admin['id'],))
        return jsonify({
            'token': token, 'role': 'admin',
            'user': {
                'id': admin['id'], 'username': admin['username'],
                'display_name': admin['display_name'] or admin['username'],
                'is_super': bool(admin['is_super']), 'permissions': perms,
            }
        })

    with get_db() as conn:
        staff = conn.execute(
            "SELECT * FROM punch_staff WHERE username=%s AND active=TRUE", (username,)
        ).fetchone()
    if staff and verify_password(password, staff['password_hash']):
        clear_login_failures(username)
        if is_legacy_hash(staff['password_hash']):
            with get_db() as conn:
                conn.execute("UPDATE punch_staff SET password_hash=%s WHERE id=%s",
                             (hash_password(password), staff['id']))
        token = _make_jwt({
            'sub': str(staff['id']), 'role': 'employee',
            'staff_id': staff['id'], 'name': staff['name'], 'username': staff['username'],
        })
        return jsonify({
            'token': token, 'role': 'employee',
            'user': {
                'id': staff['id'], 'name': staff['name'], 'username': staff['username'],
                'role': staff['role'], 'department': staff['department'],
                'position_title': staff['position_title'], 'employee_code': staff['employee_code'],
            }
        })

    record_login_failure(username)
    return jsonify({'error': '帳號或密碼錯誤'}), 401


# ── Me ─────────────────────────────────────────────────────────────

@bp.route('/api/mobile/me', methods=['GET'])
@mobile_jwt_required
def mobile_me():
    u = g.mobile_user
    if u['role'] == 'employee':
        with get_db() as conn:
            staff = conn.execute(
                """SELECT id, name, username, role, department, position_title,
                          employee_code, hire_date, birth_date, base_salary,
                          insured_salary, daily_hours, salary_type, active
                   FROM punch_staff WHERE id=%s""", (int(u['sub']),)
            ).fetchone()
        if not staff:
            return jsonify({'error': '帳號不存在'}), 404
        d = dict(staff)
        for k in ('hire_date', 'birth_date'):
            if d.get(k): d[k] = str(d[k])
        return jsonify(d)
    else:
        with get_db() as conn:
            admin = conn.execute(
                "SELECT id, username, display_name, is_super, permissions FROM admin_accounts WHERE id=%s",
                (int(u['sub']),)
            ).fetchone()
        if not admin:
            return jsonify({'error': '帳號不存在'}), 404
        d = dict(admin)
        if isinstance(d['permissions'], str):
            try: d['permissions'] = _json.loads(d['permissions'])
            except: d['permissions'] = []
        return jsonify(d)


# ── Punch ──────────────────────────────────────────────────────────

@bp.route('/api/mobile/punch', methods=['POST'])
@mobile_jwt_required
def mobile_punch():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可打卡'}), 403
    staff_id = int(u['sub'])
    b = request.get_json(force=True) or {}
    punch_type = b.get('punch_type', 'in')
    lat  = b.get('latitude')
    lng  = b.get('longitude')
    note = b.get('note', '')

    with get_db() as conn:
        cfg = conn.execute("SELECT gps_required FROM punch_config WHERE id=1").fetchone()
        gps_required = cfg['gps_required'] if cfg else False
        locs = conn.execute("SELECT * FROM punch_locations WHERE active=TRUE").fetchall()

    gps_distance = None
    location_name = ''
    if lat is not None and lng is not None and locs:
        def haversine(la1, lo1, la2, lo2):
            R = 6371000
            p = math.pi / 180
            a = (math.sin((la2-la1)*p/2)**2 +
                 math.cos(la1*p)*math.cos(la2*p)*math.sin((lo2-lo1)*p/2)**2)
            return int(2*R*math.asin(math.sqrt(a)))
        best = min(locs, key=lambda l: haversine(float(l['lat']), float(l['lng']), float(lat), float(lng)))
        gps_distance = haversine(float(best['lat']), float(best['lng']), float(lat), float(lng))
        location_name = best['location_name']
        if gps_required and gps_distance > best['radius_m']:
            return jsonify({'error': f'距離打卡地點 {gps_distance}m，超出範圍 {best["radius_m"]}m'}), 400
    elif gps_required:
        return jsonify({'error': '此門市需要 GPS 定位才能打卡'}), 400

    with get_db() as conn:
        conn.execute(
            """INSERT INTO punch_records
               (staff_id, punch_type, note, latitude, longitude, gps_distance, location_name)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (staff_id, punch_type, note, lat, lng, gps_distance, location_name)
        )
    return jsonify({'ok': True, 'location_name': location_name, 'gps_distance': gps_distance})


@bp.route('/api/mobile/punch/status', methods=['GET'])
@mobile_jwt_required
def mobile_punch_status():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    today = _dt.now(TW_TZ).date().isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT punch_type, punched_at, note, gps_distance, location_name
               FROM punch_records WHERE staff_id=%s
               AND (punched_at AT TIME ZONE 'Asia/Taipei')::date = %s::date ORDER BY punched_at""",
            (staff_id, today)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        d['punched_at'] = d['punched_at'].isoformat() if d.get('punched_at') else None
        data.append(d)
    return jsonify(data)


# ── Attendance ─────────────────────────────────────────────────────

@bp.route('/api/mobile/attendance', methods=['GET'])
@mobile_jwt_required
def mobile_attendance():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    month = request.args.get('month', _dt.now(TW_TZ).strftime('%Y-%m'))
    try:
        y, m = map(int, month.split('-'))
    except Exception:
        return jsonify({'error': '月份格式錯誤'}), 400
    with get_db() as conn:
        rows = conn.execute(
            """SELECT punch_type, punched_at, note, gps_distance, location_name, is_manual
               FROM punch_records WHERE staff_id=%s
               AND date_trunc('month', punched_at) = %s::date
               ORDER BY punched_at""",
            (staff_id, f'{y}-{m:02d}-01')
        ).fetchall()

    days = defaultdict(list)
    for r in rows:
        day = r['punched_at'].date().isoformat()
        days[day].append({
            'type': r['punch_type'], 'time': r['punched_at'].strftime('%H:%M'),
            'note': r['note'], 'gps_distance': r['gps_distance'],
            'location_name': r['location_name'], 'is_manual': r['is_manual'],
        })

    result = []
    for day in sorted(days.keys()):
        records = days[day]
        ins  = [r for r in records if r['type'] == 'in']
        outs = [r for r in records if r['type'] == 'out']
        clock_in  = ins[0]['time']   if ins  else None
        clock_out = outs[-1]['time'] if outs else None
        hours = None
        if clock_in and clock_out:
            ci = _dt.strptime(clock_in,  '%H:%M')
            co = _dt.strptime(clock_out, '%H:%M')
            hours = round((co - ci).seconds / 3600, 2)
        result.append({'date': day, 'clock_in': clock_in, 'clock_out': clock_out,
                       'hours': hours, 'records': records})
    return jsonify(result)


# ── Leave ──────────────────────────────────────────────────────────

@bp.route('/api/mobile/leave/types', methods=['GET'])
@mobile_jwt_required
def mobile_leave_types():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, max_days FROM leave_types WHERE active=TRUE ORDER BY sort_order"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@bp.route('/api/mobile/leave', methods=['GET'])
@mobile_jwt_required
def mobile_leave_list():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    with get_db() as conn:
        rows = conn.execute(
            """SELECT lr.id, lt.name AS leave_type, lr.start_date, lr.end_date,
                      lr.total_days AS days, lr.reason, lr.status, lr.created_at
               FROM leave_requests lr
               JOIN leave_types lt ON lr.leave_type_id = lt.id
               WHERE lr.staff_id=%s ORDER BY lr.created_at DESC LIMIT 50""",
            (staff_id,)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        for k in ('start_date', 'end_date', 'created_at'):
            if d.get(k): d[k] = str(d[k])
        data.append(d)
    return jsonify(data)


@bp.route('/api/mobile/leave', methods=['POST'])
@mobile_jwt_required
def mobile_leave_apply():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可申請'}), 403
    staff_id = int(u['sub'])
    b = request.get_json(force=True) or {}
    leave_type_id = b.get('leave_type_id')
    start_date    = b.get('start_date')
    end_date      = b.get('end_date', start_date)
    reason        = b.get('reason', '')
    if not leave_type_id or not start_date:
        return jsonify({'error': '缺少必填欄位'}), 400
    try:
        _dt.strptime(start_date, '%Y-%m-%d')
        _dt.strptime(end_date,   '%Y-%m-%d')
    except Exception:
        return jsonify({'error': '日期格式錯誤'}), 400

    from blueprints.leave import _calc_leave_days
    total_days = _calc_leave_days(start_date, end_date)
    if total_days <= 0:
        return jsonify({'error': '請假天數不合理，請檢查日期'}), 400
    with get_db() as conn:
        conn.execute(
            """INSERT INTO leave_requests (staff_id, leave_type_id, start_date, end_date, total_days, reason, status)
               VALUES (%s, %s, %s, %s, %s, %s, 'pending')""",
            (staff_id, leave_type_id, start_date, end_date, total_days, reason)
        )
    return jsonify({'ok': True})


# ── Schedule ───────────────────────────────────────────────────────

@bp.route('/api/mobile/schedule', methods=['GET'])
@mobile_jwt_required
def mobile_schedule():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    month = request.args.get('month', _dt.now(TW_TZ).strftime('%Y-%m'))
    try:
        y, m = map(int, month.split('-'))
    except Exception:
        return jsonify({'error': '月份格式錯誤'}), 400
    with get_db() as conn:
        rows = conn.execute(
            """SELECT sa.shift_date, st.name AS shift_name, st.start_time, st.end_time, st.color
               FROM shift_assignments sa
               JOIN shift_types st ON sa.shift_type_id = st.id
               WHERE sa.staff_id=%s AND date_trunc('month', sa.shift_date) = %s::date
               ORDER BY sa.shift_date""",
            (staff_id, f'{y}-{m:02d}-01')
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        d['shift_date'] = str(d['shift_date'])
        if d.get('start_time'): d['start_time'] = str(d['start_time'])
        if d.get('end_time'):   d['end_time']   = str(d['end_time'])
        data.append(d)
    return jsonify(data)


# ── Salary ─────────────────────────────────────────────────────────

@bp.route('/api/mobile/salary', methods=['GET'])
@mobile_jwt_required
def mobile_salary():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, month, base_salary, allowance_total, deduction_total,
                      net_pay, status, confirmed_at, created_at
               FROM salary_records
               WHERE staff_id=%s AND status IN ('confirmed', 'paid')
               ORDER BY month DESC LIMIT 12""",
            (staff_id,)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        # 對應 App 既有欄位名稱
        d['period_year']  = int(str(d['month'])[:4]) if d.get('month') else None
        d['period_month'] = int(str(d['month'])[5:7]) if d.get('month') else None
        d['bonus']        = float(d['allowance_total'] or 0)
        d['deductions']   = float(d['deduction_total'] or 0)
        d['net_salary']   = float(d['net_pay'] or 0)
        d['paid_at']      = str(d['confirmed_at']) if d.get('confirmed_at') else None
        d['base_salary']  = float(d['base_salary'] or 0)
        for k in ('confirmed_at', 'created_at'):
            if d.get(k): d[k] = str(d[k])
        data.append(d)
    return jsonify(data)


# ── Overtime ───────────────────────────────────────────────────────

@bp.route('/api/mobile/overtime', methods=['POST'])
@mobile_jwt_required
def mobile_overtime():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可申請'}), 403
    staff_id = int(u['sub'])
    b = request.get_json(force=True) or {}
    ot_date = b.get('ot_date')
    hours   = b.get('hours')
    reason  = b.get('reason', '')
    if not ot_date or not hours:
        return jsonify({'error': '缺少必填欄位'}), 400
    try:
        _dt.strptime(ot_date, '%Y-%m-%d')
        hours = float(hours)
    except Exception:
        return jsonify({'error': '格式錯誤'}), 400
    with get_db() as conn:
        conn.execute(
            """INSERT INTO overtime_requests
               (staff_id, request_date, ot_hours, reason, status)
               VALUES (%s, %s, %s, %s, 'pending')""",
            (staff_id, ot_date, hours, reason)
        )
    return jsonify({'ok': True})


@bp.route('/api/mobile/overtime', methods=['GET'])
@mobile_jwt_required
def mobile_overtime_list():
    u = g.mobile_user
    if u['role'] != 'employee':
        return jsonify({'error': '僅員工可查詢'}), 403
    staff_id = int(u['sub'])
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, request_date AS ot_date, ot_hours, reason, status, created_at
               FROM overtime_requests WHERE staff_id=%s ORDER BY request_date DESC LIMIT 30""",
            (staff_id,)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        for k in ('ot_date', 'created_at'):
            if d.get(k): d[k] = str(d[k])
        if d.get('ot_hours'): d['ot_hours'] = float(d['ot_hours'])
        data.append(d)
    return jsonify(data)


# ── Admin: Dashboard ───────────────────────────────────────────────

@bp.route('/api/mobile/admin/dashboard', methods=['GET'])
@mobile_admin_required
def mobile_admin_dashboard():
    today = _dt.now(TW_TZ).date().isoformat()
    with get_db() as conn:
        total_staff   = conn.execute("SELECT COUNT(*) AS n FROM punch_staff WHERE active=TRUE").fetchone()['n']
        punched_today = conn.execute(
            "SELECT COUNT(DISTINCT staff_id) AS n FROM punch_records WHERE (punched_at AT TIME ZONE 'Asia/Taipei')::date=%s::date", (today,)
        ).fetchone()['n']
        pending_leaves = conn.execute(
            "SELECT COUNT(*) AS n FROM leave_requests WHERE status='pending'"
        ).fetchone()['n']
        pending_ot = conn.execute(
            "SELECT COUNT(*) AS n FROM overtime_requests WHERE status='pending'"
        ).fetchone()['n']
        rows_7d = conn.execute(
            """SELECT (punched_at AT TIME ZONE 'Asia/Taipei')::date AS day, COUNT(DISTINCT staff_id) AS cnt
               FROM punch_records
               WHERE (punched_at AT TIME ZONE 'Asia/Taipei')::date >= ((NOW() AT TIME ZONE 'Asia/Taipei')::date - INTERVAL '6 days')
               GROUP BY day ORDER BY day""",
        ).fetchall()
    attendance_trend = [{'date': str(r['day']), 'count': r['cnt']} for r in rows_7d]
    return jsonify({
        'total_staff': total_staff, 'punched_today': punched_today,
        'pending_leaves': pending_leaves, 'pending_ot': pending_ot,
        'attendance_trend': attendance_trend,
    })


@bp.route('/api/mobile/admin/attendance/today', methods=['GET'])
@mobile_admin_required
def mobile_admin_attendance_today():
    today = _dt.now(TW_TZ).date().isoformat()
    with get_db() as conn:
        staff_all = conn.execute(
            "SELECT id, name, department, position_title FROM punch_staff WHERE active=TRUE ORDER BY name"
        ).fetchall()
        records = conn.execute(
            """SELECT staff_id, punch_type, punched_at
               FROM punch_records WHERE (punched_at AT TIME ZONE 'Asia/Taipei')::date=%s::date ORDER BY punched_at""",
            (today,)
        ).fetchall()
    by_staff = defaultdict(list)
    for r in records:
        by_staff[r['staff_id']].append(r)

    result = []
    for s in staff_all:
        recs = by_staff[s['id']]
        ins  = [r for r in recs if r['punch_type'] == 'in']
        outs = [r for r in recs if r['punch_type'] == 'out']
        clock_in  = ins[0]['punched_at'].strftime('%H:%M')   if ins  else None
        clock_out = outs[-1]['punched_at'].strftime('%H:%M') if outs else None
        result.append({
            'id': s['id'], 'name': s['name'],
            'department': s['department'], 'position': s['position_title'],
            'clock_in': clock_in, 'clock_out': clock_out,
            'status': 'present' if clock_in else 'absent',
        })
    return jsonify(result)


@bp.route('/api/mobile/admin/leaves', methods=['GET'])
@mobile_admin_required
def mobile_admin_leaves():
    status = request.args.get('status', 'pending')
    with get_db() as conn:
        rows = conn.execute(
            """SELECT lr.id, ps.name AS staff_name, lt.name AS leave_type,
                      lr.start_date, lr.end_date, lr.total_days AS days,
                      lr.reason, lr.status, lr.created_at
               FROM leave_requests lr
               JOIN punch_staff ps ON lr.staff_id = ps.id
               JOIN leave_types lt ON lr.leave_type_id = lt.id
               WHERE (%s = '' OR lr.status = %s)
               ORDER BY lr.created_at DESC LIMIT 50""",
            (status, status)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        for k in ('start_date', 'end_date', 'created_at'):
            if d.get(k): d[k] = str(d[k])
        data.append(d)
    return jsonify(data)


def _mobile_has_module(*mods):
    u = getattr(g, 'mobile_user', None) or {}
    if u.get('is_super'):
        return True
    perms = u.get('permissions') or []
    return any(m in perms for m in mods)


@bp.route('/api/mobile/admin/leaves/<int:lid>', methods=['PUT'])
@mobile_admin_required
def mobile_admin_leave_action(lid):
    # 與網頁版 api_leave_request_review 同步：需扣/回補假期餘額並重算薪資草稿
    from blueprints.leave import _update_leave_balance
    if not _mobile_has_module('leave'):
        return jsonify({'error': '無「請假管理」模組權限'}), 403
    b = request.get_json(force=True) or {}
    action = b.get('action')
    if action not in ('approve', 'reject'):
        return jsonify({'error': '無效操作'}), 400
    status   = 'approved' if action == 'approve' else 'rejected'
    reviewer = g.mobile_user.get('display_name', g.mobile_user.get('username', ''))
    with get_db() as conn:
        old = conn.execute("SELECT * FROM leave_requests WHERE id=%s", (lid,)).fetchone()
        if not old:
            return jsonify({'error': '找不到假單'}), 404
        old_status = old['status']
        lt = conn.execute("SELECT * FROM leave_types WHERE id=%s", (old['leave_type_id'],)).fetchone()
        delta = float(old['total_days'])
        if action == 'approve':
            if lt and lt['require_cert'] and not old.get('document_id'):
                return jsonify({'error': '此假別需要上傳病單/證明才能核准'}), 422
            if old_status != 'approved' and lt and lt['max_days'] is not None:
                year = int(str(old['start_date'])[:4])
                conn.execute("""
                    INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
                    VALUES (%s, %s, %s, 0, 0) ON CONFLICT (staff_id, leave_type_id, year) DO NOTHING
                """, (old['staff_id'], old['leave_type_id'], year))
                bal = conn.execute("""
                    SELECT COALESCE(used_days, 0) as used
                    FROM leave_balances WHERE staff_id=%s AND leave_type_id=%s AND year=%s
                    FOR UPDATE
                """, (old['staff_id'], old['leave_type_id'], year)).fetchone()
                used = float(bal['used']) if bal else 0.0
                if used + delta > float(lt['max_days']):
                    remaining = float(lt['max_days']) - used
                    return jsonify({'error': f'{lt["name"]}餘額不足（剩 {remaining} 天），無法核准'}), 422
        conn.execute(
            "UPDATE leave_requests SET status=%s, reviewed_by=%s, reviewed_at=NOW(), updated_at=NOW() WHERE id=%s",
            (status, reviewer, lid)
        )
        if action == 'approve' and old_status != 'approved':
            _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                  str(old['start_date'])[:4], delta)
        elif action == 'reject' and old_status == 'approved':
            _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                  str(old['start_date'])[:4], -delta)
        if old_status != status:
            for _m in {str(old['start_date'])[:7], str(old['end_date'])[:7]}:
                conn.execute(
                    "DELETE FROM salary_records WHERE staff_id=%s AND month=%s AND status='draft'",
                    (old['staff_id'], _m))
    return jsonify({'ok': True})


@bp.route('/api/mobile/admin/overtime', methods=['GET'])
@mobile_admin_required
def mobile_admin_overtime():
    status = request.args.get('status', 'pending')
    with get_db() as conn:
        rows = conn.execute(
            """SELECT ot.id, ps.name AS staff_name, ot.request_date AS ot_date, ot.ot_hours,
                      ot.reason, ot.status, ot.created_at
               FROM overtime_requests ot
               JOIN punch_staff ps ON ot.staff_id = ps.id
               WHERE (%s = '' OR ot.status = %s)
               ORDER BY ot.created_at DESC LIMIT 50""",
            (status, status)
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        for k in ('ot_date', 'created_at'):
            if d.get(k): d[k] = str(d[k])
        if d.get('ot_hours'): d['ot_hours'] = float(d['ot_hours'])
        data.append(d)
    return jsonify(data)


@bp.route('/api/mobile/admin/overtime/<int:oid>', methods=['PUT'])
@mobile_admin_required
def mobile_admin_overtime_action(oid):
    # 與網頁版 api_ot_review 同步：核准需計算加班費並重算薪資草稿
    from blueprints.overtime import _calc_ot_pay
    if not _mobile_has_module('punch'):
        return jsonify({'error': '無「打卡管理」模組權限'}), 403
    b = request.get_json(force=True) or {}
    action = b.get('action')
    if action not in ('approve', 'reject'):
        return jsonify({'error': '無效操作'}), 400
    status   = 'approved' if action == 'approve' else 'rejected'
    reviewer = g.mobile_user.get('display_name', g.mobile_user.get('username', ''))
    with get_db() as conn:
        req = conn.execute("SELECT * FROM overtime_requests WHERE id=%s", (oid,)).fetchone()
        if not req:
            return jsonify({'error': '找不到加班申請'}), 404
        ot_pay_final = 0.0
        if action == 'approve':
            staff = conn.execute("""
                SELECT base_salary, hourly_rate, daily_hours,
                       ot_rate1, ot_rate2, ot_rate3, salary_type
                FROM punch_staff WHERE id=%s
            """, (req['staff_id'],)).fetchone()
            if staff:
                dtype = req.get('day_type', 'weekday') or 'weekday'
                ot_pay_final, _ = _calc_ot_pay(staff, req['ot_hours'] or 0, dtype)
        conn.execute(
            "UPDATE overtime_requests SET status=%s, reviewed_by=%s, ot_pay=%s, reviewed_at=NOW() WHERE id=%s",
            (status, reviewer, ot_pay_final, oid)
        )
        conn.execute(
            "DELETE FROM salary_records WHERE staff_id=%s AND month=%s AND status='draft'",
            (req['staff_id'], str(req['request_date'])[:7]))
    return jsonify({'ok': True})


@bp.route('/api/mobile/admin/staff', methods=['GET'])
@mobile_admin_required
def mobile_admin_staff():
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, name, username, department, position_title, employee_code,
                      role, active, hire_date
               FROM punch_staff ORDER BY active DESC, name"""
        ).fetchall()
    data = []
    for r in rows:
        d = dict(r)
        if d.get('hire_date'): d['hire_date'] = str(d['hire_date'])
        data.append(d)
    return jsonify(data)


@bp.route('/api/mobile/admin/anomalies', methods=['GET'])
@mobile_admin_required
def mobile_admin_anomalies():
    import calendar
    month = request.args.get('month', _dt.now(TW_TZ).strftime('%Y-%m'))
    try:
        y, m = map(int, month.split('-'))
    except Exception:
        return jsonify({'error': '月份格式錯誤'}), 400
    with get_db() as conn:
        staff_all = conn.execute(
            "SELECT id, name, department, hire_date FROM punch_staff WHERE active=TRUE ORDER BY name"
        ).fetchall()
        records = conn.execute(
            """SELECT staff_id, punch_type, (punched_at AT TIME ZONE 'Asia/Taipei')::date AS day
               FROM punch_records
               WHERE date_trunc('month', punched_at AT TIME ZONE 'Asia/Taipei') = %s::date""",
            (f'{y}-{m:02d}-01',)
        ).fetchall()
        shift_counts = conn.execute(
            """SELECT staff_id, COUNT(*) AS n FROM shift_assignments
               WHERE TO_CHAR(shift_date,'YYYY-MM')=%s AND shift_date <= %s
               GROUP BY staff_id""",
            (month, _dt.now(TW_TZ).date())
        ).fetchall()
        holiday_rows = conn.execute(
            "SELECT date FROM public_holidays WHERE TO_CHAR(date,'YYYY-MM')=%s"
        , (month,)).fetchall()
        leave_rows = conn.execute(
            """SELECT staff_id, start_date, end_date FROM leave_requests
               WHERE status='approved' AND start_date <= %s AND end_date >= %s""",
            (f'{y}-{m:02d}-{calendar.monthrange(y, m)[1]}', f'{y}-{m:02d}-01')
        ).fetchall()
    by_staff = defaultdict(set)
    for r in records:
        by_staff[r['staff_id']].add(str(r['day']))
    expected_by_staff = {r['staff_id']: r['n'] for r in shift_counts}

    # 無排班者（固定工時公司）：預期工作天 = 本月至今的平日扣除國定假日
    holiday_dates = {str(r['date']) for r in holiday_rows}
    today = _dt.now(TW_TZ).date()
    last = min(today, date(y, m, calendar.monthrange(y, m)[1]))
    def _default_expected(hire_date=None):
        n = 0
        if last < date(y, m, 1):
            return 0
        for dnum in range(1, last.day + 1):
            d = date(y, m, dnum)
            if hire_date and d < hire_date:      # 到職日前不列入預期工作天
                continue
            if d.weekday() < 5 and d.isoformat() not in holiday_dates:
                n += 1
        return n

    # 已核准請假的日子不算缺勤
    leave_by_staff = defaultdict(set)
    for lr in leave_rows:
        cur = lr['start_date']
        while cur <= lr['end_date']:
            if cur.year == y and cur.month == m:
                leave_by_staff[lr['staff_id']].add(cur.isoformat())
            cur += timedelta(days=1)

    result = []
    for s in staff_all:
        hd = s.get('hire_date')
        work_days = len(by_staff[s['id']])
        expected  = expected_by_staff.get(s['id'], _default_expected(hd))
        leave_days = len(leave_by_staff.get(s['id'], ()))
        result.append({
            'id': s['id'], 'name': s['name'], 'department': s['department'],
            'work_days': work_days, 'missing_days': max(0, expected - work_days - leave_days),
        })
    return jsonify(result)
