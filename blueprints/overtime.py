"""
blueprints/overtime.py — 加班申請（員工端 & 管理端）、加班費計算
"""
from datetime import datetime as _dt

from flask import Blueprint, session, request, jsonify

from auth import login_required, require_module, require_any_module
from config import TW_TZ
from db import get_db
from blueprints.punch import ot_req_row
from blueprints.notifications import _notify_review_result

bp = Blueprint('overtime', __name__)


# ─── 加班費計算 helper ────────────────────────────────────────────────────────

def _calc_ot_pay(staff_row, ot_hours, day_type='weekday'):
    salary_type = staff_row.get('salary_type', 'monthly') or 'monthly'
    base_salary = float(staff_row.get('base_salary')  or 0)
    hourly_rate = float(staff_row.get('hourly_rate')  or 0)
    daily_hours = float(staff_row.get('daily_hours')  or 8)
    ot_rate1    = float(staff_row.get('ot_rate1')     or 1.33)
    ot_rate2    = float(staff_row.get('ot_rate2')     or 1.67)
    ot_rate3    = float(staff_row.get('ot_rate3')     or 2.0)

    if salary_type == 'hourly':
        base_hourly = hourly_rate
    else:
        base_hourly = base_salary / 30 / daily_hours if (base_salary and daily_hours) else 0

    if base_hourly <= 0:
        return 0.0, base_hourly

    h = float(ot_hours)
    if day_type in ('holiday', 'special'):
        pay = round(base_hourly * h * 2.0, 0)
    elif day_type == 'rest_day':
        # 休息日：2018/3/1 起核實計算（不再「做4給4」）；
        # 前 2 小時 1⅓，第 3 小時起 1⅔
        h1 = min(h, 2.0); h2 = max(0.0, h - 2.0)
        pay = round(base_hourly * (h1 * ot_rate1 + h2 * ot_rate2), 0)
    else:
        h1 = min(h, 2.0); h2 = min(max(0.0, h - 2.0), 2.0); h3 = max(0.0, h - 4.0)
        pay = round(base_hourly * (h1 * ot_rate1 + h2 * ot_rate2 + h3 * ot_rate3), 0)

    return pay, base_hourly


# ─── Employee endpoints ──────────────────────────────────────────────────────

@bp.route('/api/overtime/my-requests', methods=['GET'])
def api_ot_my_list():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM overtime_requests WHERE staff_id=%s ORDER BY request_date DESC LIMIT 30",
            (sid,)
        ).fetchall()
    return jsonify([ot_req_row(r) for r in rows])


@bp.route('/api/overtime/my-requests', methods=['POST'])
def api_ot_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b            = request.get_json(force=True)
    request_date = b.get('request_date', '').strip()
    start_time   = b.get('start_time', '').strip()
    end_time     = b.get('end_time', '').strip()
    reason       = b.get('reason', '').strip()
    if not request_date or not start_time or not end_time:
        return jsonify({'error': '請填寫加班日期及時間'}), 400
    # 日期類型由後端依日期判定，不信任前端值（避免平日被改成假日拿雙倍費率）
    from datetime import date as _dot
    try:
        _req_d = _dot.fromisoformat(request_date)
    except ValueError:
        return jsonify({'error': '日期格式錯誤'}), 400
    with get_db() as conn:
        # 無薪假日不算假日費率（加班照平日/休息日規則計）
        _is_holiday = conn.execute(
            "SELECT 1 FROM public_holidays WHERE date=%s AND holiday_type <> 'unpaid'",
            (request_date,)
        ).fetchone()
    if _is_holiday:
        day_type = 'holiday'
    elif _req_d.weekday() == 6:
        day_type = 'special'      # 週日＝例假日
    elif _req_d.weekday() == 5:
        day_type = 'rest_day'     # 週六＝休息日
    else:
        day_type = 'weekday'
    if not reason:
        return jsonify({'error': '請填寫加班原因'}), 400
    from datetime import datetime as _dtot, timedelta as _tdot
    try:
        s = _dtot.strptime(start_time, '%H:%M')
        e = _dtot.strptime(end_time,   '%H:%M')
        if e <= s: e += _tdot(days=1)
        ot_hours = round((e - s).total_seconds() / 3600, 2)
    except ValueError:
        return jsonify({'error': '時間格式錯誤'}), 400
    if ot_hours <= 0 or ot_hours > 12:
        return jsonify({'error': '加班時數不合理（0~12小時）'}), 400
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO overtime_requests
              (staff_id, request_date, start_time, end_time, ot_hours, reason, day_type)
            VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *
        """, (sid, request_date, start_time, end_time, ot_hours, reason, day_type)).fetchone()
    return jsonify(ot_req_row(row)), 201


# ─── Admin endpoints ─────────────────────────────────────────────────────────

@bp.route('/api/overtime/requests', methods=['GET'])
@require_any_module('punch', 'salary')
def api_ot_admin_list():
    status = request.args.get('status', '')
    month  = request.args.get('month', '')
    conds, params = ['TRUE'], []
    if status: conds.append('r.status=%s');                          params.append(status)
    if month:  conds.append("to_char(r.request_date,'YYYY-MM')=%s"); params.append(month)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT r.*, ps.name as staff_name, ps.role as staff_role
            FROM overtime_requests r
            JOIN punch_staff ps ON ps.id=r.staff_id
            WHERE {' AND '.join(conds)}
            ORDER BY r.request_date DESC, r.created_at DESC
        """, params).fetchall()
    return jsonify([
        ot_req_row(r) | {'staff_name': r['staff_name'], 'staff_role': r['staff_role']}
        for r in rows
    ])


@bp.route('/api/overtime/requests/<int:rid>', methods=['PUT'])
@require_module('punch')
def api_ot_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')
    reviewed_by = b.get('reviewed_by', '').strip()
    review_note = b.get('review_note', '').strip()
    if action not in ('approve', 'reject'):
        return jsonify({'error': 'invalid action'}), 400
    new_status   = 'approved' if action == 'approve' else 'rejected'
    ot_pay_final = 0.0

    with get_db() as conn:
        req = conn.execute(
            "SELECT * FROM overtime_requests WHERE id=%s", (rid,)
        ).fetchone()
        if not req: return ('', 404)

        if action == 'approve':
            staff = conn.execute("""
                SELECT base_salary, hourly_rate, daily_hours,
                       ot_rate1, ot_rate2, ot_rate3, salary_type
                FROM punch_staff WHERE id=%s
            """, (req['staff_id'],)).fetchone()
            if staff:
                dtype = req.get('day_type', 'weekday') or 'weekday'
                ot_pay_final, _ = _calc_ot_pay(staff, req['ot_hours'] or 0, dtype)

        row = conn.execute("""
            UPDATE overtime_requests
            SET status=%s, reviewed_by=%s, review_note=%s,
                ot_pay=%s, reviewed_at=NOW()
            WHERE id=%s RETURNING *
        """, (new_status, reviewed_by, review_note, ot_pay_final, rid)).fetchone()

        if row:
            ot_month = str(req['request_date'])[:7]
            conn.execute("""
                DELETE FROM salary_records
                WHERE staff_id=%s AND month=%s AND status='draft'
            """, (req['staff_id'], ot_month))

        sn = conn.execute(
            "SELECT name FROM punch_staff WHERE id=%s", (req['staff_id'],)
        ).fetchone()

    result = ot_req_row(row)
    result['staff_name'] = sn['name'] if sn else ''
    time_str = (f"{row['start_time']}～{row['end_time']}" if row.get('start_time') and row.get('end_time')
                else f"{float(row['ot_hours'])} 小時")
    extra = f"{row['request_date']} {time_str}"
    if action == 'approve' and float(row.get('ot_pay') or 0) > 0:
        extra += f"\n加班費：${float(row['ot_pay']):,.0f}"
    if review_note: extra += f"\n審核意見：{review_note}"
    _notify_review_result(req['staff_id'], '加班申請', action, extra)
    return jsonify(result)


@bp.route('/api/overtime/requests/<int:rid>', methods=['DELETE'])
@require_module('punch')
def api_ot_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM overtime_requests WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})


@bp.route('/api/overtime/monthly-summary', methods=['GET'])
@require_any_module('punch', 'salary')
def api_ot_monthly_summary():
    month = request.args.get('month', '') or _dt.now(TW_TZ).strftime('%Y-%m')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                ps.id   AS staff_id,
                ps.name AS staff_name,
                ps.role AS staff_role,
                COUNT(*)                                                      AS request_count,
                SUM(r.ot_hours)                                               AS total_hours,
                SUM(CASE WHEN r.status='approved' THEN r.ot_hours ELSE 0 END) AS approved_hours,
                SUM(CASE WHEN r.status='pending'  THEN r.ot_hours ELSE 0 END) AS pending_hours,
                SUM(CASE WHEN r.status='rejected' THEN r.ot_hours ELSE 0 END) AS rejected_hours,
                COUNT(CASE WHEN r.status='approved' THEN 1 END)               AS approved_count,
                COUNT(CASE WHEN r.status='pending'  THEN 1 END)               AS pending_count,
                COUNT(CASE WHEN r.status='rejected' THEN 1 END)               AS rejected_count
            FROM overtime_requests r
            JOIN punch_staff ps ON ps.id = r.staff_id
            WHERE to_char(r.request_date, 'YYYY-MM') = %s
            GROUP BY ps.id, ps.name, ps.role
            ORDER BY total_hours DESC
        """, (month,)).fetchall()
    return jsonify([{
        'staff_id':       r['staff_id'],
        'staff_name':     r['staff_name'],
        'staff_role':     r['staff_role'] or '',
        'request_count':  r['request_count'],
        'total_hours':    float(r['total_hours']    or 0),
        'approved_hours': float(r['approved_hours'] or 0),
        'pending_hours':  float(r['pending_hours']  or 0),
        'rejected_hours': float(r['rejected_hours'] or 0),
        'approved_count': r['approved_count'],
        'pending_count':  r['pending_count'],
        'rejected_count': r['rejected_count'],
    } for r in rows])


@bp.route('/api/overtime/calc-preview', methods=['POST'])
@require_any_module('punch', 'salary')
def api_ot_calc_preview():
    b        = request.get_json(force=True)
    staff_id = b.get('staff_id')
    ot_hours = float(b.get('ot_hours') or 0)
    if not staff_id: return jsonify({'error': 'staff_id required'}), 400
    with get_db() as conn:
        staff = conn.execute("""
            SELECT name, base_salary, hourly_rate, daily_hours,
                   ot_rate1, ot_rate2, ot_rate3, salary_type
            FROM punch_staff WHERE id=%s
        """, (staff_id,)).fetchone()
    if not staff: return ('', 404)
    day_type        = b.get('day_type', 'weekday') or 'weekday'
    ot_pay, base_hourly = _calc_ot_pay(staff, ot_hours, day_type)

    if day_type == 'rest_day':
        billed = max(ot_hours, 4.0)
        h1 = min(billed, 2.0); h2 = min(max(0.0, billed - 2.0), 2.0); h3 = max(0.0, billed - 4.0)
    elif day_type in ('holiday', 'special'):
        h1 = ot_hours; h2 = 0.0; h3 = 0.0
    else:
        h1 = min(ot_hours, 2.0); h2 = min(max(0.0, ot_hours - 2.0), 2.0); h3 = max(0.0, ot_hours - 4.0)

    return jsonify({
        'staff_name':  staff['name'],
        'salary_type': staff.get('salary_type', 'monthly'),
        'base_salary': float(staff.get('base_salary') or 0),
        'hourly_rate': float(staff.get('hourly_rate') or 0),
        'base_hourly': round(base_hourly, 2),
        'ot_hours':    ot_hours,
        'day_type':    day_type,
        'h1': h1, 'h2': h2, 'h3': h3,
        'ot_rate1': float(staff.get('ot_rate1') or 1.33),
        'ot_rate2': float(staff.get('ot_rate2') or 1.67),
        'ot_rate3': float(staff.get('ot_rate3') or 2.0),
        'ot_pay':   ot_pay,
    })


# ─── Batch overtime review ───────────────────────────────────────────────────

@bp.route('/api/overtime/requests/batch', methods=['POST'])
@require_module('punch')
def api_overtime_requests_batch():
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
            req = conn.execute("SELECT * FROM overtime_requests WHERE id=%s", (rid,)).fetchone()
            if not req: continue
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
            row = conn.execute("""
                UPDATE overtime_requests
                SET status=%s, reviewed_by=%s, review_note=%s,
                    ot_pay=%s, reviewed_at=NOW()
                WHERE id=%s RETURNING *
            """, (new_status, reviewed_by, review_note, ot_pay_final, rid)).fetchone()
            if row:
                ot_month = str(req['request_date'])[:7]
                conn.execute("""
                    DELETE FROM salary_records
                    WHERE staff_id=%s AND month=%s AND status='draft'
                """, (req['staff_id'], ot_month))
                results.append(ot_req_row(row))
    return jsonify({'updated': len(results), 'rows': results})
