"""
blueprints/leave.py — 假別管理、請假申請、假期餘額、特休計算
"""
from datetime import datetime as _dt

from flask import Blueprint, session, request, jsonify

from auth import require_module
from config import TW_TZ
from db import get_db
from blueprints.notifications import _notify_review_result

bp = Blueprint('leave', __name__)


# ─── DB init ─────────────────────────────────────────────────────────────────

def init_leave_db():
    migrations = [
        """CREATE TABLE IF NOT EXISTS leave_types (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            code        TEXT NOT NULL UNIQUE,
            pay_rate    NUMERIC(4,2) DEFAULT 1.0,
            max_days    NUMERIC(5,1),
            description TEXT DEFAULT '',
            color       TEXT DEFAULT '#4a7bda',
            active      BOOLEAN DEFAULT TRUE,
            sort_order  INT DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS leave_requests (
            id              SERIAL PRIMARY KEY,
            staff_id        INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            leave_type_id   INT REFERENCES leave_types(id),
            start_date      DATE NOT NULL,
            end_date        DATE NOT NULL,
            start_half      BOOLEAN DEFAULT FALSE,
            end_half        BOOLEAN DEFAULT FALSE,
            total_days      NUMERIC(7,4) NOT NULL DEFAULT 0,
            reason          TEXT DEFAULT '',
            status          TEXT DEFAULT 'pending',
            reviewed_by     TEXT DEFAULT '',
            review_note     TEXT DEFAULT '',
            reviewed_at     TIMESTAMPTZ,
            substitute_name TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS leave_balances (
            id          SERIAL PRIMARY KEY,
            staff_id    INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            leave_type_id INT REFERENCES leave_types(id),
            year        INT NOT NULL,
            total_days  NUMERIC(7,4) DEFAULT 0,
            used_days   NUMERIC(7,4) DEFAULT 0,
            note        TEXT DEFAULT '',
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(staff_id, leave_type_id, year)
        )""",
        "ALTER TABLE leave_types ADD COLUMN IF NOT EXISTS allow_hourly BOOLEAN DEFAULT FALSE",
        "ALTER TABLE leave_types ADD COLUMN IF NOT EXISTS require_cert BOOLEAN DEFAULT FALSE",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS total_hours NUMERIC(5,1)",
        # 時數假換算天數需要 4 位小數（0.5h = 0.0625 天），NUMERIC(5,1) 會把它進位成 0.1 天
        "ALTER TABLE leave_requests ALTER COLUMN total_days TYPE NUMERIC(7,4)",
        "ALTER TABLE leave_balances ALTER COLUMN total_days TYPE NUMERIC(7,4)",
        "ALTER TABLE leave_balances ALTER COLUMN used_days TYPE NUMERIC(7,4)",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS leave_start_time TEXT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS leave_end_time TEXT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS document_id INT",
        "ALTER TABLE leave_requests ADD COLUMN IF NOT EXISTS force_reviewed BOOLEAN DEFAULT FALSE",
    ]
    for sql in migrations:
        try:
            with get_db() as conn:
                conn.execute(sql)
        except Exception as e:
            print(f"[leave_init] {str(e)[:80]}")

    defaults = [
        ('特休假',   'annual',       1.0,  30,  '#2e9e6b', 1,  False),
        ('病假',     'sick',         0.5,  30,  '#e07b2a', 2,  True),
        ('住院病假', 'hospitalize',  1.0,  30,  '#d64242', 3,  False),
        ('事假',     'personal',     0.0,  14,  '#8892a4', 4,  True),
        ('生理假',   'menstrual',    0.5,  12,  '#c45cb8', 5,  False),
        ('婚假',     'marriage',     1.0,   8,  '#c8a96e', 6,  False),
        ('喪假',     'funeral',      1.0,   8,  '#4a7bda', 7,  False),
        ('產假',     'maternity',    1.0,  56,  '#e05c8a', 8,  False),
        ('陪產假',   'paternity',    1.0,   7,  '#5cb8c4', 9,  False),
        ('公假',     'official',     1.0, None, '#243d6e', 10, False),
        ('補休',     'compensatory', 1.0, None, '#8b5cf6', 11, False),
    ]
    try:
        with get_db() as conn:
            cnt = conn.execute("SELECT COUNT(*) as c FROM leave_types").fetchone()['c']
            if cnt == 0:
                for name, code, pay, maxd, color, sort, allow_hourly in defaults:
                    conn.execute(
                        "INSERT INTO leave_types (name,code,pay_rate,max_days,color,sort_order,allow_hourly) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (name, code, pay, maxd, color, sort, allow_hourly)
                    )
    except Exception as e:
        print(f"[leave_seed] {e}")


init_leave_db()


# ─── Row helpers ─────────────────────────────────────────────────────────────

def add_month_range_conds(conds, params, args):
    """from_month / to_month（YYYY-MM，含頭含尾）→ 以請假開始日篩選"""
    import re as _re
    f = args.get('from_month', ''); t = args.get('to_month', '')
    if _re.fullmatch(r'\d{4}-\d{2}', f or ''):
        conds.append("lr.start_date >= %s::date"); params.append(f + '-01')
    if _re.fullmatch(r'\d{4}-\d{2}', t or ''):
        conds.append("lr.start_date < (%s::date + INTERVAL '1 month')"); params.append(t + '-01')


def leave_type_row(row):
    if not row: return None
    d = dict(row)
    if d.get('max_days') is not None: d['max_days'] = float(d['max_days'])
    if d.get('pay_rate') is not None: d['pay_rate'] = float(d['pay_rate'])
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    d['require_cert'] = bool(d.get('require_cert', False))
    return d


def leave_req_row(row):
    if not row: return None
    d = dict(row)
    if d.get('start_date'): d['start_date'] = d['start_date'].isoformat()
    if d.get('end_date'):   d['end_date']   = d['end_date'].isoformat()
    if d.get('total_days'): d['total_days'] = float(d['total_days'])
    if d.get('total_hours') is not None: d['total_hours'] = float(d['total_hours'])
    if d.get('reviewed_at'): d['reviewed_at'] = d['reviewed_at'].isoformat()
    if d.get('created_at'):  d['created_at']  = d['created_at'].isoformat()
    if d.get('updated_at'):  d['updated_at']  = d['updated_at'].isoformat()
    return d


def leave_balance_row(row):
    if not row: return None
    d = dict(row)
    if d.get('total_days') is not None: d['total_days'] = float(d['total_days'])
    if d.get('used_days')  is not None: d['used_days']  = float(d['used_days'])
    if d.get('updated_at'): d['updated_at'] = d['updated_at'].isoformat()
    return d


# ─── 特休天數計算（勞基法第38條） ────────────────────────────────────────────

def _calc_annual_leave_days(hire_date_str, ref_date_str=None):
    if not hire_date_str:
        return 0
    from datetime import date as _date
    try:
        hire = _date.fromisoformat(str(hire_date_str))
    except Exception:
        return 0

    ref = _dt.now(TW_TZ).date()
    if ref_date_str:
        try:
            ref = _date.fromisoformat(str(ref_date_str))
        except Exception:
            pass

    months = (ref.year - hire.year) * 12 + (ref.month - hire.month)
    if ref.day < hire.day:
        months -= 1
    if months < 0:
        months = 0

    years_complete = months // 12

    if months < 6:    return 0
    elif months < 12: return 3
    elif years_complete < 2:  return 7
    elif years_complete < 3:  return 10
    elif years_complete < 5:  return 14
    elif years_complete < 10: return 15
    else:
        extra = years_complete - 9
        return min(15 + extra, 30)


def _calc_annual_leave_schedule(hire_date_str):
    if not hire_date_str:
        return []
    from datetime import date as _date
    import calendar as _cal

    try:
        hire = _date.fromisoformat(str(hire_date_str))
    except Exception:
        return []

    today = _dt.now(TW_TZ).date()

    milestones = [
        (6,   3,  '滿6個月'),   (12,  7,  '滿1年'),   (24, 10,  '滿2年'),
        (36, 14,  '滿3年'),     (60, 15,  '滿5年'),   (120,16,  '滿10年'),
        (132,17,  '滿11年'),    (144,18,  '滿12年'),  (156,19,  '滿13年'),
        (168,20,  '滿14年'),    (180,21,  '滿15年'),  (192,22,  '滿16年'),
        (204,23,  '滿17年'),    (216,24,  '滿18年'),  (228,25,  '滿19年'),
        (240,30,  '滿20年（上限30天）'),
    ]

    result = []
    current_days = _calc_annual_leave_days(hire_date_str)

    for months_needed, days, label in milestones:
        total_m = hire.month + months_needed
        y = hire.year + (total_m - 1) // 12
        m = (total_m - 1) % 12 + 1
        max_day = _cal.monthrange(y, m)[1]
        try:
            reached = _date(y, m, min(hire.day, max_day))
        except Exception:
            continue
        result.append({
            'label':        label,
            'days':         days,
            'date_reached': reached.isoformat(),
            'is_past':      reached <= today,
            'is_current':   (days == current_days and reached <= today),
        })
    return result


def _calc_leave_days(start_date_str, end_date_str, start_half=False, end_half=False):
    """只計工作日（週一～五、排除國定假日），與薪資計算的工作日定義一致"""
    from datetime import date as _date, timedelta as _td
    try:
        s = _date.fromisoformat(start_date_str)
        e = _date.fromisoformat(end_date_str)
    except Exception:
        return 0.0
    if e < s: return 0.0
    holidays = set()
    try:
        with get_db() as conn:
            holidays = {str(r['date']) for r in conn.execute(
                "SELECT date FROM public_holidays WHERE date BETWEEN %s AND %s", (s, e)
            ).fetchall()}
    except Exception:
        pass
    days = 0.0
    cur  = s
    while cur <= e:
        if cur.weekday() < 5 and cur.isoformat() not in holidays:
            if cur == s and start_half and cur == e and end_half:
                days += 1.0
            elif cur == s and start_half:
                days += 0.5
            elif cur == e and end_half:
                days += 0.5
            else:
                days += 1.0
        cur += _td(days=1)
    return days


def _update_leave_balance(conn, staff_id, leave_type_id, year_str, delta_days):
    year = int(year_str)
    conn.execute("""
        INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
        VALUES (%s, %s, %s, 0, %s)
        ON CONFLICT (staff_id, leave_type_id, year) DO UPDATE
          SET used_days = leave_balances.used_days + EXCLUDED.used_days,
              updated_at = NOW()
    """, (staff_id, leave_type_id, year, delta_days))


# ─── Leave Type CRUD ─────────────────────────────────────────────────────────

@bp.route('/api/leave/types', methods=['GET'])
@require_module('leave')
def api_leave_types_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM leave_types ORDER BY sort_order, id").fetchall()
    return jsonify([leave_type_row(r) for r in rows])


@bp.route('/api/leave/types/public', methods=['GET'])
def api_leave_types_public():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM leave_types WHERE active=TRUE ORDER BY sort_order, id").fetchall()
    return jsonify([leave_type_row(r) for r in rows])


@bp.route('/api/leave/types', methods=['POST'])
@require_module('leave')
def api_leave_type_create():
    b = request.get_json(force=True) or {}
    if not str(b.get('name','')).strip() or not str(b.get('code','')).strip():
        return jsonify({'error': '請填寫假別名稱與代碼'}), 400
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO leave_types (name,code,pay_rate,max_days,description,color,sort_order,require_cert)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
        """, (b['name'], b['code'], float(b.get('pay_rate', 1.0)),
              b.get('max_days') or None, b.get('description', ''),
              b.get('color', '#4a7bda'), int(b.get('sort_order', 0)),
              bool(b.get('require_cert', False)))).fetchone()
    return jsonify(leave_type_row(row)), 201


@bp.route('/api/leave/types/<int:tid>', methods=['PUT'])
@require_module('leave')
def api_leave_type_update(tid):
    b = request.get_json(force=True) or {}
    if not str(b.get('name','')).strip() or not str(b.get('code','')).strip():
        return jsonify({'error': '請填寫假別名稱與代碼'}), 400
    with get_db() as conn:
        row = conn.execute("""
            UPDATE leave_types SET name=%s,code=%s,pay_rate=%s,max_days=%s,
              description=%s,color=%s,sort_order=%s,active=%s,require_cert=%s
            WHERE id=%s RETURNING *
        """, (b['name'], b['code'], float(b.get('pay_rate', 1.0)),
              b.get('max_days') or None, b.get('description', ''),
              b.get('color', '#4a7bda'), int(b.get('sort_order', 0)),
              bool(b.get('active', True)), bool(b.get('require_cert', False)), tid)).fetchone()
    return jsonify(leave_type_row(row)) if row else ('', 404)


@bp.route('/api/leave/types/<int:tid>', methods=['DELETE'])
@require_module('leave')
def api_leave_type_delete(tid):
    with get_db() as conn:
        conn.execute("DELETE FROM leave_types WHERE id=%s", (tid,))
    return jsonify({'deleted': tid})


# ─── Admin: Leave Requests ───────────────────────────────────────────────────

@bp.route('/api/leave/requests', methods=['GET'])
@require_module('leave')
def api_leave_requests_list():
    status   = request.args.get('status', '')
    month    = request.args.get('month', '')
    staff_id = request.args.get('staff_id', '')
    conds, params = ['TRUE'], []
    if status:   conds.append('lr.status=%s');                        params.append(status)
    if staff_id: conds.append('lr.staff_id=%s');                      params.append(int(staff_id))
    if month:    conds.append("to_char(lr.start_date,'YYYY-MM')=%s"); params.append(month)
    add_month_range_conds(conds, params, request.args)
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT lr.*, ps.name as staff_name, ps.role as staff_role,
                   lt.name as leave_type_name, lt.code as leave_code,
                   lt.pay_rate, lt.color as leave_color, lt.require_cert
            FROM leave_requests lr
            JOIN punch_staff ps ON ps.id=lr.staff_id
            JOIN leave_types  lt ON lt.id=lr.leave_type_id
            WHERE {' AND '.join(conds)}
            ORDER BY lr.start_date DESC, lr.created_at DESC LIMIT 2000
        """, params).fetchall()
    result = []
    for r in rows:
        d = leave_req_row(r)
        d['staff_name']      = r['staff_name']
        d['staff_role']      = r['staff_role']
        d['leave_type_name'] = r['leave_type_name']
        d['leave_code']      = r['leave_code']
        d['pay_rate']        = float(r['pay_rate'])
        d['leave_color']     = r['leave_color']
        d['require_cert']    = bool(r['require_cert'])
        result.append(d)
    return jsonify(result)


@bp.route('/api/leave/requests', methods=['POST'])
@require_module('leave')
def api_leave_request_admin_create():
    b             = request.get_json(force=True)
    sid           = b.get('staff_id')
    leave_type_id = b.get('leave_type_id')
    start_date    = b.get('start_date', '').strip()
    end_date      = b.get('end_date', '').strip()
    start_half    = bool(b.get('start_half', False))
    end_half      = bool(b.get('end_half', False))
    reason        = b.get('reason', '').strip()
    status        = b.get('status', 'approved')
    document_id   = b.get('document_id') or None
    # 後台「編輯」：新單建立成功才刪舊單（同一交易），避免驗證失敗時舊單已被刪掉
    try:    replace_id = int(b.get('replace_id') or 0) or None
    except (TypeError, ValueError): replace_id = None

    if not all([sid, leave_type_id, start_date, end_date]):
        return jsonify({'error': '缺少必要欄位'}), 400

    total_hours_req = b.get('total_hours')
    if total_hours_req is not None:
        try:   total_hours_req = float(total_hours_req)
        except: total_hours_req = None

    if total_hours_req:
        if total_hours_req < 0.5 or total_hours_req > 8:
            return jsonify({'error': '時數需介於 0.5～8 小時'}), 400
        total_days = round(total_hours_req / 8, 4)
        end_date = start_date; start_half = False; end_half = False
    else:
        total_hours_req = None
        total_days = _calc_leave_days(start_date, end_date, start_half, end_half)
        if total_days <= 0:
            return jsonify({'error': '請假天數不合理，請檢查日期'}), 400

    with get_db() as conn:
        lt_row = conn.execute("SELECT require_cert FROM leave_types WHERE id=%s", (leave_type_id,)).fetchone()
        if lt_row and lt_row['require_cert'] and status == 'approved' and not document_id:
            return jsonify({'error': '此假別需要上傳病單/證明才能直接核准'}), 422
        old = None
        if replace_id:
            old = conn.execute("SELECT * FROM leave_requests WHERE id=%s", (replace_id,)).fetchone()
            if not old:
                return jsonify({'error': '原假單已不存在，請重新整理'}), 404
        overlap = conn.execute("""
            SELECT start_date, end_date FROM leave_requests
            WHERE staff_id=%s AND status IN ('pending','approved')
              AND start_date <= %s AND end_date >= %s
              AND (total_hours IS NULL OR %s)
              AND id <> %s
            LIMIT 1
        """, (sid, end_date, start_date, total_hours_req is None, replace_id or 0)).fetchone()
        if overlap:
            return jsonify({'error': f"該期間與現有假單（{overlap['start_date']}～{overlap['end_date']}，待審或已核准）重疊"}), 409
        if old:
            _remove_leave_request(conn, old)
        row = conn.execute("""
            INSERT INTO leave_requests
              (staff_id, leave_type_id, start_date, end_date, start_half, end_half,
               total_days, total_hours, reason, status, reviewed_by, reviewed_at, document_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
              CASE WHEN %s='approved' THEN NOW() ELSE NULL END, %s)
            RETURNING *
        """, (sid, leave_type_id, start_date, end_date, start_half, end_half,
              total_days, total_hours_req, reason, status,
              b.get('reviewed_by', '管理員'), status, document_id)).fetchone()
        if status == 'approved':
            _update_leave_balance(conn, sid, leave_type_id, start_date[:4], total_days)
    return jsonify(leave_req_row(row)), 201


@bp.route('/api/leave/requests/<int:rid>', methods=['PUT'])
@require_module('leave')
def api_leave_request_review(rid):
    b           = request.get_json(force=True)
    action      = b.get('action')
    reviewed_by = b.get('reviewed_by', '').strip()
    review_note = b.get('review_note', '').strip()
    if action not in ('approve', 'reject'):
        return jsonify({'error': 'invalid action'}), 400
    new_status = 'approved' if action == 'approve' else 'rejected'
    with get_db() as conn:
        old = conn.execute("SELECT * FROM leave_requests WHERE id=%s", (rid,)).fetchone()
        if not old: return ('', 404)
        old_status = old['status']
        if action == 'approve':
            lt_chk = conn.execute("SELECT require_cert FROM leave_types WHERE id=%s", (old['leave_type_id'],)).fetchone()
            if lt_chk and lt_chk['require_cert'] and not old.get('document_id'):
                return jsonify({'error': '此假別需要上傳病單/證明才能核准'}), 422
        delta = float(old['total_days'])
        lt = conn.execute("SELECT * FROM leave_types WHERE id=%s", (old['leave_type_id'],)).fetchone()
        # 餘額檢查必須在改狀態之前——422 return 會正常離開 with 區塊而 commit，
        # 若先 UPDATE 再檢查，假單會被標成已核准但沒扣假
        if action == 'approve' and old_status != 'approved':
            if lt and lt['max_days'] is not None:
                year = str(old['start_date'])[:4]
                conn.execute("""
                    INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
                    VALUES (%s, %s, %s, 0, 0) ON CONFLICT (staff_id, leave_type_id, year) DO NOTHING
                """, (old['staff_id'], old['leave_type_id'], int(year)))
                bal = conn.execute("""
                    SELECT COALESCE(used_days, 0) as used, COALESCE(total_days, 0) as total
                    FROM leave_balances WHERE staff_id=%s AND leave_type_id=%s AND year=%s
                    FOR UPDATE
                """, (old['staff_id'], old['leave_type_id'], int(year))).fetchone()
                used = float(bal['used']) if bal else 0.0
                _personal = float(bal['total']) if bal else 0.0
                _cap = _personal if _personal > 0 else float(lt['max_days'])
                if used + delta > _cap:
                    remaining = _cap - used
                    return jsonify({'error': f'{lt["name"]}餘額不足（剩 {remaining} 天），無法核准'}), 422
        row = conn.execute("""
            UPDATE leave_requests
            SET status=%s, reviewed_by=%s, review_note=%s,
                reviewed_at=NOW(), updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (new_status, reviewed_by, review_note, rid)).fetchone()
        if action == 'approve' and old_status != 'approved':
            _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                  str(old['start_date'])[:4], delta)
        elif action == 'reject' and old_status == 'approved':
            _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                  str(old['start_date'])[:4], -delta)
        if old_status != new_status:
            affected_months = {str(old['start_date'])[:7], str(old['end_date'])[:7]}
            for _m in affected_months:
                conn.execute("""
                    DELETE FROM salary_records WHERE staff_id=%s AND month=%s AND status='draft'
                """, (old['staff_id'], _m))
    if row:
        total_hours = old.get('total_hours')
        duration_str = f"{float(total_hours)} 小時" if total_hours else f"{float(old['total_days'])} 天"
        extra = f"{str(old['start_date'])} ~ {str(old['end_date'])} 共 {duration_str}"
        if review_note: extra += f"\n審核意見：{review_note}"
        _notify_review_result(old['staff_id'], '請假申請', action, extra)
    return jsonify(leave_req_row(row)) if row else ('', 404)


@bp.route('/api/leave/requests/<int:rid>', methods=['DELETE'])
@require_module('leave')
def api_leave_request_delete(rid):
    with get_db() as conn:
        old = conn.execute("SELECT * FROM leave_requests WHERE id=%s", (rid,)).fetchone()
        if not old:
            return jsonify({'error': '找不到假單'}), 404
        _remove_leave_request(conn, old)
    return jsonify({'deleted': rid})


def _remove_leave_request(conn, old):
    """刪除假單；已核准者退回餘額並清掉受影響月份的薪資草稿"""
    if old['status'] == 'approved':
        _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                              str(old['start_date'])[:4], -float(old['total_days']))
        affected_months = {str(old['start_date'])[:7], str(old['end_date'])[:7]}
        for _m in affected_months:
            conn.execute("""
                DELETE FROM salary_records WHERE staff_id=%s AND month=%s AND status='draft'
            """, (old['staff_id'], _m))
    conn.execute("DELETE FROM leave_requests WHERE id=%s", (old['id'],))


# ─── Employee: submit leave request ──────────────────────────────────────────

@bp.route('/api/leave/my-requests', methods=['GET'])
def api_leave_my_list():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        rows = conn.execute("""
            SELECT lr.*, lt.name as leave_type_name, lt.code as leave_code,
                   lt.color as leave_color, lt.pay_rate, lt.require_cert
            FROM leave_requests lr
            JOIN leave_types lt ON lt.id=lr.leave_type_id
            WHERE lr.staff_id=%s ORDER BY lr.start_date DESC LIMIT 30
        """, (sid,)).fetchall()
    result = []
    for r in rows:
        d = leave_req_row(r)
        d['leave_type_name'] = r['leave_type_name']
        d['leave_code']      = r['leave_code']
        d['leave_color']     = r['leave_color']
        d['pay_rate']        = float(r['pay_rate'])
        d['require_cert']    = bool(r['require_cert'])
        result.append(d)
    return jsonify(result)


@bp.route('/api/leave/my-requests', methods=['POST'])
def api_leave_submit():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b             = request.get_json(force=True)
    leave_type_id = b.get('leave_type_id')
    start_date    = b.get('start_date', '').strip()
    end_date      = b.get('end_date',   '').strip()
    start_half    = bool(b.get('start_half', False))
    end_half      = bool(b.get('end_half',   False))
    reason        = b.get('reason', '').strip()
    substitute    = b.get('substitute_name', '').strip()
    document_id   = b.get('document_id') or None

    if not all([leave_type_id, start_date, end_date]):
        return jsonify({'error': '缺少必要欄位'}), 400

    total_hours_req = b.get('total_hours')
    if total_hours_req is not None:
        try:   total_hours_req = float(total_hours_req)
        except: total_hours_req = None

    if total_hours_req:
        if total_hours_req <= 0 or total_hours_req > 24:
            return jsonify({'error': '請假時數不合理（需介於 0～24 小時）'}), 400
        total_days = round(total_hours_req / 8, 4)
        end_date = start_date; start_half = False; end_half = False
    else:
        total_hours_req = None
        total_days = _calc_leave_days(start_date, end_date, start_half, end_half)
        if total_days <= 0:
            return jsonify({'error': '請假天數不合理，請檢查日期'}), 400

    with get_db() as conn:
        if document_id and not _cert_owned_by_staff(conn, document_id, sid):
            return jsonify({'error': '附件無效，請重新上傳病單/證明'}), 400
        # 重疊檢查：整天假與任何重疊申請衝突；時數假只與涵蓋當日的整天假衝突
        # （同一天可請多筆時數假，如上午 2h + 下午 2h）
        overlap = conn.execute("""
            SELECT start_date, end_date FROM leave_requests
            WHERE staff_id=%s AND status IN ('pending','approved')
              AND start_date <= %s AND end_date >= %s
              AND (total_hours IS NULL OR %s)
            LIMIT 1
        """, (sid, end_date, start_date, total_hours_req is None)).fetchone()
        if overlap:
            return jsonify({'error': f"該期間與現有假單（{overlap['start_date']}～{overlap['end_date']}，待審或已核准）重疊，請先取消原申請"}), 409
        lt = conn.execute("SELECT * FROM leave_types WHERE id=%s", (leave_type_id,)).fetchone()
        if lt and lt['max_days'] is not None:
            year = start_date[:4]
            conn.execute("""
                INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
                VALUES (%s, %s, %s, 0, 0) ON CONFLICT (staff_id, leave_type_id, year) DO NOTHING
            """, (sid, leave_type_id, int(year)))
            bal = conn.execute("""
                SELECT COALESCE(used_days, 0) as used, COALESCE(total_days, 0) as total
                FROM leave_balances WHERE staff_id=%s AND leave_type_id=%s AND year=%s FOR UPDATE
            """, (sid, leave_type_id, int(year))).fetchone()
            used = float(bal['used']) if bal else 0.0
            # 個人額度（如特休依到職日計算）優先於假別的通用上限
            _personal = float(bal['total']) if bal else 0.0
            _cap = _personal if _personal > 0 else float(lt['max_days'])
            if used + total_days > _cap:
                remaining = _cap - used
                if total_hours_req:
                    rem_hours = round(remaining * 8, 1)
                    return jsonify({'error': f'{lt["name"]}剩餘 {rem_hours} 小時，無法申請 {total_hours_req} 小時'}), 422
                return jsonify({'error': f'{lt["name"]}剩餘 {remaining} 天，無法申請 {total_days} 天'}), 422

        row = conn.execute("""
            INSERT INTO leave_requests
              (staff_id, leave_type_id, start_date, end_date, start_half, end_half,
               total_days, total_hours, reason, substitute_name, document_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
        """, (sid, leave_type_id, start_date, end_date, start_half, end_half,
              total_days, total_hours_req, reason, substitute, document_id)).fetchone()
    return jsonify(leave_req_row(row)), 201


def _cert_owned_by_staff(conn, document_id, sid):
    # 僅允許掛上自己上傳的病單，避免靠猜 ID 取得他人附件的檢視權
    doc = conn.execute("""
        SELECT 1 FROM finance_documents
        WHERE id=%s AND doc_type='medical_cert' AND uploaded_by_staff=%s
    """, (document_id, sid)).fetchone()
    return bool(doc)


@bp.route('/api/leave/my-requests/<int:rid>/document', methods=['PUT'])
def api_leave_my_attach_document(rid):
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    b = request.get_json(force=True)
    document_id = b.get('document_id')
    if not document_id:
        return jsonify({'error': '缺少 document_id'}), 400
    with get_db() as conn:
        req = conn.execute("SELECT * FROM leave_requests WHERE id=%s AND staff_id=%s",
                           (rid, sid)).fetchone()
        if not req:
            return jsonify({'error': '找不到假單'}), 404
        if req['status'] == 'approved':
            return jsonify({'error': '假單已核准，如需更換附件請聯絡管理員'}), 422
        if not _cert_owned_by_staff(conn, document_id, sid):
            return jsonify({'error': '附件無效，請重新上傳病單/證明'}), 400
        row = conn.execute("""
            UPDATE leave_requests SET document_id=%s, updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (document_id, rid)).fetchone()
    return jsonify(leave_req_row(row))


# ─── Leave Balance ───────────────────────────────────────────────────────────

@bp.route('/api/leave/balances', methods=['GET'])
def api_leave_balances():
    year     = request.args.get('year', '')
    staff_id = request.args.get('staff_id', '')
    if not session.get('logged_in'):
        sid = session.get('punch_staff_id')
        if not sid: return jsonify({'error': 'not logged in'}), 401
        staff_id = str(sid)
    if not year:
        from datetime import datetime as _dt2
        year = str(_dt2.now(TW_TZ).year)
    conds, params = ["lb.year=%s"], [int(year)]
    if staff_id: conds.append("lb.staff_id=%s"); params.append(int(staff_id))
    with get_db() as conn:
        rows = conn.execute(f"""
            SELECT lb.*, ps.name as staff_name, lt.name as leave_type_name,
                   lt.code as leave_code, lt.max_days, lt.color as leave_color
            FROM leave_balances lb
            JOIN punch_staff ps ON ps.id=lb.staff_id
            JOIN leave_types  lt ON lt.id=lb.leave_type_id
            WHERE {' AND '.join(conds)}
            ORDER BY ps.name, lt.sort_order
        """, params).fetchall()
    result = []
    for r in rows:
        d = leave_balance_row(r)
        d['staff_name']      = r['staff_name']
        d['leave_type_name'] = r['leave_type_name']
        d['leave_code']      = r['leave_code']
        d['leave_color']     = r['leave_color']
        d['max_days']        = float(r['max_days']) if r['max_days'] is not None else None
        result.append(d)
    return jsonify(result)


@bp.route('/api/leave/balances/init', methods=['POST'])
@require_module('leave')
def api_leave_balance_init():
    b    = request.get_json(force=True)
    year = b.get('year', '')
    if not year:
        from datetime import date as _d3
        year = str(_d3.today().year)
    with get_db() as conn:
        staff_list = conn.execute(
            "SELECT id, name, hire_date FROM punch_staff WHERE active=TRUE"
        ).fetchall()
        lt = conn.execute("SELECT id FROM leave_types WHERE code='annual'").fetchone()
        if not lt: return jsonify({'error': '找不到特休假類型'}), 404
        lt_id   = lt['id']
        updated = 0
        details = []
        for s in staff_list:
            days = _calc_annual_leave_days(s['hire_date'])
            conn.execute("""
                INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
                VALUES (%s,%s,%s,%s,0)
                ON CONFLICT (staff_id, leave_type_id, year) DO UPDATE
                  SET total_days=EXCLUDED.total_days, updated_at=NOW()
            """, (s['id'], lt_id, int(year), days))
            updated += 1
            details.append({'name': s['name'], 'hire_date': str(s['hire_date']) if s['hire_date'] else None, 'days': days})
    return jsonify({'ok': True, 'updated': updated, 'year': year, 'details': details})


@bp.route('/api/leave/annual-schedule/<int:staff_id>', methods=['GET'])
@require_module('leave')
def api_annual_leave_schedule(staff_id):
    with get_db() as conn:
        staff = conn.execute(
            "SELECT name, hire_date FROM punch_staff WHERE id=%s", (staff_id,)
        ).fetchone()
    if not staff: return ('', 404)
    schedule = _calc_annual_leave_schedule(staff['hire_date'])
    current  = _calc_annual_leave_days(staff['hire_date'])
    return jsonify({
        'staff_id': staff_id, 'name': staff['name'],
        'hire_date': str(staff['hire_date']) if staff['hire_date'] else None,
        'current_days': current, 'schedule': schedule,
    })


@bp.route('/api/leave/annual-schedule/public', methods=['GET'])
def api_annual_leave_schedule_public():
    sid = session.get('punch_staff_id')
    if not sid: return jsonify({'error': 'not logged in'}), 401
    with get_db() as conn:
        staff = conn.execute(
            "SELECT name, hire_date FROM punch_staff WHERE id=%s", (sid,)
        ).fetchone()
    if not staff: return ('', 404)
    schedule = _calc_annual_leave_schedule(staff['hire_date'])
    current  = _calc_annual_leave_days(staff['hire_date'])
    return jsonify({
        'name': staff['name'],
        'hire_date': str(staff['hire_date']) if staff['hire_date'] else None,
        'current_days': current, 'schedule': schedule,
    })


@bp.route('/api/leave/balances/<int:bid>', methods=['PUT'])
@require_module('leave')
def api_leave_balance_update(bid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE leave_balances SET total_days=%s, used_days=%s, note=%s, updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (float(b.get('total_days', 0)), float(b.get('used_days', 0)),
              b.get('note', ''), bid)).fetchone()
    return jsonify(leave_balance_row(row)) if row else ('', 404)


@bp.route('/api/leave/summary/<int:staff_id>/<month>', methods=['GET'])
@require_module('leave')
def api_leave_summary(staff_id, month):
    with get_db() as conn:
        rows = conn.execute("""
            SELECT lr.*, lt.name as leave_type_name, lt.code, lt.pay_rate
            FROM leave_requests lr JOIN leave_types lt ON lt.id=lr.leave_type_id
            WHERE lr.staff_id=%s AND lr.status='approved'
              AND to_char(lr.start_date,'YYYY-MM')=%s ORDER BY lr.start_date
        """, (staff_id, month)).fetchall()
    total_leave_days = 0.0; unpaid_days = 0.0; half_pay_days = 0.0; items = []
    for r in rows:
        d = float(r['total_days']); pay_r = float(r['pay_rate'])
        total_leave_days += d
        if pay_r == 0:   unpaid_days   += d
        elif pay_r < 1:  half_pay_days += d
        items.append({
            'leave_type': r['leave_type_name'], 'code': r['code'],
            'days': d, 'pay_rate': pay_r,
            'start_date': r['start_date'].isoformat(), 'end_date': r['end_date'].isoformat(),
        })
    return jsonify({
        'staff_id': staff_id, 'month': month,
        'total_leave_days': total_leave_days, 'unpaid_days': unpaid_days,
        'half_pay_days': half_pay_days, 'items': items,
    })


# ─── Medical cert upload ──────────────────────────────────────────────────────

@bp.route('/api/leave/upload-cert', methods=['POST'])
def api_leave_upload_cert():
    if not (session.get('punch_staff_id') or session.get('logged_in')):
        return jsonify({'error': '請先登入'}), 401
    file = request.files.get('file')
    if not file: return jsonify({'error': '請上傳圖片'}), 400
    raw = file.read()
    if len(raw) > 10 * 1024 * 1024:
        return jsonify({'error': '檔案不可超過 10MB'}), 400
    import base64 as _b64c
    image_data = 'data:' + (file.content_type or 'image/jpeg') + ';base64,' + _b64c.b64encode(raw).decode()
    try:
        with get_db() as conn:
            doc = conn.execute("""
                INSERT INTO finance_documents (filename, doc_type, image_data, upload_date, uploaded_by_staff)
                VALUES (%s, 'medical_cert', %s, CURRENT_DATE, %s) RETURNING id
            """, (file.filename, image_data, session.get('punch_staff_id'))).fetchone()
        return jsonify({'document_id': doc['id'], 'filename': file.filename})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@bp.route('/api/documents/<int:doc_id>/image', methods=['GET'])
def api_document_image(doc_id):
    if not (session.get('logged_in') or session.get('punch_staff_id')):
        return jsonify({'error': 'unauthorized'}), 401
    # 員工僅能檢視自己的請假/報帳附件，避免越權查看他人診斷證明、收據
    if not session.get('logged_in'):
        sid = session.get('punch_staff_id')
        with get_db() as conn:
            owned = conn.execute("""
                SELECT 1 FROM leave_requests  WHERE document_id=%s AND staff_id=%s
                UNION ALL
                SELECT 1 FROM expense_claims  WHERE document_id=%s AND staff_id=%s
                LIMIT 1
            """, (doc_id, sid, doc_id, sid)).fetchone()
        if not owned:
            return jsonify({'error': 'forbidden'}), 403
    with get_db() as conn:
        doc = conn.execute("SELECT image_data, filename FROM finance_documents WHERE id=%s", (doc_id,)).fetchone()
    if not doc or not doc['image_data']:
        return jsonify({'error': '找不到圖片'}), 404
    from flask import Response
    from html import escape as _esc
    fname = _esc(doc['filename'] or '', quote=True)   # 跳脫檔名，避免惡意檔名造成 XSS
    src   = doc['image_data'] if str(doc['image_data']).startswith('data:image/') else ''
    html = (
        '<!doctype html><html><head><meta charset="utf-8">'
        f'<title>{fname}</title>'
        '<style>body{margin:0;background:#111;display:flex;justify-content:center;align-items:flex-start}'
        'img{max-width:100%;height:auto}</style></head>'
        f'<body><img src="{src}" alt="{fname}"></body></html>'
    )
    return Response(html, mimetype='text/html')


# ─── Batch leave request review ───────────────────────────────────────────────

@bp.route('/api/leave/requests/batch', methods=['POST'])
@require_module('leave')
def api_leave_requests_batch():
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
            old = conn.execute("SELECT * FROM leave_requests WHERE id=%s", (rid,)).fetchone()
            if not old: continue
            old_status = old['status']
            row = conn.execute("""
                UPDATE leave_requests
                SET status=%s, reviewed_by=%s, review_note=%s,
                    reviewed_at=NOW(), updated_at=NOW()
                WHERE id=%s RETURNING *
            """, (new_status, reviewed_by, review_note, rid)).fetchone()
            delta = float(old['total_days'])
            if action == 'approve' and old_status != 'approved':
                _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                      str(old['start_date'])[:4], delta)
            elif action == 'reject' and old_status == 'approved':
                _update_leave_balance(conn, old['staff_id'], old['leave_type_id'],
                                      str(old['start_date'])[:4], -delta)
            if row: results.append(leave_req_row(row))
    return jsonify({'updated': len(results), 'rows': results})
