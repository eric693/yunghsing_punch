"""
blueprints/dashboard.py — 儀表板、多店管理、勞基法監控、出勤異常偵測
"""
import threading
from datetime import datetime as _dt, timedelta as _td, timezone as _tz, date as _date

from flask import Blueprint, request, jsonify

from auth import login_required, require_module
from config import TW_TZ
from db import get_db

bp = Blueprint('dashboard', __name__)

# ── 勞基法 DB init ─────────────────────────────────────────────────

def init_labor_law_db():
    try:
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS labor_law_updates (
                    id           SERIAL PRIMARY KEY,
                    law_name     TEXT NOT NULL DEFAULT '勞動基準法',
                    amend_date   DATE NOT NULL,
                    version_note TEXT DEFAULT '',
                    summary      TEXT DEFAULT '',
                    source_url   TEXT DEFAULT 'https://law.moj.gov.tw/LawClass/LawHistory.aspx?pcode=N0030001',
                    announced    BOOLEAN DEFAULT FALSE,
                    fetched_at   TIMESTAMPTZ DEFAULT NOW(),
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(law_name, amend_date)
                )
            """)
    except Exception as e:
        print(f"[labor_law_init] {e}")


# ── 勞基法爬蟲 ────────────────────────────────────────────────────

def _scrape_labor_law_updates():
    import urllib.request as _ur
    import html.parser as _hp
    import re as _re

    LAW_HISTORY_URL = 'https://law.moj.gov.tw/LawClass/LawHistory.aspx?pcode=N0030001'

    class _HistoryParser(_hp.HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_td = False; self.current_cell = ''; self.current_row = []; self.rows = []
        def handle_starttag(self, tag, attrs):
            if tag == 'tr': self.current_row = []
            elif tag in ('td', 'th'): self.in_td = True; self.current_cell = ''
        def handle_endtag(self, tag):
            if tag in ('td', 'th'):
                self.in_td = False; self.current_row.append(self.current_cell.strip())
            elif tag == 'tr':
                if self.current_row: self.rows.append(self.current_row)
        def handle_data(self, data):
            if self.in_td: self.current_cell += data

    results = []
    try:
        req = _ur.Request(LAW_HISTORY_URL,
                          headers={'User-Agent': 'Mozilla/5.0 (compatible; LaborLawMonitor/1.0)'})
        with _ur.urlopen(req, timeout=20) as resp:
            raw = resp.read()
            try: content = raw.decode('utf-8')
            except: content = raw.decode('big5', errors='replace')
        parser = _HistoryParser()
        parser.feed(content)
        date_pat = _re.compile(r'(\d{3,4})[./年](\d{1,2})[./月](\d{1,2})')
        seen = set()
        for row in parser.rows:
            text = ' '.join(row)
            m = date_pat.search(text)
            if not m: continue
            yr, mo, dy = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if yr < 1900: yr += 1911
            if not (1984 <= yr <= 2099 and 1 <= mo <= 12 and 1 <= dy <= 31): continue
            date_str = f"{yr:04d}-{mo:02d}-{dy:02d}"
            if date_str in seen: continue
            seen.add(date_str)
            note = ' '.join(c for c in row if c and not date_pat.search(c))[:200]
            results.append({'amend_date': date_str, 'version_note': note.strip(),
                            'summary': f'勞動基準法於 {yr} 年 {mo} 月 {dy} 日修正',
                            'source_url': LAW_HISTORY_URL})
        results.sort(key=lambda x: x['amend_date'], reverse=True)
    except Exception as e:
        print(f"[labor_law_scrape] {e}")
    return results


def _run_labor_law_check():
    updates = _scrape_labor_law_updates()
    if not updates:
        return
    try:
        with get_db() as conn:
            existing_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM labor_law_updates WHERE law_name='勞動基準法'"
            ).fetchone()['cnt']
    except Exception:
        existing_count = 0
    is_initial_import = (existing_count == 0)
    new_ids = []
    for u in updates:
        try:
            with get_db() as conn:
                row = conn.execute("""
                    INSERT INTO labor_law_updates
                      (law_name, amend_date, version_note, summary, source_url, announced)
                    VALUES ('勞動基準法', %s, %s, %s, %s, %s)
                    ON CONFLICT (law_name, amend_date) DO NOTHING RETURNING id
                """, (u['amend_date'], u['version_note'], u['summary'],
                      u['source_url'], is_initial_import)).fetchone()
                if row and not is_initial_import:
                    new_ids.append(row['id'])
        except Exception as e:
            print(f"[labor_law_check] db error: {e}")
    if not new_ids:
        return
    try:
        with get_db() as conn:
            new_rows = conn.execute(
                "SELECT * FROM labor_law_updates WHERE id = ANY(%s) ORDER BY amend_date DESC",
                (new_ids,)
            ).fetchall()
            if not new_rows: return
            dates = '、'.join(str(dict(r)['amend_date']) for r in new_rows)
            title = f"勞動基準法修正公告（{dates}）"
            lines = ["系統偵測到勞動基準法新修正版本，請人資部門注意相關條文變動：\n"]
            for r in new_rows:
                rd = dict(r)
                lines.append(f"・修正日期：{rd['amend_date']}")
                if rd.get('version_note'): lines.append(f"  {rd['version_note']}")
                lines.append(f"  來源：{rd['source_url']}")
            lines.append("\n請至全國法規資料庫確認詳細條文內容。")
            conn.execute("""
                INSERT INTO announcements
                  (title, content, category, priority, is_pinned, visible_to, author, active)
                VALUES (%s, %s, 'labor_law', 'high', TRUE, 'admin', '勞基法監控系統', TRUE)
            """, (title, '\n'.join(lines)))
    except Exception as e:
        print(f"[labor_law_check] announce error: {e}")


def _labor_law_check_loop():
    import time as _t
    _t.sleep(30)
    _run_labor_law_check()
    while True:
        _t.sleep(7 * 24 * 3600)
        _run_labor_law_check()


_labor_monitor_started = False

def start_labor_law_monitor():
    global _labor_monitor_started
    if _labor_monitor_started:
        return
    _labor_monitor_started = True
    threading.Thread(target=_labor_law_check_loop, daemon=True).start()


# ── Dashboard ──────────────────────────────────────────────────────

@bp.route('/api/dashboard', methods=['GET'])
@login_required
def api_dashboard():
    import calendar as _cal
    today = _dt.now(TW_TZ).date()
    req_month = request.args.get('month', '').strip()
    if req_month and len(req_month) == 7:
        month = req_month
        try: int(month[:4]); int(month[5:])
        except: month = today.strftime('%Y-%m')
    else:
        month = today.strftime('%Y-%m')

    with get_db() as conn:
        total_staff = conn.execute(
            "SELECT COUNT(*) as c FROM punch_staff WHERE active=TRUE"
        ).fetchone()['c']
        clocked_in = conn.execute("""
            SELECT COUNT(DISTINCT staff_id) as c FROM punch_records
            WHERE punch_type='in' AND (punched_at AT TIME ZONE 'Asia/Taipei')::date = %s
        """, (today,)).fetchone()['c']
        clocked_out = conn.execute("""
            SELECT COUNT(DISTINCT staff_id) as c FROM punch_records
            WHERE punch_type='out' AND (punched_at AT TIME ZONE 'Asia/Taipei')::date = %s
        """, (today,)).fetchone()['c']
        on_leave_today = conn.execute("""
            SELECT COUNT(DISTINCT staff_id) as c FROM leave_requests
            WHERE status='approved' AND start_date <= %s AND end_date >= %s
        """, (today, today)).fetchone()['c']
        today_detail_rows = conn.execute("""
            SELECT ps.id, ps.name, ps.role,
                   MAX(CASE WHEN pr.punch_type='in'  THEN to_char(pr.punched_at AT TIME ZONE 'Asia/Taipei','HH24:MI') END) as clock_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN to_char(pr.punched_at AT TIME ZONE 'Asia/Taipei','HH24:MI') END) as clock_out,
                   COUNT(pr.id) as punch_count
            FROM punch_staff ps
            LEFT JOIN punch_records pr ON pr.staff_id = ps.id
              AND (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date = %s
            WHERE ps.active = TRUE
            GROUP BY ps.id, ps.name, ps.role ORDER BY ps.name
        """, (today,)).fetchall()
        today_detail = []
        for r in today_detail_rows:
            leave_row = conn.execute("""
                SELECT lt.name as leave_name FROM leave_requests lr
                JOIN leave_types lt ON lt.id = lr.leave_type_id
                WHERE lr.staff_id=%s AND lr.status='approved'
                  AND lr.start_date <= %s AND lr.end_date >= %s LIMIT 1
            """, (r['id'], today, today)).fetchone()
            if r['clock_in']:
                status = 'done' if r['clock_out'] else 'working'
                status_label = '已下班' if r['clock_out'] else '上班中'
            elif leave_row:
                status = 'leave'; status_label = leave_row['leave_name']
            else:
                status = 'absent'; status_label = '未出勤'
            today_detail.append({
                'id': r['id'], 'name': r['name'], 'role': r['role'] or '',
                'clock_in': r['clock_in'] or '', 'clock_out': r['clock_out'] or '',
                'punch_count': r['punch_count'], 'status': status, 'status_label': status_label,
            })
        pending_punch = conn.execute("SELECT COUNT(*) as c FROM punch_requests WHERE status='pending'").fetchone()['c']
        pending_ot    = conn.execute("SELECT COUNT(*) as c FROM overtime_requests WHERE status='pending'").fetchone()['c']
        pending_sched = conn.execute("SELECT COUNT(*) as c FROM schedule_requests WHERE status IN ('pending','modified_pending')").fetchone()['c']
        pending_leave = conn.execute("SELECT COUNT(*) as c FROM leave_requests WHERE status='pending'").fetchone()['c']
        sal_rows = conn.execute("""
            SELECT COUNT(*) as total_count,
                   COUNT(*) FILTER (WHERE status='confirmed') as confirmed_count,
                   COALESCE(SUM(net_pay),0) as total_net,
                   COALESCE(SUM(allowance_total),0) as total_allow,
                   COALESCE(SUM(deduction_total),0) as total_deduct
            FROM salary_records WHERE month=%s
        """, (month,)).fetchone()
        days_in_month = _cal.monthrange(today.year, today.month)[1]
        daily_rows = conn.execute("""
            SELECT (punched_at AT TIME ZONE 'Asia/Taipei')::date as d,
                   COUNT(DISTINCT staff_id) as cnt
            FROM punch_records WHERE punch_type='in'
              AND to_char(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
            GROUP BY (punched_at AT TIME ZONE 'Asia/Taipei')::date ORDER BY d
        """, (month,)).fetchall()
        daily_map = {str(r['d']): r['cnt'] for r in daily_rows}
        daily_attendance = [
            {'date': f"{month}-{d:02d}", 'day': d,
             'count': daily_map.get(f"{month}-{d:02d}", 0),
             'is_past': _date(today.year, today.month, d) <= today,
             'weekday': _date(today.year, today.month, d).weekday()}
            for d in range(1, days_in_month + 1)
        ]
        leave_dist_rows = conn.execute("""
            SELECT lt.name, lt.color, COUNT(*) as cnt, COALESCE(SUM(lr.total_days),0) as days
            FROM leave_requests lr JOIN leave_types lt ON lt.id = lr.leave_type_id
            WHERE lr.status='approved' AND to_char(lr.start_date,'YYYY-MM')=%s
            GROUP BY lt.name, lt.color ORDER BY days DESC
        """, (month,)).fetchall()
        ot_rank_rows = conn.execute("""
            SELECT ps.name, ps.role, COALESCE(SUM(r.ot_pay),0) as total_pay,
                   COALESCE(SUM(r.ot_hours),0) as total_hours
            FROM overtime_requests r JOIN punch_staff ps ON ps.id = r.staff_id
            WHERE r.status='approved' AND to_char(r.request_date,'YYYY-MM')=%s
            GROUP BY ps.name, ps.role ORDER BY total_pay DESC LIMIT 8
        """, (month,)).fetchall()

    return jsonify({
        'month': month, 'today': str(today), 'is_current_month': month == today.strftime('%Y-%m'),
        'today_summary': {
            'total': total_staff, 'working': clocked_in - clocked_out,
            'clocked_in': clocked_in, 'clocked_out': clocked_out,
            'on_leave': on_leave_today, 'absent': total_staff - clocked_in - on_leave_today,
        },
        'today_detail': today_detail,
        'pending': {'punch': pending_punch, 'ot': pending_ot, 'sched': pending_sched,
                    'leave': pending_leave, 'total': pending_punch + pending_ot + pending_sched + pending_leave},
        'salary_summary': {
            'total_count': sal_rows['total_count'], 'confirmed_count': sal_rows['confirmed_count'],
            'total_net': float(sal_rows['total_net']), 'total_allow': float(sal_rows['total_allow']),
            'total_deduct': float(sal_rows['total_deduct']),
        },
        'daily_attendance': daily_attendance,
        'leave_distribution': [{'name': r['name'], 'color': r['color'], 'count': r['cnt'], 'days': float(r['days'])} for r in leave_dist_rows],
        'ot_ranking': [{'name': r['name'], 'role': r['role'] or '', 'pay': float(r['total_pay']), 'hours': float(r['total_hours'])} for r in ot_rank_rows],
    })


@bp.route('/api/dashboard/labor-cost', methods=['GET'])
@login_required
def api_dashboard_labor_cost():
    today = _date.today()
    months = []
    for i in range(11, -1, -1):
        m = today.month - i; y = today.year
        while m <= 0: m += 12; y -= 1
        months.append(f'{y}-{m:02d}')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT month, COALESCE(SUM(net_pay),0) as total
            FROM salary_records WHERE month = ANY(%s) GROUP BY month
        """, (months,)).fetchall()
    cost_map = {r['month']: float(r['total']) for r in rows}
    return jsonify({'months': months, 'labor_cost': [cost_map.get(m, 0) for m in months]})


@bp.route('/api/dashboard/attendance-heatmap', methods=['GET'])
@login_required
def api_dashboard_attendance_heatmap():
    import calendar as _calh
    month = request.args.get('month', '') or _date.today().strftime('%Y-%m')
    y, mo = int(month[:4]), int(month[5:7])
    days_in = _calh.monthrange(y, mo)[1]
    with get_db() as conn:
        total_staff = conn.execute("SELECT COUNT(*) as c FROM punch_staff WHERE active=TRUE").fetchone()['c']
        punch_rows = conn.execute("""
            SELECT (punched_at AT TIME ZONE 'Asia/Taipei')::date as d, COUNT(DISTINCT staff_id) as cnt
            FROM punch_records WHERE punch_type='in'
              AND to_char(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s GROUP BY d
        """, (month,)).fetchall()
    punch_map = {str(r['d']): int(r['cnt']) for r in punch_rows}
    days = []
    for d in range(1, days_in + 1):
        ds = f'{y}-{mo:02d}-{d:02d}'
        cnt = punch_map.get(ds, 0)
        days.append({
            'date': ds, 'day_of_week': _date(y, mo, d).weekday(),
            'count': cnt, 'attendance_rate': round(cnt / total_staff, 3) if total_staff > 0 else 0,
        })
    return jsonify({'month': month, 'total_staff': total_staff, 'days': days})


@bp.route('/api/dashboard/leave-distribution', methods=['GET'])
@login_required
def api_dashboard_leave_distribution():
    year = request.args.get('year', str(_date.today().year))
    with get_db() as conn:
        rows = conn.execute("""
            SELECT lt.name, lt.color, COUNT(*) as cnt, COALESCE(SUM(lr.total_days), 0) as days
            FROM leave_requests lr JOIN leave_types lt ON lt.id=lr.leave_type_id
            WHERE lr.status='approved' AND EXTRACT(YEAR FROM lr.start_date)=%s
            GROUP BY lt.name, lt.color ORDER BY days DESC
        """, (int(year),)).fetchall()
    total = sum(float(r['days']) for r in rows)
    return jsonify({
        'year': year, 'total_leave_days': total,
        'breakdown': [{'name': r['name'], 'color': r['color'] or '#4a7bda',
                       'days': float(r['days']), 'count': int(r['cnt']),
                       'pct': round(float(r['days']) / total * 100, 1) if total > 0 else 0}
                      for r in rows],
    })


# ── Stores ─────────────────────────────────────────────────────────

@bp.route('/api/stores', methods=['GET'])
@login_required
def api_stores_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM stores ORDER BY id").fetchall()
    return jsonify([dict(r) for r in rows])


@bp.route('/api/stores', methods=['POST'])
@login_required
def api_stores_create():
    b = request.get_json(force=True)
    name = (b.get('name') or '').strip()
    if not name: return jsonify({'error': '店名為必填'}), 400
    with get_db() as conn:
        row = conn.execute(
            "INSERT INTO stores (name, address) VALUES (%s,%s) RETURNING *",
            (name, (b.get('address') or '').strip())
        ).fetchone()
    return jsonify(dict(row)), 201


@bp.route('/api/stores/<int:sid>', methods=['PUT'])
@login_required
def api_stores_update(sid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute(
            "UPDATE stores SET name=%s, address=%s, active=%s WHERE id=%s RETURNING *",
            ((b.get('name') or '').strip(), (b.get('address') or '').strip(),
             bool(b.get('active', True)), sid)
        ).fetchone()
    return jsonify(dict(row)) if row else ('', 404)


@bp.route('/api/stores/<int:sid>', methods=['DELETE'])
@login_required
def api_stores_delete(sid):
    with get_db() as conn:
        conn.execute("UPDATE punch_staff SET store_id=NULL WHERE store_id=%s", (sid,))
        conn.execute("DELETE FROM stores WHERE id=%s", (sid,))
    return jsonify({'deleted': sid})


@bp.route('/api/stores/<int:sid>/staff', methods=['GET'])
@login_required
def api_store_staff(sid):
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, role, active FROM punch_staff WHERE store_id=%s ORDER BY name", (sid,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@bp.route('/api/staff/<int:sid>/store', methods=['PUT'])
@login_required
def api_staff_assign_store(sid):
    b = request.get_json(force=True)
    store_id = b.get('store_id')
    with get_db() as conn:
        conn.execute("UPDATE punch_staff SET store_id=%s WHERE id=%s", (store_id, sid))
    return jsonify({'ok': True})


# ── Labor Law ──────────────────────────────────────────────────────

@bp.route('/api/labor-law/updates', methods=['GET'])
@login_required
def api_labor_law_list():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM labor_law_updates ORDER BY amend_date DESC LIMIT 100"
        ).fetchall()
        conn.execute("UPDATE labor_law_updates SET announced=TRUE WHERE announced=FALSE")
    result = []
    for r in rows:
        d = dict(r)
        if d.get('amend_date'): d['amend_date'] = str(d['amend_date'])
        if d.get('fetched_at'): d['fetched_at'] = d['fetched_at'].isoformat()
        if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
        result.append(d)
    return jsonify(result)


@bp.route('/api/labor-law/check', methods=['POST'])
@login_required
def api_labor_law_trigger_check():
    threading.Thread(target=_run_labor_law_check, daemon=True).start()
    return jsonify({'ok': True, 'message': '已開始背景檢查，請稍後重新整理'})


@bp.route('/api/labor-law/badge', methods=['GET'])
@login_required
def api_labor_law_badge():
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM labor_law_updates WHERE announced=FALSE"
        ).fetchone()
    return jsonify({'unread': row['cnt'] if row else 0})


# ── Attendance Anomalies ───────────────────────────────────────────

@bp.route('/api/attendance/anomalies', methods=['GET'])
@login_required
def api_attendance_anomalies():
    today     = _dt.now(TW_TZ).date()
    date_from = today - _td(days=7)

    with get_db() as conn:
        rows = conn.execute("""
            SELECT ps.id as staff_id, ps.name, ps.role, ps.department,
                   (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date as work_date,
                   array_agg(pr.punch_type ORDER BY pr.punched_at) as types,
                   MIN(CASE WHEN pr.punch_type='in'  THEN to_char(pr.punched_at AT TIME ZONE 'Asia/Taipei','HH24:MI') END) as first_in,
                   MAX(CASE WHEN pr.punch_type='out' THEN to_char(pr.punched_at AT TIME ZONE 'Asia/Taipei','HH24:MI') END) as last_out
            FROM punch_records pr JOIN punch_staff ps ON ps.id = pr.staff_id
            WHERE (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date BETWEEN %s AND %s AND ps.active = TRUE
            GROUP BY ps.id, ps.name, ps.role, ps.department,
                     (pr.punched_at AT TIME ZONE 'Asia/Taipei')::date
            ORDER BY work_date DESC, ps.name
        """, (date_from, today)).fetchall()
        shift_rows = conn.execute("""
            SELECT sa.staff_id, sa.shift_date as date, st.start_time, st.end_time
            FROM shift_assignments sa JOIN shift_types st ON st.id = sa.shift_type_id
            WHERE sa.shift_date BETWEEN %s AND %s
        """, (date_from, today)).fetchall()
        shift_map = {(r['staff_id'], str(r['date'])): r for r in shift_rows}
        all_staff = conn.execute(
            "SELECT id, name, role, department FROM punch_staff WHERE active=TRUE"
        ).fetchall()
        today_punched_ids = {r['staff_id'] for r in rows if str(r['work_date']) == str(today)}
        leave_today = conn.execute("""
            SELECT DISTINCT staff_id FROM leave_requests
            WHERE status='approved' AND start_date <= %s AND end_date >= %s
        """, (today, today)).fetchall()
        on_leave_today_ids = {r['staff_id'] for r in leave_today}

    anomalies = []
    for r in rows:
        types   = list(r['types']) if r['types'] else []
        has_in  = 'in' in types
        has_out = 'out' in types
        ds      = str(r['work_date'])
        if has_in and not has_out and ds != str(today):
            anomalies.append({'type': 'missing_out', 'label': '忘記下班打卡', 'severity': 'warning',
                               'staff_id': r['staff_id'], 'name': r['name'], 'role': r['role'] or '',
                               'department': r['department'] or '', 'date': ds,
                               'detail': f"上班 {r['first_in']}，無下班記錄"})
        if not has_in and has_out:
            anomalies.append({'type': 'missing_in', 'label': '忘記上班打卡', 'severity': 'warning',
                               'staff_id': r['staff_id'], 'name': r['name'], 'role': r['role'] or '',
                               'department': r['department'] or '', 'date': ds,
                               'detail': f"下班 {r['last_out']}，無上班記錄"})
        if has_in and r['first_in']:
            shift = shift_map.get((r['staff_id'], ds))
            if shift and shift['start_time']:
                try:
                    sh, sm = map(int, str(shift['start_time'])[:5].split(':'))
                    ih, im = map(int, r['first_in'].split(':'))
                    late_mins = (ih * 60 + im) - (sh * 60 + sm)
                    if late_mins > 10:
                        anomalies.append({'type': 'late', 'label': '遲到', 'severity': 'warning',
                                          'staff_id': r['staff_id'], 'name': r['name'], 'role': r['role'] or '',
                                          'department': r['department'] or '', 'date': ds,
                                          'detail': f"應 {str(shift['start_time'])[:5]} 上班，實際 {r['first_in']}（晚 {late_mins} 分鐘）"})
                except Exception:
                    pass
        if has_out and r['last_out'] and ds != str(today):
            shift = shift_map.get((r['staff_id'], ds))
            if shift and shift['end_time']:
                try:
                    eh, em = map(int, str(shift['end_time'])[:5].split(':'))
                    oh, om = map(int, r['last_out'].split(':'))
                    early_mins = (eh * 60 + em) - (oh * 60 + om)
                    if early_mins > 15:
                        anomalies.append({'type': 'early', 'label': '早退', 'severity': 'warning',
                                          'staff_id': r['staff_id'], 'name': r['name'], 'role': r['role'] or '',
                                          'department': r['department'] or '', 'date': ds,
                                          'detail': f"應 {str(shift['end_time'])[:5]} 下班，實際 {r['last_out']}（早 {early_mins} 分鐘）"})
                except Exception:
                    pass

    for s in all_staff:
        if s['id'] not in today_punched_ids and s['id'] not in on_leave_today_ids:
            anomalies.append({'type': 'absent', 'label': '今日未出勤', 'severity': 'error',
                               'staff_id': s['id'], 'name': s['name'], 'role': s['role'] or '',
                               'department': s['department'] or '', 'date': str(today),
                               'detail': '今日尚無打卡記錄且未請假'})

    sev_order = {'error': 0, 'warning': 1, 'info': 2}
    anomalies.sort(key=lambda x: (sev_order.get(x['severity'], 9), x['date']))
    return jsonify({'anomalies': anomalies, 'count': len(anomalies), 'checked_from': str(date_from)})
