"""
blueprints/salary.py — 薪資模組（薪資項目、薪資記錄、員工薪資設定、薪資單 PDF）
"""
import json as _json
import logging

from flask import Blueprint, session, request, jsonify

from auth import require_module
from db import get_db
from blueprints.punch import punch_staff_row
from blueprints.leave import _calc_annual_leave_days
from blueprints.notifications import _notify_review_result

bp = Blueprint('salary', __name__)


# ─── DB init ────────────────────────────────────────────────────────────────

def init_salary_db():
    migrations = [
        """CREATE TABLE IF NOT EXISTS salary_calc_settings (
            setting_key   TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL DEFAULT 'true'
        )""",
        """CREATE TABLE IF NOT EXISTS salary_items (
            id          SERIAL PRIMARY KEY,
            name        TEXT NOT NULL,
            item_type   TEXT NOT NULL DEFAULT 'allowance',
            formula     TEXT DEFAULT '',
            amount      NUMERIC(12,2) DEFAULT 0,
            description TEXT DEFAULT '',
            color       TEXT DEFAULT '#4a7bda',
            active      BOOLEAN DEFAULT TRUE,
            sort_order  INT DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE salary_items ADD COLUMN IF NOT EXISTS code TEXT DEFAULT ''",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_item_ids JSONB DEFAULT NULL",
        "ALTER TABLE punch_staff ADD COLUMN IF NOT EXISTS salary_item_overrides JSONB DEFAULT NULL",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS income_tax_withheld    NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS absent_days           NUMERIC(5,1)  DEFAULT 0",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS whole_day_leave_days  NUMERIC(5,1)  DEFAULT 0",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS hourly_base_pay       NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE salary_records ADD COLUMN IF NOT EXISTS actual_work_hours     NUMERIC(8,2)  DEFAULT 0",
        """CREATE TABLE IF NOT EXISTS salary_records (
            id              SERIAL PRIMARY KEY,
            staff_id        INT REFERENCES punch_staff(id) ON DELETE CASCADE,
            month           TEXT NOT NULL,
            base_salary     NUMERIC(12,2) DEFAULT 0,
            insured_salary  NUMERIC(12,2) DEFAULT 0,
            work_days       NUMERIC(5,1)  DEFAULT 0,
            actual_days     NUMERIC(5,1)  DEFAULT 0,
            leave_days      NUMERIC(5,1)  DEFAULT 0,
            unpaid_days     NUMERIC(5,1)  DEFAULT 0,
            ot_pay          NUMERIC(12,2) DEFAULT 0,
            allowance_total NUMERIC(12,2) DEFAULT 0,
            deduction_total NUMERIC(12,2) DEFAULT 0,
            net_pay         NUMERIC(12,2) DEFAULT 0,
            items           JSONB         DEFAULT '[]',
            status          TEXT          DEFAULT 'draft',
            note            TEXT          DEFAULT '',
            confirmed_by    TEXT          DEFAULT '',
            confirmed_at    TIMESTAMPTZ,
            created_at      TIMESTAMPTZ   DEFAULT NOW(),
            updated_at      TIMESTAMPTZ   DEFAULT NOW(),
            UNIQUE(staff_id, month)
        )""",
    ]
    for sql in migrations:
        try:
            with get_db() as conn:
                conn.execute(sql)
        except Exception as e:
            print(f"[salary_init] {str(e)[:80]}")

    # Seed default salary items
    defaults = [
        ('本薪',        'allowance', 'base_salary+service_years*1000', 0,    '#2e9e6b', 1),
        ('職務加給',    'allowance', '',                                0,    '#0ea5e9', 2),
        ('全勤獎金',    'allowance', '',                                0,    '#c8a96e', 3),
        ('獎金',        'allowance', '',                                0,    '#8b5cf6', 4),
        ('生日禮金',    'allowance', '',                                1000, '#e05c8a', 5),
        ('勞退6%',      'allowance', 'base_salary*0.06+service_years*1000*0.06', 0, '#4a7bda', 6),
        ('病/事/假',    'deduction', '',                                0,    '#8892a4', 7),
        ('勞保費',      'deduction', 'insured_salary*0.125*0.2',       0,    '#d64242', 8),
        ('健保費',      'deduction', 'insured_salary*0.0517*0.3',      0,    '#e07b2a', 9),
        ('勞退提撥6%',  'deduction', 'base_salary*0.06+service_years*1000*0.06', 0, '#4a7bda', 10),
    ]
    try:
        with get_db() as conn:
            cnt = conn.execute("SELECT COUNT(*) as c FROM salary_items").fetchone()['c']
            if cnt == 0:
                for name, itype, formula, amount, color, sort in defaults:
                    conn.execute("""
                        INSERT INTO salary_items (name,item_type,formula,amount,color,sort_order)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (name, itype, formula, amount, color, sort))
    except Exception as e:
        print(f"[salary_seed] {e}")


# ─── Helper functions ────────────────────────────────────────────────────────

def _get_salary_calc_settings():
    """讀取薪資計算設定（帶預設值）"""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT setting_key, setting_value FROM salary_calc_settings"
            ).fetchall()
            cfg = {r['setting_key']: r['setting_value'] for r in rows}
    except Exception:
        cfg = {}
    return {
        'auto_leave_deduction':  cfg.get('auto_leave_deduction',  'true') == 'true',
        'auto_absent_deduction': cfg.get('auto_absent_deduction', 'true') == 'true',
        'auto_income_tax':       cfg.get('auto_income_tax',       'true') == 'true',
    }


def salary_item_row(row):
    if not row: return None
    d = dict(row)
    if d.get('amount') is not None: d['amount'] = float(d['amount'])
    if d.get('created_at'): d['created_at'] = d['created_at'].isoformat()
    return d


def salary_record_row(row):
    if not row: return None
    d = dict(row)
    for f in ['base_salary','insured_salary','work_days','actual_days','leave_days',
              'unpaid_days','ot_pay','allowance_total','deduction_total','net_pay',
              'income_tax_withheld','absent_days','whole_day_leave_days',
              'hourly_base_pay','actual_work_hours']:
        if d.get(f) is not None: d[f] = float(d[f])
    if isinstance(d.get('items'), str):
        try: d['items'] = _json.loads(d['items'])
        except: d['items'] = []
    if d.get('confirmed_at'): d['confirmed_at'] = d['confirmed_at'].isoformat()
    if d.get('created_at'):   d['created_at']   = d['created_at'].isoformat()
    if d.get('updated_at'):   d['updated_at']   = d['updated_at'].isoformat()
    return d


def _eval_formula(formula, base_salary, insured_salary, service_years, extra=None, item_amounts=None):
    """安全計算薪資公式
    可用變數：base_salary, insured_salary, service_years,
              actual_days, work_days, leave_days, unpaid_days,
              whole_day_leave_days（整天假天數，小時請假不計入，全勤判斷用此變數）,
              personal_days, sick_days, daily_wage
    支援條件式：例如 3000 if whole_day_leave_days==0 else 0
    支援項目代號引用：例如 01/30*personal_days（01 為本薪項目代號）
    """
    if not formula: return 0.0
    import re as _re
    try:
        ctx = {
            'base_salary':    float(base_salary or 0),
            'insured_salary': float(insured_salary or 0),
            'service_years':  float(service_years or 0),
        }
        if extra:
            ctx.update({k: float(v or 0) for k, v in extra.items()})
        # Replace item code references (e.g. 01, 02) with their computed amounts
        processed = formula
        if item_amounts:
            def _sub_code(m):
                code = m.group(0)
                return str(float(item_amounts[code])) if code in item_amounts else code
            processed = _re.sub(r'\b\d{2}\b', _sub_code, formula)
        from simpleeval import simple_eval
        result = float(simple_eval(processed, names=ctx))
        if result != result or abs(result) == float('inf'):  # NaN or Inf
            logging.warning(f"[FORMULA] 無效結果(NaN/Inf): formula={formula!r} ctx={ctx}")
            return 0.0
        return round(result, 2)
    except ZeroDivisionError:
        logging.error(f"[FORMULA] 除以零: formula={formula!r}")
        return 0.0
    except Exception as e:
        logging.error(f"[FORMULA] 計算錯誤: formula={formula!r} error={e}")
        return 0.0


def _calc_service_years(hire_date_str):
    if not hire_date_str: return 0.0
    from datetime import date as _d4
    try:
        hire = _d4.fromisoformat(str(hire_date_str))
        return round((_d4.today() - hire).days / 365.25, 2)
    except Exception:
        return 0.0


def _calc_punch_hours(conn, staff_id, month):
    """
    從打卡記錄計算實際工時（時薪制用）
    邏輯：每天找最早 in + 最晚 out，扣除休息時間
    回傳 (total_hours, work_days, details)
    """
    from datetime import datetime as _dth, timezone as _tzh, timedelta as _tdh
    TW = _tzh(_tdh(hours=8))

    rows = conn.execute("""
        SELECT punch_type, punched_at
        FROM punch_records
        WHERE staff_id=%s
          AND to_char(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
        ORDER BY punched_at ASC
    """, (staff_id, month)).fetchall()

    # Group by date
    day_map = {}
    for r in rows:
        pa = r['punched_at']
        if pa.tzinfo is None:
            pa = pa.replace(tzinfo=_tzh.utc)
        pa_tw  = pa.astimezone(TW)
        ds     = pa_tw.strftime('%Y-%m-%d')
        if ds not in day_map:
            day_map[ds] = []
        day_map[ds].append({'type': r['punch_type'], 'dt': pa_tw})

    # Merge cross-midnight pairs
    from datetime import date as _dateh2
    _sorted_ds  = sorted(day_map.keys())
    _merged_keys = set()
    for _i in range(len(_sorted_ds) - 1):
        _ds_cur = _sorted_ds[_i]
        _ds_nxt = _sorted_ds[_i + 1]
        if _ds_cur in _merged_keys or _ds_nxt in _merged_keys:
            continue
        try:
            if (_dateh2.fromisoformat(_ds_nxt) - _dateh2.fromisoformat(_ds_cur)).days != 1:
                continue
        except Exception:
            continue
        _cur = day_map[_ds_cur]
        _nxt = day_map[_ds_nxt]
        if (any(p['type'] == 'in'  for p in _cur)
                and not any(p['type'] == 'out' for p in _cur)
                and not any(p['type'] == 'in'  for p in _nxt)
                and any(p['type'] == 'out' for p in _nxt)):
            day_map[_ds_cur] = _cur + _nxt
            _merged_keys.add(_ds_nxt)
    for _k in _merged_keys:
        del day_map[_k]

    total_hours = 0.0
    details     = []
    for ds, punches in sorted(day_map.items()):
        ins   = [p['dt'] for p in punches if p['type'] == 'in']
        outs  = [p['dt'] for p in punches if p['type'] == 'out']
        b_out = [p['dt'] for p in punches if p['type'] == 'break_out']
        b_in  = [p['dt'] for p in punches if p['type'] == 'break_in']

        if not ins or not outs:
            continue

        work_start = min(ins)
        work_end   = max(outs)
        gross_mins = (work_end - work_start).total_seconds() / 60

        # 扣除休息時間
        break_mins = 0.0
        _available_b_in = sorted(b_in)
        for bo in sorted(b_out):
            matched = [bi for bi in _available_b_in if bi > bo]
            if matched:
                bi_used = min(matched)
                break_mins += (bi_used - bo).total_seconds() / 60
                _available_b_in.remove(bi_used)

        net_mins = max(0.0, gross_mins - break_mins)
        net_hrs  = round(net_mins / 60, 2)
        total_hours += net_hrs
        details.append({
            'date':        ds,
            'clock_in':    work_start.strftime('%H:%M'),
            'clock_out':   work_end.strftime('%H:%M'),
            'break_mins':  round(break_mins),
            'net_hours':   net_hrs,
        })

    return round(total_hours, 2), len(day_map), details


def _auto_generate_salary(conn, staff, month, work_days=None):
    """
    自動產生員工月薪資料
    ─ 月薪制：底薪 + 薪資項目公式 + 加班費 - 請假扣款
    ─ 時薪制：打卡實際工時 × 時薪 + 加班費 - 請假扣款
    """
    import calendar as _cal2
    from datetime import date as _d5, timedelta as _td5, datetime as _dts5, timezone as _tz5
    _TW5 = _tz5(_td5(hours=8))
    _today5 = _dts5.now(_TW5).date()
    y, m = int(month[:4]), int(month[5:])
    total_work_days = work_days
    scheduled_dates = set()

    _sal_cfg = _get_salary_calc_settings()

    if total_work_days is None:
        shift_date_rows = conn.execute("""
            SELECT DISTINCT shift_date FROM shift_assignments
            WHERE staff_id=%s AND TO_CHAR(shift_date,'YYYY-MM')=%s
            ORDER BY shift_date
        """, (staff['id'], month)).fetchall()
        if shift_date_rows:
            scheduled_dates = {r['shift_date'].isoformat() if hasattr(r['shift_date'], 'isoformat') else str(r['shift_date']) for r in shift_date_rows}
            total_work_days = len(scheduled_dates)
        else:
            holiday_rows = conn.execute("""
                SELECT date FROM public_holidays
                WHERE TO_CHAR(date,'YYYY-MM')=%s
            """, (month,)).fetchall()
            holiday_dates = {r['date'].isoformat() if hasattr(r['date'], 'isoformat') else str(r['date']) for r in holiday_rows}
            days_in_month = _cal2.monthrange(y, m)[1]
            for _d in range(1, days_in_month + 1):
                _dt = _d5(y, m, _d)
                _ds = _dt.isoformat()
                if _dt.weekday() < 5 and _ds not in holiday_dates:
                    scheduled_dates.add(_ds)
            total_work_days = len(scheduled_dates)

    salary_type    = staff.get('salary_type', 'monthly') or 'monthly'
    base_salary    = float(staff.get('base_salary')    or 0)
    hourly_rate    = float(staff.get('hourly_rate')    or 0)
    insured_salary = float(staff.get('insured_salary') or base_salary)
    daily_hours    = float(staff.get('daily_hours')    or 8)
    service_years  = _calc_service_years(staff.get('hire_date'))

    actual_work_hours = 0.0
    punch_work_days   = 0
    punch_details     = []
    if salary_type == 'hourly':
        actual_work_hours, punch_work_days, punch_details = _calc_punch_hours(
            conn, staff['id'], month
        )
        hourly_base_pay = round(actual_work_hours * hourly_rate, 2)
    else:
        hourly_base_pay = 0.0

    ot_rows = conn.execute("""
        SELECT COALESCE(SUM(ot_pay), 0) as total
        FROM overtime_requests
        WHERE staff_id=%s AND status='approved'
          AND to_char(request_date,'YYYY-MM')=%s
    """, (staff['id'], month)).fetchone()
    ot_pay = float(ot_rows['total']) if ot_rows else 0.0

    _month_first = f'{y}-{m:02d}-01'
    _month_last  = f'{y}-{m:02d}-{_cal2.monthrange(y, m)[1]:02d}'
    _mf_date = _d5.fromisoformat(_month_first)
    _ml_date = _d5.fromisoformat(_month_last)
    _leave_raw = conn.execute("""
        SELECT lt.pay_rate, lt.code, lt.name as leave_name,
               GREATEST(lr.start_date, %s::date) AS eff_start,
               LEAST(lr.end_date, %s::date)      AS eff_end,
               COALESCE(lr.total_hours, 0)        AS leave_hours
        FROM leave_requests lr
        JOIN leave_types lt ON lt.id = lr.leave_type_id
        WHERE lr.staff_id=%s AND lr.status='approved'
          AND lr.start_date <= %s AND lr.end_date >= %s
    """, (_month_first, _month_last, staff['id'], _month_last, _month_first)).fetchall()

    def _count_working_days(start, end):
        if not start or not end: return 0.0
        if isinstance(start, str): start = _d5.fromisoformat(start)
        if isinstance(end,   str): end   = _d5.fromisoformat(end)
        count = 0.0
        cur = start
        while cur <= end:
            if scheduled_dates:
                if cur.isoformat() in scheduled_dates:
                    count += 1.0
            else:
                if cur.weekday() < 5:
                    count += 1.0
            cur += _td5(days=1)
        return count

    _leave_wd = []
    _hourly_unpaid_hours = 0.0
    _hourly_halfpay_hours = 0.0
    for r in _leave_raw:
        lh = float(r['leave_hours'] or 0)
        is_hourly_leave = lh > 0
        if is_hourly_leave:
            _leave_wd.append({
                'pay_rate':      float(r['pay_rate']),
                'code':          r['code'],
                'leave_name':    r['leave_name'],
                'days_in_month': 0.0,
                'is_hourly':     True,
                'hours':         lh,
            })
            if float(r['pay_rate']) < 0.001:
                _hourly_unpaid_hours += lh
            elif 0.001 <= float(r['pay_rate']) <= 0.999:
                _hourly_halfpay_hours += lh
        else:
            wd = _count_working_days(r['eff_start'], r['eff_end'])
            _leave_wd.append({
                'pay_rate':      float(r['pay_rate']),
                'code':          r['code'],
                'leave_name':    r['leave_name'],
                'days_in_month': wd,
                'is_hourly':     False,
                'hours':         0.0,
            })

    leave_days    = sum(x['days_in_month'] for x in _leave_wd)
    unpaid_days   = sum(x['days_in_month'] for x in _leave_wd if float(x['pay_rate']) < 0.001)
    half_pay_days = sum(x['days_in_month'] for x in _leave_wd if 0.001 <= float(x['pay_rate']) <= 0.999)
    personal_days = sum(x['days_in_month'] for x in _leave_wd if x['code'] == 'personal')
    sick_days     = sum(x['days_in_month'] for x in _leave_wd if x['code'] == 'sick')
    whole_day_leave_days = leave_days
    leave_rows    = _leave_wd
    if salary_type == 'hourly':
        actual_days = max(0.0, float(punch_work_days) - leave_days)
    else:
        actual_days = max(0.0, total_work_days - leave_days)

    if salary_type == 'hourly':
        daily_wage  = hourly_rate * daily_hours
        hourly_wage = hourly_rate
    else:
        daily_wage  = base_salary / 30 if base_salary > 0 else 0
        hourly_wage = daily_wage / daily_hours if daily_hours > 0 else 0

    absent_days = 0
    _absent_date_list = []
    if _sal_cfg['auto_absent_deduction'] and salary_type == 'monthly' and scheduled_dates and daily_wage > 0:
        _punch_rows2 = conn.execute("""
            SELECT DISTINCT (punched_at AT TIME ZONE 'Asia/Taipei')::date as work_date
            FROM punch_records WHERE staff_id=%s
              AND TO_CHAR(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
        """, (staff['id'], month)).fetchall()
        _punched_dates2 = {
            r['work_date'].isoformat() if hasattr(r['work_date'], 'isoformat') else str(r['work_date'])
            for r in _punch_rows2
        }
        _mf2 = _d5.fromisoformat(_month_first)
        _ml2 = _d5.fromisoformat(_month_last)
        _leave_date_rows2 = conn.execute("""
            SELECT start_date, end_date FROM leave_requests
            WHERE staff_id=%s AND status='approved'
              AND start_date <= %s AND end_date >= %s
        """, (staff['id'], _month_last, _month_first)).fetchall()
        _leave_date_set2 = set()
        for _lr2 in _leave_date_rows2:
            _ld2 = _lr2['start_date']
            _le2 = _lr2['end_date']
            if isinstance(_ld2, str): _ld2 = _d5.fromisoformat(_ld2)
            if isinstance(_le2, str): _le2 = _d5.fromisoformat(_le2)
            _ld2 = max(_ld2, _mf2)
            _le2 = min(_le2, _ml2)
            while _ld2 <= _le2:
                _leave_date_set2.add(_ld2.isoformat())
                _ld2 += _td5(days=1)
        _absent_date_list = sorted(
            ds for ds in scheduled_dates
            if ds not in _punched_dates2 and ds not in _leave_date_set2
               and _d5.fromisoformat(ds) < _today5
        )
        absent_days = len(_absent_date_list)

    _formula_extra = {
        'actual_days':          max(0.0, actual_days - absent_days),
        'work_days':            float(total_work_days),
        'leave_days':           leave_days,
        'whole_day_leave_days': whole_day_leave_days,
        'unpaid_days':          unpaid_days,
        'personal_days':        personal_days,
        'sick_days':            sick_days,
        'daily_wage':           daily_wage,
    }

    items           = []
    allowance_total = 0.0
    deduction_total = 0.0
    _item_amounts_by_code = {}  # accumulates code -> amount for cross-item formula references
    _overrides = staff.get('salary_item_overrides') or {}
    if isinstance(_overrides, str):
        try: _overrides = _json.loads(_overrides)
        except Exception: _overrides = {}

    def _apply_override(item_id, calculated_amt):
        key = str(item_id)
        if key in _overrides and _overrides[key] is not None and _overrides[key] != '':
            return float(_overrides[key]), True
        return calculated_amt, False

    if salary_type == 'hourly':
        items.append({
            'id': 'hourly_base', 'name': '本薪（工時）', 'type': 'allowance',
            'amount': hourly_base_pay, 'formula': '',
            'calc_note': (
                f'{actual_work_hours}h × 時薪${hourly_rate}'
                + (f'（{len(punch_details)}天出勤）' if punch_details else '')
            ),
        })
        allowance_total += hourly_base_pay

        if ot_pay == 0 and actual_work_hours > 0:
            rate1 = float(staff.get('ot_rate1') or 1.33)
            rate2 = float(staff.get('ot_rate2') or 1.67)
            rate3 = float(staff.get('ot_rate3') or 2.0)
            for pd in punch_details:
                overtime_h = max(0.0, pd['net_hours'] - daily_hours)
                if overtime_h > 0:
                    h1 = min(overtime_h, 2.0)
                    h2 = min(max(0.0, overtime_h - 2.0), 2.0)
                    h3 = max(0.0, overtime_h - 4.0)
                    ot_pay += round(hourly_rate * (h1 * rate1 + h2 * rate2 + h3 * rate3), 2)

        if insured_salary == 0:
            insured_salary = round(hourly_rate * daily_hours * 30, 0)

        staff_item_ids = staff.get('salary_item_ids')
        if staff_item_ids:
            placeholders = ','.join(['%s'] * len(staff_item_ids))
            salary_items_rows = conn.execute(f"""
                SELECT * FROM salary_items
                WHERE active=TRUE AND id IN ({placeholders})
                  AND item_type='deduction'
                  AND (formula LIKE '%insured_salary%' OR formula LIKE '%base_salary%')
                ORDER BY sort_order, id
            """, staff_item_ids).fetchall()
        else:
            salary_items_rows = conn.execute("""
                SELECT * FROM salary_items
                WHERE active=TRUE
                  AND item_type='deduction'
                  AND (formula LIKE '%insured_salary%' OR formula LIKE '%base_salary%')
                ORDER BY sort_order, id
            """).fetchall()
        for it in salary_items_rows:
            calc_amt = _eval_formula(it['formula'] or '', base_salary or insured_salary,
                                     insured_salary, service_years, _formula_extra, _item_amounts_by_code)
            amt, overridden = _apply_override(it['id'], calc_amt)
            note = f'手動設定 ${amt}' if overridden else (it['formula'] or '')
            items.append({
                'id': it['id'], 'name': it['name'], 'type': 'deduction',
                'amount': round(amt, 2), 'formula': it['formula'] or '',
                'calc_note': note,
            })
            if it.get('code'):
                _item_amounts_by_code[it['code']] = round(amt, 2)
            deduction_total += amt

    else:
        staff_item_ids = staff.get('salary_item_ids')
        if staff_item_ids:
            placeholders = ','.join(['%s'] * len(staff_item_ids))
            items_rows = conn.execute(
                f"SELECT * FROM salary_items WHERE active=TRUE AND id IN ({placeholders}) ORDER BY sort_order, id",
                staff_item_ids
            ).fetchall()
        else:
            items_rows = conn.execute(
                "SELECT * FROM salary_items WHERE active=TRUE ORDER BY sort_order, id"
            ).fetchall()
        for it in items_rows:
            formula  = it['formula'] or ''
            calc_amt = float(it['amount'] or 0)
            if formula:
                calc_amt = _eval_formula(formula, base_salary, insured_salary, service_years, _formula_extra, _item_amounts_by_code)
            amt, overridden = _apply_override(it['id'], calc_amt)
            note = f'手動設定 ${amt}' if overridden else formula
            items.append({
                'id':        it['id'],
                'name':      it['name'],
                'type':      it['item_type'],
                'amount':    round(amt, 2),
                'formula':   formula,
                'calc_note': note,
            })
            if it.get('code'):
                _item_amounts_by_code[it['code']] = round(amt, 2)
            if it['item_type'] == 'allowance':
                allowance_total += amt
            else:
                deduction_total += amt

    if ot_pay > 0:
        items.append({
            'id': 'ot', 'name': '加班費（申請）', 'type': 'allowance',
            'amount': round(ot_pay, 2), 'formula': '',
            'calc_note': '核准加班費合計',
        })
        allowance_total += ot_pay

    if _sal_cfg['auto_leave_deduction']:
        if unpaid_days > 0 and daily_wage > 0:
            leave_names = '、'.join(set(
                r['leave_name'] for r in leave_rows
                if float(r['pay_rate']) < 0.001 and not r['is_hourly']
            ))
            deduct = round(daily_wage * unpaid_days, 2)
            items.append({
                'id': 'unpaid', 'name': f'無薪假扣款（{leave_names}）', 'type': 'deduction',
                'amount': deduct, 'formula': '',
                'calc_note': f'{unpaid_days}天 × 日薪${round(daily_wage, 0)}',
            })
            deduction_total += deduct

        if _hourly_unpaid_hours > 0 and hourly_wage > 0:
            leave_names = '、'.join(set(
                r['leave_name'] for r in leave_rows
                if float(r['pay_rate']) < 0.001 and r['is_hourly']
            ))
            deduct = round(hourly_wage * _hourly_unpaid_hours, 2)
            items.append({
                'id': 'unpaid_hourly', 'name': f'無薪假扣款-小時（{leave_names}）', 'type': 'deduction',
                'amount': deduct, 'formula': '',
                'calc_note': f'{_hourly_unpaid_hours}h × 時薪${round(hourly_wage, 1)}',
            })
            deduction_total += deduct

        if half_pay_days > 0 and daily_wage > 0:
            leave_names = '、'.join(set(
                r['leave_name'] for r in leave_rows
                if 0.001 <= float(r['pay_rate']) <= 0.999 and not r['is_hourly']
            ))
            deduct = round(daily_wage * half_pay_days * 0.5, 2)
            items.append({
                'id': 'halfpay', 'name': f'半薪假扣款（{leave_names}）', 'type': 'deduction',
                'amount': deduct, 'formula': '',
                'calc_note': f'{half_pay_days}天 × 日薪${round(daily_wage, 0)} × 0.5',
            })
            deduction_total += deduct

        if _hourly_halfpay_hours > 0 and hourly_wage > 0:
            leave_names = '、'.join(set(
                r['leave_name'] for r in leave_rows
                if 0.001 <= float(r['pay_rate']) <= 0.999 and r['is_hourly']
            ))
            deduct = round(hourly_wage * _hourly_halfpay_hours * 0.5, 2)
            items.append({
                'id': 'halfpay_hourly', 'name': f'半薪假扣款-小時（{leave_names}）', 'type': 'deduction',
                'amount': deduct, 'formula': '',
                'calc_note': f'{_hourly_halfpay_hours}h × 時薪${round(hourly_wage, 1)} × 0.5',
            })
            deduction_total += deduct

    if absent_days > 0:
        deduct = round(daily_wage * absent_days, 2)
        sample = '、'.join(_absent_date_list[:3]) + ('等' if absent_days > 3 else '')
        items.append({
            'id': 'absent', 'name': f'缺勤扣款（{absent_days} 天）', 'type': 'deduction',
            'amount': deduct, 'formula': '',
            'calc_note': f'{absent_days} 天 × 日薪 ${round(daily_wage, 0)}（{sample}）',
        })
        deduction_total += deduct

    _TAX_THRESHOLD = 88501
    _existing_tax = sum(
        float(it['amount']) for it in items
        if isinstance(it.get('id'), str) and it['id'] == 'income_tax'
    )
    income_tax_withheld = 0.0
    if _sal_cfg['auto_income_tax'] and _existing_tax == 0 and allowance_total > _TAX_THRESHOLD:
        income_tax_withheld = round(allowance_total * 0.05, 0)
        items.append({
            'id': 'income_tax', 'name': '薪資所得扣繳稅款', 'type': 'deduction',
            'amount': income_tax_withheld, 'formula': '',
            'calc_note': f'總支給 ${round(allowance_total,0):,.0f} × 5%（超過起徵點 ${_TAX_THRESHOLD:,}）',
        })
        deduction_total += income_tax_withheld
    else:
        income_tax_withheld = _existing_tax

    net_pay = round(allowance_total - deduction_total, 2)

    return {
        'staff_id':              staff['id'],
        'month':                 month,
        'salary_type':           salary_type,
        'base_salary':           base_salary if salary_type == 'monthly' else 0,
        'hourly_rate':           hourly_rate if salary_type == 'hourly' else 0,
        'hourly_base_pay':       hourly_base_pay if salary_type == 'hourly' else 0,
        'actual_work_hours':     actual_work_hours if salary_type == 'hourly' else 0,
        'insured_salary':        insured_salary,
        'work_days':             total_work_days,
        'actual_days':           max(0, actual_days - absent_days),
        'leave_days':            leave_days,
        'unpaid_days':           unpaid_days,
        'absent_days':           absent_days,
        'whole_day_leave_days':  whole_day_leave_days,
        'ot_pay':                ot_pay,
        'allowance_total':       round(allowance_total, 2),
        'deduction_total':       round(deduction_total, 2),
        'net_pay':               net_pay,
        'income_tax_withheld':   income_tax_withheld,
        'items':                 items,
        'punch_details':         punch_details,
        'status':                'draft',
    }


# ─── Employee endpoint ───────────────────────────────────────────────────────

@bp.route('/api/salary/my-payslip', methods=['GET'])
def api_my_payslip():
    sid = session.get('punch_staff_id')
    if not sid:
        return jsonify({'error': '請先登入'}), 401
    month = request.args.get('month', '')
    if not month:
        from datetime import date as _dp
        month = _dp.today().strftime('%Y-%m')
    with get_db() as conn:
        row = conn.execute("""
            SELECT sr.*, ps.name as staff_name, ps.role as staff_role,
                   ps.employee_code, ps.department, ps.salary_type, ps.hourly_rate
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id = sr.staff_id
            WHERE sr.staff_id = %s AND sr.month = %s
        """, (sid, month)).fetchone()
        if not row:
            staff = conn.execute("SELECT * FROM punch_staff WHERE id=%s", (sid,)).fetchone()
            if not staff:
                return jsonify({'error': '找不到員工資料'}), 404
            data = _auto_generate_salary(conn, dict(staff), month)
            data['staff_name']    = staff['name']
            data['staff_role']    = staff['role'] or ''
            data['employee_code'] = staff['employee_code'] or ''
            data['department']    = staff['department'] or ''
            data['is_preview']    = True
            return jsonify(data)
    d = salary_record_row(row)
    d['staff_name']    = row['staff_name']
    d['staff_role']    = row['staff_role']
    d['employee_code'] = row['employee_code'] or ''
    d['department']    = row['department']    or ''
    d['salary_type']   = row['salary_type']   or 'monthly'
    d['hourly_rate']   = float(row['hourly_rate'] or 0)
    d['is_preview']    = False
    return jsonify(d)


# ─── Salary Calc Settings ────────────────────────────────────────────────────

@bp.route('/api/salary/calc-settings', methods=['GET'])
@require_module('salary')
def api_salary_calc_settings_get():
    return jsonify(_get_salary_calc_settings())


@bp.route('/api/salary/calc-settings', methods=['POST'])
@require_module('salary')
def api_salary_calc_settings_post():
    b = request.get_json(force=True) or {}
    allowed = {'auto_leave_deduction', 'auto_absent_deduction', 'auto_income_tax'}
    with get_db() as conn:
        for key in allowed:
            if key in b:
                val = 'true' if b[key] else 'false'
                conn.execute(
                    "INSERT INTO salary_calc_settings(setting_key,setting_value) VALUES(%s,%s)"
                    " ON CONFLICT(setting_key) DO UPDATE SET setting_value=EXCLUDED.setting_value",
                    (key, val)
                )
    return jsonify({'ok': True, 'settings': _get_salary_calc_settings()})


# ─── Salary Items CRUD ───────────────────────────────────────────────────────

@bp.route('/api/salary/items', methods=['GET'])
@require_module('salary')
def api_salary_items_list():
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM salary_items ORDER BY sort_order, id").fetchall()
    return jsonify([salary_item_row(r) for r in rows])


@bp.route('/api/salary/items', methods=['POST'])
@require_module('salary')
def api_salary_item_create():
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            INSERT INTO salary_items (name, item_type, formula, amount, description, color, sort_order, code)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *
        """, (b['name'], b.get('item_type','allowance'), b.get('formula',''),
              float(b.get('amount',0)), b.get('description',''),
              b.get('color','#4a7bda'), int(b.get('sort_order',0)),
              b.get('code',''))).fetchone()
    return jsonify(salary_item_row(row)), 201


@bp.route('/api/salary/items/<int:iid>', methods=['PUT'])
@require_module('salary')
def api_salary_item_update(iid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE salary_items SET name=%s, item_type=%s, formula=%s, amount=%s,
              description=%s, color=%s, sort_order=%s, active=%s, code=%s
            WHERE id=%s RETURNING *
        """, (b['name'], b.get('item_type','allowance'), b.get('formula',''),
              float(b.get('amount',0)), b.get('description',''),
              b.get('color','#4a7bda'), int(b.get('sort_order',0)),
              bool(b.get('active',True)), b.get('code',''), iid)).fetchone()
    return jsonify(salary_item_row(row)) if row else ('', 404)


@bp.route('/api/salary/items/<int:iid>', methods=['DELETE'])
@require_module('salary')
def api_salary_item_delete(iid):
    with get_db() as conn:
        conn.execute("DELETE FROM salary_items WHERE id=%s", (iid,))
    return jsonify({'deleted': iid})


# ─── Salary Records ──────────────────────────────────────────────────────────

@bp.route('/api/salary/records', methods=['GET'])
@require_module('salary')
def api_salary_records_list():
    month = request.args.get('month', '')
    if not month:
        from datetime import date as _d6
        month = _d6.today().strftime('%Y-%m')
    with get_db() as conn:
        rows = conn.execute("""
            SELECT sr.*, ps.name as staff_name, ps.role as staff_role,
                   ps.employee_code, ps.department
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id=sr.staff_id
            WHERE sr.month=%s
            ORDER BY ps.name
        """, (month,)).fetchall()
    result = []
    for r in rows:
        d = salary_record_row(r)
        d['staff_name']    = r['staff_name']
        d['staff_role']    = r['staff_role']
        d['employee_code'] = r['employee_code'] or ''
        d['department']    = r['department'] or ''
        result.append(d)
    return jsonify(result)


@bp.route('/api/salary/records/generate', methods=['POST'])
@require_module('salary')
def api_salary_generate():
    """自動產生或更新該月薪資"""
    b     = request.get_json(force=True)
    month = b.get('month', '').strip()
    force = bool(b.get('force', False))
    if not month: return jsonify({'error': '請指定月份'}), 400
    with get_db() as conn:
        try:
            month_int = int(month.replace('-', ''))
        except ValueError:
            return jsonify({'error': '月份格式錯誤，請使用 YYYY-MM'}), 400
        lock_key = 4_200_000_000 + month_int
        locked = conn.execute(
            "SELECT pg_try_advisory_xact_lock(%s) AS locked", (lock_key,)
        ).fetchone()['locked']
        if not locked:
            return jsonify({'error': f'{month} 薪資正在產生中，請稍後再試'}), 409

        staff_list = conn.execute(
            "SELECT * FROM punch_staff WHERE active=TRUE"
        ).fetchall()
        generated = 0
        skipped   = 0
        for staff in staff_list:
            if not force:
                existing = conn.execute(
                    "SELECT status FROM salary_records WHERE staff_id=%s AND month=%s",
                    (staff['id'], month)
                ).fetchone()
                if existing and existing['status'] == 'confirmed':
                    skipped += 1
                    continue

            data = _auto_generate_salary(conn, dict(staff), month)
            items_json = _json.dumps(data['items'], ensure_ascii=False)
            conn.execute("""
                INSERT INTO salary_records
                  (staff_id, month, base_salary, insured_salary, work_days, actual_days,
                   leave_days, unpaid_days, ot_pay, allowance_total, deduction_total,
                   net_pay, income_tax_withheld, items, status, updated_at,
                   absent_days, whole_day_leave_days, hourly_base_pay, actual_work_hours)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,'draft',NOW(),%s,%s,%s,%s)
                ON CONFLICT (staff_id, month) DO UPDATE
                  SET base_salary=%s, insured_salary=%s, work_days=%s, actual_days=%s,
                      leave_days=%s, unpaid_days=%s, ot_pay=%s, allowance_total=%s,
                      deduction_total=%s, net_pay=%s, income_tax_withheld=%s, items=%s::jsonb,
                      absent_days=%s, whole_day_leave_days=%s, hourly_base_pay=%s, actual_work_hours=%s,
                      status='draft', updated_at=NOW()
            """, (
                data['staff_id'], month, data['base_salary'], data['insured_salary'],
                data['work_days'], data['actual_days'], data['leave_days'], data['unpaid_days'],
                data['ot_pay'], data['allowance_total'], data['deduction_total'],
                data['net_pay'], data['income_tax_withheld'], items_json,
                data.get('absent_days', 0), data.get('whole_day_leave_days', 0),
                data.get('hourly_base_pay', 0), data.get('actual_work_hours', 0),
                data['base_salary'], data['insured_salary'], data['work_days'], data['actual_days'],
                data['leave_days'], data['unpaid_days'], data['ot_pay'], data['allowance_total'],
                data['deduction_total'], data['net_pay'], data['income_tax_withheld'], items_json,
                data.get('absent_days', 0), data.get('whole_day_leave_days', 0),
                data.get('hourly_base_pay', 0), data.get('actual_work_hours', 0),
            ))
            generated += 1
    return jsonify({'ok': True, 'generated': generated, 'skipped': skipped, 'month': month})


@bp.route('/api/salary/records/preview', methods=['POST'])
@require_module('salary')
def api_salary_preview():
    """預覽薪資計算結果（不儲存）"""
    b     = request.get_json(force=True) or {}
    month = b.get('month', '').strip()
    if not month:
        return jsonify({'error': '請指定月份'}), 400
    result = []
    try:
        with get_db() as conn:
            staff_list = conn.execute(
                "SELECT * FROM punch_staff WHERE active=TRUE ORDER BY name"
            ).fetchall()
            for staff in staff_list:
                data = _auto_generate_salary(conn, dict(staff), month)
                punch_days = conn.execute("""
                    SELECT COUNT(DISTINCT (punched_at AT TIME ZONE 'Asia/Taipei')::date) AS n
                    FROM punch_records WHERE staff_id=%s
                      AND to_char(punched_at AT TIME ZONE 'Asia/Taipei','YYYY-MM')=%s
                """, (staff['id'], month)).fetchone()['n']
                approved_ot = conn.execute("""
                    SELECT COUNT(*) AS n, COALESCE(SUM(ot_hours),0) AS hrs
                    FROM overtime_requests WHERE staff_id=%s
                      AND status='approved'
                      AND to_char(request_date,'YYYY-MM')=%s
                """, (staff['id'], month)).fetchone()
                result.append({
                    'staff_id':        data['staff_id'],
                    'staff_name':      staff['name'],
                    'department':      staff['department'],
                    'salary_type':     staff['salary_type'],
                    'punch_days':      punch_days,
                    'work_days':       float(data['work_days']),
                    'actual_days':     float(data['actual_days']),
                    'leave_days':      float(data['leave_days']),
                    'unpaid_days':     float(data['unpaid_days']),
                    'ot_count':        int(approved_ot['n']),
                    'ot_hours':        float(approved_ot['hrs']),
                    'ot_pay':          float(data['ot_pay']),
                    'base_salary':     float(data['base_salary']),
                    'allowance_total': float(data['allowance_total']),
                    'deduction_total': float(data['deduction_total']),
                    'net_pay':         float(data['net_pay']),
                })
    except Exception as e:
        import traceback
        print(f"[salary_preview] ERROR: {e}\n{traceback.format_exc()}")
        return jsonify({'ok': False, 'error': f'計算失敗：{e}'}), 500
    return jsonify({'ok': True, 'month': month, 'records': result})


@bp.route('/api/salary/records/<int:rid>', methods=['GET'])
@require_module('salary')
def api_salary_record_get(rid):
    with get_db() as conn:
        row = conn.execute("""
            SELECT sr.*, ps.name as staff_name, ps.role as staff_role,
                   ps.employee_code, ps.department, ps.hire_date
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id=sr.staff_id
            WHERE sr.id=%s
        """, (rid,)).fetchone()
    if not row: return ('', 404)
    d = salary_record_row(row)
    d['staff_name']    = row['staff_name']
    d['staff_role']    = row['staff_role']
    d['employee_code'] = row['employee_code'] or ''
    d['department']    = row['department'] or ''
    d['hire_date']     = row['hire_date'].isoformat() if row['hire_date'] else ''
    return jsonify(d)


@bp.route('/api/salary/records/<int:rid>', methods=['PUT'])
@require_module('salary')
def api_salary_record_update(rid):
    b = request.get_json(force=True)
    items = b.get('items', [])
    items_json = _json.dumps(items, ensure_ascii=False)
    tax_withheld = sum(
        float(it.get('amount', 0)) for it in items
        if it.get('type') == 'deduction' and (
            it.get('id') == 'income_tax' or '扣繳' in (it.get('name') or '')
        )
    )
    with get_db() as conn:
        row = conn.execute("""
            UPDATE salary_records SET
              allowance_total=%s, deduction_total=%s, net_pay=%s,
              income_tax_withheld=%s, items=%s::jsonb, note=%s, updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (float(b.get('allowance_total',0)), float(b.get('deduction_total',0)),
              float(b.get('net_pay',0)), tax_withheld, items_json,
              b.get('note',''), rid)).fetchone()
    return jsonify(salary_record_row(row)) if row else ('', 404)


@bp.route('/api/salary/records/confirm-all', methods=['POST'])
@require_module('salary')
def api_salary_confirm_all():
    b    = request.get_json(force=True)
    month = b.get('month','').strip()
    by   = b.get('confirmed_by','管理員')
    if not month: return jsonify({'error': '請指定月份'}), 400
    with get_db() as conn:
        rows = conn.execute("""
            UPDATE salary_records SET status='confirmed', confirmed_by=%s,
              confirmed_at=NOW(), updated_at=NOW()
            WHERE month=%s AND status='draft'
            RETURNING id, staff_id, month, net_pay
        """, (by, month)).fetchall()
    confirmed = len(rows)
    for row in rows:
        extra = f"{row['month']} 薪資已確認\n實領金額：${float(row['net_pay'] or 0):,.0f}"
        _notify_review_result(row['staff_id'], '薪資', 'confirmed', extra)
    return jsonify({'ok': True, 'confirmed': confirmed})


@bp.route('/api/salary/records/<int:rid>/confirm', methods=['POST'])
@require_module('salary')
def api_salary_confirm(rid):
    b = request.get_json(force=True)
    with get_db() as conn:
        row = conn.execute("""
            UPDATE salary_records SET status='confirmed', confirmed_by=%s,
              confirmed_at=NOW(), updated_at=NOW()
            WHERE id=%s RETURNING *
        """, (b.get('confirmed_by','管理員'), rid)).fetchone()
    if row:
        extra = f"{row['month']} 薪資已確認\n實領金額：${float(row['net_pay'] or 0):,.0f}"
        _notify_review_result(row['staff_id'], '薪資', 'confirmed', extra)
    return jsonify(salary_record_row(row)) if row else ('', 404)


@bp.route('/api/salary/records/<int:rid>', methods=['DELETE'])
@require_module('salary')
def api_salary_record_delete(rid):
    with get_db() as conn:
        conn.execute("DELETE FROM salary_records WHERE id=%s", (rid,))
    return jsonify({'deleted': rid})


# ─── Salary Staff Settings ───────────────────────────────────────────────────

@bp.route('/api/salary/staff', methods=['GET'])
@require_module('salary')
def api_salary_staff_list():
    with get_db() as conn:
        rows = conn.execute("""
            SELECT id, name, username, role, active, employee_code, department,
                   position_title, hire_date, birth_date, base_salary, insured_salary,
                   daily_hours, ot_rate1, ot_rate2, salary_type, hourly_rate,
                   vacation_quota, salary_notes, salary_item_ids, salary_item_overrides,
                   national_id, gender, insurance_type, address
            FROM punch_staff ORDER BY name
        """).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        for f in ['base_salary','insured_salary','daily_hours','ot_rate1','ot_rate2','hourly_rate']:
            if d.get(f) is not None: d[f] = float(d[f])
        if d.get('hire_date'):  d['hire_date']  = d['hire_date'].isoformat()
        if d.get('birth_date'): d['birth_date'] = d['birth_date'].isoformat()
        d['annual_leave_days'] = _calc_annual_leave_days(d.get('hire_date'))
        d['service_years']     = _calc_service_years(d.get('hire_date'))
        result.append(d)
    return jsonify(result)


@bp.route('/api/salary/staff/<int:sid>', methods=['PUT'])
@require_module('salary')
def api_salary_staff_update(sid):
    b = request.get_json(force=True)
    def _f(k, default=0): return float(b.get(k, default) or default)
    def _s(k): return b.get(k, '').strip() if b.get(k) else None
    with get_db() as conn:
        salary_item_ids = b.get('salary_item_ids')
        salary_item_ids_json = _json.dumps(salary_item_ids) if salary_item_ids is not None else None
        overrides = b.get('salary_item_overrides')
        overrides_json = _json.dumps(overrides) if overrides else None
        _old = conn.execute(
            "SELECT base_salary, insured_salary, daily_hours, hourly_rate, salary_type, ot_rate1, ot_rate2 FROM punch_staff WHERE id=%s", (sid,)
        ).fetchone()
        conn.execute("""
            UPDATE punch_staff SET
              employee_code=%s, department=%s, position_title=%s,
              hire_date=%s, birth_date=%s,
              base_salary=%s, insured_salary=%s, daily_hours=%s,
              ot_rate1=%s, ot_rate2=%s, salary_type=%s,
              hourly_rate=%s, vacation_quota=%s, salary_notes=%s,
              salary_item_ids=%s, salary_item_overrides=%s,
              national_id=%s, gender=%s, insurance_type=%s, address=%s
            WHERE id=%s
        """, (_s('employee_code'), _s('department'), _s('position_title'),
              _s('hire_date'), _s('birth_date'),
              _f('base_salary'), _f('insured_salary'), _f('daily_hours') or 8,
              _f('ot_rate1') or 1.33, _f('ot_rate2') or 1.67,
              b.get('salary_type','monthly'),
              _f('hourly_rate'), b.get('vacation_quota') or None,
              b.get('salary_notes',''), salary_item_ids_json, overrides_json,
              (b.get('national_id') or '').strip(),
              (b.get('gender') or '').strip(),
              (b.get('insurance_type') or 'regular').strip(),
              (b.get('address') or '').strip(),
              sid))
        _salary_keys = ('base_salary','insured_salary','daily_hours','hourly_rate','salary_type','ot_rate1','ot_rate2')
        if _old and any(
            str(b.get(k, '')) != str(float(_old[k] or 0) if k != 'salary_type' else (_old[k] or 'monthly'))
            for k in _salary_keys if k in b
        ):
            conn.execute("DELETE FROM salary_records WHERE staff_id=%s AND status='draft'", (sid,))
        row = conn.execute("SELECT * FROM punch_staff WHERE id=%s", (sid,)).fetchone()
    return jsonify(punch_staff_row(row)) if row else ('', 404)


# ─── Formula Preview ─────────────────────────────────────────────────────────

@bp.route('/api/salary/formula/preview', methods=['POST'])
@require_module('salary')
def api_formula_preview():
    """即時預覽公式計算結果"""
    b             = request.get_json(force=True)
    formula       = b.get('formula', '').strip()
    base_salary   = float(b.get('base_salary', 30000))
    insured_salary= float(b.get('insured_salary', 30000))
    service_years = float(b.get('service_years', 1))
    extra = {
        'actual_days':          float(b.get('actual_days', 22)),
        'work_days':            float(b.get('work_days', 22)),
        'leave_days':           float(b.get('leave_days', 0)),
        'unpaid_days':          float(b.get('unpaid_days', 0)),
        'whole_day_leave_days': float(b.get('whole_day_leave_days', 0)),
        'personal_days':        float(b.get('personal_days', 0)),
        'sick_days':            float(b.get('sick_days', 0)),
        'daily_wage':           base_salary / 30 if base_salary > 0 else 0,
    }

    if not formula:
        return jsonify({'result': 0, 'error': None})
    try:
        # Pre-calculate all items in order so code references (e.g. 01) resolve correctly
        preview_amounts = {}
        with get_db() as _conn:
            _all_items = _conn.execute(
                "SELECT * FROM salary_items WHERE active=TRUE ORDER BY sort_order, id"
            ).fetchall()
        for _it in _all_items:
            _f = _it['formula'] or ''
            _amt = float(_it['amount'] or 0)
            if _f:
                _amt = _eval_formula(_f, base_salary, insured_salary, service_years, extra, preview_amounts)
            if _it.get('code'):
                preview_amounts[_it['code']] = round(_amt, 2)
        result = _eval_formula(formula, base_salary, insured_salary, service_years, extra, preview_amounts)
        return jsonify({'result': round(result, 2), 'error': None})
    except Exception as e:
        return jsonify({'result': None, 'error': str(e)})


# ─── Salary PDF ──────────────────────────────────────────────────────────────

@bp.route('/api/salary/records/<int:rid>/pdf', methods=['GET'])
@require_module('salary')
def api_salary_pdf(rid):
    """回傳薪資單 HTML（供瀏覽器列印/另存 PDF）"""
    if not session.get('logged_in'):
        sid = session.get('punch_staff_id')
        if not sid:
            return '未登入', 401
    with get_db() as conn:
        row = conn.execute("""
            SELECT sr.*, ps.name as staff_name, ps.employee_code,
                   ps.department, ps.role, ps.salary_type,
                   ps.hourly_rate, ps.hire_date
            FROM salary_records sr
            JOIN punch_staff ps ON ps.id = sr.staff_id
            WHERE sr.id = %s
        """, (rid,)).fetchone()
    if not row:
        return '找不到薪資記錄', 404
    if not session.get('logged_in'):
        if row['staff_id'] != session.get('punch_staff_id'):
            return '無權限', 403

    d         = salary_record_row(row)
    items     = d.get('items') or []
    allow_items  = [i for i in items if i.get('type') == 'allowance']
    deduct_items = [i for i in items if i.get('type') == 'deduction']
    is_hourly = (row['salary_type'] == 'hourly')

    def money(v):
        try: return f"${float(v):,.0f}"
        except: return '$0'

    def esc_h(s):
        return str(s or '').replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

    allow_rows = ''.join(f"""
        <tr>
          <td>{esc_h(i['name'])}</td>
          <td class="num green">{money(i['amount'])}</td>
          <td class="note">{esc_h(i.get('calc_note',''))}</td>
        </tr>""" for i in allow_items)

    deduct_rows = ''.join(f"""
        <tr>
          <td>{esc_h(i['name'])}</td>
          <td class="num red">-{money(i['amount'])}</td>
          <td class="note">{esc_h(i.get('calc_note',''))}</td>
        </tr>""" for i in deduct_items)

    punch_table = ''
    if is_hourly and d.get('punch_details'):
        punch_rows = ''.join(f"""
            <tr>
              <td>{p['date']}</td>
              <td>{p['clock_in']}</td>
              <td>{p['clock_out']}</td>
              <td>{p.get('break_mins',0)} min</td>
              <td class="num">{p['net_hours']} h</td>
            </tr>""" for p in d['punch_details'])
        punch_table = f"""
        <h3>每日工時明細</h3>
        <table>
          <thead><tr><th>日期</th><th>上班</th><th>下班</th><th>休息</th><th>工時</th></tr></thead>
          <tbody>{punch_rows}</tbody>
          <tfoot><tr><td colspan="4"><strong>合計</strong></td><td class="num"><strong>{d.get('actual_work_hours',0)} h</strong></td></tr></tfoot>
        </table>"""

    status_str = '已確認' if row['status'] == 'confirmed' else '草稿（未確認）'
    sal_type   = '時薪制' if is_hourly else '月薪制'
    attend_str = (f"實際工時 {d.get('actual_work_hours',0)}h × 時薪 ${float(row['hourly_rate'] or 0):,.0f}"
                  if is_hourly else
                  f"出勤 {d.get('actual_days',0)} 天 / 工作日 {d.get('work_days',0)} 天")
    if float(d.get('leave_days',0)) > 0:
        attend_str += f"，請假 {d.get('leave_days',0)} 天"
    if float(d.get('unpaid_days',0)) > 0:
        attend_str += f"（無薪 {d.get('unpaid_days',0)} 天）"

    html = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<title>薪資單 {esc_h(row['staff_name'])} {esc_h(row['month'])}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Noto Sans TC', 'PingFang TC', 'Microsoft JhengHei', sans-serif;
          font-size: 13px; color: #1a2340; background: #fff; padding: 32px; }}
  .header {{ display: flex; justify-content: space-between; align-items: flex-start;
             border-bottom: 3px solid #1a2340; padding-bottom: 16px; margin-bottom: 24px; }}
  .company {{ font-size: 20px; font-weight: 800; color: #1a2340; }}
  .slip-title {{ font-size: 14px; color: #666; margin-top: 4px; }}
  .staff-info {{ font-size: 12px; color: #444; text-align: right; line-height: 1.8; }}
  .summary {{ display: grid; grid-template-columns: repeat(3,1fr); gap: 12px; margin-bottom: 24px; }}
  .sum-card {{ border: 1.5px solid #e2e8f0; border-radius: 8px; padding: 12px 16px; text-align: center; }}
  .sum-label {{ font-size: 10px; color: #888; margin-bottom: 4px; text-transform: uppercase; letter-spacing: .06em; }}
  .sum-val {{ font-size: 22px; font-weight: 800; font-family: 'DM Mono', monospace; }}
  .sum-val.green {{ color: #2e9e6b; }}
  .sum-val.red   {{ color: #d64242; }}
  .sum-val.navy  {{ color: #1a2340; }}
  .attend {{ background: #f8fafc; border-radius: 6px; padding: 8px 14px;
             font-size: 12px; color: #666; margin-bottom: 20px; }}
  h3 {{ font-size: 12px; font-weight: 700; color: #888; letter-spacing: .08em;
        text-transform: uppercase; margin: 20px 0 8px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #f1f5f9; padding: 8px 12px; text-align: left;
        font-size: 11px; font-weight: 700; color: #666;
        border-bottom: 2px solid #e2e8f0; }}
  td {{ padding: 7px 12px; border-bottom: 1px solid #f0f2f8; }}
  td.num {{ text-align: right; font-family: 'DM Mono', monospace; font-weight: 600; }}
  td.note {{ font-size: 11px; color: #999; }}
  td.green {{ color: #2e9e6b; }}
  td.red   {{ color: #d64242; }}
  tfoot td {{ font-weight: 700; background: #f8fafc; border-top: 2px solid #e2e8f0; }}
  .net-row td {{ font-size: 16px; font-weight: 800; background: #1a2340; color: #fff; }}
  .net-row td.num {{ color: #f0c040; font-size: 20px; }}
  .footer {{ margin-top: 32px; padding-top: 16px; border-top: 1px solid #e2e8f0;
             display: flex; justify-content: space-between; font-size: 11px; color: #999; }}
  .sign-area {{ display: flex; gap: 48px; margin-top: 40px; }}
  .sign-box {{ flex: 1; border-top: 1px solid #ccc; padding-top: 6px; font-size: 11px; color: #666; }}
  @media print {{
    body {{ padding: 16px; }}
    @page {{ margin: 12mm; size: A4; }}
    .no-print {{ display: none !important; }}
  }}
</style>
</head>
<body>

<div class="no-print" style="text-align:right;margin-bottom:20px">
  <button onclick="window.print()"
    style="padding:10px 24px;background:#1a2340;color:#fff;border:none;border-radius:6px;
           font-size:13px;font-weight:700;cursor:pointer">列印 / 儲存 PDF</button>
</div>

<div class="header">
  <div>
    <div class="company">薪資明細單</div>
    <div class="slip-title">{esc_h(row['month'])} · {sal_type}</div>
  </div>
  <div class="staff-info">
    <div><strong>{esc_h(row['staff_name'])}</strong></div>
    <div>{esc_h(row['employee_code'] or '')}　{esc_h(row['department'] or '')}　{esc_h(row['role'] or '')}</div>
    <div>到職日：{esc_h(str(row['hire_date']) if row['hire_date'] else '—')}</div>
    <div>狀態：<strong>{status_str}</strong></div>
  </div>
</div>

<div class="summary">
  <div class="sum-card">
    <div class="sum-label">津貼合計</div>
    <div class="sum-val green">{money(d.get('allowance_total',0))}</div>
  </div>
  <div class="sum-card">
    <div class="sum-label">扣除合計</div>
    <div class="sum-val red">-{money(d.get('deduction_total',0))}</div>
  </div>
  <div class="sum-card" style="border-color:#1a2340">
    <div class="sum-label">實領金額</div>
    <div class="sum-val navy">{money(d.get('net_pay',0))}</div>
  </div>
</div>

<div class="attend">{attend_str}</div>

<h3>津貼項目</h3>
<table>
  <thead><tr><th>項目</th><th style="text-align:right">金額</th><th>計算說明</th></tr></thead>
  <tbody>{allow_rows}</tbody>
  <tfoot>
    <tr><td><strong>津貼合計</strong></td><td class="num green"><strong>{money(d.get('allowance_total',0))}</strong></td><td></td></tr>
  </tfoot>
</table>

<h3>扣除項目</h3>
<table>
  <thead><tr><th>項目</th><th style="text-align:right">金額</th><th>計算說明</th></tr></thead>
  <tbody>{deduct_rows if deduct_rows else '<tr><td colspan="3" style="color:#ccc;text-align:center;padding:12px">無扣除項目</td></tr>'}</tbody>
  <tfoot>
    <tr><td><strong>扣除合計</strong></td><td class="num red"><strong>-{money(d.get('deduction_total',0))}</strong></td><td></td></tr>
  </tfoot>
</table>

<table style="margin-top:12px">
  <tbody>
    <tr class="net-row">
      <td><strong>實領金額</strong></td>
      <td class="num">{money(d.get('net_pay',0))}</td>
      <td style="color:#ccc;font-size:11px">= 津貼 {money(d.get('allowance_total',0))} - 扣除 {money(d.get('deduction_total',0))}</td>
    </tr>
  </tbody>
</table>

{punch_table}

<div class="sign-area">
  <div class="sign-box">員工簽名</div>
  <div class="sign-box">主管確認</div>
  <div class="sign-box">人資確認</div>
</div>

<div class="footer">
  <span>本薪資單由系統自動產生</span>
  <span>列印日期：<script>document.write(new Date().toLocaleDateString('zh-TW'))</script></span>
</div>

</body>
</html>"""

    return html, 200, {'Content-Type': 'text/html; charset=utf-8'}
