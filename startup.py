"""
startup.py — 背景執行緒：keep_alive、年假同步
"""
import threading
import time
import urllib.request
from datetime import datetime, timedelta

from config import RENDER_EXTERNAL_URL, TW_TZ


def keep_alive():
    """每 14 分鐘 ping 自身，避免 Render free tier 休眠"""
    url = RENDER_EXTERNAL_URL + '/health' if RENDER_EXTERNAL_URL else None
    if not url:
        return
    while True:
        try:
            urllib.request.urlopen(url, timeout=10)
        except Exception:
            pass
        time.sleep(14 * 60)


def _run_annual_leave_sync():
    """依勞基法第38條，依到職日計算特休天數，寫入 leave_balances。每日午夜自動執行。"""
    from datetime import date as _date
    year = _date.today().year
    try:
        from blueprints.leave import _calc_annual_leave_days
        from db import get_db
        with get_db() as conn:
            staff_rows = conn.execute(
                "SELECT id, hire_date FROM punch_staff WHERE active=TRUE AND hire_date IS NOT NULL"
            ).fetchall()
            lt = conn.execute(
                "SELECT id FROM leave_types WHERE code='annual'"
            ).fetchone()
            if not lt:
                return
            lt_id = lt['id']
            for s in staff_rows:
                try:
                    days = _calc_annual_leave_days(s['hire_date'])
                    conn.execute("""
                        INSERT INTO leave_balances (staff_id, leave_type_id, year, total_days, used_days)
                        VALUES (%s, %s, %s, %s, 0)
                        ON CONFLICT (staff_id, leave_type_id, year) DO UPDATE
                          SET total_days=EXCLUDED.total_days, updated_at=NOW()
                    """, (s['id'], lt_id, year, days))
                except Exception as e:
                    print(f'[annual_leave_sync staff {s["id"]}] {e}')
    except Exception as e:
        print(f'[annual_leave_sync] {e}')


def _annual_leave_sync_loop():
    """啟動時立即執行一次，之後每天 00:05 台灣時間再執行"""
    _run_annual_leave_sync()
    while True:
        now = datetime.now(TW_TZ)
        tomorrow_05 = (now + timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)
        sleep_sec = (tomorrow_05 - now).total_seconds()
        if sleep_sec < 0:
            sleep_sec = 3600
        time.sleep(sleep_sec)
        _run_annual_leave_sync()


def start_background_threads():
    """啟動所有背景執行緒"""
    t1 = threading.Thread(target=keep_alive, daemon=True)
    t1.start()

    t2 = threading.Thread(target=_annual_leave_sync_loop, daemon=True)
    t2.start()
