"""
blueprints/export_utils.py — Excel 匯出輔助函式（跨模組共用）

單一來源：一律轉匯出 blueprints.exports 的正宗實作，避免兩份簽章漂移。
（先前這裡有一份舊簽章的重複版，導致 finance/performance/training 匯出 500。）
"""
from blueprints.exports import (   # noqa: F401
    _xl_workbook,
    _xl_write_header,
    _xl_write_rows,
    _xl_response,
)
