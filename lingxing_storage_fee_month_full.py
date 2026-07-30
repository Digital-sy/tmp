#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""领星 FBA 月仓储费：字段扫描、表审计、接口探测、接口对比和完整落库。"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from typing import Any

import pymysql
import pymysql.cursors

from common import settings
from lingxing import OpenApiBase, LingxingTokenProvider

ROUTE = "/erp/sc/data/fba_report/storageFeeMonth"
PAGE_LENGTH = 1000
RATE_LIMIT_CODE = 3001008
TOKEN_ERROR_CODES = {401, 403, 2001003, 2001005, 3001001, 3001002}
DEFAULT_TARGET = "ods_lx_fba_storage_fee_month"
DEFAULT_OLD = "lingxing.lingxing_storage_monthly"

STORES = {
    "CY-US": 11544, "MT-US": 11545, "MT-CA": 11546, "SY-US": 11547,
    "JQ-US": 11548, "JQ-CA": 11549, "RKZ-US": 11550, "RR-UK": 11551,
    "RR-IT": 11552, "RR-DE": 11553, "RR-FR": 11554, "RR-ES": 11555,
    "RR-NL": 13247, "JQ-AU": 13639, "JQ-MX": 15353,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def obj_get(obj: Any, key: str, default: Any = None) -> Any:
    return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)


def lower_row(row: dict[str, Any] | None) -> dict[str, Any]:
    """information_schema 在部分 MySQL 环境返回大写键，统一转小写。"""
    if not row:
        return {}
    return {str(k).lower(): v for k, v in row.items()}


def lower_rows(rows) -> list[dict[str, Any]]:
    return [lower_row(r) for r in rows]


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def decimal_zero(value: Any) -> Decimal:
    return to_decimal(value) or Decimal("0")


def unit(value: Any) -> str:
    return re.sub(r"\s+", " ", text(value).lower()).replace("³", "3")


def convert(value: Any, source_unit: Any, factors: dict[str, Decimal]) -> Decimal | None:
    number = to_decimal(value)
    factor = factors.get(unit(source_unit))
    if number is None or factor is None:
        return None
    return (number * factor).quantize(Decimal("0.00000001"))


CM_FACTORS = {
    "in": Decimal("2.54"), "inch": Decimal("2.54"), "inches": Decimal("2.54"),
    "cm": Decimal("1"), "centimeter": Decimal("1"), "centimeters": Decimal("1"),
    "mm": Decimal("0.1"),
}
LB_FACTORS = {
    "lb": Decimal("1"), "lbs": Decimal("1"), "pound": Decimal("1"),
    "pounds": Decimal("1"), "kg": Decimal("2.2046226218"),
    "g": Decimal("0.0022046226"), "oz": Decimal("0.0625"),
}
KG_FACTORS = {
    "kg": Decimal("1"), "g": Decimal("0.001"), "lb": Decimal("0.45359237"),
    "lbs": Decimal("0.45359237"), "pound": Decimal("0.45359237"),
    "pounds": Decimal("0.45359237"), "oz": Decimal("0.0283495231"),
}
CUFT_FACTORS = {
    "cubic foot": Decimal("1"), "cubic feet": Decimal("1"), "ft3": Decimal("1"),
    "cu ft": Decimal("1"), "cubic inch": Decimal("0.0005787037"),
    "cubic inches": Decimal("0.0005787037"), "in3": Decimal("0.0005787037"),
    "cm3": Decimal("0.0000353147"),
}


def db(autocommit: bool = False):
    cfg = settings.db_config
    return pymysql.connect(
        host=cfg["host"], port=int(cfg.get("port", 3306)), user=cfg["user"],
        password=cfg["password"], database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=pymysql.cursors.DictCursor, autocommit=autocommit,
        connect_timeout=30, read_timeout=900, write_timeout=900,
    )


def split_table(name: str) -> tuple[str, str]:
    parts = name.split(".")
    result = (settings.db_config["database"], parts[0]) if len(parts) == 1 else tuple(parts)
    if len(result) != 2 or any(
        not re.fullmatch(r"[A-Za-z0-9_\u4e00-\u9fff]+", str(x)) for x in result
    ):
        raise ValueError(f"非法表名：{name}")
    return str(result[0]), str(result[1])


def quoted_table(name: str) -> str:
    schema_name, table_name = split_table(name)
    return f"`{schema_name}`.`{table_name}`"


def table_exists(conn, name: str) -> bool:
    schema_name, table_name = split_table(name)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (schema_name, table_name),
        )
        return bool(to_int(lower_row(cur.fetchone()).get("cnt")))


def table_columns(conn, name: str) -> list[dict[str, Any]]:
    schema_name, table_name = split_table(name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ordinal_position AS ordinal_position,
                column_name AS column_name,
                column_type AS column_type,
                is_nullable AS is_nullable,
                column_key AS column_key,
                column_comment AS column_comment
            FROM information_schema.columns
            WHERE table_schema=%s AND table_name=%s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )
        return lower_rows(cur.fetchall())


def show(rows, fields: list[str], title: str) -> None:
    rows = lower_rows(rows)
    print(f"\n===== {title} =====")
    if not rows:
        print("（无结果）")
        return
    print("\t".join(fields))
    for row in rows:
        print("\t".join(text(row.get(field.lower())) for field in fields))


def inspect_schema(conn, schemas: list[str]) -> None:
    keywords = [
        "fnsku", "asin", "sku", "msku", "包装", "规格", "重量", "毛重", "体积",
        "package", "length", "width", "height", "weight", "volume",
    ]
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name AS schema_name FROM information_schema.schemata")
        visible = {text(r.get("schema_name")) for r in lower_rows(cur.fetchall())}

    schemas = [s for s in schemas if s in visible]
    if not schemas:
        raise RuntimeError(f"没有可扫描的数据库；当前可见数据库：{sorted(visible)}")

    schema_placeholders = ",".join(["%s"] * len(schemas))
    clauses: list[str] = []
    params: list[Any] = list(schemas)
    for keyword in keywords:
        clauses += [
            "LOWER(column_name) LIKE LOWER(%s)",
            "LOWER(column_comment) LIKE LOWER(%s)",
            "LOWER(table_name) LIKE LOWER(%s)",
        ]
        params += [f"%{keyword}%"] * 3

    sql = f"""
        SELECT
            table_schema AS table_schema,
            table_name AS table_name,
            ordinal_position AS ordinal_position,
            column_name AS column_name,
            column_type AS column_type,
            column_comment AS column_comment
        FROM information_schema.columns
        WHERE table_schema IN ({schema_placeholders})
          AND ({' OR '.join(clauses)})
        ORDER BY table_schema, table_name, ordinal_position
        LIMIT 1500
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = lower_rows(cur.fetchall())
    show(
        rows,
        ["table_schema", "table_name", "ordinal_position", "column_name", "column_type", "column_comment"],
        "候选字段",
    )

    for candidate in ["lingxing.listing", "lingxing.产品管理"]:
        if table_exists(conn, candidate):
            show(
                table_columns(conn, candidate),
                ["ordinal_position", "column_name", "column_type", "is_nullable", "column_key", "column_comment"],
                f"{candidate} 完整字段",
            )
        else:
            log.warning("候选表不存在或当前账号不可见：%s", candidate)


def audit_table(conn, table_name: str) -> None:
    if not table_exists(conn, table_name):
        print(f"表不存在或当前账号不可见：{table_name}")
        return

    columns = table_columns(conn, table_name)
    show(
        columns,
        ["ordinal_position", "column_name", "column_type", "is_nullable", "column_key", "column_comment"],
        f"{table_name} 字段",
    )
    names = {text(row.get("column_name")) for row in columns}
    required = {"sid", "fnsku", "month_of_charge"}
    if not required.issubset(names):
        raise RuntimeError(f"仓储费表缺少字段：{sorted(required - names)}")

    fc_expr = "COALESCE(fulfillment_center,'')" if "fulfillment_center" in names else "''"
    fee_col = next(
        (name for name in ["estimated_monthly_storage_fee", "monthly_storage_fee", "fba_storage_fee"] if name in names),
        None,
    )
    fee_expr = f"ROUND(SUM(COALESCE(`{fee_col}`,0)),6)" if fee_col else "NULL"
    time_cols = [name for name in ["updated_at", "fetched_at", "created_at"] if name in names]
    time_sql = "".join(
        f", MIN(`{name}`) AS min_{name}, MAX(`{name}`) AS max_{name}" for name in time_cols
    )

    sql = f"""
        SELECT
            month_of_charge AS month_of_charge,
            sid AS sid,
            COUNT(*) AS row_cnt,
            COUNT(DISTINCT fnsku) AS fnsku_cnt,
            COUNT(DISTINCT CONCAT_WS('|',sid,fnsku,month_of_charge,{fc_expr})) AS unique_key_cnt,
            {fee_expr} AS fee_sum
            {time_sql}
        FROM {quoted_table(table_name)}
        GROUP BY month_of_charge, sid
        ORDER BY month_of_charge DESC, sid
        LIMIT 200
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = lower_rows(cur.fetchall())
    fields = ["month_of_charge", "sid", "row_cnt", "fnsku_cnt", "unique_key_cnt", "fee_sum"]
    for name in time_cols:
        fields += [f"min_{name}", f"max_{name}"]
    show(rows, fields, "月份覆盖与最近刷新")

    dup_sql = f"""
        SELECT sid AS sid, fnsku AS fnsku, month_of_charge AS month_of_charge,
               {fc_expr} AS fulfillment_center, COUNT(*) AS duplicate_cnt
        FROM {quoted_table(table_name)}
        GROUP BY sid, fnsku, month_of_charge, {fc_expr}
        HAVING COUNT(*) > 1
        ORDER BY duplicate_cnt DESC
        LIMIT 50
    """
    with conn.cursor() as cur:
        cur.execute(dup_sql)
        duplicates = lower_rows(cur.fetchall())
    show(
        duplicates,
        ["sid", "fnsku", "month_of_charge", "fulfillment_center", "duplicate_cnt"],
        "重复唯一键",
    )


async def request_with_retry(api, token_provider, body: dict[str, Any]):
    last_error = None
    for attempt in range(5):
        try:
            response = await api.request(
                await token_provider.get_token(), ROUTE, "POST", req_body=body
            )
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(min(10 * 2**attempt, 120))
            continue

        code = to_int(obj_get(response, "code", -1))
        if code == 0:
            return response
        if code == RATE_LIMIT_CODE:
            await asyncio.sleep(min(10 * 2**attempt, 120))
            continue
        if code in TOKEN_ERROR_CODES:
            try:
                await token_provider.refresh()
            except Exception:
                pass
            continue
        raise RuntimeError(
            f"接口失败 code={code}, message={obj_get(response, 'message', '')}"
        )
    raise RuntimeError(f"连续请求失败：{last_error}")


async def fetch_pages(api, token_provider, sid: int, month: str, max_pages: int = 0):
    rows: list[dict[str, Any]] = []
    offset = 0
    total = 0
    page_no = 0
    truncated = False

    while True:
        response = await request_with_retry(
            api, token_provider,
            {"sid": sid, "month": month, "offset": offset, "length": PAGE_LENGTH},
        )
        page = obj_get(response, "data", []) or []
        total = max(total, to_int(obj_get(response, "total", 0)))
        page_no += 1
        if not page:
            break

        rows.extend(page)
        log.info("sid=%s month=%s page=%s rows=%s/%s", sid, month, page_no, len(rows), total)

        if max_pages and page_no >= max_pages:
            truncated = len(rows) < total
            break
        if (total and len(rows) >= total) or len(page) < PAGE_LENGTH:
            break

        offset += PAGE_LENGTH
        await asyncio.sleep(2)

    if not truncated and total and len(rows) != total:
        raise RuntimeError(f"分页不完整 total={total}, actual={len(rows)}")
    return rows, total, truncated


def mapped_row(store: str, sid: int, month: str, raw: dict[str, Any], include_raw: bool):
    dimension_unit = text(raw.get("measurement_units"))
    weight_unit = text(raw.get("weight_units"))
    volume_unit = text(raw.get("volume_units"))
    weight_lb = convert(raw.get("weight"), weight_unit, LB_FACTORS)
    result = {
        "store_name": store, "sid": sid, "asin": text(raw.get("asin")),
        "fnsku": text(raw.get("fnsku")), "product_name": text(raw.get("product_name")),
        "fulfillment_center": text(raw.get("fulfillment_center")),
        "country_code": text(raw.get("country_code")),
        "longest_side": to_decimal(raw.get("longest_side")),
        "median_side": to_decimal(raw.get("median_side")),
        "shortest_side": to_decimal(raw.get("shortest_side")),
        "measurement_units": dimension_unit,
        "longest_side_cm": convert(raw.get("longest_side"), dimension_unit, CM_FACTORS),
        "median_side_cm": convert(raw.get("median_side"), dimension_unit, CM_FACTORS),
        "shortest_side_cm": convert(raw.get("shortest_side"), dimension_unit, CM_FACTORS),
        "weight": to_decimal(raw.get("weight")), "weight_units": weight_unit,
        "weight_kg": convert(raw.get("weight"), weight_unit, KG_FACTORS),
        "weight_lb": weight_lb,
        "weight_lb_ceiling_0_1": None if weight_lb is None else (
            (weight_lb * 10).to_integral_value(rounding=ROUND_CEILING) / 10
        ),
        "item_volume": to_decimal(raw.get("item_volume")), "volume_units": volume_unit,
        "item_volume_cuft": convert(raw.get("item_volume"), volume_unit, CUFT_FACTORS),
        "product_size_tier": text(raw.get("product_size_tier")),
        "average_quantity_on_hand": to_decimal(raw.get("average_quantity_on_hand")),
        "average_quantity_pending_removal": to_decimal(raw.get("average_quantity_pending_removal")),
        "estimated_total_item_volume": to_decimal(raw.get("estimated_total_item_volume")),
        "estimated_total_item_volume_cuft": convert(
            raw.get("estimated_total_item_volume"), volume_unit, CUFT_FACTORS
        ),
        "month_of_charge": text(raw.get("month_of_charge")) or month,
        "storage_rate": to_decimal(raw.get("storage_rate")),
        "currency": text(raw.get("currency")),
        "estimated_monthly_storage_fee": to_decimal(raw.get("estimated_monthly_storage_fee")),
        "v_uuid": text(raw.get("v_uuid")),
        "company_id": to_int(raw.get("company_id")) or None,
    }
    if include_raw:
        result["raw_json"] = json.dumps(raw, ensure_ascii=False, default=str, separators=(",", ":"))
    return result


def map_rows(store: str, sid: int, month: str, raw_rows, include_raw: bool):
    rows = [mapped_row(store, sid, month, raw, include_raw) for raw in raw_rows]
    keys = [(r["sid"], r["fnsku"], r["month_of_charge"], r["fulfillment_center"]) for r in rows]
    if any(not r["fnsku"] for r in rows):
        raise RuntimeError("发现空 FNSKU，停止后续操作")
    if len(keys) != len(set(keys)):
        raise RuntimeError("发现重复唯一键，停止后续操作")
    return rows


def create_target(conn, table_name: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {quoted_table(table_name)} (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        store_name VARCHAR(50) NOT NULL, sid INT NOT NULL, asin VARCHAR(50),
        fnsku VARCHAR(50) NOT NULL, product_name TEXT,
        fulfillment_center VARCHAR(30) NOT NULL DEFAULT '', country_code VARCHAR(10),
        longest_side DECIMAL(18,8), median_side DECIMAL(18,8), shortest_side DECIMAL(18,8),
        measurement_units VARCHAR(30), longest_side_cm DECIMAL(18,8),
        median_side_cm DECIMAL(18,8), shortest_side_cm DECIMAL(18,8),
        weight DECIMAL(18,8), weight_units VARCHAR(30), weight_kg DECIMAL(18,8),
        weight_lb DECIMAL(18,8), weight_lb_ceiling_0_1 DECIMAL(18,8),
        item_volume DECIMAL(18,8), volume_units VARCHAR(30), item_volume_cuft DECIMAL(18,8),
        product_size_tier VARCHAR(100), average_quantity_on_hand DECIMAL(18,6),
        average_quantity_pending_removal DECIMAL(18,6),
        estimated_total_item_volume DECIMAL(18,8),
        estimated_total_item_volume_cuft DECIMAL(18,8),
        month_of_charge CHAR(7) NOT NULL, storage_rate DECIMAL(18,8),
        currency VARCHAR(10), estimated_monthly_storage_fee DECIMAL(18,8),
        v_uuid VARCHAR(64), company_id BIGINT, raw_json JSON,
        fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uq_sid_fnsku_month_fc (sid,fnsku,month_of_charge,fulfillment_center),
        KEY idx_store_month (store_name,month_of_charge), KEY idx_asin (asin), KEY idx_fnsku (fnsku)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def replace_rows(conn, table_name: str, sid: int, month: str, rows) -> None:
    create_target(conn, table_name)
    if not rows:
        log.warning("接口返回空数据，不执行删旧写新 sid=%s month=%s", sid, month)
        return
    fields = list(rows[0].keys())
    columns = ",".join(f"`{name}`" for name in fields)
    placeholders = ",".join(f"%({name})s" for name in fields)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {quoted_table(table_name)} WHERE sid=%s AND month_of_charge=%s",
                (sid, month),
            )
            cur.executemany(
                f"INSERT INTO {quoted_table(table_name)} ({columns}) VALUES ({placeholders})",
                rows,
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def compare_with_db(conn, table_name: str, sid: int, month: str, api_rows) -> None:
    if not table_exists(conn, table_name):
        print(f"表不存在或当前账号不可见：{table_name}")
        return
    names = {text(r.get("column_name")) for r in table_columns(conn, table_name)}
    fc_expr = "COALESCE(fulfillment_center,'')" if "fulfillment_center" in names else "''"
    fee_col = next(
        (name for name in ["estimated_monthly_storage_fee", "monthly_storage_fee", "fba_storage_fee"] if name in names),
        None,
    )
    if fee_col is None:
        raise RuntimeError(f"{table_name} 未识别到月仓储费字段")

    sql = f"""
        SELECT sid AS sid, fnsku AS fnsku, month_of_charge AS month_of_charge,
               {fc_expr} AS fulfillment_center, COALESCE(`{fee_col}`,0) AS fee
        FROM {quoted_table(table_name)}
        WHERE sid=%s AND month_of_charge=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (sid, month))
        db_rows = lower_rows(cur.fetchall())

    api_keys = {(r["sid"], r["fnsku"], r["month_of_charge"], r["fulfillment_center"]) for r in api_rows}
    db_keys = {
        (to_int(r.get("sid")), text(r.get("fnsku")), text(r.get("month_of_charge")), text(r.get("fulfillment_center")))
        for r in db_rows
    }
    result = [{
        "sid": sid, "month": month, "api_rows": len(api_rows), "db_rows": len(db_rows),
        "api_fee": sum((decimal_zero(r.get("estimated_monthly_storage_fee")) for r in api_rows), Decimal("0")),
        "db_fee": sum((decimal_zero(r.get("fee")) for r in db_rows), Decimal("0")),
        "only_api": len(api_keys - db_keys), "only_db": len(db_keys - api_keys),
    }]
    show(
        result,
        ["sid", "month", "api_rows", "db_rows", "api_fee", "db_fee", "only_api", "only_db"],
        "API 与数据库对比",
    )


def store_list(value: str | None):
    if not value:
        return list(STORES.items())
    names = [x.strip().upper() for x in value.split(",") if x.strip()]
    invalid = [name for name in names if name not in STORES]
    if invalid:
        raise ValueError(f"未知店铺：{invalid}")
    return [(name, STORES[name]) for name in names]


def build_api():
    cfg = settings.lingxing_config
    api = OpenApiBase(
        host=cfg["host"], app_id=cfg["app_id"], app_secret=cfg["app_secret"],
        proxy_url=cfg.get("proxy_url"),
    )
    token_provider = LingxingTokenProvider(
        op_api=api, refresh_margin_seconds=300, logger=log
    )
    return api, token_provider


async def main(args) -> None:
    if args.action in {"inspect-schema", "audit-db"}:
        conn = db(True)
        try:
            if args.action == "inspect-schema":
                schemas = [x.strip() for x in args.schemas.split(",") if x.strip()] or [
                    settings.db_config["database"], "lingxing", "ods_db", "dim_db", "dwd_db"
                ]
                inspect_schema(conn, schemas)
            else:
                audit_table(conn, args.audit_table)
        finally:
            conn.close()
        return

    if not args.month:
        raise ValueError("必须传 --month YYYY-MM")
    datetime.strptime(args.month, "%Y-%m")
    api, token_provider = build_api()
    conn = db(False)
    try:
        for store_name, sid in store_list(args.store):
            max_pages = 1 if args.action == "probe-api" and args.max_pages == 0 else args.max_pages
            raw_rows, total, truncated = await fetch_pages(
                api, token_provider, sid, args.month, max_pages=max_pages
            )
            log.info(
                "%s API结果 fetched=%s total=%s truncated=%s",
                store_name, len(raw_rows), total, truncated,
            )
            if raw_rows:
                print(f"\n===== {store_name} API 第一条原始记录 =====")
                print(json.dumps(raw_rows[0], ensure_ascii=False, indent=2, default=str))

            if args.action == "probe-api":
                continue
            if truncated:
                raise RuntimeError("compare-api/fetch 不允许使用截断分页结果")

            rows = map_rows(
                store_name, sid, args.month, raw_rows,
                include_raw=args.action == "fetch",
            )
            log.info(
                "%s rows=%s fee=%s", store_name, len(rows),
                sum((decimal_zero(r.get("estimated_monthly_storage_fee")) for r in rows), Decimal("0")),
            )
            if args.action == "compare-api":
                compare_with_db(conn, args.compare_table, sid, args.month, rows)
            elif args.dry_run:
                log.info("dry-run：未写库")
            else:
                replace_rows(conn, args.target_table, sid, args.month, rows)
    finally:
        conn.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action", required=True,
        choices=["inspect-schema", "audit-db", "probe-api", "compare-api", "fetch"],
    )
    parser.add_argument("--month")
    parser.add_argument("--store")
    parser.add_argument("--target-table", default=DEFAULT_TARGET)
    parser.add_argument("--audit-table", default=DEFAULT_OLD)
    parser.add_argument("--compare-table", default=DEFAULT_OLD)
    parser.add_argument("--schemas", default="")
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        log.warning("用户中断")
        sys.exit(130)
    except Exception as exc:
        log.exception("执行失败：%s", exc)
        sys.exit(1)
