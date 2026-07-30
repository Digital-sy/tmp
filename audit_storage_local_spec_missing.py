#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""审计仓储费宽表中 NO_DIM_ROW 的安全补齐候选，不修改数据库。"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime
from typing import Any

import pymysql
import pymysql.cursors

from common import settings

DEFAULT_WIDE_TABLE = "dws_db.dws_fba_storage_local_spec_monthly"
DEFAULT_DIM_TABLE = "dim_db.dim_local_package_spec_resolved"


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def lower_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {str(k).lower(): v for k, v in row.items()}


def split_table(name: str) -> tuple[str, str]:
    parts = [part.strip() for part in name.split(".")]
    if len(parts) == 1:
        result = (str(settings.db_config["database"]), parts[0])
    elif len(parts) == 2:
        result = (parts[0], parts[1])
    else:
        raise ValueError(f"表名格式错误：{name}")
    valid = re.compile(r"^[A-Za-z0-9_\u4e00-\u9fff]+$")
    if not all(valid.fullmatch(part) for part in result):
        raise ValueError(f"非法数据库或表名：{name}")
    return result


def quoted_table(name: str) -> str:
    schema_name, table_name = split_table(name)
    return f"`{schema_name}`.`{table_name}`"


def db_connect():
    cfg = settings.db_config
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=30,
        read_timeout=900,
        write_timeout=900,
    )


def table_exists(conn, name: str) -> bool:
    schema_name, table_name = split_table(name)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (schema_name, table_name),
        )
        return bool(to_int(lower_row(cur.fetchone()).get("cnt")))


def classification_sql(wide_table: str, dim_table: str) -> str:
    wide = quoted_table(wide_table)
    dim = quoted_table(dim_table)
    return f"""
    WITH missing AS (
        SELECT DISTINCT sid, fnsku, COALESCE(asin,'') AS asin
        FROM {wide}
        WHERE month_of_charge=%s
          AND local_spec_match_status='NO_DIM_ROW'
    ),
    fnsku_map AS (
        SELECT
            fnsku,
            COUNT(DISTINCT NULLIF(sku,'')) AS sku_cnt,
            COUNT(DISTINCT sid) AS sid_cnt,
            MAX(NULLIF(sku,'')) AS candidate_sku,
            MAX(NULLIF(msku,'')) AS candidate_msku,
            MAX(NULLIF(raw_spu,'')) AS candidate_spu
        FROM {dim}
        WHERE COALESCE(fnsku,'')<>''
        GROUP BY fnsku
    ),
    sid_asin_map AS (
        SELECT
            sid,
            asin,
            COUNT(DISTINCT NULLIF(sku,'')) AS sku_cnt,
            COUNT(DISTINCT fnsku) AS fnsku_cnt,
            MAX(NULLIF(sku,'')) AS candidate_sku,
            MAX(NULLIF(msku,'')) AS candidate_msku,
            MAX(NULLIF(raw_spu,'')) AS candidate_spu
        FROM {dim}
        WHERE COALESCE(asin,'')<>''
        GROUP BY sid, asin
    ),
    asin_map AS (
        SELECT
            asin,
            COUNT(DISTINCT NULLIF(sku,'')) AS sku_cnt,
            COUNT(DISTINCT fnsku) AS fnsku_cnt,
            MAX(NULLIF(sku,'')) AS candidate_sku,
            MAX(NULLIF(msku,'')) AS candidate_msku,
            MAX(NULLIF(raw_spu,'')) AS candidate_spu
        FROM {dim}
        WHERE COALESCE(asin,'')<>''
        GROUP BY asin
    )
    SELECT
        m.sid,
        m.fnsku,
        m.asin,
        CASE
            WHEN COALESCE(f.sku_cnt,0)=1 THEN 'FNSKU_UNIQUE_SKU'
            WHEN COALESCE(sa.sku_cnt,0)=1 AND COALESCE(sa.fnsku_cnt,0)=1
                THEN 'SID_ASIN_UNIQUE'
            WHEN COALESCE(a.sku_cnt,0)=1 AND COALESCE(a.fnsku_cnt,0)=1
                THEN 'ASIN_GLOBAL_UNIQUE'
            WHEN COALESCE(f.sku_cnt,0)>0 OR COALESCE(sa.sku_cnt,0)>0
              OR COALESCE(a.sku_cnt,0)>0 THEN 'AMBIGUOUS'
            ELSE 'NO_CANDIDATE'
        END AS candidate_status,
        CASE
            WHEN COALESCE(f.sku_cnt,0)=1 THEN f.candidate_sku
            WHEN COALESCE(sa.sku_cnt,0)=1 AND COALESCE(sa.fnsku_cnt,0)=1
                THEN sa.candidate_sku
            WHEN COALESCE(a.sku_cnt,0)=1 AND COALESCE(a.fnsku_cnt,0)=1
                THEN a.candidate_sku
            ELSE NULL
        END AS candidate_sku,
        CASE
            WHEN COALESCE(f.sku_cnt,0)=1 THEN f.candidate_msku
            WHEN COALESCE(sa.sku_cnt,0)=1 AND COALESCE(sa.fnsku_cnt,0)=1
                THEN sa.candidate_msku
            WHEN COALESCE(a.sku_cnt,0)=1 AND COALESCE(a.fnsku_cnt,0)=1
                THEN a.candidate_msku
            ELSE NULL
        END AS candidate_msku,
        CASE
            WHEN COALESCE(f.sku_cnt,0)=1 THEN f.candidate_spu
            WHEN COALESCE(sa.sku_cnt,0)=1 AND COALESCE(sa.fnsku_cnt,0)=1
                THEN sa.candidate_spu
            WHEN COALESCE(a.sku_cnt,0)=1 AND COALESCE(a.fnsku_cnt,0)=1
                THEN a.candidate_spu
            ELSE NULL
        END AS candidate_spu,
        COALESCE(f.sku_cnt,0) AS same_fnsku_sku_cnt,
        COALESCE(f.sid_cnt,0) AS same_fnsku_sid_cnt,
        COALESCE(sa.sku_cnt,0) AS same_sid_asin_sku_cnt,
        COALESCE(sa.fnsku_cnt,0) AS same_sid_asin_fnsku_cnt,
        COALESCE(a.sku_cnt,0) AS global_asin_sku_cnt,
        COALESCE(a.fnsku_cnt,0) AS global_asin_fnsku_cnt
    FROM missing m
    LEFT JOIN fnsku_map f ON f.fnsku=m.fnsku
    LEFT JOIN sid_asin_map sa ON sa.sid=m.sid AND sa.asin=m.asin AND m.asin<>''
    LEFT JOIN asin_map a ON a.asin=m.asin AND m.asin<>''
    """


def main() -> None:
    parser = argparse.ArgumentParser(description="审计仓储费宽表 NO_DIM_ROW 补齐候选")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--wide-table", default=DEFAULT_WIDE_TABLE)
    parser.add_argument("--dim-table", default=DEFAULT_DIM_TABLE)
    parser.add_argument("--sample", type=int, default=50)
    args = parser.parse_args()
    datetime.strptime(args.month, "%Y-%m")

    conn = db_connect()
    try:
        for table_name in (args.wide_table, args.dim_table):
            if not table_exists(conn, table_name):
                raise RuntimeError(f"表不存在或不可访问：{table_name}")

        sql = classification_sql(args.wide_table, args.dim_table)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT candidate_status, COUNT(*) AS row_cnt FROM ({sql}) x "
                "GROUP BY candidate_status ORDER BY row_cnt DESC",
                (args.month,),
            )
            summary = [lower_row(row) for row in cur.fetchall()]

            cur.execute(
                f"SELECT * FROM ({sql}) x "
                "ORDER BY FIELD(candidate_status,'FNSKU_UNIQUE_SKU','SID_ASIN_UNIQUE',"
                "'ASIN_GLOBAL_UNIQUE','AMBIGUOUS','NO_CANDIDATE'), sid, fnsku LIMIT %s",
                (args.month, max(0, args.sample)),
            )
            samples = [lower_row(row) for row in cur.fetchall()]

        print(f"\n===== {args.month} NO_DIM_ROW 补齐候选汇总 =====")
        print("candidate_status\trow_cnt")
        total = 0
        for row in summary:
            total += to_int(row.get("row_cnt"))
            print(f"{text(row.get('candidate_status'))}\t{text(row.get('row_cnt'))}")
        print(f"TOTAL\t{total}")

        print(f"\n===== 候选明细样例（最多 {max(0, args.sample)} 条） =====")
        fields = [
            "sid", "fnsku", "asin", "candidate_status", "candidate_sku",
            "candidate_msku", "candidate_spu", "same_fnsku_sku_cnt",
            "same_fnsku_sid_cnt", "same_sid_asin_sku_cnt",
            "same_sid_asin_fnsku_cnt", "global_asin_sku_cnt",
            "global_asin_fnsku_cnt",
        ]
        print("\t".join(fields))
        for row in samples:
            print("\t".join(text(row.get(field)) for field in fields))

        print("\n说明：本脚本只读，不会修改宽表或维表。")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("用户中断", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"执行失败：{exc}", file=sys.stderr)
        sys.exit(1)
