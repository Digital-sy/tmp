#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
为仓储费宽表中 NO_DIM_ROW 建立安全的规格来源映射。

不会修改 dim_local_package_spec_resolved；映射单独落入：
    dim_db.dim_storage_local_spec_match_override

安全规则：
1. FNSKU_UNIQUE_SKU：目标 FNSKU 在规格维表中只对应一个 SKU；
2. SID_ASIN_UNIQUE：同店铺同 ASIN 在规格维表中只对应一个 FNSKU 和一个 SKU。

AMBIGUOUS / NO_CANDIDATE 不写入映射表。
"""
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
DEFAULT_OVERRIDE_TABLE = "dim_db.dim_storage_local_spec_match_override"


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


def db_connect(autocommit: bool = False):
    cfg = settings.db_config
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=autocommit,
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


def candidate_cte(wide_table: str, dim_table: str) -> str:
    wide = quoted_table(wide_table)
    dim = quoted_table(dim_table)
    return f"""
    WITH missing_fact AS (
        SELECT
            sid AS target_sid,
            fnsku AS target_fnsku,
            COALESCE(asin,'') AS target_asin,
            COUNT(*) AS fact_rows,
            ROUND(SUM(COALESCE(monthly_storage_fee,0)),8) AS monthly_fee,
            ROUND(SUM(COALESCE(long_term_storage_fee,0)),8) AS long_term_fee
        FROM {wide}
        WHERE month_of_charge=%s
          AND local_spec_match_status='NO_DIM_ROW'
        GROUP BY sid, fnsku, COALESCE(asin,'')
    ),
    dim_usable AS (
        SELECT
            d.*,
            CASE
              WHEN d.local_package_length IS NOT NULL
               AND d.local_package_width IS NOT NULL
               AND d.local_package_height IS NOT NULL
               AND d.local_gross_weight IS NOT NULL THEN 2
              WHEN d.local_package_length IS NOT NULL
                OR d.local_package_width IS NOT NULL
                OR d.local_package_height IS NOT NULL
                OR d.local_gross_weight IS NOT NULL THEN 1
              ELSE 0
            END AS completeness_score
        FROM {dim} d
        WHERE d.local_package_length IS NOT NULL
           OR d.local_package_width IS NOT NULL
           OR d.local_package_height IS NOT NULL
           OR d.local_gross_weight IS NOT NULL
    ),
    fnsku_stats AS (
        SELECT
            fnsku,
            COUNT(DISTINCT NULLIF(sku,'')) AS sku_cnt,
            COUNT(DISTINCT sid) AS sid_cnt
        FROM dim_usable
        WHERE COALESCE(fnsku,'')<>''
        GROUP BY fnsku
    ),
    fnsku_ranked AS (
        SELECT
            d.*,
            ROW_NUMBER() OVER (
                PARTITION BY d.fnsku
                ORDER BY d.completeness_score DESC,
                         d.resolved_at DESC,
                         d.sid ASC,
                         d.fnsku ASC
            ) AS rn
        FROM dim_usable d
    ),
    sid_asin_stats AS (
        SELECT
            sid,
            asin,
            COUNT(DISTINCT NULLIF(sku,'')) AS sku_cnt,
            COUNT(DISTINCT fnsku) AS fnsku_cnt
        FROM dim_usable
        WHERE COALESCE(asin,'')<>''
        GROUP BY sid, asin
    ),
    sid_asin_ranked AS (
        SELECT
            d.*,
            ROW_NUMBER() OVER (
                PARTITION BY d.sid, d.asin
                ORDER BY d.completeness_score DESC,
                         d.resolved_at DESC,
                         d.fnsku ASC
            ) AS rn
        FROM dim_usable d
        WHERE COALESCE(d.asin,'')<>''
    )
    SELECT
        m.target_sid,
        m.target_fnsku,
        m.target_asin,
        m.fact_rows,
        m.monthly_fee,
        m.long_term_fee,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN 'FNSKU_UNIQUE_SKU'
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1
            THEN 'SID_ASIN_UNIQUE'
          WHEN COALESCE(fs.sku_cnt,0)>0 OR COALESCE(ss.sku_cnt,0)>0
            THEN 'AMBIGUOUS'
          ELSE 'NO_CANDIDATE'
        END AS candidate_status,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.sid
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.sid
          ELSE NULL
        END AS source_sid,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.fnsku
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.fnsku
          ELSE NULL
        END AS source_fnsku,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.asin
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.asin
          ELSE NULL
        END AS source_asin,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.sku
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.sku
          ELSE NULL
        END AS source_sku,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.msku
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.msku
          ELSE NULL
        END AS source_msku,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.raw_spu
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1 THEN sr.raw_spu
          ELSE NULL
        END AS source_spu,
        CASE
          WHEN COALESCE(fs.sku_cnt,0)=1 THEN fr.local_spec_source
          WHEN COALESCE(ss.sku_cnt,0)=1 AND COALESCE(ss.fnsku_cnt,0)=1
            THEN sr.local_spec_source
          ELSE NULL
        END AS source_local_spec_source,
        COALESCE(fs.sku_cnt,0) AS same_fnsku_sku_cnt,
        COALESCE(fs.sid_cnt,0) AS same_fnsku_sid_cnt,
        COALESCE(ss.sku_cnt,0) AS same_sid_asin_sku_cnt,
        COALESCE(ss.fnsku_cnt,0) AS same_sid_asin_fnsku_cnt
    FROM missing_fact m
    LEFT JOIN fnsku_stats fs
      ON fs.fnsku=m.target_fnsku
    LEFT JOIN fnsku_ranked fr
      ON fr.fnsku=m.target_fnsku AND fr.rn=1
    LEFT JOIN sid_asin_stats ss
      ON ss.sid=m.target_sid AND ss.asin=m.target_asin AND m.target_asin<>''
    LEFT JOIN sid_asin_ranked sr
      ON sr.sid=m.target_sid AND sr.asin=m.target_asin AND sr.rn=1
    """


def create_override_table(conn, table_name: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {quoted_table(table_name)} (
        target_sid INT NOT NULL COMMENT '待补齐店铺ID',
        target_fnsku VARCHAR(100) NOT NULL COMMENT '待补齐FNSKU',
        target_asin VARCHAR(50) NOT NULL DEFAULT '' COMMENT '待补齐ASIN',
        source_sid INT NOT NULL COMMENT '规格来源店铺ID',
        source_fnsku VARCHAR(100) NOT NULL COMMENT '规格来源FNSKU',
        source_asin VARCHAR(50) NULL COMMENT '规格来源ASIN',
        source_sku VARCHAR(500) NULL COMMENT '规格来源SKU',
        source_msku VARCHAR(255) NULL COMMENT '规格来源MSKU',
        source_spu VARCHAR(500) NULL COMMENT '规格来源SPU',
        source_local_spec_source VARCHAR(40) NULL COMMENT '来源维表规格状态',
        match_rule VARCHAR(40) NOT NULL COMMENT 'FNSKU_UNIQUE_SKU/SID_ASIN_UNIQUE',
        first_seen_month CHAR(7) NOT NULL,
        last_seen_month CHAR(7) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (target_sid,target_fnsku,target_asin),
        KEY idx_source_key (source_sid,source_fnsku),
        KEY idx_match_rule (match_rule)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='仓储费缺失FNSKU到本地规格维表的安全覆盖映射'
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def print_audit(conn, sql: str, month: str, sample: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT candidate_status, COUNT(*) AS key_cnt, "
            f"SUM(fact_rows) AS fact_rows, "
            f"ROUND(SUM(monthly_fee),8) AS monthly_fee, "
            f"ROUND(SUM(long_term_fee),8) AS long_term_fee "
            f"FROM ({sql}) x GROUP BY candidate_status "
            f"ORDER BY FIELD(candidate_status,'FNSKU_UNIQUE_SKU','SID_ASIN_UNIQUE',"
            f"'AMBIGUOUS','NO_CANDIDATE')",
            (month,),
        )
        summary = [lower_row(row) for row in cur.fetchall()]

        cur.execute(
            f"SELECT * FROM ({sql}) x "
            f"ORDER BY FIELD(candidate_status,'FNSKU_UNIQUE_SKU','SID_ASIN_UNIQUE',"
            f"'AMBIGUOUS','NO_CANDIDATE'), target_sid, target_fnsku LIMIT %s",
            (month, max(0, sample)),
        )
        samples = [lower_row(row) for row in cur.fetchall()]

    print(f"\n===== {month} 覆盖映射候选汇总 =====")
    print("candidate_status\tkey_cnt\tfact_rows\tmonthly_fee\tlong_term_fee")
    for row in summary:
        print("\t".join(text(row.get(name)) for name in (
            "candidate_status", "key_cnt", "fact_rows", "monthly_fee", "long_term_fee"
        )))

    print(f"\n===== 候选样例（最多 {max(0, sample)} 条） =====")
    fields = [
        "target_sid", "target_fnsku", "target_asin", "fact_rows",
        "candidate_status", "source_sid", "source_fnsku", "source_asin",
        "source_sku", "source_msku", "source_spu", "source_local_spec_source",
        "same_fnsku_sku_cnt", "same_fnsku_sid_cnt",
        "same_sid_asin_sku_cnt", "same_sid_asin_fnsku_cnt",
    ]
    print("\t".join(fields))
    for row in samples:
        print("\t".join(text(row.get(field)) for field in fields))


def build_overrides(conn, sql: str, month: str, override_table: str) -> None:
    create_override_table(conn, override_table)
    insert_sql = f"""
    INSERT INTO {quoted_table(override_table)} (
        target_sid,target_fnsku,target_asin,
        source_sid,source_fnsku,source_asin,
        source_sku,source_msku,source_spu,source_local_spec_source,
        match_rule,first_seen_month,last_seen_month
    )
    SELECT
        target_sid,target_fnsku,target_asin,
        source_sid,source_fnsku,source_asin,
        source_sku,source_msku,source_spu,source_local_spec_source,
        candidate_status,%s,%s
    FROM ({sql}) x
    WHERE candidate_status IN ('FNSKU_UNIQUE_SKU','SID_ASIN_UNIQUE')
      AND source_sid IS NOT NULL
      AND COALESCE(source_fnsku,'')<>''
    ON DUPLICATE KEY UPDATE
        source_sid=VALUES(source_sid),
        source_fnsku=VALUES(source_fnsku),
        source_asin=VALUES(source_asin),
        source_sku=VALUES(source_sku),
        source_msku=VALUES(source_msku),
        source_spu=VALUES(source_spu),
        source_local_spec_source=VALUES(source_local_spec_source),
        match_rule=VALUES(match_rule),
        first_seen_month=LEAST(first_seen_month,VALUES(first_seen_month)),
        last_seen_month=GREATEST(last_seen_month,VALUES(last_seen_month))
    """
    try:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (month, month, month))
            affected = cur.rowcount
        conn.commit()
        print(f"\n覆盖映射写入完成，affected_rows={affected}")
    except Exception:
        conn.rollback()
        raise

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT match_rule,COUNT(*) AS row_cnt "
            f"FROM {quoted_table(override_table)} "
            f"WHERE last_seen_month=%s GROUP BY match_rule ORDER BY match_rule",
            (month,),
        )
        rows = [lower_row(row) for row in cur.fetchall()]
    print(f"\n===== {month} 已落库覆盖映射 =====")
    print("match_rule\trow_cnt")
    for row in rows:
        print(f"{text(row.get('match_rule'))}\t{text(row.get('row_cnt'))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="构建仓储费本地规格安全覆盖映射")
    parser.add_argument("--action", required=True, choices=["audit", "build"])
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--wide-table", default=DEFAULT_WIDE_TABLE)
    parser.add_argument("--dim-table", default=DEFAULT_DIM_TABLE)
    parser.add_argument("--override-table", default=DEFAULT_OVERRIDE_TABLE)
    parser.add_argument("--sample", type=int, default=50)
    args = parser.parse_args()
    datetime.strptime(args.month, "%Y-%m")

    conn = db_connect(autocommit=False)
    try:
        for table_name in (args.wide_table, args.dim_table):
            if not table_exists(conn, table_name):
                raise RuntimeError(f"表不存在或不可访问：{table_name}")
        sql = candidate_cte(args.wide_table, args.dim_table)
        print_audit(conn, sql, args.month, args.sample)
        if args.action == "build":
            build_overrides(conn, sql, args.month, args.override_table)
        else:
            conn.rollback()
            print("\naudit 模式：未修改数据库")
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
