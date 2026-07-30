#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""构建支持安全覆盖映射的 FBA 月仓储费本地规格宽表。"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from typing import Any

import pymysql
import pymysql.cursors

from common import settings

DEFAULT_STORAGE_TABLE = "lingxing.lingxing_storage_monthly"
DEFAULT_SPEC_TABLE = "dim_db.dim_local_package_spec_resolved"
DEFAULT_OVERRIDE_TABLE = "dim_db.dim_storage_local_spec_match_override"
DEFAULT_TARGET_TABLE = "dws_db.dws_fba_storage_local_spec_monthly"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("build_fba_storage_local_spec_monthly_v2")


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
    return {str(key).lower(): value for key, value in row.items()}


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


def table_columns(conn, name: str) -> set[str]:
    schema_name, table_name = split_table(name)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s",
            (schema_name, table_name),
        )
        return {text(lower_row(row).get("column_name")) for row in cur.fetchall()}


def validate_month(value: str) -> str:
    datetime.strptime(value, "%Y-%m")
    return value


def create_target_table(conn, target_table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {quoted_table(target_table)} (
        month_of_charge CHAR(7) NOT NULL,
        store_name VARCHAR(100), sid INT NOT NULL, fnsku VARCHAR(100) NOT NULL,
        asin VARCHAR(50), fulfillment_center VARCHAR(30) NOT NULL DEFAULT '',
        country_code VARCHAR(10), product_size_tier VARCHAR(100),
        average_quantity_on_hand DECIMAL(18,6), storage_rate DECIMAL(18,8),
        currency VARCHAR(10), monthly_storage_fee DECIMAL(18,8),
        long_term_storage_fee_6mo DECIMAL(18,8),
        long_term_storage_fee_12mo DECIMAL(18,8),
        long_term_storage_fee DECIMAL(18,8),
        msku VARCHAR(255), sku VARCHAR(500), raw_spu VARCHAR(500),
        size_token VARCHAR(100), dimension_alias_spu VARCHAR(500),
        dimension_alias_rule VARCHAR(60),
        local_package_length DECIMAL(18,6), local_package_length_source VARCHAR(40),
        local_package_width DECIMAL(18,6), local_package_width_source VARCHAR(40),
        local_package_height DECIMAL(18,6), local_package_height_source VARCHAR(40),
        local_dimension_unit VARCHAR(20), local_package_volume_cm3 DECIMAL(24,6),
        local_gross_weight DECIMAL(18,6), local_gross_weight_source VARCHAR(40),
        local_weight_unit VARCHAR(20), local_spec_source VARCHAR(40),
        local_spec_match_status VARCHAR(30) NOT NULL,
        local_spec_match_rule VARCHAR(60),
        local_spec_source_sid INT,
        local_spec_source_fnsku VARCHAR(100),
        local_spec_source_asin VARCHAR(50),
        local_spec_resolved_at DATETIME,
        storage_source_created_at DATETIME,
        built_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (month_of_charge,sid,fnsku,fulfillment_center),
        KEY idx_store_month (store_name,month_of_charge),
        KEY idx_sku_month (sku,month_of_charge),
        KEY idx_spu_month (raw_spu,month_of_charge),
        KEY idx_match_status (local_spec_match_status),
        KEY idx_match_rule (local_spec_match_rule),
        KEY idx_spec_source_key (local_spec_source_sid,local_spec_source_fnsku)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='FBA月仓储费与本地包装规格关联宽表（支持安全覆盖映射）'
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def ensure_target_columns(conn, target_table: str) -> None:
    create_target_table(conn, target_table)
    columns = table_columns(conn, target_table)
    additions = {
        "local_spec_match_rule": "VARCHAR(60) NULL COMMENT 'DIRECT或覆盖映射规则'",
        "local_spec_source_sid": "INT NULL COMMENT '实际规格来源店铺ID'",
        "local_spec_source_fnsku": "VARCHAR(100) NULL COMMENT '实际规格来源FNSKU'",
        "local_spec_source_asin": "VARCHAR(50) NULL COMMENT '实际规格来源ASIN'",
    }
    with conn.cursor() as cur:
        for column, ddl in additions.items():
            if column not in columns:
                logger.info("目标表新增字段：%s", column)
                cur.execute(
                    f"ALTER TABLE {quoted_table(target_table)} ADD COLUMN `{column}` {ddl}"
                )
    conn.commit()


def effective_join_sql(storage_table: str, spec_table: str, override_table: str) -> str:
    s = quoted_table(storage_table)
    d = quoted_table(spec_table)
    o = quoted_table(override_table)
    return f"""
    FROM {s} s
    LEFT JOIN {d} d0
      ON d0.sid=s.sid AND d0.fnsku=s.fnsku
    LEFT JOIN {o} o
      ON d0.fnsku IS NULL
     AND o.target_sid=s.sid
     AND o.target_fnsku=s.fnsku
     AND o.target_asin=COALESCE(s.asin,'')
    LEFT JOIN {d} d1
      ON d1.sid=o.source_sid AND d1.fnsku=o.source_fnsku
    """


def source_summary(
    conn, storage_table: str, spec_table: str, override_table: str, month: str
) -> dict[str, Any]:
    joins = effective_join_sql(storage_table, spec_table, override_table)
    sql = f"""
    SELECT
        COUNT(*) AS source_rows,
        COUNT(DISTINCT CONCAT_WS('|',s.sid,s.fnsku,s.month_of_charge,
            COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),''))) AS source_unique_rows,
        SUM(d0.fnsku IS NOT NULL) AS direct_match_rows,
        SUM(d0.fnsku IS NULL AND d1.fnsku IS NOT NULL) AS override_match_rows,
        SUM(d0.fnsku IS NULL AND d1.fnsku IS NULL) AS missing_rows,
        SUM(COALESCE(d0.fnsku,d1.fnsku) IS NOT NULL
            AND COALESCE(d0.local_package_length,d1.local_package_length) IS NOT NULL
            AND COALESCE(d0.local_package_width,d1.local_package_width) IS NOT NULL
            AND COALESCE(d0.local_package_height,d1.local_package_height) IS NOT NULL
            AND COALESCE(d0.local_gross_weight,d1.local_gross_weight) IS NOT NULL
        ) AS complete_local_spec_rows,
        SUM(COALESCE(d0.fnsku,d1.fnsku) IS NOT NULL AND (
            COALESCE(d0.local_package_length,d1.local_package_length) IS NOT NULL OR
            COALESCE(d0.local_package_width,d1.local_package_width) IS NOT NULL OR
            COALESCE(d0.local_package_height,d1.local_package_height) IS NOT NULL OR
            COALESCE(d0.local_gross_weight,d1.local_gross_weight) IS NOT NULL
        )) AS any_local_spec_rows,
        ROUND(SUM(COALESCE(s.monthly_storage_fee,0)),6) AS monthly_storage_fee_sum,
        ROUND(SUM(COALESCE(s.long_term_storage_fee,0)),6) AS long_term_storage_fee_sum
    {joins}
    WHERE s.month_of_charge=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (month,))
        return lower_row(cur.fetchone())


def print_source_summary(month: str, row: dict[str, Any]) -> None:
    print(f"\n===== {month} 覆盖映射关联审计 =====")
    for key in (
        "source_rows", "source_unique_rows", "direct_match_rows",
        "override_match_rows", "missing_rows", "complete_local_spec_rows",
        "any_local_spec_rows", "monthly_storage_fee_sum",
        "long_term_storage_fee_sum",
    ):
        print(f"{key}\t{text(row.get(key))}")


def build_month(
    conn,
    storage_table: str,
    spec_table: str,
    override_table: str,
    target_table: str,
    month: str,
) -> None:
    summary = source_summary(conn, storage_table, spec_table, override_table, month)
    source_rows = to_int(summary.get("source_rows"))
    unique_rows = to_int(summary.get("source_unique_rows"))
    if source_rows == 0:
        raise RuntimeError(f"源表在 {month} 没有数据，拒绝构建")
    if source_rows != unique_rows:
        raise RuntimeError(
            f"源表存在重复唯一键：source={source_rows}, unique={unique_rows}"
        )

    ensure_target_columns(conn, target_table)
    joins = effective_join_sql(storage_table, spec_table, override_table)
    insert_sql = f"""
    INSERT INTO {quoted_table(target_table)} (
        month_of_charge,store_name,sid,fnsku,asin,fulfillment_center,
        country_code,product_size_tier,average_quantity_on_hand,storage_rate,
        currency,monthly_storage_fee,long_term_storage_fee_6mo,
        long_term_storage_fee_12mo,long_term_storage_fee,
        msku,sku,raw_spu,size_token,dimension_alias_spu,dimension_alias_rule,
        local_package_length,local_package_length_source,
        local_package_width,local_package_width_source,
        local_package_height,local_package_height_source,local_dimension_unit,
        local_package_volume_cm3,local_gross_weight,local_gross_weight_source,
        local_weight_unit,local_spec_source,local_spec_match_status,
        local_spec_match_rule,local_spec_source_sid,local_spec_source_fnsku,
        local_spec_source_asin,local_spec_resolved_at,
        storage_source_created_at,built_at
    )
    SELECT
        s.month_of_charge,s.store_name,s.sid,s.fnsku,
        COALESCE(NULLIF(s.asin,''),d0.asin,d1.asin),
        COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),''),
        s.country_code,s.product_size_tier,s.average_quantity_on_hand,
        s.storage_rate,s.currency,s.monthly_storage_fee,
        s.long_term_storage_fee_6mo,s.long_term_storage_fee_12mo,
        s.long_term_storage_fee,
        COALESCE(d0.msku,d1.msku),COALESCE(d0.sku,d1.sku),
        COALESCE(d0.raw_spu,d1.raw_spu),COALESCE(d0.size_token,d1.size_token),
        COALESCE(d0.dimension_alias_spu,d1.dimension_alias_spu),
        COALESCE(d0.dimension_alias_rule,d1.dimension_alias_rule),
        COALESCE(d0.local_package_length,d1.local_package_length),
        COALESCE(d0.local_package_length_source,d1.local_package_length_source),
        COALESCE(d0.local_package_width,d1.local_package_width),
        COALESCE(d0.local_package_width_source,d1.local_package_width_source),
        COALESCE(d0.local_package_height,d1.local_package_height),
        COALESCE(d0.local_package_height_source,d1.local_package_height_source),
        COALESCE(d0.local_dimension_unit,d1.local_dimension_unit),
        CASE
          WHEN COALESCE(d0.local_package_length,d1.local_package_length) IS NOT NULL
           AND COALESCE(d0.local_package_width,d1.local_package_width) IS NOT NULL
           AND COALESCE(d0.local_package_height,d1.local_package_height) IS NOT NULL
          THEN ROUND(
              COALESCE(d0.local_package_length,d1.local_package_length)
            * COALESCE(d0.local_package_width,d1.local_package_width)
            * COALESCE(d0.local_package_height,d1.local_package_height),6)
          ELSE NULL
        END,
        COALESCE(d0.local_gross_weight,d1.local_gross_weight),
        COALESCE(d0.local_gross_weight_source,d1.local_gross_weight_source),
        COALESCE(d0.local_weight_unit,d1.local_weight_unit),
        COALESCE(d0.local_spec_source,d1.local_spec_source),
        CASE
          WHEN COALESCE(d0.fnsku,d1.fnsku) IS NULL THEN 'NO_DIM_ROW'
          WHEN COALESCE(d0.local_package_length,d1.local_package_length) IS NOT NULL
           AND COALESCE(d0.local_package_width,d1.local_package_width) IS NOT NULL
           AND COALESCE(d0.local_package_height,d1.local_package_height) IS NOT NULL
           AND COALESCE(d0.local_gross_weight,d1.local_gross_weight) IS NOT NULL
            THEN 'COMPLETE'
          WHEN COALESCE(d0.local_package_length,d1.local_package_length) IS NOT NULL
            OR COALESCE(d0.local_package_width,d1.local_package_width) IS NOT NULL
            OR COALESCE(d0.local_package_height,d1.local_package_height) IS NOT NULL
            OR COALESCE(d0.local_gross_weight,d1.local_gross_weight) IS NOT NULL
            THEN 'PARTIAL'
          ELSE 'NO_FIELDS'
        END,
        CASE
          WHEN d0.fnsku IS NOT NULL THEN 'DIRECT_SID_FNSKU'
          WHEN d1.fnsku IS NOT NULL THEN CONCAT('OVERRIDE_',o.match_rule)
          ELSE 'NO_MATCH'
        END,
        COALESCE(d0.sid,d1.sid),COALESCE(d0.fnsku,d1.fnsku),
        COALESCE(d0.asin,d1.asin),COALESCE(d0.resolved_at,d1.resolved_at),
        s.created_at,NOW()
    {joins}
    WHERE s.month_of_charge=%s
    """

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {quoted_table(target_table)} WHERE month_of_charge=%s",
                (month,),
            )
            logger.info("删除目标表旧月份数据：month=%s rows=%s", month, cur.rowcount)
            cur.execute(insert_sql, (month,))
            inserted = cur.rowcount
            logger.info("写入目标表：month=%s rows=%s", month, inserted)
        if inserted != source_rows:
            raise RuntimeError(
                f"写入行数不一致：source={source_rows}, inserted={inserted}"
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("构建失败，事务已回滚")
        raise


def target_summary(conn, target_table: str, month: str) -> dict[str, Any]:
    sql = f"""
    SELECT
        COUNT(*) AS target_rows,
        COUNT(DISTINCT CONCAT_WS('|',sid,fnsku,month_of_charge,fulfillment_center))
            AS target_unique_rows,
        SUM(local_spec_match_status='COMPLETE') AS complete_rows,
        SUM(local_spec_match_status='PARTIAL') AS partial_rows,
        SUM(local_spec_match_status='NO_FIELDS') AS no_field_rows,
        SUM(local_spec_match_status='NO_DIM_ROW') AS no_dim_rows,
        SUM(local_spec_match_rule='DIRECT_SID_FNSKU') AS direct_rows,
        SUM(local_spec_match_rule='OVERRIDE_FNSKU_UNIQUE_SKU') AS override_fnsku_rows,
        SUM(local_spec_match_rule='OVERRIDE_SID_ASIN_UNIQUE') AS override_asin_rows,
        SUM(local_spec_match_rule='NO_MATCH') AS no_match_rows,
        ROUND(SUM(COALESCE(monthly_storage_fee,0)),6) AS monthly_storage_fee_sum,
        ROUND(SUM(COALESCE(long_term_storage_fee,0)),6) AS long_term_storage_fee_sum,
        MIN(built_at) AS min_built_at,MAX(built_at) AS max_built_at
    FROM {quoted_table(target_table)}
    WHERE month_of_charge=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (month,))
        return lower_row(cur.fetchone())


def print_target_summary(row: dict[str, Any]) -> None:
    print("\n===== 目标宽表结果 =====")
    for key in (
        "target_rows", "target_unique_rows", "complete_rows", "partial_rows",
        "no_field_rows", "no_dim_rows", "direct_rows", "override_fnsku_rows",
        "override_asin_rows", "no_match_rows", "monthly_storage_fee_sum",
        "long_term_storage_fee_sum", "min_built_at", "max_built_at",
    ):
        print(f"{key}\t{text(row.get(key))}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="构建支持安全覆盖映射的FBA仓储费本地规格月度宽表"
    )
    parser.add_argument("--action", required=True, choices=["audit", "build"])
    parser.add_argument("--month", required=True, type=validate_month)
    parser.add_argument("--storage-table", default=DEFAULT_STORAGE_TABLE)
    parser.add_argument("--spec-table", default=DEFAULT_SPEC_TABLE)
    parser.add_argument("--override-table", default=DEFAULT_OVERRIDE_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    args = parser.parse_args()

    conn = db_connect(autocommit=False)
    try:
        for table_name in (args.storage_table, args.spec_table, args.override_table):
            if not table_exists(conn, table_name):
                raise RuntimeError(f"表不存在或不可访问：{table_name}")
        summary = source_summary(
            conn,args.storage_table,args.spec_table,args.override_table,args.month
        )
        print_source_summary(args.month, summary)
        if args.action == "audit":
            conn.rollback()
            logger.info("audit模式：未修改数据库")
            return
        build_month(
            conn,args.storage_table,args.spec_table,args.override_table,
            args.target_table,args.month,
        )
        print_target_summary(target_summary(conn,args.target_table,args.month))
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("用户中断")
        sys.exit(130)
    except Exception as exc:
        logger.exception("执行失败：%s", exc)
        sys.exit(1)
