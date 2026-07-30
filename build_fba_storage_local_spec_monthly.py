#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 lingxing.lingxing_storage_monthly 与
 dim_db.dim_local_package_spec_resolved 关联，生成月度仓储费本地规格宽表。

粒度：month_of_charge + sid + fnsku + fulfillment_center

只读审计：
    python build_fba_storage_local_spec_monthly.py --action audit --month 2026-06

构建指定月份：
    python build_fba_storage_local_spec_monthly.py --action build --month 2026-06
"""
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
DEFAULT_TARGET_TABLE = "dws_db.dws_fba_storage_local_spec_monthly"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("build_fba_storage_local_spec_monthly")


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
            """
            SELECT COUNT(*) AS cnt
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema_name, table_name),
        )
        return bool(to_int(lower_row(cur.fetchone()).get("cnt")))


def validate_month(value: str) -> str:
    datetime.strptime(value, "%Y-%m")
    return value


def create_target_table(conn, target_table: str) -> None:
    sql = f"""
    CREATE TABLE IF NOT EXISTS {quoted_table(target_table)} (
        `month_of_charge` CHAR(7) NOT NULL COMMENT '仓储费月份YYYY-MM',
        `store_name` VARCHAR(100) NULL COMMENT '店铺名',
        `sid` INT NOT NULL COMMENT '领星店铺ID',
        `fnsku` VARCHAR(100) NOT NULL COMMENT 'FNSKU',
        `asin` VARCHAR(50) NULL COMMENT 'ASIN',
        `fulfillment_center` VARCHAR(30) NOT NULL DEFAULT '' COMMENT '亚马逊仓库编号',
        `country_code` VARCHAR(10) NULL COMMENT '国家',
        `product_size_tier` VARCHAR(100) NULL COMMENT '亚马逊尺寸分段',
        `average_quantity_on_hand` DECIMAL(18,6) NULL COMMENT '平均库存数量',
        `storage_rate` DECIMAL(18,8) NULL COMMENT '月仓储费率',
        `currency` VARCHAR(10) NULL COMMENT '币种',
        `monthly_storage_fee` DECIMAL(18,8) NULL COMMENT '月仓储费',
        `long_term_storage_fee_6mo` DECIMAL(18,8) NULL COMMENT '长期仓储费6至12个月',
        `long_term_storage_fee_12mo` DECIMAL(18,8) NULL COMMENT '长期仓储费12个月以上',
        `long_term_storage_fee` DECIMAL(18,8) NULL COMMENT '长期仓储费合计',

        `msku` VARCHAR(255) NULL COMMENT 'MSKU',
        `sku` VARCHAR(500) NULL COMMENT '本地SKU',
        `raw_spu` VARCHAR(500) NULL COMMENT '产品管理原始SPU',
        `size_token` VARCHAR(100) NULL COMMENT '尺码标识',
        `dimension_alias_spu` VARCHAR(500) NULL COMMENT '包装尺寸降级基础SPU',
        `dimension_alias_rule` VARCHAR(60) NULL COMMENT '包装尺寸SPU别名规则',

        `local_package_length` DECIMAL(18,6) NULL COMMENT '本地包装长度',
        `local_package_length_source` VARCHAR(40) NULL COMMENT '本地包装长度来源',
        `local_package_width` DECIMAL(18,6) NULL COMMENT '本地包装宽度',
        `local_package_width_source` VARCHAR(40) NULL COMMENT '本地包装宽度来源',
        `local_package_height` DECIMAL(18,6) NULL COMMENT '本地包装高度',
        `local_package_height_source` VARCHAR(40) NULL COMMENT '本地包装高度来源',
        `local_dimension_unit` VARCHAR(20) NULL COMMENT '本地包装尺寸单位',
        `local_package_volume_cm3` DECIMAL(24,6) NULL COMMENT '本地包装体积cm3',
        `local_gross_weight` DECIMAL(18,6) NULL COMMENT '本地单品毛重',
        `local_gross_weight_source` VARCHAR(40) NULL COMMENT '本地单品毛重来源',
        `local_weight_unit` VARCHAR(20) NULL COMMENT '本地单品毛重单位',
        `local_spec_source` VARCHAR(40) NULL COMMENT '本地规格整体来源',
        `local_spec_match_status` VARCHAR(30) NOT NULL COMMENT '本地规格匹配状态',
        `local_spec_resolved_at` DATETIME NULL COMMENT '本地规格解析时间',

        `storage_source_created_at` DATETIME NULL COMMENT '仓储费源表写入时间',
        `built_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '宽表构建时间',

        PRIMARY KEY (`month_of_charge`,`sid`,`fnsku`,`fulfillment_center`),
        KEY `idx_store_month` (`store_name`,`month_of_charge`),
        KEY `idx_sku_month` (`sku`,`month_of_charge`),
        KEY `idx_spu_month` (`raw_spu`,`month_of_charge`),
        KEY `idx_match_status` (`local_spec_match_status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
      COMMENT='FBA月仓储费与本地包装规格关联宽表'
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def source_summary(conn, storage_table: str, spec_table: str, month: str) -> dict[str, Any]:
    sql = f"""
    SELECT
        COUNT(*) AS source_rows,
        COUNT(DISTINCT CONCAT_WS('|',s.sid,s.fnsku,s.month_of_charge,
            COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),''))) AS source_unique_rows,
        SUM(d.fnsku IS NOT NULL) AS matched_dim_rows,
        SUM(d.fnsku IS NULL) AS missing_dim_rows,
        SUM(d.fnsku IS NOT NULL AND d.local_package_length IS NOT NULL
            AND d.local_package_width IS NOT NULL
            AND d.local_package_height IS NOT NULL
            AND d.local_gross_weight IS NOT NULL) AS complete_local_spec_rows,
        SUM(d.fnsku IS NOT NULL AND (
            d.local_package_length IS NOT NULL OR d.local_package_width IS NOT NULL
            OR d.local_package_height IS NOT NULL OR d.local_gross_weight IS NOT NULL
        )) AS any_local_spec_rows,
        ROUND(SUM(COALESCE(s.monthly_storage_fee,0)),6) AS monthly_storage_fee_sum,
        ROUND(SUM(COALESCE(s.long_term_storage_fee,0)),6) AS long_term_storage_fee_sum
    FROM {quoted_table(storage_table)} s
    LEFT JOIN {quoted_table(spec_table)} d
      ON d.sid=s.sid AND d.fnsku=s.fnsku
    WHERE s.month_of_charge=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (month,))
        return lower_row(cur.fetchone())


def print_summary(title: str, row: dict[str, Any]) -> None:
    print(f"\n===== {title} =====")
    for key in (
        "source_rows", "source_unique_rows", "matched_dim_rows", "missing_dim_rows",
        "complete_local_spec_rows", "any_local_spec_rows",
        "monthly_storage_fee_sum", "long_term_storage_fee_sum",
    ):
        print(f"{key}\t{text(row.get(key))}")


def build_month(
    conn,
    storage_table: str,
    spec_table: str,
    target_table: str,
    month: str,
) -> None:
    summary = source_summary(conn, storage_table, spec_table, month)
    source_rows = to_int(summary.get("source_rows"))
    source_unique_rows = to_int(summary.get("source_unique_rows"))
    if source_rows == 0:
        raise RuntimeError(f"源表 {storage_table} 在 {month} 没有数据，拒绝构建")
    if source_rows != source_unique_rows:
        raise RuntimeError(
            f"源表存在重复唯一键：source_rows={source_rows}, unique={source_unique_rows}"
        )

    create_target_table(conn, target_table)
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
        local_spec_resolved_at,storage_source_created_at,built_at
    )
    SELECT
        s.month_of_charge,
        s.store_name,
        s.sid,
        s.fnsku,
        COALESCE(NULLIF(s.asin,''),d.asin) AS asin,
        COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),'') AS fulfillment_center,
        s.country_code,
        s.product_size_tier,
        s.average_quantity_on_hand,
        s.storage_rate,
        s.currency,
        s.monthly_storage_fee,
        s.long_term_storage_fee_6mo,
        s.long_term_storage_fee_12mo,
        s.long_term_storage_fee,
        d.msku,
        d.sku,
        d.raw_spu,
        d.size_token,
        d.dimension_alias_spu,
        d.dimension_alias_rule,
        d.local_package_length,
        d.local_package_length_source,
        d.local_package_width,
        d.local_package_width_source,
        d.local_package_height,
        d.local_package_height_source,
        d.local_dimension_unit,
        CASE
          WHEN d.local_package_length IS NOT NULL
           AND d.local_package_width IS NOT NULL
           AND d.local_package_height IS NOT NULL
          THEN ROUND(d.local_package_length*d.local_package_width*d.local_package_height,6)
          ELSE NULL
        END AS local_package_volume_cm3,
        d.local_gross_weight,
        d.local_gross_weight_source,
        d.local_weight_unit,
        d.local_spec_source,
        CASE
          WHEN d.fnsku IS NULL THEN 'NO_DIM_ROW'
          WHEN d.local_package_length IS NOT NULL
           AND d.local_package_width IS NOT NULL
           AND d.local_package_height IS NOT NULL
           AND d.local_gross_weight IS NOT NULL THEN 'COMPLETE'
          WHEN d.local_package_length IS NOT NULL
            OR d.local_package_width IS NOT NULL
            OR d.local_package_height IS NOT NULL
            OR d.local_gross_weight IS NOT NULL THEN 'PARTIAL'
          ELSE 'NO_FIELDS'
        END AS local_spec_match_status,
        d.resolved_at,
        s.created_at,
        NOW()
    FROM {quoted_table(storage_table)} s
    LEFT JOIN {quoted_table(spec_table)} d
      ON d.sid=s.sid AND d.fnsku=s.fnsku
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
        ROUND(SUM(COALESCE(monthly_storage_fee,0)),6) AS monthly_storage_fee_sum,
        ROUND(SUM(COALESCE(long_term_storage_fee,0)),6) AS long_term_storage_fee_sum,
        MIN(built_at) AS min_built_at,
        MAX(built_at) AS max_built_at
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
        "no_field_rows", "no_dim_rows", "monthly_storage_fee_sum",
        "long_term_storage_fee_sum", "min_built_at", "max_built_at",
    ):
        print(f"{key}\t{text(row.get(key))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="构建FBA仓储费本地规格月度宽表")
    parser.add_argument("--action", required=True, choices=["audit", "build"])
    parser.add_argument("--month", required=True, type=validate_month)
    parser.add_argument("--storage-table", default=DEFAULT_STORAGE_TABLE)
    parser.add_argument("--spec-table", default=DEFAULT_SPEC_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    args = parser.parse_args()

    conn = db_connect(autocommit=False)
    try:
        for table_name in (args.storage_table, args.spec_table):
            if not table_exists(conn, table_name):
                raise RuntimeError(f"表不存在或不可访问：{table_name}")

        summary = source_summary(conn, args.storage_table, args.spec_table, args.month)
        print_summary(f"{args.month} 源数据关联审计", summary)

        if args.action == "audit":
            conn.rollback()
            logger.info("audit模式：未修改数据库")
            return

        build_month(
            conn,
            args.storage_table,
            args.spec_table,
            args.target_table,
            args.month,
        )
        result = target_summary(conn, args.target_table, args.month)
        print_target_summary(result)
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
