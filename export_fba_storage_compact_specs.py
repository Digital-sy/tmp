#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按月份与店铺导出一行一个店铺+FNSKU的精简后台规格 CSV。"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pymysql
import pymysql.cursors

from common import settings

DEFAULT_WIDE_TABLE = "dws_db.dws_fba_storage_local_spec_monthly"
DEFAULT_STORAGE_TABLE = "lingxing.lingxing_storage_monthly"
DEFAULT_STORES = [
    "SY-US",
    "RR-UK",
    "RR-EU",
    "RKZ-US",
    "MT-US",
    "JQ-US",
    "JQ-CA",
    "JQ-AU",
    "CY-US",
]

OUTPUT_COLUMNS = [
    "ASIN",
    "FNSKU",
    "没有去店铺前缀的SKU",
    "SKU",
    "店铺",
    "国家",
    "商品体积item_volume立方厘米（cm³）",
    "弃置尺寸类型",
    "计费重量（lb)",
    "后台规格长",
    "后台规格宽",
    "后台规格高",
    "后台规格单位",
]


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


def connect(cursorclass=pymysql.cursors.DictCursor):
    cfg = settings.db_config
    return pymysql.connect(
        host=cfg["host"],
        port=int(cfg.get("port", 3306)),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=cursorclass,
        autocommit=True,
        connect_timeout=30,
        read_timeout=1800,
        write_timeout=1800,
    )


def normalize_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def parse_stores(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_STORES)
    stores = [item.strip() for item in value.split(",") if item.strip()]
    if not stores:
        raise ValueError("店铺列表不能为空")
    return list(dict.fromkeys(stores))


def main() -> None:
    parser = argparse.ArgumentParser(description="导出精简FBA后台规格表")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--wide-table", default=DEFAULT_WIDE_TABLE)
    parser.add_argument("--storage-table", default=DEFAULT_STORAGE_TABLE)
    parser.add_argument("--stores", help="英文逗号分隔；默认使用内置9个店铺")
    parser.add_argument("--output", help="输出CSV路径")
    args = parser.parse_args()

    datetime.strptime(args.month, "%Y-%m")
    stores = parse_stores(args.stores)
    placeholders = ",".join(["%s"] * len(stores))
    params = [args.month, *stores, args.month, *stores]

    wide = quoted_table(args.wide_table)
    storage = quoted_table(args.storage_table)

    output = Path(
        args.output
        or f"/data/exports/fba_storage_compact_specs_{args.month}_selected_stores.csv"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output.with_suffix(output.suffix + ".part")
    temp_output.unlink(missing_ok=True)

    sql = f"""
    WITH wide_group AS (
        SELECT
            sid,
            store_name,
            fnsku,
            MAX(NULLIF(asin,'')) AS asin,
            MAX(NULLIF(sku,'')) AS no_store_prefix_sku,
            MAX(NULLIF(msku,'')) AS sku,
            MAX(NULLIF(country_code,'')) AS country_code,
            MAX(NULLIF(product_size_tier,'')) AS product_size_tier
        FROM {wide}
        WHERE month_of_charge=%s
          AND store_name IN ({placeholders})
        GROUP BY sid,store_name,fnsku
    ),
    storage_group AS (
        SELECT
            sid,
            store_name,
            fnsku,
            MAX(NULLIF(asin,'')) AS asin,
            MAX(NULLIF(country_code,'')) AS country_code,
            MAX(NULLIF(product_size_tier,'')) AS product_size_tier,
            MAX(longest_side) AS longest_side,
            MAX(median_side) AS median_side,
            MAX(shortest_side) AS shortest_side,
            MAX(NULLIF(measurement_units,'')) AS measurement_units,
            MAX(weight) AS weight,
            MAX(NULLIF(weight_units,'')) AS weight_units,
            MAX(item_volume) AS item_volume,
            MAX(NULLIF(volume_units,'')) AS volume_units
        FROM {storage}
        WHERE month_of_charge=%s
          AND store_name IN ({placeholders})
        GROUP BY sid,store_name,fnsku
    )
    SELECT
        COALESCE(w.asin,s.asin) AS `ASIN`,
        w.fnsku AS `FNSKU`,
        w.no_store_prefix_sku AS `没有去店铺前缀的SKU`,
        COALESCE(w.sku,w.no_store_prefix_sku) AS `SKU`,
        w.store_name AS `店铺`,
        COALESCE(w.country_code,s.country_code) AS `国家`,
        ROUND(
            CASE
                WHEN s.item_volume IS NOT NULL AND LOWER(COALESCE(s.volume_units,'')) IN
                    ('cubic centimeters','cubic centimeter','cm3','cm³')
                    THEN s.item_volume
                WHEN s.item_volume IS NOT NULL AND LOWER(COALESCE(s.volume_units,'')) IN
                    ('cubic inches','cubic inch','in3','in³')
                    THEN s.item_volume * 16.387064
                WHEN s.item_volume IS NOT NULL AND LOWER(COALESCE(s.volume_units,'')) IN
                    ('cubic feet','cubic foot','ft3','ft³')
                    THEN s.item_volume * 28316.846592
                WHEN s.item_volume IS NOT NULL AND LOWER(COALESCE(s.volume_units,'')) IN
                    ('cubic meters','cubic meter','m3','m³')
                    THEN s.item_volume * 1000000
                WHEN s.item_volume IS NOT NULL AND LOWER(COALESCE(s.volume_units,'')) IN
                    ('liters','liter','litres','litre','l')
                    THEN s.item_volume * 1000
                WHEN s.longest_side IS NOT NULL
                 AND s.median_side IS NOT NULL
                 AND s.shortest_side IS NOT NULL
                    THEN
                        (CASE
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.longest_side*2.54
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.longest_side*30.48
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.longest_side*100
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.longest_side/10
                            ELSE s.longest_side
                         END)
                        *
                        (CASE
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.median_side*2.54
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.median_side*30.48
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.median_side*100
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.median_side/10
                            ELSE s.median_side
                         END)
                        *
                        (CASE
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.shortest_side*2.54
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.shortest_side*30.48
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.shortest_side*100
                            WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.shortest_side/10
                            ELSE s.shortest_side
                         END)
                ELSE NULL
            END,
            6
        ) AS `商品体积item_volume立方厘米（cm³）`,
        COALESCE(w.product_size_tier,s.product_size_tier) AS `弃置尺寸类型`,
        ROUND(
            CASE
                WHEN s.weight IS NULL THEN NULL
                WHEN LOWER(COALESCE(s.weight_units,'')) IN ('pounds','pound','lbs','lb') THEN s.weight
                WHEN LOWER(COALESCE(s.weight_units,'')) IN ('ounces','ounce','oz') THEN s.weight/16
                WHEN LOWER(COALESCE(s.weight_units,'')) IN ('kilograms','kilogram','kgs','kg') THEN s.weight*2.2046226218
                WHEN LOWER(COALESCE(s.weight_units,'')) IN ('grams','gram','g') THEN s.weight*0.0022046226218
                ELSE s.weight
            END,
            6
        ) AS `计费重量（lb)`,
        ROUND(
            CASE
                WHEN s.longest_side IS NULL THEN NULL
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.longest_side*2.54
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.longest_side*30.48
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.longest_side*100
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.longest_side/10
                ELSE s.longest_side
            END,
            6
        ) AS `后台规格长`,
        ROUND(
            CASE
                WHEN s.median_side IS NULL THEN NULL
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.median_side*2.54
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.median_side*30.48
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.median_side*100
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.median_side/10
                ELSE s.median_side
            END,
            6
        ) AS `后台规格宽`,
        ROUND(
            CASE
                WHEN s.shortest_side IS NULL THEN NULL
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('inches','inch','in') THEN s.shortest_side*2.54
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('feet','foot','ft') THEN s.shortest_side*30.48
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('meters','meter','m') THEN s.shortest_side*100
                WHEN LOWER(COALESCE(s.measurement_units,'')) IN ('millimeters','millimeter','mm') THEN s.shortest_side/10
                ELSE s.shortest_side
            END,
            6
        ) AS `后台规格高`,
        CASE
            WHEN s.longest_side IS NULL AND s.median_side IS NULL AND s.shortest_side IS NULL THEN ''
            ELSE 'cm'
        END AS `后台规格单位`
    FROM wide_group w
    LEFT JOIN storage_group s
      ON s.sid=w.sid
     AND s.store_name=w.store_name
     AND s.fnsku=w.fnsku
    ORDER BY FIELD(w.store_name,{','.join(['%s']*len(stores))}),w.store_name,w.fnsku
    """

    query_params = [*params, *stores]
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, query_params)
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError("筛选后没有数据，拒绝生成空文件")

    store_counts: dict[str, int] = {store: 0 for store in stores}
    for row in rows:
        store_counts[str(row.get("店铺") or "")] = store_counts.get(str(row.get("店铺") or ""), 0) + 1

    with temp_output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_csv_value(row.get(key)) for key in OUTPUT_COLUMNS})

    os.replace(temp_output, output)

    print(f"\n===== {args.month} 精简规格导出汇总 =====")
    print("store_name\trow_cnt")
    for store in stores:
        print(f"{store}\t{store_counts.get(store,0)}")
    print(f"TOTAL\t{len(rows)}")
    print("\n===== 导出完成 =====")
    print(f"output\t{output}")
    print(f"rows\t{len(rows)}")
    print(f"size_mb\t{output.stat().st_size/1024/1024:.2f}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("用户中断，未完成的 .part 文件可删除", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"导出失败：{exc}", file=sys.stderr)
        sys.exit(1)
