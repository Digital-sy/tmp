#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 ods_db.ods_lx_fba_storage_fee_month 导出后台仓储规格精简表。

处理规则：
1. 先按接口唯一键 sid + fnsku + month_of_charge + fulfillment_center 去重，
   保留 etl_load_time 最新、id 最大的一条；
2. 最终粒度：店铺 + FNSKU + 国家；
3. 同一最终粒度出现多组后台规格时，选择覆盖平均库存量最高的一组；
   库存相同时，依次选择覆盖仓库数更多、ETL 时间更新的一组；
4. 本地 SKU 优先使用 sid + fnsku 直接匹配，直接匹配不到时使用安全覆盖映射；
5. item_volume 和 volume_units 保留接口原始值，不做体积单位换算；
6. weight 统一换算为 lb；
7. 只输出业务指定的 14 列。
"""
from __future__ import annotations

import argparse
import csv
import hashlib
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

DEFAULT_SOURCE_TABLE = "ods_db.ods_lx_fba_storage_fee_month"
DEFAULT_DIM_TABLE = "dim_db.dim_local_package_spec_resolved"
DEFAULT_OVERRIDE_TABLE = "dim_db.dim_storage_local_spec_match_override"
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

OUTPUT_FIELDS = [
    "ASIN",
    "FNSKU",
    "没有去店铺前缀的SKU",
    "SKU",
    "店铺",
    "国家",
    "商品体积item_volume",
    "商品体积单位volume_units",
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


def table_exists(conn, name: str) -> bool:
    schema_name, table_name = split_table(name)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s",
            (schema_name, table_name),
        )
        row = cur.fetchone() or {}
        return int(row.get("cnt") or 0) > 0


def parse_stores(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_STORES)
    stores = [item.strip() for item in value.split(",") if item.strip()]
    if not stores:
        raise ValueError("店铺列表不能为空")
    return list(dict.fromkeys(stores))


def normalize_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cte_sql(source_table: str, stores: list[str]) -> tuple[str, list[Any]]:
    source = quoted_table(source_table)
    placeholders = ",".join(["%s"] * len(stores))
    params: list[Any] = stores.copy()
    sql = f"""
    WITH source_ranked AS (
        SELECT
            s.*,
            COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),'') AS fc_norm,
            ROW_NUMBER() OVER (
                PARTITION BY
                    s.sid,
                    s.fnsku,
                    s.month_of_charge,
                    COALESCE(TRIM(BOTH '\\'' FROM s.fulfillment_center),'')
                ORDER BY s.etl_load_time DESC, s.id DESC
            ) AS dedup_rn
        FROM {source} s
        WHERE s.month_of_charge=%s
          AND s.store_name IN ({placeholders})
    ),
    dedup AS (
        SELECT *
        FROM source_ranked
        WHERE dedup_rn=1
    ),
    spec_group AS (
        SELECT
            store_name,
            sid,
            fnsku,
            COALESCE(country_code,'') AS country_code,
            COALESCE(asin,'') AS asin,
            longest_side,
            median_side,
            shortest_side,
            COALESCE(measurement_units,'') AS measurement_units,
            weight,
            COALESCE(weight_units,'') AS weight_units,
            item_volume,
            COALESCE(volume_units,'') AS volume_units,
            COALESCE(product_size_tier,'') AS product_size_tier,
            SUM(GREATEST(COALESCE(average_quantity_on_hand,0),0)) AS covered_inventory,
            COUNT(DISTINCT fc_norm) AS warehouse_cnt,
            MAX(etl_load_time) AS latest_etl_load_time,
            MAX(id) AS latest_id
        FROM dedup
        GROUP BY
            store_name,
            sid,
            fnsku,
            COALESCE(country_code,''),
            COALESCE(asin,''),
            longest_side,
            median_side,
            shortest_side,
            COALESCE(measurement_units,''),
            weight,
            COALESCE(weight_units,''),
            item_volume,
            COALESCE(volume_units,''),
            COALESCE(product_size_tier,'')
    ),
    spec_ranked AS (
        SELECT
            g.*,
            ROW_NUMBER() OVER (
                PARTITION BY g.sid,g.fnsku,g.country_code
                ORDER BY
                    g.covered_inventory DESC,
                    g.warehouse_cnt DESC,
                    g.latest_etl_load_time DESC,
                    g.latest_id DESC,
                    g.asin ASC
            ) AS spec_rn,
            COUNT(*) OVER (
                PARTITION BY g.sid,g.fnsku,g.country_code
            ) AS spec_group_cnt
        FROM spec_group g
    )
    """
    return sql, params


def weight_lb_sql(alias: str = "r") -> str:
    return f"""
    CASE
      WHEN {alias}.weight IS NULL THEN NULL
      WHEN LOWER(TRIM({alias}.weight_units)) IN
           ('pounds','pound','lbs','lb')
        THEN ROUND({alias}.weight,6)
      WHEN LOWER(TRIM({alias}.weight_units)) IN
           ('ounces','ounce','oz')
        THEN ROUND({alias}.weight / 16,6)
      WHEN LOWER(TRIM({alias}.weight_units)) IN
           ('kilograms','kilogram','kgs','kg')
        THEN ROUND({alias}.weight * 2.20462262185,6)
      WHEN LOWER(TRIM({alias}.weight_units)) IN
           ('grams','gram','g')
        THEN ROUND({alias}.weight / 453.59237,6)
      ELSE NULL
    END
    """


def main() -> None:
    parser = argparse.ArgumentParser(
        description="导出按店铺+FNSKU+国家聚合的后台仓储规格精简表"
    )
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--stores", help="英文逗号分隔；默认使用内置9个店铺")
    parser.add_argument("--source-table", default=DEFAULT_SOURCE_TABLE)
    parser.add_argument("--dim-table", default=DEFAULT_DIM_TABLE)
    parser.add_argument("--override-table", default=DEFAULT_OVERRIDE_TABLE)
    parser.add_argument("--output", help="输出CSV路径")
    args = parser.parse_args()

    datetime.strptime(args.month, "%Y-%m")
    stores = parse_stores(args.stores)
    output = Path(
        args.output
        or f"/data/exports/fba_storage_backend_specs_{args.month}_selected_stores.csv"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output.with_suffix(output.suffix + ".part")
    temp_output.unlink(missing_ok=True)

    dim = quoted_table(args.dim_table)
    override = quoted_table(args.override_table)
    cte, store_params = cte_sql(args.source_table, stores)
    params = [args.month, *store_params]

    conn = connect()
    try:
        for table_name in (args.source_table, args.dim_table, args.override_table):
            if not table_exists(conn, table_name):
                raise RuntimeError(f"表不存在或不可访问：{table_name}")

        placeholders = ",".join(["%s"] * len(stores))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS raw_rows,
                    COUNT(DISTINCT CONCAT_WS('|',sid,fnsku,month_of_charge,
                        COALESCE(TRIM(BOTH '\\'' FROM fulfillment_center),''))) AS unique_rows
                FROM {quoted_table(args.source_table)}
                WHERE month_of_charge=%s
                  AND store_name IN ({placeholders})
                """,
                params,
            )
            raw_summary = cur.fetchone() or {}

            cur.execute(
                cte
                + """
                SELECT
                    COUNT(*) AS final_rows,
                    SUM(spec_group_cnt>1) AS multi_spec_rows,
                    SUM(item_volume IS NULL) AS blank_item_volume_rows,
                    SUM(COALESCE(TRIM(volume_units),'')='') AS blank_volume_unit_rows,
                    SUM(LOWER(TRIM(weight_units)) NOT IN (
                        'pounds','pound','lbs','lb','ounces','ounce','oz',
                        'kilograms','kilogram','kgs','kg','grams','gram','g'
                    )) AS unsupported_weight_unit_rows
                FROM spec_ranked
                WHERE spec_rn=1
                """,
                params,
            )
            final_summary = cur.fetchone() or {}

            cur.execute(
                cte
                + f"""
                SELECT
                    r.asin AS `ASIN`,
                    r.fnsku AS `FNSKU`,
                    COALESCE(NULLIF(d0.sku,''),NULLIF(d1.sku,''),'')
                        AS `没有去店铺前缀的SKU`,
                    COALESCE(NULLIF(d0.msku,''),NULLIF(d1.msku,''),
                             NULLIF(d0.sku,''),NULLIF(d1.sku,''),'')
                        AS `SKU`,
                    r.store_name AS `店铺`,
                    r.country_code AS `国家`,
                    r.item_volume AS `商品体积item_volume`,
                    r.volume_units AS `商品体积单位volume_units`,
                    r.product_size_tier AS `弃置尺寸类型`,
                    {weight_lb_sql('r')} AS `计费重量（lb)`,
                    r.longest_side AS `后台规格长`,
                    r.median_side AS `后台规格宽`,
                    r.shortest_side AS `后台规格高`,
                    r.measurement_units AS `后台规格单位`
                FROM spec_ranked r
                LEFT JOIN {dim} d0
                  ON d0.sid=r.sid AND d0.fnsku=r.fnsku
                LEFT JOIN {override} o
                  ON o.target_sid=r.sid
                 AND o.target_fnsku=r.fnsku
                 AND o.target_asin=r.asin
                LEFT JOIN {dim} d1
                  ON d1.sid=o.source_sid AND d1.fnsku=o.source_fnsku
                WHERE r.spec_rn=1
                ORDER BY FIELD(r.store_name,{placeholders}),r.fnsku,r.country_code
                """,
                [*params, *stores],
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    raw_rows = int(raw_summary.get("raw_rows") or 0)
    unique_rows = int(raw_summary.get("unique_rows") or 0)
    final_rows = int(final_summary.get("final_rows") or 0)
    if final_rows == 0:
        raise RuntimeError("筛选后没有最终数据，拒绝生成空文件")
    if len(rows) != final_rows:
        raise RuntimeError(
            f"最终查询行数不一致：summary={final_rows}, fetched={len(rows)}"
        )

    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("店铺") or ""),
            str(row.get("FNSKU") or ""),
            str(row.get("国家") or ""),
        )
        if key in seen:
            raise RuntimeError(f"最终粒度重复：{key}")
        seen.add(key)

    with temp_output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {field: normalize_csv_value(row.get(field)) for field in OUTPUT_FIELDS}
            )
    os.replace(temp_output, output)

    print(f"\n===== {args.month} 后台规格精简导出审计 =====")
    print(f"raw_rows\t{raw_rows}")
    print(f"unique_interface_key_rows\t{unique_rows}")
    print(f"duplicate_rows_removed\t{raw_rows - unique_rows}")
    print(f"final_store_fnsku_country_rows\t{final_rows}")
    print(f"multi_spec_keys\t{int(final_summary.get('multi_spec_rows') or 0)}")
    print(
        "blank_item_volume_rows\t"
        f"{int(final_summary.get('blank_item_volume_rows') or 0)}"
    )
    print(
        "blank_volume_unit_rows\t"
        f"{int(final_summary.get('blank_volume_unit_rows') or 0)}"
    )
    print(
        "unsupported_weight_unit_rows\t"
        f"{int(final_summary.get('unsupported_weight_unit_rows') or 0)}"
    )
    print("\n===== 导出完成 =====")
    print(f"output\t{output}")
    print(f"rows\t{final_rows}")
    print(f"size_mb\t{output.stat().st_size / 1024 / 1024:.2f}")
    print(f"sha256\t{sha256_file(output)}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("用户中断，未完成的 .part 文件可删除", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"导出失败：{exc}", file=sys.stderr)
        sys.exit(1)
