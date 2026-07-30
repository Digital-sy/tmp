#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按月份和店铺流式导出 FBA 仓储费本地规格宽表为 CSV.GZ。"""
from __future__ import annotations

import argparse
import csv
import gzip
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

DEFAULT_TABLE = "dws_db.dws_fba_storage_local_spec_monthly"
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


def connect(cursorclass):
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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_stores(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_STORES)
    stores = [item.strip() for item in value.split(",") if item.strip()]
    if not stores:
        raise ValueError("店铺列表不能为空")
    return list(dict.fromkeys(stores))


def main() -> None:
    parser = argparse.ArgumentParser(description="筛选店铺导出 FBA 仓储费本地规格宽表")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument(
        "--stores",
        help="英文逗号分隔；未传时使用脚本内置的9个店铺",
    )
    parser.add_argument("--output", help="输出 .csv.gz 路径")
    parser.add_argument("--progress-every", type=int, default=50000)
    args = parser.parse_args()

    datetime.strptime(args.month, "%Y-%m")
    stores = parse_stores(args.stores)
    table = quoted_table(args.table)
    placeholders = ",".join(["%s"] * len(stores))
    params = [args.month, *stores]

    output = Path(
        args.output
        or f"/data/exports/fba_storage_local_spec_{args.month}_selected_stores.csv.gz"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output.with_suffix(output.suffix + ".part")
    if temp_output.exists():
        temp_output.unlink()

    summary_conn = connect(pymysql.cursors.DictCursor)
    try:
        with summary_conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    store_name,
                    COUNT(*) AS row_cnt,
                    COUNT(DISTINCT CONCAT_WS('|',sid,fnsku)) AS store_fnsku_cnt,
                    ROUND(SUM(COALESCE(monthly_storage_fee,0)),8) AS monthly_fee,
                    ROUND(SUM(COALESCE(long_term_storage_fee,0)),8) AS long_term_fee
                FROM {table}
                WHERE month_of_charge=%s
                  AND store_name IN ({placeholders})
                GROUP BY store_name
                """,
                params,
            )
            summary_rows = cur.fetchall()
    finally:
        summary_conn.close()

    summary_map = {str(row["store_name"]): row for row in summary_rows}
    expected_rows = sum(int(row["row_cnt"] or 0) for row in summary_rows)
    if expected_rows == 0:
        raise RuntimeError("筛选后没有数据，拒绝生成空文件")

    print(f"\n===== {args.month} 导出店铺汇总 =====")
    print("store_name\trow_cnt\tstore_fnsku_cnt\tmonthly_fee\tlong_term_fee")
    for store in stores:
        row = summary_map.get(store)
        if row is None:
            print(f"{store}\t0\t0\t0\t0")
        else:
            print(
                f"{store}\t{row['row_cnt']}\t{row['store_fnsku_cnt']}\t"
                f"{row['monthly_fee']}\t{row['long_term_fee']}"
            )
    print(f"TOTAL\t{expected_rows}")

    export_sql = f"""
        SELECT *
        FROM {table}
        WHERE month_of_charge=%s
          AND store_name IN ({placeholders})
    """

    export_conn = connect(pymysql.cursors.SSDictCursor)
    exported_rows = 0
    try:
        with export_conn.cursor() as cur:
            cur.execute(export_sql, params)
            fieldnames = [column[0] for column in cur.description]
            with gzip.open(
                temp_output,
                mode="wt",
                encoding="utf-8-sig",
                newline="",
                compresslevel=6,
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                while True:
                    batch = cur.fetchmany(5000)
                    if not batch:
                        break
                    for row in batch:
                        writer.writerow(
                            {key: normalize_csv_value(value) for key, value in row.items()}
                        )
                    exported_rows += len(batch)
                    if args.progress_every > 0 and exported_rows % args.progress_every < len(batch):
                        print(f"导出进度：{exported_rows}/{expected_rows}")
    finally:
        export_conn.close()

    if exported_rows != expected_rows:
        temp_output.unlink(missing_ok=True)
        raise RuntimeError(
            f"导出行数不一致：expected={expected_rows}, exported={exported_rows}"
        )

    os.replace(temp_output, output)
    size_mb = output.stat().st_size / 1024 / 1024
    digest = sha256_file(output)

    print("\n===== 导出完成 =====")
    print(f"output\t{output}")
    print(f"rows\t{exported_rows}")
    print(f"size_mb\t{size_mb:.2f}")
    print(f"sha256\t{digest}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("用户中断，未完成的 .part 文件可删除", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"导出失败：{exc}", file=sys.stderr)
        sys.exit(1)
