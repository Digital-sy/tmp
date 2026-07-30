#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
local_spec_resolver.py
======================

从 lingxing.listing 与 lingxing.产品管理生成“本地包装规格解析维表”。

解析优先级：
1. SKU_EXACT      ：目标 SKU 自身存在完整规格；
2. SPU_SIZE_AVG   ：目标 SKU 无完整规格，取同 SPU、同尺码、其他颜色 SKU 的平均规格；
3. SPU_AVG        ：仍无数据，取同 SPU 所有有效 SKU 的平均规格；
4. UNRESOLVED     ：无法得到规格。

“完整规格”定义：包装长度、包装宽度、包装高度、单品毛重均为大于 0 的数值。
同组平均按字段分别求算术平均，只纳入完整规格 SKU；每个 SKU 只采用最新一条完整记录。

默认单位：
- 包装长度/宽度/高度：cm
- 单品毛重：g
可通过命令行或环境变量修改。

只读审计：
    python local_spec_resolver.py --action audit

构建维表：
    python local_spec_resolver.py --action build \
        --target-table dim_db.dim_local_package_spec_resolved

抽查指定 SKU：
    python local_spec_resolver.py --action resolve --sku BX528-BLACK-S

注意：
- 默认按 SKU 最后一个“-”分段识别尺码。
- SPU 优先使用产品管理表中的 SPU；目标 SKU 不在产品管理表时，才降级为 SKU 第一个“-”前缀。
- 本脚本不会修改仓储费事实表。
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Iterable

import pymysql
import pymysql.cursors

from common import settings


DEFAULT_LISTING_TABLE = "lingxing.listing"
DEFAULT_PRODUCT_TABLE = "lingxing.产品管理"
DEFAULT_TARGET_TABLE = "dim_db.dim_local_package_spec_resolved"
DEFAULT_DIMENSION_UNIT = os.getenv("LOCAL_SPEC_DIMENSION_UNIT", "cm")
DEFAULT_WEIGHT_UNIT = os.getenv("LOCAL_SPEC_WEIGHT_UNIT", "g")
BATCH_SIZE = 5000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("local_spec_resolver")


def text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def normalize_key(value: Any) -> str:
    return text(value).upper()


def to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def to_decimal(value: Any) -> Decimal | None:
    if value in (None, "", "None", "null"):
        return None
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None
    return number if number > 0 else None


def lower_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return {str(key).lower(): value for key, value in row.items()}


def lower_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [lower_row(row) for row in rows]


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


SIZE_ALIASES = {
    "XXXS": "XXXS", "XXS": "XXS", "XS": "XS", "XSMALL": "XS",
    "X-SMALL": "XS", "SMALL": "S", "S": "S", "MEDIUM": "M", "M": "M",
    "LARGE": "L", "L": "L", "XL": "XL", "X-LARGE": "XL", "XLARGE": "XL",
    "XXL": "XXL", "2XL": "XXL", "XX-LARGE": "XXL", "XXXL": "XXXL",
    "3XL": "XXXL", "XXX-LARGE": "XXXL", "0X": "0X", "1X": "1X",
    "2X": "2X", "3X": "3X", "4X": "4X", "5X": "5X", "6X": "6X",
    "OS": "OS", "ONESIZE": "OS", "ONE-SIZE": "OS",
}


def normalize_sku_for_parse(sku: Any) -> str:
    value = normalize_key(sku)
    value = re.sub(r"[\s_/]+", "-", value)
    value = re.sub(r"-+", "-", value)
    return value.strip("-")


def extract_size_token(sku: Any) -> str:
    """默认取 SKU 最后一段，并标准化常见尺码别名。"""
    value = normalize_sku_for_parse(sku)
    if not value:
        return ""

    compound_suffixes = [
        "XXX-LARGE", "XX-LARGE", "X-LARGE", "X-SMALL", "ONE-SIZE",
    ]
    for suffix in compound_suffixes:
        if value == suffix or value.endswith("-" + suffix):
            return SIZE_ALIASES.get(suffix, suffix)

    token = value.rsplit("-", 1)[-1]
    return SIZE_ALIASES.get(token, token)


def infer_spu_from_sku(sku: Any) -> str:
    """产品管理不存在目标 SKU 时，才使用 SKU 第一段作为 SPU 兜底。"""
    value = normalize_sku_for_parse(sku)
    return value.split("-", 1)[0] if value else ""


@dataclass(frozen=True)
class Spec:
    package_length: Decimal
    package_width: Decimal
    package_height: Decimal
    gross_weight: Decimal
    source_skus: tuple[str, ...]
    source_row_ids: tuple[int, ...]

    @property
    def source_sku_count(self) -> int:
        return len(self.source_skus)


@dataclass
class ProductMeta:
    sku: str
    spu: str
    row_id: int


@dataclass
class ResolverIndex:
    sku_meta: dict[str, ProductMeta]
    exact_specs: dict[str, Spec]
    spu_size_specs: dict[tuple[str, str], Spec]
    spu_specs: dict[str, Spec]


def valid_complete_spec(row: dict[str, Any]) -> bool:
    fields = ["package_length", "package_width", "package_height", "gross_weight"]
    return all(to_decimal(row.get(field)) is not None for field in fields)


def make_spec(rows: list[dict[str, Any]]) -> Spec:
    if not rows:
        raise ValueError("make_spec 不接受空列表")

    def avg(field: str) -> Decimal:
        values = [to_decimal(row.get(field)) for row in rows]
        clean = [value for value in values if value is not None]
        if len(clean) != len(rows):
            raise ValueError(f"平均规格包含无效字段：{field}")
        result = sum(clean, Decimal("0")) / Decimal(len(clean))
        return result.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)

    source_skus = tuple(sorted({normalize_key(row.get("sku")) for row in rows if text(row.get("sku"))}))
    source_row_ids = tuple(sorted({to_int(row.get("id")) for row in rows if to_int(row.get("id"))}))
    return Spec(
        package_length=avg("package_length"),
        package_width=avg("package_width"),
        package_height=avg("package_height"),
        gross_weight=avg("gross_weight"),
        source_skus=source_skus,
        source_row_ids=source_row_ids,
    )


def load_listing_rows(conn, listing_table: str, limit: int = 0) -> list[dict[str, Any]]:
    limit_sql = f" LIMIT {int(limit)}" if limit > 0 else ""
    sql = f"""
        SELECT
            `id` AS id, `店铺id` AS sid, `店铺` AS store_name,
            `FNSKU` AS fnsku, `SKU` AS sku, `MSKU` AS msku, `ASIN` AS asin
        FROM {quoted_table(listing_table)}
        WHERE COALESCE(TRIM(`FNSKU`), '') <> ''
          AND COALESCE(TRIM(`SKU`), '') <> ''
        ORDER BY `id` DESC
        {limit_sql}
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return lower_rows(cur.fetchall())


def load_product_rows(conn, product_table: str) -> list[dict[str, Any]]:
    sql = f"""
        SELECT
            `id` AS id, `SKU` AS sku, `SPU` AS spu,
            `包装长度` AS package_length, `包装宽度` AS package_width,
            `包装高度` AS package_height, `单品毛重` AS gross_weight,
            `更新时间` AS updated_at
        FROM {quoted_table(product_table)}
        WHERE COALESCE(TRIM(`SKU`), '') <> ''
        ORDER BY `id` DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return lower_rows(cur.fetchall())


def dedupe_listing_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按 sid+FNSKU 保留 id 最大的 listing 记录。"""
    seen: set[tuple[int, str]] = set()
    result: list[dict[str, Any]] = []
    for row in rows:
        key = (to_int(row.get("sid")), normalize_key(row.get("fnsku")))
        if key[0] == 0 or not key[1] or key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def build_resolver_index(product_rows: list[dict[str, Any]]) -> ResolverIndex:
    sku_meta: dict[str, ProductMeta] = {}
    latest_complete_by_sku: dict[str, dict[str, Any]] = {}

    for row in product_rows:
        sku = normalize_key(row.get("sku"))
        if not sku:
            continue
        spu = normalize_key(row.get("spu")) or infer_spu_from_sku(sku)

        if sku not in sku_meta:
            sku_meta[sku] = ProductMeta(sku=sku, spu=spu, row_id=to_int(row.get("id")))
        if sku not in latest_complete_by_sku and valid_complete_spec(row):
            normalized = dict(row)
            normalized["sku"] = sku
            normalized["spu"] = spu
            latest_complete_by_sku[sku] = normalized

    exact_specs = {sku: make_spec([row]) for sku, row in latest_complete_by_sku.items()}
    by_spu_size: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    by_spu: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for sku, row in latest_complete_by_sku.items():
        spu = normalize_key(row.get("spu")) or infer_spu_from_sku(sku)
        size_token = extract_size_token(sku)
        if spu:
            by_spu[spu].append(row)
            if size_token:
                by_spu_size[(spu, size_token)].append(row)

    spu_size_specs = {key: make_spec(rows) for key, rows in by_spu_size.items() if rows}
    spu_specs = {spu: make_spec(rows) for spu, rows in by_spu.items() if rows}

    logger.info(
        "产品规格索引完成：sku_meta=%s exact=%s spu_size=%s spu=%s",
        len(sku_meta), len(exact_specs), len(spu_size_specs), len(spu_specs),
    )
    return ResolverIndex(
        sku_meta=sku_meta,
        exact_specs=exact_specs,
        spu_size_specs=spu_size_specs,
        spu_specs=spu_specs,
    )


def resolve_spec_for_sku(sku_value: Any, index: ResolverIndex) -> dict[str, Any]:
    sku = normalize_key(sku_value)
    meta = index.sku_meta.get(sku)
    spu = meta.spu if meta and meta.spu else infer_spu_from_sku(sku)
    spu_source = "PRODUCT_MANAGEMENT" if meta and meta.spu else ("SKU_PREFIX" if spu else "")
    size_token = extract_size_token(sku)

    source = "UNRESOLVED"
    spec: Spec | None = None
    if sku in index.exact_specs:
        source, spec = "SKU_EXACT", index.exact_specs[sku]
    elif spu and size_token and (spu, size_token) in index.spu_size_specs:
        source, spec = "SPU_SIZE_AVG", index.spu_size_specs[(spu, size_token)]
    elif spu and spu in index.spu_specs:
        source, spec = "SPU_AVG", index.spu_specs[spu]

    result = {
        "sku": sku, "spu": spu, "spu_source": spu_source, "size_token": size_token,
        "local_package_length": None, "local_package_width": None,
        "local_package_height": None, "local_gross_weight": None,
        "local_spec_source": source, "local_spec_source_sku_count": 0,
        "local_spec_source_skus": "", "local_spec_source_row_ids": "",
    }
    if spec is not None:
        result.update({
            "local_package_length": spec.package_length,
            "local_package_width": spec.package_width,
            "local_package_height": spec.package_height,
            "local_gross_weight": spec.gross_weight,
            "local_spec_source_sku_count": spec.source_sku_count,
            "local_spec_source_skus": ",".join(spec.source_skus),
            "local_spec_source_row_ids": ",".join(str(row_id) for row_id in spec.source_row_ids),
        })
    return result


def resolve_listing_rows(
    listing_rows: list[dict[str, Any]],
    index: ResolverIndex,
    dimension_unit: str,
    weight_unit: str,
) -> list[dict[str, Any]]:
    resolved: list[dict[str, Any]] = []
    now = datetime.now()
    for listing in dedupe_listing_rows(listing_rows):
        spec = resolve_spec_for_sku(listing.get("sku"), index)
        resolved.append({
            "sid": to_int(listing.get("sid")),
            "store_name": text(listing.get("store_name")),
            "fnsku": normalize_key(listing.get("fnsku")),
            "asin": normalize_key(listing.get("asin")),
            "msku": text(listing.get("msku")),
            "sku": spec["sku"], "spu": spec["spu"], "spu_source": spec["spu_source"],
            "size_token": spec["size_token"],
            "local_package_length": spec["local_package_length"],
            "local_package_width": spec["local_package_width"],
            "local_package_height": spec["local_package_height"],
            "local_dimension_unit": dimension_unit,
            "local_gross_weight": spec["local_gross_weight"],
            "local_weight_unit": weight_unit,
            "local_spec_source": spec["local_spec_source"],
            "local_spec_source_sku_count": spec["local_spec_source_sku_count"],
            "local_spec_source_skus": spec["local_spec_source_skus"],
            "local_spec_source_row_ids": spec["local_spec_source_row_ids"],
            "resolved_at": now,
        })
    return resolved


def print_audit(rows: list[dict[str, Any]], sample_unresolved: int = 30) -> None:
    counts = Counter(row["local_spec_source"] for row in rows)
    total = len(rows)
    print("\n===== 本地规格解析覆盖率 =====")
    print("source\trow_cnt\tcoverage_pct")
    for source in ["SKU_EXACT", "SPU_SIZE_AVG", "SPU_AVG", "UNRESOLVED"]:
        count = counts.get(source, 0)
        pct = round(count / total * 100, 2) if total else 0
        print(f"{source}\t{count}\t{pct}")
    print(f"TOTAL\t{total}\t100.0")

    unresolved = [row for row in rows if row["local_spec_source"] == "UNRESOLVED"]
    print(f"\n===== 未解析样例（最多 {sample_unresolved} 条） =====")
    print("sid\tstore_name\tfnsku\tsku\tspu\tsize_token")
    for row in unresolved[:sample_unresolved]:
        print(
            f"{row['sid']}\t{row['store_name']}\t{row['fnsku']}\t"
            f"{row['sku']}\t{row['spu']}\t{row['size_token']}"
        )


def create_target_table(conn, target_table: str) -> None:
    sql = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table(target_table)} (
            `sid` INT NOT NULL COMMENT '领星店铺ID',
            `store_name` VARCHAR(100) NULL COMMENT '店铺名',
            `fnsku` VARCHAR(100) NOT NULL COMMENT 'FNSKU',
            `asin` VARCHAR(50) NULL COMMENT 'ASIN',
            `msku` VARCHAR(255) NULL COMMENT 'MSKU',
            `sku` VARCHAR(500) NOT NULL COMMENT '本地SKU',
            `spu` VARCHAR(500) NULL COMMENT 'SPU',
            `spu_source` VARCHAR(30) NULL COMMENT 'SPU来源：产品管理或SKU前缀',
            `size_token` VARCHAR(100) NULL COMMENT '从SKU尾段识别的尺码',
            `local_package_length` DECIMAL(18,6) NULL COMMENT '本地包装长度',
            `local_package_width` DECIMAL(18,6) NULL COMMENT '本地包装宽度',
            `local_package_height` DECIMAL(18,6) NULL COMMENT '本地包装高度',
            `local_dimension_unit` VARCHAR(20) NULL COMMENT '本地包装尺寸单位',
            `local_gross_weight` DECIMAL(18,6) NULL COMMENT '本地单品毛重',
            `local_weight_unit` VARCHAR(20) NULL COMMENT '本地单品毛重单位',
            `local_spec_source` VARCHAR(30) NOT NULL COMMENT 'SKU_EXACT/SPU_SIZE_AVG/SPU_AVG/UNRESOLVED',
            `local_spec_source_sku_count` INT NOT NULL DEFAULT 0 COMMENT '参与规格计算的SKU数量',
            `local_spec_source_skus` TEXT NULL COMMENT '参与计算的来源SKU',
            `local_spec_source_row_ids` TEXT NULL COMMENT '产品管理来源行ID',
            `resolved_at` DATETIME NOT NULL COMMENT '解析时间',
            PRIMARY KEY (`sid`, `fnsku`),
            KEY `idx_sku` (`sku`),
            KEY `idx_spu_size` (`spu`, `size_token`),
            KEY `idx_spec_source` (`local_spec_source`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COMMENT='FNSKU本地包装规格三级降级解析维表'
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def write_target_table(conn, target_table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("解析结果为空，拒绝清空目标表")

    create_target_table(conn, target_table)
    fields = [
        "sid", "store_name", "fnsku", "asin", "msku", "sku", "spu", "spu_source",
        "size_token", "local_package_length", "local_package_width",
        "local_package_height", "local_dimension_unit", "local_gross_weight",
        "local_weight_unit", "local_spec_source", "local_spec_source_sku_count",
        "local_spec_source_skus", "local_spec_source_row_ids", "resolved_at",
    ]
    field_sql = ",".join(f"`{field}`" for field in fields)
    value_sql = ",".join(f"%({field})s" for field in fields)
    insert_sql = f"INSERT INTO {quoted_table(target_table)} ({field_sql}) VALUES ({value_sql})"

    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {quoted_table(target_table)}")
            deleted = cur.rowcount
            logger.info("目标表旧数据删除：%s 行", deleted)
            inserted = 0
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start:start + BATCH_SIZE]
                cur.executemany(insert_sql, batch)
                inserted += len(batch)
                logger.info("目标表写入进度：%s/%s", inserted, len(rows))
        conn.commit()
        logger.info("本地规格维表写入完成：%s 行", len(rows))
    except Exception:
        conn.rollback()
        logger.exception("写入失败，事务已回滚")
        raise


def print_single_result(result: dict[str, Any], dimension_unit: str, weight_unit: str) -> None:
    printable = dict(result)
    printable["local_dimension_unit"] = dimension_unit
    printable["local_weight_unit"] = weight_unit
    print("\n===== 单个 SKU 解析结果 =====")
    for key, value in printable.items():
        print(f"{key}: {value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="本地包装规格三级降级解析")
    parser.add_argument(
        "--action", required=True, choices=["audit", "build", "resolve"],
        help="audit只读审计；build重建维表；resolve抽查一个SKU",
    )
    parser.add_argument("--sku", help="action=resolve 时必填")
    parser.add_argument("--listing-table", default=DEFAULT_LISTING_TABLE)
    parser.add_argument("--product-table", default=DEFAULT_PRODUCT_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--dimension-unit", default=DEFAULT_DIMENSION_UNIT)
    parser.add_argument("--weight-unit", default=DEFAULT_WEIGHT_UNIT)
    parser.add_argument(
        "--listing-limit", type=int, default=0,
        help="测试时限制listing读取条数；0表示不限制",
    )
    parser.add_argument("--sample-unresolved", type=int, default=30)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    conn = db_connect(autocommit=False)
    try:
        if not table_exists(conn, args.product_table):
            raise RuntimeError(f"表不存在或不可访问：{args.product_table}")

        product_rows = load_product_rows(conn, args.product_table)
        logger.info("读取产品管理：%s 行", len(product_rows))
        index = build_resolver_index(product_rows)

        if args.action == "resolve":
            if not args.sku:
                raise ValueError("action=resolve 必须传 --sku")
            result = resolve_spec_for_sku(args.sku, index)
            print_single_result(result, args.dimension_unit, args.weight_unit)
            return

        if not table_exists(conn, args.listing_table):
            raise RuntimeError(f"表不存在或不可访问：{args.listing_table}")

        listing_rows = load_listing_rows(conn, args.listing_table, args.listing_limit)
        logger.info("读取listing：%s 行", len(listing_rows))
        resolved_rows = resolve_listing_rows(
            listing_rows, index, args.dimension_unit, args.weight_unit,
        )
        logger.info("完成解析：%s 行", len(resolved_rows))
        print_audit(resolved_rows, max(0, args.sample_unresolved))

        if args.action == "build":
            write_target_table(conn, args.target_table, resolved_rows)
        else:
            conn.rollback()
            logger.info("audit模式：未修改数据库")
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
