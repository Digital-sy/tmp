#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
local_spec_resolver_v3.py

从 lingxing.listing 与 lingxing.产品管理生成 FNSKU 本地包装规格解析维表。

每个字段独立解析。

普通规则：
1. SKU_EXACT：目标 SKU 自身存在有效值；
2. SPU_SIZE_AVG：同一原始 SPU、同尺码其他 SKU 的平均值；
3. SPU_AVG：同一原始 SPU 所有有效 SKU 的平均值；
4. UNRESOLVED：仍无数据。

包装长/宽/高额外规则：
4. ALIAS_SPU_SIZE_AVG：市场前缀 SPU 对应的基础 SPU 同尺码平均；
5. ALIAS_SPU_AVG：市场前缀 SPU 对应的基础 SPU 整体平均；
6. UNRESOLVED。

例如：EULTY303 自身没有包装尺寸、LTY303 有包装尺寸时，
EULTY303-NRO-S 的重量仍优先使用自身数据，包装尺寸可降级到 LTY303 的 S 码平均。
只有原始 SPU 没有任何包装尺寸，并且去前缀后的 SPU 确有包装尺寸时，才启用别名。
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
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

SPEC_FIELDS = (
    "package_length",
    "package_width",
    "package_height",
    "gross_weight",
)
DIMENSION_FIELDS = {
    "package_length",
    "package_width",
    "package_height",
}
OUTPUT_FIELD_MAP = {
    "package_length": "local_package_length",
    "package_width": "local_package_width",
    "package_height": "local_package_height",
    "gross_weight": "local_gross_weight",
}
FIELD_LABELS = {
    "package_length": "包装长度",
    "package_width": "包装宽度",
    "package_height": "包装高度",
    "gross_weight": "单品毛重",
}
MARKET_PREFIXES = (
    "EU", "US", "CA", "AU", "MX", "UK", "DE", "FR", "IT", "ES", "NL"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("local_spec_resolver_v3")


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


def quantize(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


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
    value = normalize_sku_for_parse(sku)
    if not value:
        return ""

    for suffix in ("XXX-LARGE", "XX-LARGE", "X-LARGE", "X-SMALL", "ONE-SIZE"):
        if value == suffix or value.endswith("-" + suffix):
            return SIZE_ALIASES.get(suffix, suffix)

    token = value.rsplit("-", 1)[-1]
    return SIZE_ALIASES.get(token, token)


def infer_spu_from_sku(sku: Any) -> str:
    value = normalize_sku_for_parse(sku)
    return value.split("-", 1)[0] if value else ""


@dataclass
class ProductMeta:
    sku: str
    raw_spu: str
    row_id: int


@dataclass
class FieldValue:
    value: Decimal | None = None
    source: str = "UNRESOLVED"
    source_spu: str = ""
    source_skus: tuple[str, ...] = ()
    source_row_ids: tuple[int, ...] = ()

    @property
    def source_sku_count(self) -> int:
        return len(self.source_skus)


@dataclass
class SkuPartialSpec:
    sku: str
    raw_spu: str
    values: dict[str, Decimal | None] = field(default_factory=dict)
    row_ids: dict[str, int | None] = field(default_factory=dict)

    def has_any(self) -> bool:
        return any(self.values.get(name) is not None for name in SPEC_FIELDS)

    def has_all(self) -> bool:
        return all(self.values.get(name) is not None for name in SPEC_FIELDS)


@dataclass
class GroupAggregate:
    values: dict[str, Decimal | None]
    source_skus: dict[str, tuple[str, ...]]
    source_row_ids: dict[str, tuple[int, ...]]


@dataclass
class ResolverIndex:
    sku_meta: dict[str, ProductMeta]
    sku_specs: dict[str, SkuPartialSpec]
    spu_size_specs: dict[tuple[str, str], GroupAggregate]
    spu_specs: dict[str, GroupAggregate]
    dimension_spus: set[str]
    known_spus: set[str]


def load_listing_rows(conn, listing_table: str, limit: int = 0) -> list[dict[str, Any]]:
    limit_sql = f" LIMIT {int(limit)}" if limit > 0 else ""
    sql = f"""
        SELECT
            `id` AS id,
            `店铺id` AS sid,
            `店铺` AS store_name,
            `FNSKU` AS fnsku,
            `SKU` AS sku,
            `MSKU` AS msku,
            `ASIN` AS asin
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
            `id` AS id,
            `SKU` AS sku,
            `SPU` AS spu,
            `包装长度` AS package_length,
            `包装宽度` AS package_width,
            `包装高度` AS package_height,
            `单品毛重` AS gross_weight,
            `更新时间` AS updated_at
        FROM {quoted_table(product_table)}
        WHERE COALESCE(TRIM(`SKU`), '') <> ''
        ORDER BY `id` DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return lower_rows(cur.fetchall())


def dedupe_listing_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[int, str]] = set()
    result: list[dict[str, Any]] = []

    for row in rows:
        key = (to_int(row.get("sid")), normalize_key(row.get("fnsku")))
        if key[0] == 0 or not key[1] or key in seen:
            continue
        seen.add(key)
        result.append(row)

    return result


def build_group_aggregate(rows: list[SkuPartialSpec]) -> GroupAggregate:
    values: dict[str, Decimal | None] = {}
    source_skus: dict[str, tuple[str, ...]] = {}
    source_row_ids: dict[str, tuple[int, ...]] = {}

    for field_name in SPEC_FIELDS:
        contributors = [
            row for row in rows
            if row.values.get(field_name) is not None
        ]
        if not contributors:
            values[field_name] = None
            source_skus[field_name] = ()
            source_row_ids[field_name] = ()
            continue

        total = sum(
            (
                row.values[field_name]
                for row in contributors
                if row.values[field_name] is not None
            ),
            Decimal("0"),
        )
        values[field_name] = quantize(total / Decimal(len(contributors)))
        source_skus[field_name] = tuple(sorted(row.sku for row in contributors))
        source_row_ids[field_name] = tuple(sorted({
            row.row_ids[field_name]
            for row in contributors
            if row.row_ids.get(field_name)
        }))

    return GroupAggregate(
        values=values,
        source_skus=source_skus,
        source_row_ids=source_row_ids,
    )


def build_resolver_index(product_rows: list[dict[str, Any]]) -> ResolverIndex:
    """
    同一 SKU 的每个字段分别取按 id 倒序遇到的第一个有效值。
    新记录缺少某字段时，可由旧记录补充，但旧值不会覆盖新记录中的有效值。
    """
    sku_meta: dict[str, ProductMeta] = {}
    sku_values: dict[str, dict[str, Decimal | None]] = {}
    sku_row_ids: dict[str, dict[str, int | None]] = {}
    known_spus: set[str] = set()

    for row in product_rows:
        sku = normalize_key(row.get("sku"))
        if not sku:
            continue

        raw_spu = normalize_key(row.get("spu")) or infer_spu_from_sku(sku)
        if raw_spu:
            known_spus.add(raw_spu)

        if sku not in sku_meta:
            sku_meta[sku] = ProductMeta(
                sku=sku,
                raw_spu=raw_spu,
                row_id=to_int(row.get("id")),
            )
            sku_values[sku] = {name: None for name in SPEC_FIELDS}
            sku_row_ids[sku] = {name: None for name in SPEC_FIELDS}

        for field_name in SPEC_FIELDS:
            if sku_values[sku][field_name] is not None:
                continue

            value = to_decimal(row.get(field_name))
            if value is not None:
                sku_values[sku][field_name] = value
                sku_row_ids[sku][field_name] = to_int(row.get("id")) or None

    sku_specs: dict[str, SkuPartialSpec] = {}
    for sku, meta in sku_meta.items():
        sku_specs[sku] = SkuPartialSpec(
            sku=sku,
            raw_spu=meta.raw_spu,
            values=sku_values[sku],
            row_ids=sku_row_ids[sku],
        )

    by_spu_size: dict[tuple[str, str], list[SkuPartialSpec]] = defaultdict(list)
    by_spu: dict[str, list[SkuPartialSpec]] = defaultdict(list)

    for spec in sku_specs.values():
        if not spec.has_any() or not spec.raw_spu:
            continue

        by_spu[spec.raw_spu].append(spec)
        size_token = extract_size_token(spec.sku)
        if size_token:
            by_spu_size[(spec.raw_spu, size_token)].append(spec)

    spu_size_specs = {
        key: build_group_aggregate(rows)
        for key, rows in by_spu_size.items()
    }
    spu_specs = {
        spu: build_group_aggregate(rows)
        for spu, rows in by_spu.items()
    }

    dimension_spus = {
        spu
        for spu, group in spu_specs.items()
        if any(group.values.get(field_name) is not None for field_name in DIMENSION_FIELDS)
    }

    complete_count = sum(spec.has_all() for spec in sku_specs.values())
    partial_count = sum(spec.has_any() and not spec.has_all() for spec in sku_specs.values())
    empty_count = len(sku_specs) - complete_count - partial_count

    logger.info(
        "产品规格索引完成：sku_meta=%s complete=%s partial=%s empty=%s "
        "spu_size=%s spu=%s dimension_spu=%s",
        len(sku_meta),
        complete_count,
        partial_count,
        empty_count,
        len(spu_size_specs),
        len(spu_specs),
        len(dimension_spus),
    )
    for field_name in SPEC_FIELDS:
        field_count = sum(
            spec.values.get(field_name) is not None
            for spec in sku_specs.values()
        )
        logger.info("字段有效SKU：%s=%s", FIELD_LABELS[field_name], field_count)

    return ResolverIndex(
        sku_meta=sku_meta,
        sku_specs=sku_specs,
        spu_size_specs=spu_size_specs,
        spu_specs=spu_specs,
        dimension_spus=dimension_spus,
        known_spus=known_spus,
    )


def find_dimension_alias_spu(
    raw_spu: str,
    index: ResolverIndex,
) -> tuple[str, str]:
    """
    仅在原 SPU 没有任何包装尺寸，且去市场前缀后的 SPU 确有包装尺寸时返回别名。
    """
    raw_spu = normalize_key(raw_spu)
    if not raw_spu or raw_spu in index.dimension_spus:
        return "", ""

    for prefix in MARKET_PREFIXES:
        if not raw_spu.startswith(prefix) or len(raw_spu) <= len(prefix):
            continue

        candidate = raw_spu[len(prefix):]
        if candidate in index.dimension_spus:
            return candidate, f"STRIP_{prefix}_PREFIX_FOR_DIMENSIONS"

    return "", ""


def group_field_value(
    field_name: str,
    group: GroupAggregate | None,
    source: str,
    source_spu: str,
) -> FieldValue | None:
    if group is None or group.values.get(field_name) is None:
        return None

    return FieldValue(
        value=group.values[field_name],
        source=source,
        source_spu=source_spu,
        source_skus=group.source_skus[field_name],
        source_row_ids=group.source_row_ids[field_name],
    )


def resolve_field(
    field_name: str,
    sku: str,
    raw_spu: str,
    size_token: str,
    dimension_alias_spu: str,
    index: ResolverIndex,
) -> FieldValue:
    exact_spec = index.sku_specs.get(sku)
    if exact_spec and exact_spec.values.get(field_name) is not None:
        row_id = exact_spec.row_ids.get(field_name)
        return FieldValue(
            value=exact_spec.values[field_name],
            source="SKU_EXACT",
            source_spu=raw_spu,
            source_skus=(sku,),
            source_row_ids=((row_id,) if row_id else ()),
        )

    if raw_spu and size_token:
        resolved = group_field_value(
            field_name,
            index.spu_size_specs.get((raw_spu, size_token)),
            "SPU_SIZE_AVG",
            raw_spu,
        )
        if resolved:
            return resolved

    if raw_spu:
        resolved = group_field_value(
            field_name,
            index.spu_specs.get(raw_spu),
            "SPU_AVG",
            raw_spu,
        )
        if resolved:
            return resolved

    if field_name in DIMENSION_FIELDS and dimension_alias_spu:
        if size_token:
            resolved = group_field_value(
                field_name,
                index.spu_size_specs.get((dimension_alias_spu, size_token)),
                "ALIAS_SPU_SIZE_AVG",
                dimension_alias_spu,
            )
            if resolved:
                return resolved

        resolved = group_field_value(
            field_name,
            index.spu_specs.get(dimension_alias_spu),
            "ALIAS_SPU_AVG",
            dimension_alias_spu,
        )
        if resolved:
            return resolved

    return FieldValue()


def summarize_overall_source(field_values: dict[str, FieldValue]) -> str:
    resolved_sources = [
        value.source
        for value in field_values.values()
        if value.value is not None
    ]
    if not resolved_sources:
        return "UNRESOLVED"
    if len(resolved_sources) < len(SPEC_FIELDS):
        return "PARTIAL"
    if len(set(resolved_sources)) == 1:
        return resolved_sources[0]
    return "MIXED"


def resolve_spec_for_sku(
    sku_value: Any,
    index: ResolverIndex,
) -> dict[str, Any]:
    sku = normalize_key(sku_value)
    meta = index.sku_meta.get(sku)

    raw_spu = (
        meta.raw_spu
        if meta and meta.raw_spu
        else infer_spu_from_sku(sku)
    )
    spu_source = (
        "PRODUCT_MANAGEMENT"
        if meta and meta.raw_spu
        else ("SKU_PREFIX" if raw_spu else "")
    )
    size_token = extract_size_token(sku)
    dimension_alias_spu, dimension_alias_rule = find_dimension_alias_spu(
        raw_spu,
        index,
    )

    field_values = {
        field_name: resolve_field(
            field_name=field_name,
            sku=sku,
            raw_spu=raw_spu,
            size_token=size_token,
            dimension_alias_spu=dimension_alias_spu,
            index=index,
        )
        for field_name in SPEC_FIELDS
    }

    all_source_skus = sorted({
        source_sku
        for field_value in field_values.values()
        for source_sku in field_value.source_skus
    })
    all_source_row_ids = sorted({
        row_id
        for field_value in field_values.values()
        for row_id in field_value.source_row_ids
    })

    result: dict[str, Any] = {
        "sku": sku,
        "raw_spu": raw_spu,
        "spu_source": spu_source,
        "size_token": size_token,
        "dimension_alias_spu": dimension_alias_spu,
        "dimension_alias_rule": dimension_alias_rule,
        "local_spec_source": summarize_overall_source(field_values),
        "local_spec_source_sku_count": len(all_source_skus),
        "local_spec_source_skus": ",".join(all_source_skus),
        "local_spec_source_row_ids": ",".join(
            str(row_id) for row_id in all_source_row_ids
        ),
    }

    for field_name, field_value in field_values.items():
        output_name = OUTPUT_FIELD_MAP[field_name]
        result[output_name] = field_value.value
        result[f"{output_name}_source"] = field_value.source
        result[f"{output_name}_source_spu"] = field_value.source_spu
        result[f"{output_name}_source_sku_count"] = field_value.source_sku_count
        result[f"{output_name}_source_skus"] = ",".join(field_value.source_skus)
        result[f"{output_name}_source_row_ids"] = ",".join(
            str(row_id) for row_id in field_value.source_row_ids
        )

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
        row = {
            "sid": to_int(listing.get("sid")),
            "store_name": text(listing.get("store_name")),
            "fnsku": normalize_key(listing.get("fnsku")),
            "asin": normalize_key(listing.get("asin")),
            "msku": text(listing.get("msku")),
            "sku": spec["sku"],
            "raw_spu": spec["raw_spu"],
            "spu_source": spec["spu_source"],
            "size_token": spec["size_token"],
            "dimension_alias_spu": spec["dimension_alias_spu"],
            "dimension_alias_rule": spec["dimension_alias_rule"],
            "local_dimension_unit": dimension_unit,
            "local_weight_unit": weight_unit,
            "local_spec_source": spec["local_spec_source"],
            "local_spec_source_sku_count": spec["local_spec_source_sku_count"],
            "local_spec_source_skus": spec["local_spec_source_skus"],
            "local_spec_source_row_ids": spec["local_spec_source_row_ids"],
            "resolved_at": now,
        }

        for field_name in SPEC_FIELDS:
            output_name = OUTPUT_FIELD_MAP[field_name]
            for suffix in (
                "",
                "_source",
                "_source_spu",
                "_source_sku_count",
                "_source_skus",
                "_source_row_ids",
            ):
                row[f"{output_name}{suffix}"] = spec[f"{output_name}{suffix}"]

        resolved.append(row)

    return resolved


def print_audit(
    rows: list[dict[str, Any]],
    sample_unresolved: int = 30,
) -> None:
    total = len(rows)
    counts = Counter(row["local_spec_source"] for row in rows)

    print("\n===== 本地规格整体来源 =====")
    print("source\trow_cnt\tcoverage_pct")
    source_order = (
        "SKU_EXACT",
        "SPU_SIZE_AVG",
        "SPU_AVG",
        "ALIAS_SPU_SIZE_AVG",
        "ALIAS_SPU_AVG",
        "MIXED",
        "PARTIAL",
        "UNRESOLVED",
    )
    for source in source_order:
        count = counts.get(source, 0)
        pct = round(count / total * 100, 2) if total else 0
        print(f"{source}\t{count}\t{pct}")
    print(f"TOTAL\t{total}\t100.0")

    complete_count = sum(
        all(row.get(OUTPUT_FIELD_MAP[name]) is not None for name in SPEC_FIELDS)
        for row in rows
    )
    any_count = sum(
        any(row.get(OUTPUT_FIELD_MAP[name]) is not None for name in SPEC_FIELDS)
        for row in rows
    )

    print("\n===== 完整度汇总 =====")
    print("metric\trow_cnt\tcoverage_pct")
    for metric, count in (
        ("COMPLETE_4_FIELDS", complete_count),
        ("ANY_FIELD", any_count),
        ("NO_FIELD", total - any_count),
    ):
        pct = round(count / total * 100, 2) if total else 0
        print(f"{metric}\t{count}\t{pct}")

    print("\n===== 各字段覆盖率 =====")
    print("field\tresolved_rows\tcoverage_pct")
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        count = sum(row.get(output_name) is not None for row in rows)
        pct = round(count / total * 100, 2) if total else 0
        print(f"{output_name}\t{count}\t{pct}")

    print("\n===== 各字段来源分布 =====")
    print("field\tsource\trow_cnt")
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        field_counts = Counter(
            row.get(f"{output_name}_source", "UNRESOLVED")
            for row in rows
        )
        for source, count in field_counts.most_common():
            print(f"{output_name}\t{source}\t{count}")

    alias_counts = Counter(
        (row["dimension_alias_rule"], row["raw_spu"], row["dimension_alias_spu"])
        for row in rows
        if row["dimension_alias_spu"]
    )
    print("\n===== 包装尺寸 SPU 别名命中（前30） =====")
    print("rule\traw_spu\talias_spu\trow_cnt")
    if alias_counts:
        for (rule, raw_spu, alias_spu), count in alias_counts.most_common(30):
            print(f"{rule}\t{raw_spu}\t{alias_spu}\t{count}")
    else:
        print("（无命中）")

    unresolved_dimensions = [
        row
        for row in rows
        if all(
            row.get(OUTPUT_FIELD_MAP[name]) is None
            for name in DIMENSION_FIELDS
        )
    ]
    unresolved_spu = Counter(
        row["raw_spu"]
        for row in unresolved_dimensions
        if row["raw_spu"]
    )

    print("\n===== 包装尺寸未解析最多的 SPU（前20） =====")
    print("raw_spu\trow_cnt")
    for raw_spu, count in unresolved_spu.most_common(20):
        print(f"{raw_spu}\t{count}")

    print(f"\n===== 包装尺寸未解析样例（最多 {sample_unresolved} 条） =====")
    print(
        "sid\tstore_name\tfnsku\tsku\traw_spu\tsize_token\t"
        "dimension_alias_spu\tdimension_alias_rule"
    )
    for row in unresolved_dimensions[:sample_unresolved]:
        print(
            f"{row['sid']}\t{row['store_name']}\t{row['fnsku']}\t"
            f"{row['sku']}\t{row['raw_spu']}\t{row['size_token']}\t"
            f"{row['dimension_alias_spu']}\t{row['dimension_alias_rule']}"
        )


def create_table_sql(table_name: str) -> str:
    field_columns: list[str] = []
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        field_columns.extend([
            f"`{output_name}` DECIMAL(18,6) NULL COMMENT '{FIELD_LABELS[field_name]}'",
            f"`{output_name}_source` VARCHAR(40) NOT NULL COMMENT '该字段来源'",
            f"`{output_name}_source_spu` VARCHAR(500) NULL COMMENT '该字段来源SPU'",
            f"`{output_name}_source_sku_count` INT NOT NULL DEFAULT 0 COMMENT '该字段来源SKU数'",
            f"`{output_name}_source_skus` TEXT NULL COMMENT '该字段来源SKU'",
            f"`{output_name}_source_row_ids` TEXT NULL COMMENT '该字段来源产品管理行ID'",
        ])

    return f"""
        CREATE TABLE {quoted_table(table_name)} (
            `sid` INT NOT NULL COMMENT '领星店铺ID',
            `store_name` VARCHAR(100) NULL COMMENT '店铺名',
            `fnsku` VARCHAR(100) NOT NULL COMMENT 'FNSKU',
            `asin` VARCHAR(50) NULL COMMENT 'ASIN',
            `msku` VARCHAR(255) NULL COMMENT 'MSKU',
            `sku` VARCHAR(500) NOT NULL COMMENT '本地SKU',
            `raw_spu` VARCHAR(500) NULL COMMENT '产品管理原始SPU',
            `spu_source` VARCHAR(30) NULL COMMENT '原始SPU来源',
            `size_token` VARCHAR(100) NULL COMMENT 'SKU尾段识别尺码',
            `dimension_alias_spu` VARCHAR(500) NULL COMMENT '包装尺寸降级基础SPU',
            `dimension_alias_rule` VARCHAR(60) NULL COMMENT '包装尺寸SPU别名规则',
            {", ".join(field_columns)},
            `local_dimension_unit` VARCHAR(20) NULL COMMENT '本地包装尺寸单位',
            `local_weight_unit` VARCHAR(20) NULL COMMENT '本地单品毛重单位',
            `local_spec_source` VARCHAR(40) NOT NULL COMMENT '整体解析来源',
            `local_spec_source_sku_count` INT NOT NULL DEFAULT 0 COMMENT '全部字段来源SKU数',
            `local_spec_source_skus` TEXT NULL COMMENT '全部字段来源SKU',
            `local_spec_source_row_ids` TEXT NULL COMMENT '全部字段来源产品管理行ID',
            `resolved_at` DATETIME NOT NULL COMMENT '解析时间',
            PRIMARY KEY (`sid`, `fnsku`),
            KEY `idx_sku` (`sku`),
            KEY `idx_raw_spu_size` (`raw_spu`, `size_token`),
            KEY `idx_alias_spu_size` (`dimension_alias_spu`, `size_token`),
            KEY `idx_spec_source` (`local_spec_source`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COMMENT='FNSKU本地包装规格按字段降级解析维表'
    """


def write_target_table(
    conn,
    target_table: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        raise RuntimeError("解析结果为空，拒绝重建目标表")

    schema_name, base_name = split_table(target_table)
    temp_name = f"{base_name}__tmp"
    backup_name = f"{base_name}__bak"
    temp_table = f"{schema_name}.{temp_name}"
    backup_table = f"{schema_name}.{backup_name}"

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {quoted_table(temp_table)}")
        cur.execute(create_table_sql(temp_table))

    fields = [
        "sid", "store_name", "fnsku", "asin", "msku", "sku",
        "raw_spu", "spu_source", "size_token",
        "dimension_alias_spu", "dimension_alias_rule",
    ]
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        fields.extend([
            output_name,
            f"{output_name}_source",
            f"{output_name}_source_spu",
            f"{output_name}_source_sku_count",
            f"{output_name}_source_skus",
            f"{output_name}_source_row_ids",
        ])
    fields.extend([
        "local_dimension_unit",
        "local_weight_unit",
        "local_spec_source",
        "local_spec_source_sku_count",
        "local_spec_source_skus",
        "local_spec_source_row_ids",
        "resolved_at",
    ])

    field_sql = ",".join(f"`{field_name}`" for field_name in fields)
    value_sql = ",".join(f"%({field_name})s" for field_name in fields)
    insert_sql = (
        f"INSERT INTO {quoted_table(temp_table)} "
        f"({field_sql}) VALUES ({value_sql})"
    )

    try:
        with conn.cursor() as cur:
            inserted = 0
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start:start + BATCH_SIZE]
                cur.executemany(insert_sql, batch)
                inserted += len(batch)
                logger.info("临时维表写入进度：%s/%s", inserted, len(rows))
        conn.commit()
    except Exception:
        conn.rollback()
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {quoted_table(temp_table)}")
        logger.exception("临时维表写入失败")
        raise

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {quoted_table(backup_table)}")

        if table_exists(conn, target_table):
            cur.execute(
                f"RENAME TABLE "
                f"{quoted_table(target_table)} TO {quoted_table(backup_table)}, "
                f"{quoted_table(temp_table)} TO {quoted_table(target_table)}"
            )
            cur.execute(f"DROP TABLE {quoted_table(backup_table)}")
        else:
            cur.execute(
                f"RENAME TABLE "
                f"{quoted_table(temp_table)} TO {quoted_table(target_table)}"
            )

    logger.info("本地规格维表替换完成：%s 行", len(rows))


def print_single_result(
    result: dict[str, Any],
    dimension_unit: str,
    weight_unit: str,
) -> None:
    printable = dict(result)
    printable["local_dimension_unit"] = dimension_unit
    printable["local_weight_unit"] = weight_unit

    print("\n===== 单个 SKU 解析结果 =====")
    for key, value in printable.items():
        print(f"{key}: {value}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="本地包装规格按字段降级解析（含市场前缀SPU尺寸降级）"
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["audit", "build", "resolve"],
        help="audit只读审计；build原子替换维表；resolve抽查一个SKU",
    )
    parser.add_argument("--sku", help="action=resolve 时必填")
    parser.add_argument("--listing-table", default=DEFAULT_LISTING_TABLE)
    parser.add_argument("--product-table", default=DEFAULT_PRODUCT_TABLE)
    parser.add_argument("--target-table", default=DEFAULT_TARGET_TABLE)
    parser.add_argument("--dimension-unit", default=DEFAULT_DIMENSION_UNIT)
    parser.add_argument("--weight-unit", default=DEFAULT_WEIGHT_UNIT)
    parser.add_argument(
        "--listing-limit",
        type=int,
        default=0,
        help="测试时限制 listing 读取条数；0 表示不限制",
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
            print_single_result(
                result,
                args.dimension_unit,
                args.weight_unit,
            )
            return

        if not table_exists(conn, args.listing_table):
            raise RuntimeError(f"表不存在或不可访问：{args.listing_table}")

        listing_rows = load_listing_rows(
            conn,
            args.listing_table,
            args.listing_limit,
        )
        logger.info("读取 listing：%s 行", len(listing_rows))

        resolved_rows = resolve_listing_rows(
            listing_rows,
            index,
            args.dimension_unit,
            args.weight_unit,
        )
        logger.info("完成解析：%s 行", len(resolved_rows))
        print_audit(
            resolved_rows,
            max(0, args.sample_unresolved),
        )

        if args.action == "build":
            write_target_table(
                conn,
                args.target_table,
                resolved_rows,
            )
        else:
            conn.rollback()
            logger.info("audit 模式：未修改数据库")
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
