#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
local_spec_resolver.py
======================

从 lingxing.listing 与 lingxing.产品管理生成“本地包装规格解析维表”。

解析逻辑按字段独立执行，优先级为：
1. SKU_EXACT      ：目标 SKU 自身该字段存在有效值；
2. SPU_SIZE_AVG   ：同一标准化 SPU、同尺码的其他 SKU 该字段平均值；
3. SPU_AVG        ：同一标准化 SPU 下所有有效 SKU 该字段平均值；
4. UNRESOLVED     ：该字段仍无法得到。

与旧版相比：
- 不再要求长、宽、高、毛重四个字段必须同时存在才使用 SKU；
- 每个字段分别降级，最大限度保留 SKU 自身真实数据；
- 对 EU/US/CA/AU/MX/UK/DE/FR/IT/ES/NL 等前缀做“有条件标准化”：
  只有去掉前缀后的 SPU 在规格数据中真实存在时才映射，避免误合并；
- 输出每个字段自己的来源，并保留参与平均的 SKU 和产品管理行 ID；
- audit 模式只读；build 模式才重建目标维表。

默认单位：
- 包装长度/宽度/高度：cm
- 单品毛重：g

示例：
    python local_spec_resolver.py --action audit
    python local_spec_resolver.py --action resolve --sku EULTY303-NRO-S
    python local_spec_resolver.py --action build \
        --target-table dim_db.dim_local_package_spec_resolved
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
    """取 SKU 最后一段并标准化常见尺码别名。"""
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
    source_skus: tuple[str, ...] = ()
    source_row_ids: tuple[int, ...] = ()

    @property
    def source_sku_count(self) -> int:
        return len(self.source_skus)


@dataclass
class SkuPartialSpec:
    sku: str
    raw_spu: str
    canonical_spu: str
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
    spec_spus: set[str]
    known_spus: set[str]


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


def canonicalize_spu(raw_spu: str, spec_spus: set[str]) -> tuple[str, str]:
    """
    仅在原 SPU 本身没有规格、去市场前缀后的 SPU 确实有规格时才映射。
    """
    raw_spu = normalize_key(raw_spu)
    if not raw_spu:
        return "", ""
    if raw_spu in spec_spus:
        return raw_spu, "EXACT_SPU"

    for prefix in MARKET_PREFIXES:
        if raw_spu.startswith(prefix) and len(raw_spu) > len(prefix):
            candidate = raw_spu[len(prefix):]
            if candidate in spec_spus:
                return candidate, f"STRIP_{prefix}_PREFIX"

    return raw_spu, "NO_SPEC_ALIAS"


def build_group_aggregate(rows: list[SkuPartialSpec]) -> GroupAggregate:
    values: dict[str, Decimal | None] = {}
    source_skus: dict[str, tuple[str, ...]] = {}
    source_row_ids: dict[str, tuple[int, ...]] = {}

    for field_name in SPEC_FIELDS:
        contributors = [
            row for row in rows if row.values.get(field_name) is not None
        ]
        if not contributors:
            values[field_name] = None
            source_skus[field_name] = ()
            source_row_ids[field_name] = ()
            continue

        total = sum(
            (row.values[field_name] for row in contributors if row.values[field_name] is not None),
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
    每个 SKU 每个字段分别取按 id 倒序遇到的第一个有效值。
    因此旧记录可补充新记录缺失字段，但不会覆盖更新记录中的有效值。
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
            if sku_values[sku][field_name] is None:
                value = to_decimal(row.get(field_name))
                if value is not None:
                    sku_values[sku][field_name] = value
                    sku_row_ids[sku][field_name] = to_int(row.get("id")) or None

    spec_spus: set[str] = set()
    for sku, meta in sku_meta.items():
        if any(sku_values[sku].get(name) is not None for name in SPEC_FIELDS):
            if meta.raw_spu:
                spec_spus.add(meta.raw_spu)

    sku_specs: dict[str, SkuPartialSpec] = {}
    for sku, meta in sku_meta.items():
        canonical_spu, _ = canonicalize_spu(meta.raw_spu, spec_spus)
        sku_specs[sku] = SkuPartialSpec(
            sku=sku,
            raw_spu=meta.raw_spu,
            canonical_spu=canonical_spu,
            values=sku_values[sku],
            row_ids=sku_row_ids[sku],
        )

    by_spu_size: dict[tuple[str, str], list[SkuPartialSpec]] = defaultdict(list)
    by_spu: dict[str, list[SkuPartialSpec]] = defaultdict(list)
    for spec in sku_specs.values():
        if not spec.has_any() or not spec.canonical_spu:
            continue
        by_spu[spec.canonical_spu].append(spec)
        size_token = extract_size_token(spec.sku)
        if size_token:
            by_spu_size[(spec.canonical_spu, size_token)].append(spec)

    spu_size_specs = {
        key: build_group_aggregate(rows) for key, rows in by_spu_size.items()
    }
    spu_specs = {
        spu: build_group_aggregate(rows) for spu, rows in by_spu.items()
    }

    complete_count = sum(spec.has_all() for spec in sku_specs.values())
    partial_count = sum(spec.has_any() and not spec.has_all() for spec in sku_specs.values())
    empty_count = len(sku_specs) - complete_count - partial_count

    logger.info(
        "产品规格索引完成：sku_meta=%s complete=%s partial=%s empty=%s "
        "spu_size=%s spu=%s",
        len(sku_meta), complete_count, partial_count, empty_count,
        len(spu_size_specs), len(spu_specs),
    )
    for field_name in SPEC_FIELDS:
        field_count = sum(
            spec.values.get(field_name) is not None for spec in sku_specs.values()
        )
        logger.info("字段有效SKU：%s=%s", FIELD_LABELS[field_name], field_count)

    return ResolverIndex(
        sku_meta=sku_meta,
        sku_specs=sku_specs,
        spu_size_specs=spu_size_specs,
        spu_specs=spu_specs,
        spec_spus=spec_spus,
        known_spus=known_spus,
    )


def resolve_field(
    field_name: str,
    sku: str,
    canonical_spu: str,
    size_token: str,
    index: ResolverIndex,
) -> FieldValue:
    exact_spec = index.sku_specs.get(sku)
    if exact_spec and exact_spec.values.get(field_name) is not None:
        row_id = exact_spec.row_ids.get(field_name)
        return FieldValue(
            value=exact_spec.values[field_name],
            source="SKU_EXACT",
            source_skus=(sku,),
            source_row_ids=((row_id,) if row_id else ()),
        )

    if canonical_spu and size_token:
        group = index.spu_size_specs.get((canonical_spu, size_token))
        if group and group.values.get(field_name) is not None:
            return FieldValue(
                value=group.values[field_name],
                source="SPU_SIZE_AVG",
                source_skus=group.source_skus[field_name],
                source_row_ids=group.source_row_ids[field_name],
            )

    if canonical_spu:
        group = index.spu_specs.get(canonical_spu)
        if group and group.values.get(field_name) is not None:
            return FieldValue(
                value=group.values[field_name],
                source="SPU_AVG",
                source_skus=group.source_skus[field_name],
                source_row_ids=group.source_row_ids[field_name],
            )

    return FieldValue()


def summarize_overall_source(field_values: dict[str, FieldValue]) -> str:
    resolved_sources = [
        value.source for value in field_values.values()
        if value.value is not None
    ]
    resolved_count = len(resolved_sources)
    if resolved_count == 0:
        return "UNRESOLVED"
    if resolved_count < len(SPEC_FIELDS):
        return "PARTIAL"
    unique_sources = set(resolved_sources)
    if len(unique_sources) == 1:
        return resolved_sources[0]
    return "MIXED"


def resolve_spec_for_sku(sku_value: Any, index: ResolverIndex) -> dict[str, Any]:
    sku = normalize_key(sku_value)
    meta = index.sku_meta.get(sku)
    raw_spu = (
        meta.raw_spu if meta and meta.raw_spu
        else infer_spu_from_sku(sku)
    )
    raw_spu_source = (
        "PRODUCT_MANAGEMENT"
        if meta and meta.raw_spu
        else ("SKU_PREFIX" if raw_spu else "")
    )
    canonical_spu, spu_normalize_rule = canonicalize_spu(raw_spu, index.spec_spus)
    size_token = extract_size_token(sku)

    field_values = {
        field_name: resolve_field(
            field_name, sku, canonical_spu, size_token, index
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
        "spu": canonical_spu,
        "spu_source": raw_spu_source,
        "spu_normalize_rule": spu_normalize_rule,
        "size_token": size_token,
        "local_spec_source": summarize_overall_source(field_values),
        "local_spec_source_sku_count": len(all_source_skus),
        "local_spec_source_skus": ",".join(all_source_skus),
        "local_spec_source_row_ids": ",".join(str(row_id) for row_id in all_source_row_ids),
    }

    for field_name, field_value in field_values.items():
        output_name = OUTPUT_FIELD_MAP[field_name]
        result[output_name] = field_value.value
        result[f"{output_name}_source"] = field_value.source
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
            "spu": spec["spu"],
            "spu_source": spec["spu_source"],
            "spu_normalize_rule": spec["spu_normalize_rule"],
            "size_token": spec["size_token"],
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
            row[output_name] = spec[output_name]
            row[f"{output_name}_source"] = spec[f"{output_name}_source"]
            row[f"{output_name}_source_sku_count"] = spec[
                f"{output_name}_source_sku_count"
            ]
            row[f"{output_name}_source_skus"] = spec[
                f"{output_name}_source_skus"
            ]
            row[f"{output_name}_source_row_ids"] = spec[
                f"{output_name}_source_row_ids"
            ]
        resolved.append(row)

    return resolved


def print_audit(rows: list[dict[str, Any]], sample_unresolved: int = 30) -> None:
    counts = Counter(row["local_spec_source"] for row in rows)
    total = len(rows)

    print("\n===== 本地规格解析覆盖率 =====")
    print("source\trow_cnt\tcoverage_pct")
    for source in [
        "SKU_EXACT", "SPU_SIZE_AVG", "SPU_AVG",
        "MIXED", "PARTIAL", "UNRESOLVED",
    ]:
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
    print(
        f"COMPLETE_4_FIELDS\t{complete_count}\t"
        f"{round(complete_count / total * 100, 2) if total else 0}"
    )
    print(
        f"ANY_FIELD\t{any_count}\t"
        f"{round(any_count / total * 100, 2) if total else 0}"
    )
    print(f"NO_FIELD\t{total - any_count}\t{round((total-any_count)/total*100,2) if total else 0}")

    print("\n===== 各字段覆盖率 =====")
    print("field\tresolved_rows\tcoverage_pct")
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        count = sum(row.get(output_name) is not None for row in rows)
        pct = round(count / total * 100, 2) if total else 0
        print(f"{output_name}\t{count}\t{pct}")

    alias_counts = Counter(
        row["spu_normalize_rule"]
        for row in rows
        if row["spu_normalize_rule"].startswith("STRIP_")
    )
    print("\n===== SPU 前缀标准化命中 =====")
    print("rule\trow_cnt")
    if alias_counts:
        for rule, count in alias_counts.most_common():
            print(f"{rule}\t{count}")
    else:
        print("（无命中）")

    unresolved = [
        row for row in rows if row["local_spec_source"] == "UNRESOLVED"
    ]
    print(f"\n===== 未解析样例（最多 {sample_unresolved} 条） =====")
    print("sid\tstore_name\tfnsku\tsku\traw_spu\tspu\tspu_normalize_rule\tsize_token")
    for row in unresolved[:sample_unresolved]:
        print(
            f"{row['sid']}\t{row['store_name']}\t{row['fnsku']}\t"
            f"{row['sku']}\t{row['raw_spu']}\t{row['spu']}\t"
            f"{row['spu_normalize_rule']}\t{row['size_token']}"
        )

    unresolved_spu = Counter(row["raw_spu"] for row in unresolved if row["raw_spu"])
    print("\n===== 未解析最多的原始 SPU（前20） =====")
    print("raw_spu\trow_cnt")
    for raw_spu, count in unresolved_spu.most_common(20):
        print(f"{raw_spu}\t{count}")

    unresolved_store = Counter(
        (row["sid"], row["store_name"]) for row in unresolved
    )
    print("\n===== 未解析按店铺（前30） =====")
    print("sid\tstore_name\trow_cnt")
    for (sid, store_name), count in unresolved_store.most_common(30):
        print(f"{sid}\t{store_name}\t{count}")


def create_target_table(conn, target_table: str) -> None:
    field_columns = []
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        field_columns.extend([
            f"`{output_name}` DECIMAL(18,6) NULL COMMENT '{FIELD_LABELS[field_name]}'",
            f"`{output_name}_source` VARCHAR(30) NOT NULL COMMENT '该字段来源'",
            f"`{output_name}_source_sku_count` INT NOT NULL DEFAULT 0 COMMENT '该字段来源SKU数'",
            f"`{output_name}_source_skus` TEXT NULL COMMENT '该字段来源SKU'",
            f"`{output_name}_source_row_ids` TEXT NULL COMMENT '该字段来源产品管理行ID'",
        ])

    sql = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table(target_table)} (
            `sid` INT NOT NULL COMMENT '领星店铺ID',
            `store_name` VARCHAR(100) NULL COMMENT '店铺名',
            `fnsku` VARCHAR(100) NOT NULL COMMENT 'FNSKU',
            `asin` VARCHAR(50) NULL COMMENT 'ASIN',
            `msku` VARCHAR(255) NULL COMMENT 'MSKU',
            `sku` VARCHAR(500) NOT NULL COMMENT '本地SKU',
            `raw_spu` VARCHAR(500) NULL COMMENT '原始SPU',
            `spu` VARCHAR(500) NULL COMMENT '标准化SPU',
            `spu_source` VARCHAR(30) NULL COMMENT 'SPU来源',
            `spu_normalize_rule` VARCHAR(40) NULL COMMENT 'SPU标准化规则',
            `size_token` VARCHAR(100) NULL COMMENT 'SKU尾段识别尺码',
            {", ".join(field_columns)},
            `local_dimension_unit` VARCHAR(20) NULL COMMENT '本地包装尺寸单位',
            `local_weight_unit` VARCHAR(20) NULL COMMENT '本地单品毛重单位',
            `local_spec_source` VARCHAR(30) NOT NULL COMMENT
                'SKU_EXACT/SPU_SIZE_AVG/SPU_AVG/MIXED/PARTIAL/UNRESOLVED',
            `local_spec_source_sku_count` INT NOT NULL DEFAULT 0 COMMENT '全部字段来源SKU数',
            `local_spec_source_skus` TEXT NULL COMMENT '全部字段来源SKU',
            `local_spec_source_row_ids` TEXT NULL COMMENT '全部字段来源产品管理行ID',
            `resolved_at` DATETIME NOT NULL COMMENT '解析时间',
            PRIMARY KEY (`sid`, `fnsku`),
            KEY `idx_sku` (`sku`),
            KEY `idx_spu_size` (`spu`, `size_token`),
            KEY `idx_spec_source` (`local_spec_source`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
          COMMENT='FNSKU本地包装规格按字段三级降级解析维表'
    """
    with conn.cursor() as cur:
        cur.execute(sql)


def write_target_table(conn, target_table: str, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise RuntimeError("解析结果为空，拒绝清空目标表")

    create_target_table(conn, target_table)

    fields = [
        "sid", "store_name", "fnsku", "asin", "msku", "sku",
        "raw_spu", "spu", "spu_source", "spu_normalize_rule", "size_token",
    ]
    for field_name in SPEC_FIELDS:
        output_name = OUTPUT_FIELD_MAP[field_name]
        fields.extend([
            output_name,
            f"{output_name}_source",
            f"{output_name}_source_sku_count",
            f"{output_name}_source_skus",
            f"{output_name}_source_row_ids",
        ])
    fields.extend([
        "local_dimension_unit", "local_weight_unit",
        "local_spec_source", "local_spec_source_sku_count",
        "local_spec_source_skus", "local_spec_source_row_ids", "resolved_at",
    ])

    field_sql = ",".join(f"`{field}`" for field in fields)
    value_sql = ",".join(f"%({field})s" for field in fields)
    insert_sql = (
        f"INSERT INTO {quoted_table(target_table)} "
        f"({field_sql}) VALUES ({value_sql})"
    )

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
        description="本地包装规格按字段三级降级解析"
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["audit", "build", "resolve"],
        help="audit只读审计；build重建维表；resolve抽查一个SKU",
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

        listing_rows = load_listing_rows(
            conn, args.listing_table, args.listing_limit
        )
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
