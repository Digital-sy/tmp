#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 SKU + 国家 + 移除类型汇总 JQ 移除订单并导出为真正的 .xlsx。"""

import argparse
import os
import zipfile
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Sequence, Tuple
from xml.sax.saxutils import escape

import pymysql
import pymysql.cursors

from common import settings

TABLE = "lingxing_disposal_orders"
DEFAULT_ACCOUNT_NAME = "JQ-NA"

SUMMARY_HEADERS = [
    "SKU",
    "国家",
    "移除类型",
    "明细行数",
    "订单数",
    "申请数量",
    "取消数量",
    "净申请数量",
    "已销毁数量",
    "已发货数量",
    "处理中数量",
    "移除费用",
]

SUMMARY_KEYS = [
    "sku",
    "country",
    "order_type",
    "row_count",
    "order_count",
    "requested_quantity",
    "cancelled_quantity",
    "net_requested_quantity",
    "disposed_quantity",
    "shipped_quantity",
    "in_process_quantity",
    "removal_fee",
]

REQUIRED_COLUMNS = {
    "account_name",
    "country_code",
    "msku",
    "order_type",
    "order_id",
    "requested_quantity",
    "cancelled_quantity",
    "disposed_quantity",
    "shipped_quantity",
    "in_process_quantity",
    "removal_fee",
    "update_month",
}


def get_db_conn():
    cfg = settings.db_config
    return pymysql.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        charset=cfg.get("charset", "utf8mb4"),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def validate_table_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{TABLE}`")
        columns = {row["Field"] for row in cur.fetchall()}

    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        raise RuntimeError(
            "正式表尚未完成国家字段迁移，缺少字段："
            + ", ".join(missing)
            + "。请先运行 import_jq_removal_raw_jsonl.py。"
        )


def fetch_summary(conn, account_name: str, month: str) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
        msku AS sku,
        COALESCE(NULLIF(country_code, ''), 'UNKNOWN') AS country,
        COALESCE(NULLIF(order_type, ''), 'UNKNOWN') AS order_type,
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(COALESCE(requested_quantity, 0)) AS requested_quantity,
        SUM(COALESCE(cancelled_quantity, 0)) AS cancelled_quantity,
        SUM(COALESCE(requested_quantity, 0) - COALESCE(cancelled_quantity, 0))
            AS net_requested_quantity,
        SUM(COALESCE(disposed_quantity, 0)) AS disposed_quantity,
        SUM(COALESCE(shipped_quantity, 0)) AS shipped_quantity,
        SUM(COALESCE(in_process_quantity, 0)) AS in_process_quantity,
        SUM(COALESCE(removal_fee, 0)) AS removal_fee
    FROM `{TABLE}`
    WHERE account_name = %s
      AND update_month = %s
    GROUP BY
        msku,
        COALESCE(NULLIF(country_code, ''), 'UNKNOWN'),
        COALESCE(NULLIF(order_type, ''), 'UNKNOWN')
    ORDER BY
        country,
        sku,
        order_type
    """
    with conn.cursor() as cur:
        cur.execute(sql, (account_name, month))
        return list(cur.fetchall())


def fetch_validation(conn, account_name: str, month: str) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
        COALESCE(NULLIF(country_code, ''), 'UNKNOWN') AS country,
        COALESCE(NULLIF(order_type, ''), 'UNKNOWN') AS order_type,
        COALESCE(NULLIF(order_status, ''), 'UNKNOWN') AS order_status,
        COUNT(*) AS row_count,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT msku) AS sku_count,
        SUM(COALESCE(requested_quantity, 0)) AS requested_quantity,
        SUM(COALESCE(cancelled_quantity, 0)) AS cancelled_quantity,
        SUM(COALESCE(requested_quantity, 0) - COALESCE(cancelled_quantity, 0))
            AS net_requested_quantity,
        SUM(COALESCE(disposed_quantity, 0)) AS disposed_quantity,
        SUM(COALESCE(shipped_quantity, 0)) AS shipped_quantity,
        SUM(COALESCE(in_process_quantity, 0)) AS in_process_quantity,
        SUM(COALESCE(removal_fee, 0)) AS removal_fee
    FROM `{TABLE}`
    WHERE account_name = %s
      AND update_month = %s
    GROUP BY
        COALESCE(NULLIF(country_code, ''), 'UNKNOWN'),
        COALESCE(NULLIF(order_type, ''), 'UNKNOWN'),
        COALESCE(NULLIF(order_status, ''), 'UNKNOWN')
    ORDER BY country, order_type, order_status
    """
    with conn.cursor() as cur:
        cur.execute(sql, (account_name, month))
        return list(cur.fetchall())


def normalize_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return ""
    return value


def column_name(index: int) -> str:
    result = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        result = chr(65 + remainder) + result
    return result


def cell_ref(row: int, col: int) -> str:
    return f"{column_name(col)}{row}"


def xml_text(value: Any) -> str:
    return escape(str(value), {'"': "&quot;"})


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def cell_xml(row: int, col: int, value: Any, style_id: int = 0) -> str:
    ref = cell_ref(row, col)
    value = normalize_value(value)
    if value == "":
        return f'<c r="{ref}" s="{style_id}"/>'
    if is_number(value):
        return f'<c r="{ref}" s="{style_id}"><v>{value}</v></c>'
    return (
        f'<c r="{ref}" s="{style_id}" t="inlineStr">'
        f'<is><t xml:space="preserve">{xml_text(value)}</t></is></c>'
    )


def row_xml(row_num: int, values: Sequence[Any], styles: Sequence[int], height: float = 18) -> str:
    cells = "".join(
        cell_xml(row_num, col_num, value, styles[col_num - 1])
        for col_num, value in enumerate(values, 1)
    )
    return f'<row r="{row_num}" ht="{height}" customHeight="1">{cells}</row>'


def build_sheet_xml(
    title: str,
    note: str,
    headers: Sequence[str],
    data_rows: Sequence[Sequence[Any]],
    data_styles: Sequence[int],
    widths: Sequence[float],
    total_row: Sequence[Any] = (),
    total_styles: Sequence[int] = (),
) -> str:
    last_col = column_name(len(headers))
    data_start_row = 5
    data_end_row = data_start_row + len(data_rows) - 1
    total_row_num = data_end_row + 1 if total_row else data_end_row
    filter_end_row = max(data_start_row, data_end_row)

    rows = [
        row_xml(1, [title] + [""] * (len(headers) - 1), [1] + [0] * (len(headers) - 1), 28),
        row_xml(2, [note] + [""] * (len(headers) - 1), [2] + [0] * (len(headers) - 1), 32),
        row_xml(4, headers, [3] * len(headers), 26),
    ]

    for index, values in enumerate(data_rows, data_start_row):
        rows.append(row_xml(index, values, data_styles, 18))

    if total_row:
        rows.append(row_xml(total_row_num, total_row, total_styles, 20))

    cols = "".join(
        f'<col min="{idx}" max="{idx}" width="{width}" customWidth="1"/>'
        for idx, width in enumerate(widths, 1)
    )

    merge_refs = f"A1:{last_col}1 A2:{last_col}2".split()
    merges = "".join(f'<mergeCell ref="{ref}"/>' for ref in merge_refs)

    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetPr><outlinePr summaryBelow="1" summaryRight="1"/></sheetPr>
  <dimension ref="A1:{last_col}{max(4, total_row_num)}"/>
  <sheetViews>
    <sheetView workbookViewId="0">
      <pane ySplit="4" topLeftCell="A5" activePane="bottomLeft" state="frozen"/>
      <selection pane="bottomLeft" activeCell="A5" sqref="A5"/>
    </sheetView>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="18"/>
  <cols>{cols}</cols>
  <sheetData>{''.join(rows)}</sheetData>
  <autoFilter ref="A4:{last_col}{filter_end_row}"/>
  <mergeCells count="2">{merges}</mergeCells>
  <pageMargins left="0.3" right="0.3" top="0.5" bottom="0.5" header="0.2" footer="0.2"/>
  <pageSetup orientation="landscape" fitToWidth="1" fitToHeight="0"/>
</worksheet>'''


def styles_xml() -> str:
    return '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <numFmts count="1"><numFmt numFmtId="164" formatCode="0.0000"/></numFmts>
  <fonts count="5">
    <font><sz val="11"/><name val="Calibri"/><family val="2"/></font>
    <font><b/><color rgb="FFFFFFFF"/><sz val="16"/><name val="Calibri"/></font>
    <font><b/><color rgb="FF7F6000"/><sz val="11"/><name val="Calibri"/></font>
    <font><b/><color rgb="FFFFFFFF"/><sz val="11"/><name val="Calibri"/></font>
    <font><b/><color rgb="FF1F1F1F"/><sz val="11"/><name val="Calibri"/></font>
  </fonts>
  <fills count="6">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF1F4E78"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFF2CC"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FF5B9BD5"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD9EAF7"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="2">
    <border><left/><right/><top/><bottom/><diagonal/></border>
    <border>
      <left style="thin"><color rgb="FFD9D9D9"/></left>
      <right style="thin"><color rgb="FFD9D9D9"/></right>
      <top style="thin"><color rgb="FFD9D9D9"/></top>
      <bottom style="thin"><color rgb="FFD9D9D9"/></bottom>
      <diagonal/>
    </border>
  </borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="10">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center"/></xf>
    <xf numFmtId="0" fontId="2" fillId="3" borderId="0" xfId="0" applyAlignment="1"><alignment vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="3" fillId="4" borderId="1" xfId="0" applyAlignment="1"><alignment horizontal="center" vertical="center" wrapText="1"/></xf>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center"/></xf>
    <xf numFmtId="1" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
    <xf numFmtId="164" fontId="0" fillId="0" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
    <xf numFmtId="0" fontId="4" fillId="5" borderId="1" xfId="0" applyAlignment="1"><alignment vertical="center"/></xf>
    <xf numFmtId="1" fontId="4" fillId="5" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
    <xf numFmtId="164" fontId="4" fillId="5" borderId="1" xfId="0" applyNumberFormat="1" applyAlignment="1"><alignment horizontal="right" vertical="center"/></xf>
  </cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>'''


def workbook_xml(sheet_names: Sequence[str]) -> str:
    sheets = "".join(
        f'<sheet name="{xml_text(name)}" sheetId="{idx}" r:id="rId{idx}"/>'
        for idx, name in enumerate(sheet_names, 1)
    )
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <bookViews><workbookView xWindow="0" yWindow="0" windowWidth="24000" windowHeight="12000"/></bookViews>
  <sheets>{sheets}</sheets>
  <calcPr calcId="191029" fullCalcOnLoad="1"/>
</workbook>'''


def workbook_rels_xml(sheet_count: int) -> str:
    rels = []
    for idx in range(1, sheet_count + 1):
        rels.append(
            f'<Relationship Id="rId{idx}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{idx}.xml"/>'
        )
    rels.append(
        f'<Relationship Id="rId{sheet_count + 1}" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
    )
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  {''.join(rels)}
</Relationships>'''


def content_types_xml(sheet_count: int) -> str:
    sheet_overrides = "".join(
        f'<Override PartName="/xl/worksheets/sheet{idx}.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        for idx in range(1, sheet_count + 1)
    )
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  {sheet_overrides}
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>'''


def root_rels_xml() -> str:
    return '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>'''


def core_xml() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:title>JQ 移除订单 SKU 国家类型汇总</dc:title>
  <dc:creator>amazon-fee-pipeline</dc:creator>
  <cp:lastModifiedBy>amazon-fee-pipeline</cp:lastModifiedBy>
  <dcterms:created xsi:type="dcterms:W3CDTF">{now}</dcterms:created>
  <dcterms:modified xsi:type="dcterms:W3CDTF">{now}</dcterms:modified>
</cp:coreProperties>'''


def app_xml(sheet_names: Sequence[str]) -> str:
    titles = "".join(f'<vt:lpstr>{xml_text(name)}</vt:lpstr>' for name in sheet_names)
    return f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>amazon-fee-pipeline</Application>
  <DocSecurity>0</DocSecurity>
  <ScaleCrop>false</ScaleCrop>
  <HeadingPairs>
    <vt:vector size="2" baseType="variant">
      <vt:variant><vt:lpstr>Worksheets</vt:lpstr></vt:variant>
      <vt:variant><vt:i4>{len(sheet_names)}</vt:i4></vt:variant>
    </vt:vector>
  </HeadingPairs>
  <TitlesOfParts><vt:vector size="{len(sheet_names)}" baseType="lpstr">{titles}</vt:vector></TitlesOfParts>
  <Company>尚亿数据</Company>
  <AppVersion>1.0</AppVersion>
</Properties>'''


def write_xlsx(path: str, sheets: Sequence[Tuple[str, str]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    sheet_names = [name for name, _ in sheets]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types_xml(len(sheets)))
        archive.writestr("_rels/.rels", root_rels_xml())
        archive.writestr("docProps/core.xml", core_xml())
        archive.writestr("docProps/app.xml", app_xml(sheet_names))
        archive.writestr("xl/workbook.xml", workbook_xml(sheet_names))
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml(len(sheets)))
        archive.writestr("xl/styles.xml", styles_xml())
        for idx, (_, xml) in enumerate(sheets, 1):
            archive.writestr(f"xl/worksheets/sheet{idx}.xml", xml)


def sum_field(rows: Iterable[Dict[str, Any]], key: str) -> Any:
    total = Decimal("0") if key == "removal_fee" else 0
    for row in rows:
        total += row.get(key) or 0
    return total


def build_workbook(summary: List[Dict[str, Any]], validation: List[Dict[str, Any]], account_name: str, month: str, output: str) -> None:
    summary_rows = [[normalize_value(row.get(key)) for key in SUMMARY_KEYS] for row in summary]
    summary_total = [
        "合计",
        "",
        "",
        sum_field(summary, "row_count"),
        "",
        sum_field(summary, "requested_quantity"),
        sum_field(summary, "cancelled_quantity"),
        sum_field(summary, "net_requested_quantity"),
        sum_field(summary, "disposed_quantity"),
        sum_field(summary, "shipped_quantity"),
        sum_field(summary, "in_process_quantity"),
        sum_field(summary, "removal_fee"),
    ]

    summary_sheet = build_sheet_xml(
        title=f"{account_name} {month} 移除订单：SKU × 国家 × 移除类型汇总",
        note="国家为空的记录显示为 UNKNOWN；移除费用按原币统计，US 通常为 USD、CA 通常为 CAD。",
        headers=SUMMARY_HEADERS,
        data_rows=summary_rows,
        data_styles=[4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 6],
        widths=[34, 10, 16, 12, 12, 14, 14, 14, 14, 14, 14, 14],
        total_row=summary_total,
        total_styles=[7, 7, 7, 8, 7, 8, 8, 8, 8, 8, 8, 9],
    )

    validation_headers = [
        "国家",
        "移除类型",
        "订单状态",
        "明细行数",
        "订单数",
        "SKU数",
        "申请数量",
        "取消数量",
        "净申请数量",
        "已销毁数量",
        "已发货数量",
        "处理中数量",
        "移除费用",
    ]
    validation_keys = [
        "country",
        "order_type",
        "order_status",
        "row_count",
        "order_count",
        "sku_count",
        "requested_quantity",
        "cancelled_quantity",
        "net_requested_quantity",
        "disposed_quantity",
        "shipped_quantity",
        "in_process_quantity",
        "removal_fee",
    ]
    validation_rows = [[normalize_value(row.get(key)) for key in validation_keys] for row in validation]
    validation_total = [
        "合计",
        "",
        "",
        sum_field(validation, "row_count"),
        "",
        "",
        sum_field(validation, "requested_quantity"),
        sum_field(validation, "cancelled_quantity"),
        sum_field(validation, "net_requested_quantity"),
        sum_field(validation, "disposed_quantity"),
        sum_field(validation, "shipped_quantity"),
        sum_field(validation, "in_process_quantity"),
        sum_field(validation, "removal_fee"),
    ]
    validation_sheet = build_sheet_xml(
        title=f"{account_name} {month} 汇总校验",
        note="用于核对国家、移除类型和订单状态分布；总行数应与正式表该月份记录数一致。",
        headers=validation_headers,
        data_rows=validation_rows,
        data_styles=[4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 6],
        widths=[10, 16, 16, 12, 12, 12, 14, 14, 14, 14, 14, 14, 14],
        total_row=validation_total,
        total_styles=[7, 7, 7, 8, 7, 7, 8, 8, 8, 8, 8, 8, 9],
    )

    write_xlsx(output, [("SKU国家类型汇总", summary_sheet), ("整体校验", validation_sheet)])


def main() -> None:
    parser = argparse.ArgumentParser(description="导出 JQ 移除订单 SKU×国家×移除类型 Excel 汇总")
    parser.add_argument("--month", default="2026-05", help="月份 YYYY-MM，默认 2026-05")
    parser.add_argument("--account-name", default=DEFAULT_ACCOUNT_NAME, help="共享账号名称，默认 JQ-NA")
    parser.add_argument("--output", default=None, help="输出 xlsx 路径")
    args = parser.parse_args()

    if len(args.month) != 7 or args.month[4] != "-":
        raise ValueError("--month 必须为 YYYY-MM，例如 2026-05")

    output = args.output or os.path.join(
        "reports",
        "removal_orders",
        f"{args.account_name}_{args.month}_SKU_国家_移除类型汇总.xlsx",
    )

    conn = get_db_conn()
    try:
        validate_table_schema(conn)
        summary = fetch_summary(conn, args.account_name, args.month)
        validation = fetch_validation(conn, args.account_name, args.month)
    finally:
        conn.close()

    if not summary:
        raise RuntimeError(
            f"未查询到数据：account_name={args.account_name}, update_month={args.month}。"
            "请确认 5 月原始 JSONL 已完成迁移入库。"
        )

    build_workbook(summary, validation, args.account_name, args.month, output)

    print(f"导出完成: {output}")
    print(f"SKU×国家×类型汇总行数: {len(summary)}")
    print(f"校验汇总行数: {len(validation)}")
    print(f"净申请数量合计: {sum_field(summary, 'net_requested_quantity')}")
    print(f"移除费用合计: {sum_field(summary, 'removal_fee')}")


if __name__ == "__main__":
    main()
