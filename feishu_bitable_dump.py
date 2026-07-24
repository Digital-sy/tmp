#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书多维表格附件批量导出工具
============================

绕过前端「附件超过最大限制 (1G)」的打包下载限制，走开放平台 API 逐个拉取。
支持断点续传（SQLite 记账）、并发下载、限流退避、可选直传阿里云 OSS。

用法
----
    export FEISHU_APP_ID=cli_xxx
    export FEISHU_APP_SECRET=xxx
    export FEISHU_APP_TOKEN=XT6pbXxxxxxxxxxx      # 多维表格 app_token
    export FEISHU_TABLE_ID=tblxxxxxxxx            # 数据表 table_id

    python3 feishu_bitable_dump.py scan           # ① 扫描记录，建立任务清单
    python3 feishu_bitable_dump.py download       # ② 下载（可反复执行，自动续传）
    python3 feishu_bitable_dump.py stats          # 查看进度
    python3 feishu_bitable_dump.py retry          # 把 failed 重置为 pending
    python3 feishu_bitable_dump.py manifest       # 导出 record_id -> 文件/OSS 映射 CSV

依赖
----
    pip install httpx
    pip install oss2        # 仅在需要直传 OSS 时

作者备注：所有状态存在 ./dump.db，删掉即可重头再来。
"""

import argparse
import asyncio
import csv
import json
import os
import random
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import httpx
except ImportError:
    sys.exit("缺少依赖，请执行: pip install httpx")

FEISHU_BASE = "https://open.feishu.cn/open-apis"

# ---------------------------------------------------------------- 配置

class Cfg:
    APP_ID = os.getenv("FEISHU_APP_ID", "")
    APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
    APP_TOKEN = os.getenv("FEISHU_APP_TOKEN", "")
    TABLE_ID = os.getenv("FEISHU_TABLE_ID", "")

    DB_PATH = os.getenv("DUMP_DB", "./dump.db")
    OUT_DIR = Path(os.getenv("DUMP_OUT", "./images"))

    # 并发。飞书素材下载接口频控不高，8~10 比较稳；报 99991400 就调小
    CONCURRENCY = int(os.getenv("DUMP_CONCURRENCY", "8"))
    MAX_RETRY = int(os.getenv("DUMP_MAX_RETRY", "5"))
    TIMEOUT = float(os.getenv("DUMP_TIMEOUT", "60"))

    # 是否保留本地文件（直传 OSS 成功后可设为 0 省磁盘）
    KEEP_LOCAL = os.getenv("DUMP_KEEP_LOCAL", "1") == "1"

    # 附件字段白名单，逗号分隔；留空则自动识别所有附件字段
    FIELDS = [f for f in os.getenv("DUMP_FIELDS", "").split(",") if f.strip()]

    # 权限兜底：若下载报无权限，设为 1，会附带 bitablePerm extra 参数
    USE_EXTRA = os.getenv("DUMP_USE_EXTRA", "0") == "1"

    # --- OSS（可选）---
    OSS_ENABLED = os.getenv("OSS_ENABLED", "0") == "1"
    OSS_ENDPOINT = os.getenv("OSS_ENDPOINT", "")        # 如 oss-cn-shenzhen-internal.aliyuncs.com
    OSS_BUCKET = os.getenv("OSS_BUCKET", "")
    OSS_AK = os.getenv("OSS_ACCESS_KEY_ID", "")
    OSS_SK = os.getenv("OSS_ACCESS_KEY_SECRET", "")
    OSS_PREFIX = os.getenv("OSS_PREFIX", "bitable/")


def check_cfg(need_table: bool = True) -> None:
    missing = [k for k in ("APP_ID", "APP_SECRET", "APP_TOKEN") if not getattr(Cfg, k)]
    if need_table and not Cfg.TABLE_ID:
        missing.append("TABLE_ID")
    if missing:
        sys.exit("缺少环境变量: " + ", ".join("FEISHU_" + m for m in missing))


# ---------------------------------------------------------------- SQLite

DDL = """
CREATE TABLE IF NOT EXISTS files (
    file_token  TEXT PRIMARY KEY,
    record_id   TEXT,
    field_name  TEXT,
    seq         INTEGER,
    name        TEXT,
    size        INTEGER,
    mime        TEXT,
    url         TEXT,
    status      TEXT DEFAULT 'pending',   -- pending / done / failed
    attempts    INTEGER DEFAULT 0,
    local_path  TEXT,
    oss_key     TEXT,
    err         TEXT,
    updated_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_status ON files(status);
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
"""


def db_open() -> sqlite3.Connection:
    conn = sqlite3.connect(Cfg.DB_PATH, check_same_thread=False)
    conn.executescript(DDL)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.commit()
    return conn


# ---------------------------------------------------------------- 飞书客户端

class Feishu:
    def __init__(self) -> None:
        self._token = ""
        self._expire_at = 0.0
        self._lock = asyncio.Lock()
        limits = httpx.Limits(max_connections=Cfg.CONCURRENCY + 4,
                              max_keepalive_connections=Cfg.CONCURRENCY + 4)
        self.cli = httpx.AsyncClient(timeout=Cfg.TIMEOUT, limits=limits, follow_redirects=True)

    async def close(self) -> None:
        await self.cli.aclose()

    async def token(self) -> str:
        async with self._lock:
            if self._token and time.time() < self._expire_at - 300:
                return self._token
            r = await self.cli.post(
                f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal",
                json={"app_id": Cfg.APP_ID, "app_secret": Cfg.APP_SECRET},
            )
            d = r.json()
            if d.get("code") != 0:
                raise RuntimeError(f"获取 tenant_access_token 失败: {d}")
            self._token = d["tenant_access_token"]
            self._expire_at = time.time() + int(d.get("expire", 7200))
            return self._token

    async def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {await self.token()}"}

    async def app_revision(self) -> int:
        r = await self.cli.get(f"{FEISHU_BASE}/bitable/v1/apps/{Cfg.APP_TOKEN}",
                               headers=await self.headers())
        d = r.json()
        if d.get("code") != 0:
            raise RuntimeError(f"app_token 无效: {d}")
        app = d["data"]["app"]
        print(f"✅ 多维表格: {app.get('name')}  revision={app.get('revision')}")
        return int(app.get("revision", 0))

    async def list_tables(self) -> List[Dict[str, str]]:
        r = await self.cli.get(f"{FEISHU_BASE}/bitable/v1/apps/{Cfg.APP_TOKEN}/tables",
                               headers=await self.headers())
        d = r.json()
        if d.get("code") != 0:
            raise RuntimeError(f"列举数据表失败: {d}")
        return d["data"]["items"]

    async def iter_records(self):
        """分页遍历全部记录。page_size 上限 500。"""
        url = (f"{FEISHU_BASE}/bitable/v1/apps/{Cfg.APP_TOKEN}"
               f"/tables/{Cfg.TABLE_ID}/records/search")
        page_token: Optional[str] = None
        page = 0
        while True:
            params: Dict[str, Any] = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            body: Dict[str, Any] = {}
            if Cfg.FIELDS:
                body["field_names"] = Cfg.FIELDS

            for attempt in range(Cfg.MAX_RETRY):
                r = await self.cli.post(url, headers=await self.headers(),
                                        params=params, json=body)
                d = r.json()
                code = d.get("code")
                if code == 0:
                    break
                if code in (99991400, 1254290) or r.status_code == 429:
                    await asyncio.sleep(2 ** attempt + random.random())
                    continue
                raise RuntimeError(f"拉取记录失败: {d}")
            else:
                raise RuntimeError("拉取记录连续失败，已达最大重试次数")

            data = d["data"]
            page += 1
            items = data.get("items") or []
            print(f"  第 {page} 页: {len(items)} 条记录")
            for it in items:
                yield it
            if not data.get("has_more"):
                return
            page_token = data.get("page_token")
            await asyncio.sleep(0.2)   # 轻微节流，避免撞 bitable 频控


# ---------------------------------------------------------------- ① scan

def is_attachment_value(v: Any) -> bool:
    return (isinstance(v, list) and v
            and isinstance(v[0], dict) and "file_token" in v[0])


async def cmd_scan(_args) -> None:
    check_cfg()
    fs = Feishu()
    conn = db_open()
    try:
        rev = await fs.app_revision()
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('revision', ?)", (str(rev),))
        conn.commit()

        print(f"开始扫描 table={Cfg.TABLE_ID} ...")
        total_rec = 0
        total_file = 0
        buf: List[Tuple] = []
        now = int(time.time())

        async for rec in fs.iter_records():
            total_rec += 1
            rid = rec.get("record_id", "")
            for fname, val in (rec.get("fields") or {}).items():
                if Cfg.FIELDS and fname not in Cfg.FIELDS:
                    continue
                if not is_attachment_value(val):
                    continue
                for i, att in enumerate(val):
                    ft = att.get("file_token")
                    if not ft:
                        continue
                    total_file += 1
                    buf.append((ft, rid, fname, i,
                                att.get("name") or f"{ft}.bin",
                                int(att.get("size") or 0),
                                att.get("type") or "",
                                att.get("url") or "",
                                now))
            if len(buf) >= 1000:
                _flush(conn, buf)
                buf.clear()
        if buf:
            _flush(conn, buf)

        print(f"\n扫描完成：{total_rec} 条记录，{total_file} 个附件引用")
        _print_stats(conn)
    finally:
        conn.close()
        await fs.close()


def _flush(conn: sqlite3.Connection, rows: List[Tuple]) -> None:
    # 已 done 的不覆盖状态；新 token 插入为 pending
    conn.executemany(
        """INSERT INTO files
           (file_token, record_id, field_name, seq, name, size, mime, url, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?)
           ON CONFLICT(file_token) DO UPDATE SET
             record_id=excluded.record_id,
             field_name=excluded.field_name,
             seq=excluded.seq,
             name=excluded.name,
             size=excluded.size,
             mime=excluded.mime,
             url=excluded.url""",
        rows,
    )
    conn.commit()


# ---------------------------------------------------------------- ② download

def safe_name(name: str) -> str:
    keep = "".join(c if (c.isalnum() or c in "._-") else "_" for c in name)
    return keep[-80:] or "file"


def target_path(file_token: str, name: str) -> Path:
    # 两级分片，避免单目录几万文件
    shard = Path(file_token[:2]) / file_token[2:4]
    return Cfg.OUT_DIR / shard / f"{file_token}_{safe_name(name)}"


class OSSUploader:
    def __init__(self) -> None:
        import oss2  # 延迟导入
        auth = oss2.Auth(Cfg.OSS_AK, Cfg.OSS_SK)
        self.bucket = oss2.Bucket(auth, Cfg.OSS_ENDPOINT, Cfg.OSS_BUCKET)

    def put(self, key: str, data: bytes, mime: str) -> None:
        headers = {"Content-Type": mime} if mime else None
        self.bucket.put_object(key, data, headers=headers)


async def download_one(fs: Feishu, sem: asyncio.Semaphore, conn: sqlite3.Connection,
                       oss: Optional[OSSUploader], extra: Optional[str],
                       row: sqlite3.Row, counter: Dict[str, int]) -> None:
    ft = row["file_token"]
    url = row["url"] or f"{FEISHU_BASE}/drive/v1/medias/{ft}/download"
    params = {"extra": extra} if extra else None

    async with sem:
        err = ""
        for attempt in range(Cfg.MAX_RETRY):
            try:
                r = await fs.cli.get(url, headers=await fs.headers(), params=params)

                # 出错时飞书返回 JSON，成功返回二进制
                ctype = r.headers.get("content-type", "")
                if r.status_code == 429 or "json" in ctype:
                    body = {}
                    try:
                        body = r.json()
                    except Exception:
                        pass
                    code = body.get("code")
                    if r.status_code == 429 or code in (99991400, 1254290):
                        await asyncio.sleep(2 ** attempt + random.random())
                        continue
                    if code in (99991663, 99991661):      # token 过期
                        fs._expire_at = 0
                        continue
                    err = f"code={code} msg={body.get('msg')}"
                    break

                r.raise_for_status()
                data = r.content
                expect = int(row["size"] or 0)
                if expect and abs(len(data) - expect) > 0:
                    err = f"大小不符 got={len(data)} expect={expect}"
                    await asyncio.sleep(1 + attempt)
                    continue

                local_path = ""
                oss_key = ""
                if oss:
                    oss_key = f"{Cfg.OSS_PREFIX}{ft}_{safe_name(row['name'])}"
                    await asyncio.to_thread(oss.put, oss_key, data, row["mime"] or "")
                if Cfg.KEEP_LOCAL or not oss:
                    p = target_path(ft, row["name"])
                    p.parent.mkdir(parents=True, exist_ok=True)
                    tmp = p.with_suffix(p.suffix + ".part")
                    tmp.write_bytes(data)
                    tmp.replace(p)
                    local_path = str(p)

                conn.execute(
                    "UPDATE files SET status='done', local_path=?, oss_key=?, err='',"
                    " attempts=attempts+1, updated_at=? WHERE file_token=?",
                    (local_path, oss_key, int(time.time()), ft))
                counter["done"] += 1
                _tick(counter)
                return

            except Exception as e:  # noqa: BLE001
                err = f"{type(e).__name__}: {e}"
                await asyncio.sleep(2 ** attempt + random.random())

        conn.execute(
            "UPDATE files SET status='failed', err=?, attempts=attempts+1, updated_at=?"
            " WHERE file_token=?", (err[:500], int(time.time()), ft))
        counter["failed"] += 1
        _tick(counter)


def _tick(counter: Dict[str, int]) -> None:
    n = counter["done"] + counter["failed"]
    if n % 50 == 0 or n == counter["total"]:
        pct = n * 100 / max(counter["total"], 1)
        el = time.time() - counter["t0"]
        rate = n / el if el > 0 else 0
        eta = (counter["total"] - n) / rate if rate > 0 else 0
        print(f"  [{n}/{counter['total']}] {pct:5.1f}%  "
              f"成功 {counter['done']} 失败 {counter['failed']}  "
              f"{rate:.1f}/s  ETA {eta/60:.1f}min", flush=True)


async def cmd_download(args) -> None:
    check_cfg()
    conn = db_open()
    conn.row_factory = sqlite3.Row
    fs = Feishu()
    oss = None
    try:
        if Cfg.OSS_ENABLED:
            oss = OSSUploader()
            print(f"✅ OSS 直传已开启: {Cfg.OSS_BUCKET} prefix={Cfg.OSS_PREFIX}")

        extra = None
        if Cfg.USE_EXTRA:
            rev = conn.execute("SELECT v FROM meta WHERE k='revision'").fetchone()
            extra = json.dumps({"bitablePerm": {
                "tableId": Cfg.TABLE_ID,
                "rev": int(rev["v"]) if rev else 0}}, separators=(",", ":"))
            print(f"  使用 extra 权限参数: {extra}")

        rows = conn.execute(
            "SELECT * FROM files WHERE status='pending'"
            + (f" LIMIT {int(args.limit)}" if args.limit else "")).fetchall()
        if not rows:
            print("没有待下载任务。先跑 scan，或用 retry 重置失败项。")
            _print_stats(conn)
            return

        Cfg.OUT_DIR.mkdir(parents=True, exist_ok=True)
        counter = {"total": len(rows), "done": 0, "failed": 0, "t0": time.time()}
        print(f"开始下载 {len(rows)} 个文件，并发 {Cfg.CONCURRENCY} ...")

        sem = asyncio.Semaphore(Cfg.CONCURRENCY)
        tasks = [download_one(fs, sem, conn, oss, extra, r, counter) for r in rows]
        for i in range(0, len(tasks), 500):
            await asyncio.gather(*tasks[i:i + 500])
            conn.commit()
        conn.commit()

        print("\n下载结束。")
        _print_stats(conn)
    finally:
        conn.close()
        await fs.close()


# ---------------------------------------------------------------- 辅助命令

def _print_stats(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT status, COUNT(*), COALESCE(SUM(size),0) FROM files GROUP BY status"
    ).fetchall()
    print("\n--- 进度 ---")
    for st, cnt, sz in rows:
        print(f"  {st:<8} {cnt:>7} 个   {sz/1024/1024/1024:.2f} GB")
    bad = conn.execute(
        "SELECT file_token, name, err FROM files WHERE status='failed' LIMIT 5").fetchall()
    if bad:
        print("  失败样例:")
        for b in bad:
            print(f"    {b[0]} {b[1]} -> {b[2]}")


def cmd_stats(_args) -> None:
    conn = db_open()
    _print_stats(conn)
    conn.close()


def cmd_retry(_args) -> None:
    conn = db_open()
    n = conn.execute("UPDATE files SET status='pending' WHERE status='failed'").rowcount
    conn.commit()
    print(f"已将 {n} 个失败任务重置为 pending，重新执行 download 即可。")
    conn.close()


def cmd_manifest(args) -> None:
    conn = db_open()
    conn.row_factory = sqlite3.Row
    out = args.out or "./manifest.csv"
    rows = conn.execute(
        "SELECT record_id, field_name, seq, file_token, name, size, mime,"
        " status, local_path, oss_key FROM files ORDER BY record_id, field_name, seq"
    ).fetchall()
    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["record_id", "field_name", "seq", "file_token", "name",
                    "size", "mime", "status", "local_path", "oss_key", "oss_url"])
        base = (f"https://{Cfg.OSS_BUCKET}.{Cfg.OSS_ENDPOINT}/"
                if Cfg.OSS_BUCKET and Cfg.OSS_ENDPOINT else "")
        for r in rows:
            w.writerow(list(r) + [base + r["oss_key"] if (base and r["oss_key"]) else ""])
    print(f"已导出 {len(rows)} 行 -> {out}")
    conn.close()


async def cmd_tables(_args) -> None:
    check_cfg(need_table=False)
    fs = Feishu()
    try:
        await fs.app_revision()
        for t in await fs.list_tables():
            print(f"  {t['name']:<30} {t['table_id']}")
    finally:
        await fs.close()


# ---------------------------------------------------------------- main

def main() -> None:
    p = argparse.ArgumentParser(description="飞书多维表格附件批量导出")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("tables", help="列出所有数据表及 table_id")
    sub.add_parser("scan", help="扫描记录，建立下载清单")

    d = sub.add_parser("download", help="执行下载（可反复运行，自动续传）")
    d.add_argument("--limit", type=int, default=0, help="本次最多处理多少个（0=全部）")

    sub.add_parser("stats", help="查看进度")
    sub.add_parser("retry", help="把 failed 重置为 pending")

    m = sub.add_parser("manifest", help="导出映射 CSV")
    m.add_argument("--out", default="./manifest.csv")

    args = p.parse_args()
    handlers = {
        "tables": cmd_tables, "scan": cmd_scan, "download": cmd_download,
        "stats": cmd_stats, "retry": cmd_retry, "manifest": cmd_manifest,
    }
    fn = handlers[args.cmd]
    if asyncio.iscoroutinefunction(fn):
        asyncio.run(fn(args))
    else:
        fn(args)


if __name__ == "__main__":
    main()
