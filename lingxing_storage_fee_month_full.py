#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""领星FBA月仓储费：查字段、查更新、接口对比、完整落库。"""
import argparse, asyncio, json, logging, re, sys
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import pymysql, pymysql.cursors
from common import settings
from lingxing import OpenApiBase, LingxingTokenProvider

ROUTE='/erp/sc/data/fba_report/storageFeeMonth'; PAGE=1000
RATE_LIMIT=3001008; TOKEN_CODES={401,403,2001003,2001005,3001001,3001002}
TARGET='ods_lx_fba_storage_fee_month'; OLD='lingxing.lingxing_storage_monthly'
STORES={'CY-US':11544,'MT-US':11545,'MT-CA':11546,'SY-US':11547,'JQ-US':11548,
'JQ-CA':11549,'RKZ-US':11550,'RR-UK':11551,'RR-IT':11552,'RR-DE':11553,
'RR-FR':11554,'RR-ES':11555,'RR-NL':13247,'JQ-AU':13639,'JQ-MX':15353}
logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(message)s'); log=logging.getLogger(__name__)

def gv(o,k,d=None): return o.get(k,d) if isinstance(o,dict) else getattr(o,k,d)
def txt(v): return '' if v is None else str(v).strip()
def de(v):
    if v in (None,'','None','null'): return None
    try:return Decimal(str(v).strip())
    except (InvalidOperation,ValueError,TypeError):return None
def d0(v): return de(v) or Decimal('0')
def num(v):
    try:return int(v)
    except:return 0
def unit(v): return re.sub(r'\s+',' ',txt(v).lower()).replace('³','3')
def cv(v,u,m):
    x=de(v); f=m.get(unit(u)); return (x*f).quantize(Decimal('0.00000001')) if x is not None and f else None
CM={'in':Decimal('2.54'),'inch':Decimal('2.54'),'inches':Decimal('2.54'),'cm':Decimal(1),'mm':Decimal('.1')}
LB={'lb':Decimal(1),'lbs':Decimal(1),'pound':Decimal(1),'pounds':Decimal(1),'kg':Decimal('2.2046226218'),'g':Decimal('.0022046226'),'oz':Decimal('.0625')}
KG={'kg':Decimal(1),'g':Decimal('.001'),'lb':Decimal('.45359237'),'lbs':Decimal('.45359237'),'pounds':Decimal('.45359237'),'oz':Decimal('.0283495231')}
CUFT={'cubic foot':Decimal(1),'cubic feet':Decimal(1),'ft3':Decimal(1),'cu ft':Decimal(1),'cubic inches':Decimal('.0005787037'),'in3':Decimal('.0005787037'),'cm3':Decimal('.0000353147')}

def db(auto=False):
    c=settings.db_config
    return pymysql.connect(host=c['host'],port=int(c.get('port',3306)),user=c['user'],password=c['password'],database=c['database'],charset=c.get('charset','utf8mb4'),cursorclass=pymysql.cursors.DictCursor,autocommit=auto,connect_timeout=30,read_timeout=900,write_timeout=900)
def split(t):
    p=t.split('.'); s=(settings.db_config['database'],p[0]) if len(p)==1 else tuple(p)
    if len(s)!=2 or any(not re.fullmatch(r'[A-Za-z0-9_\u4e00-\u9fff]+',x) for x in s): raise ValueError('非法表名:'+t)
    return s
def qt(t): s,n=split(t); return f'`{s}`.`{n}`'
def exists(c,t):
    s,n=split(t)
    with c.cursor() as x:x.execute('SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema=%s AND table_name=%s',(s,n));return bool(x.fetchone()['n'])
def cols(c,t):
    s,n=split(t)
    with c.cursor() as x:x.execute('SELECT ordinal_position,column_name,column_type,is_nullable,column_key,column_comment FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position',(s,n));return list(x.fetchall())
def show(rows,fields,title):
    print('\n=====',title,'=====')
    if not rows: print('（无结果）'); return
    print('\t'.join(fields))
    for r in rows: print('\t'.join(txt(r.get(f)) for f in fields))

async def req(api,tp,body):
    last=None
    for i in range(5):
        try:r=await api.request(await tp.get_token(),ROUTE,'POST',req_body=body)
        except Exception as e:last=e; await asyncio.sleep(min(10*2**i,120)); continue
        code=num(gv(r,'code',-1))
        if code==0:return r
        if code==RATE_LIMIT:await asyncio.sleep(min(10*2**i,120));continue
        if code in TOKEN_CODES:
            try:await tp.refresh()
            except:pass
            continue
        raise RuntimeError(f"接口失败 code={code}, message={gv(r,'message','')}")
    raise RuntimeError('连续请求失败:'+str(last))
async def fetch(api,tp,sid,month):
    out=[];off=0;total=0
    while True:
        r=await req(api,tp,{'sid':sid,'month':month,'offset':off,'length':PAGE}); page=gv(r,'data',[]) or []; total=max(total,num(gv(r,'total',0)))
        if not page:break
        out+=page;log.info('sid=%s month=%s %s/%s',sid,month,len(out),total)
        if (total and len(out)>=total) or len(page)<PAGE:break
        off+=PAGE;await asyncio.sleep(2)
    if total and len(out)!=total:raise RuntimeError(f'分页不完整 total={total}, actual={len(out)}')
    return out

def row(store,sid,month,r):
    du,wu,vu=txt(r.get('measurement_units')),txt(r.get('weight_units')),txt(r.get('volume_units')); wl=cv(r.get('weight'),wu,LB)
    return {'store_name':store,'sid':sid,'asin':txt(r.get('asin')),'fnsku':txt(r.get('fnsku')),'product_name':txt(r.get('product_name')),
    'fulfillment_center':txt(r.get('fulfillment_center')),'country_code':txt(r.get('country_code')),'longest_side':de(r.get('longest_side')),
    'median_side':de(r.get('median_side')),'shortest_side':de(r.get('shortest_side')),'measurement_units':du,'longest_side_cm':cv(r.get('longest_side'),du,CM),
    'median_side_cm':cv(r.get('median_side'),du,CM),'shortest_side_cm':cv(r.get('shortest_side'),du,CM),'weight':de(r.get('weight')),'weight_units':wu,
    'weight_kg':cv(r.get('weight'),wu,KG),'weight_lb':wl,'weight_lb_ceiling_0_1':None if wl is None else (wl*10).to_integral_value(rounding=ROUND_CEILING)/10,
    'item_volume':de(r.get('item_volume')),'volume_units':vu,'item_volume_cuft':cv(r.get('item_volume'),vu,CUFT),'product_size_tier':txt(r.get('product_size_tier')),
    'average_quantity_on_hand':de(r.get('average_quantity_on_hand')),'average_quantity_pending_removal':de(r.get('average_quantity_pending_removal')),
    'estimated_total_item_volume':de(r.get('estimated_total_item_volume')),'estimated_total_item_volume_cuft':cv(r.get('estimated_total_item_volume'),vu,CUFT),
    'month_of_charge':txt(r.get('month_of_charge')) or month,'storage_rate':de(r.get('storage_rate')),'currency':txt(r.get('currency')),
    'estimated_monthly_storage_fee':de(r.get('estimated_monthly_storage_fee')),'v_uuid':txt(r.get('v_uuid')),'company_id':num(r.get('company_id')) or None,
    'raw_json':json.dumps(r,ensure_ascii=False,default=str,separators=(',',':'))}
def mapped(store,sid,month,raw):
    rs=[row(store,sid,month,r) for r in raw]; keys=[(x['sid'],x['fnsku'],x['month_of_charge'],x['fulfillment_center']) for x in rs]
    if any(not x['fnsku'] for x in rs) or len(keys)!=len(set(keys)):raise RuntimeError('空FNSKU或重复唯一键，停止写库')
    return rs

def create(c,t):
    with c.cursor() as x:x.execute(f'''CREATE TABLE IF NOT EXISTS {qt(t)}(
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,store_name VARCHAR(50) NOT NULL,sid INT NOT NULL,asin VARCHAR(50),fnsku VARCHAR(50) NOT NULL,product_name TEXT,
    fulfillment_center VARCHAR(30) NOT NULL DEFAULT '',country_code VARCHAR(10),longest_side DECIMAL(18,8),median_side DECIMAL(18,8),shortest_side DECIMAL(18,8),measurement_units VARCHAR(30),
    longest_side_cm DECIMAL(18,8),median_side_cm DECIMAL(18,8),shortest_side_cm DECIMAL(18,8),weight DECIMAL(18,8),weight_units VARCHAR(30),weight_kg DECIMAL(18,8),weight_lb DECIMAL(18,8),
    weight_lb_ceiling_0_1 DECIMAL(18,8),item_volume DECIMAL(18,8),volume_units VARCHAR(30),item_volume_cuft DECIMAL(18,8),product_size_tier VARCHAR(100),average_quantity_on_hand DECIMAL(18,6),
    average_quantity_pending_removal DECIMAL(18,6),estimated_total_item_volume DECIMAL(18,8),estimated_total_item_volume_cuft DECIMAL(18,8),month_of_charge CHAR(7) NOT NULL,storage_rate DECIMAL(18,8),currency VARCHAR(10),
    estimated_monthly_storage_fee DECIMAL(18,8),v_uuid VARCHAR(64),company_id BIGINT,raw_json JSON,fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_sid_fnsku_month_fc(sid,fnsku,month_of_charge,fulfillment_center),KEY idx_store_month(store_name,month_of_charge),KEY idx_asin(asin),KEY idx_fnsku(fnsku))ENGINE=InnoDB DEFAULT CHARSET=utf8mb4''')
def write(c,t,sid,month,rs):
    create(c,t); fs=list(rs[0]) if rs else []
    try:
        with c.cursor() as x:
            x.execute(f'DELETE FROM {qt(t)} WHERE sid=%s AND month_of_charge=%s',(sid,month))
            if rs:x.executemany(f"INSERT INTO {qt(t)} ({','.join('`'+f+'`' for f in fs)}) VALUES ({','.join('%('+f+')s' for f in fs)})",rs)
        c.commit()
    except:c.rollback();raise

def inspect(c,schemas):
    keys=['fnsku','asin','sku','msku','包装','规格','重量','毛重','体积','package','length','width','height','weight','volume']
    with c.cursor() as x:x.execute('SELECT schema_name FROM information_schema.schemata'); vis={r['schema_name'] for r in x.fetchall()}
    schemas=[s for s in schemas if s in vis]; sp=','.join(['%s']*len(schemas)); parts=[];params=list(schemas)
    for k in keys:parts+=['LOWER(column_name) LIKE LOWER(%s)','LOWER(column_comment) LIKE LOWER(%s)','LOWER(table_name) LIKE LOWER(%s)'];params += [f'%{k}%']*3
    with c.cursor() as x:x.execute(f"SELECT table_schema,table_name,ordinal_position,column_name,column_type,column_comment FROM information_schema.columns WHERE table_schema IN ({sp}) AND ({' OR '.join(parts)}) ORDER BY table_schema,table_name,ordinal_position LIMIT 1500",params);r=list(x.fetchall())
    show(r,['table_schema','table_name','ordinal_position','column_name','column_type','column_comment'],'候选字段')
    for t in ['lingxing.listing','lingxing.产品管理']:
        if exists(c,t):show(cols(c,t),['ordinal_position','column_name','column_type','is_nullable','column_key','column_comment'],t+'完整字段')
def audit(c,t):
    if not exists(c,t):print('表不存在：'+t);return
    cs=cols(c,t);show(cs,['ordinal_position','column_name','column_type','is_nullable','column_key','column_comment'],t+'字段'); names={r['column_name'] for r in cs}
    fc="COALESCE(fulfillment_center,'')" if 'fulfillment_center' in names else "''"; fee=next((f for f in ['estimated_monthly_storage_fee','monthly_storage_fee','fba_storage_fee'] if f in names),None)
    times=[f for f in ['updated_at','fetched_at','created_at'] if f in names]; ts=''.join(f',MIN(`{f}`) min_{f},MAX(`{f}`) max_{f}' for f in times)
    with c.cursor() as x:x.execute(f"SELECT month_of_charge,sid,COUNT(*) row_cnt,COUNT(DISTINCT fnsku) fnsku_cnt,COUNT(DISTINCT CONCAT_WS('|',sid,fnsku,month_of_charge,{fc})) unique_key_cnt,ROUND(SUM(COALESCE(`{fee}`,0)),6) fee_sum {ts} FROM {qt(t)} GROUP BY month_of_charge,sid ORDER BY month_of_charge DESC,sid LIMIT 200");r=list(x.fetchall())
    show(r,['month_of_charge','sid','row_cnt','fnsku_cnt','unique_key_cnt','fee_sum']+sum(([f'min_{z}',f'max_{z}'] for z in times),[]),'月份覆盖与最近刷新')
def compare(c,t,sid,month,rs):
    if not exists(c,t):print('表不存在：'+t);return
    names={r['column_name'] for r in cols(c,t)};fc="COALESCE(fulfillment_center,'')" if 'fulfillment_center' in names else "''";fee=next((f for f in ['estimated_monthly_storage_fee','monthly_storage_fee','fba_storage_fee'] if f in names),None)
    with c.cursor() as x:x.execute(f"SELECT sid,fnsku,month_of_charge,{fc} fulfillment_center,COALESCE(`{fee}`,0) fee FROM {qt(t)} WHERE sid=%s AND month_of_charge=%s",(sid,month));old=list(x.fetchall())
    ak={(r['sid'],r['fnsku'],r['month_of_charge'],r['fulfillment_center']) for r in rs};dk={(num(r['sid']),txt(r['fnsku']),txt(r['month_of_charge']),txt(r['fulfillment_center'])) for r in old}
    show([{'sid':sid,'month':month,'api_rows':len(rs),'db_rows':len(old),'api_fee':sum((d0(r['estimated_monthly_storage_fee']) for r in rs),Decimal(0)),'db_fee':sum((d0(r['fee']) for r in old),Decimal(0)),'only_api':len(ak-dk),'only_db':len(dk-ak)}],['sid','month','api_rows','db_rows','api_fee','db_fee','only_api','only_db'],'API与数据库对比')

def store_list(v):
    if not v:return list(STORES.items())
    ns=[x.strip().upper() for x in v.split(',') if x.strip()]; bad=[x for x in ns if x not in STORES]
    if bad:raise ValueError('未知店铺:'+str(bad))
    return [(x,STORES[x]) for x in ns]
async def main(a):
    if a.action in ('inspect-schema','audit-db'):
        c=db(True)
        try:inspect(c,[x.strip() for x in a.schemas.split(',') if x.strip()] or [settings.db_config['database'],'lingxing','ods_db','dim_db','dwd_db']) if a.action=='inspect-schema' else audit(c,a.audit_table)
        finally:c.close()
        return
    if not a.month:raise ValueError('必须传 --month YYYY-MM')
    datetime.strptime(a.month,'%Y-%m');cfg=settings.lingxing_config;api=OpenApiBase(host=cfg['host'],app_id=cfg['app_id'],app_secret=cfg['app_secret'],proxy_url=cfg.get('proxy_url'));tp=LingxingTokenProvider(op_api=api,refresh_margin_seconds=300,logger=log);c=db(False)
    try:
        for store,sid in store_list(a.store):
            raw=await fetch(api,tp,sid,a.month);rs=mapped(store,sid,a.month,raw);log.info('%s rows=%s fee=%s',store,len(rs),sum((d0(r['estimated_monthly_storage_fee']) for r in rs),Decimal(0)))
            if a.print_first_row and raw:print(json.dumps(raw[0],ensure_ascii=False,indent=2,default=str))
            if a.action=='compare-api':compare(c,a.compare_table,sid,a.month,rs)
            elif a.dry_run:log.info('dry-run：未写库')
            else:write(c,a.target_table,sid,a.month,rs)
    finally:c.close()

def args():
    p=argparse.ArgumentParser();p.add_argument('--action',required=True,choices=['inspect-schema','audit-db','compare-api','fetch']);p.add_argument('--month');p.add_argument('--store');p.add_argument('--target-table',default=TARGET);p.add_argument('--audit-table',default=OLD);p.add_argument('--compare-table',default=OLD);p.add_argument('--schemas',default='');p.add_argument('--dry-run',action='store_true');p.add_argument('--print-first-row',action='store_true');return p.parse_args()
if __name__=='__main__':
    try:asyncio.run(main(args()))
    except Exception as e:log.exception('执行失败：%s',e);sys.exit(1)
