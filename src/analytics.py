import csv, sqlite3
from pathlib import Path
def percentile(values,p):
    if not values:return None
    a=sorted(float(x) for x in values); pos=(len(a)-1)*p
    lo=int(pos); hi=min(lo+1,len(a)-1); w=pos-lo
    return a[lo]*(1-w)+a[hi]*w
def build_summaries(db_path,output_dir):
    output_dir=Path(output_dir); output_dir.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(db_path)
    rows=con.execute("SELECT service,level,latency_ms FROM log_events ORDER BY service").fetchall()
    allrows=con.execute("SELECT * FROM log_events ORDER BY timestamp,record_id").fetchall()
    cols=[r[1] for r in con.execute("PRAGMA table_info(log_events)").fetchall()]
    con.close()
    groups={}
    for service,level,lat in rows: groups.setdefault(service,[]).append((level,lat))
    out=[]
    for service,events in sorted(groups.items()):
        lat=[float(x) for _,x in events if x is not None]; total=len(events)
        errors=sum(level in ("ERROR","FATAL") for level,_ in events)
        out.append({"service":service,"total_events":total,"error_events":errors,
          "error_rate_pct":round(errors/total*100,2),
          "avg_latency_ms":round(sum(lat)/len(lat),2) if lat else "",
          "p50_latency_ms":round(percentile(lat,.5),2) if lat else "",
          "p95_latency_ms":round(percentile(lat,.95),2) if lat else "",
          "min_latency_ms":min(lat) if lat else "","max_latency_ms":max(lat) if lat else ""})
    fields=["service","total_events","error_events","error_rate_pct","avg_latency_ms","p50_latency_ms","p95_latency_ms","min_latency_ms","max_latency_ms"]
    _atomic_csv(output_dir/"log_summary.csv",fields,out)
    _atomic_csv(output_dir/"cleaned_logs.csv",cols,[dict(zip(cols,r)) for r in allrows])
    return out
def _atomic_csv(path,fields,rows):
    tmp=path.with_suffix(path.suffix+".tmp")
    with tmp.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
    tmp.replace(path)
def overall_metrics(db_path):
    con=sqlite3.connect(db_path)
    total,errors,avg=con.execute("SELECT COUNT(*),SUM(level IN ('ERROR','FATAL')),AVG(latency_ms) FROM log_events").fetchone()
    con.close(); total=total or 0; errors=errors or 0
    return {"total_events":total,"error_events":errors,"error_rate_pct":round(errors/total*100,2) if total else 0,
            "avg_latency_ms":round(avg,2) if avg is not None else None}
