"""Local quality and pipeline monitoring dashboard.

Run with: docker compose run --rm -p 8050:8050 serve python serve/monitor.py --host 0.0.0.0
"""
import argparse
import json
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg

from common import RESULTS_DIR, connect


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Crash Platform | Quality Monitor</title>
<style>
:root{color-scheme:dark;--bg:#101820;--panel:#192832;--line:#31444e;--text:#eaf2f4;--muted:#a9bdc4;--green:#47d7a0;--yellow:#ffc857;--red:#ff6b6b;--blue:#77bdfb}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,sans-serif}
main{max-width:1180px;margin:auto;padding:34px 22px 60px}header{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:24px}
h1{font-size:27px;margin:0}h2{font-size:17px;margin:0 0 16px}.muted{color:var(--muted)}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:13px;margin:20px 0}
.card,.panel{background:var(--panel);border:1px solid var(--line);border-radius:11px;padding:18px}.label{color:var(--muted);font-size:13px}.value{font-size:25px;font-weight:650;margin-top:5px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:15px;margin-top:15px}.wide{grid-column:1/-1}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:9px 8px;border-bottom:1px solid var(--line);vertical-align:top}th{color:var(--muted);font-weight:550}
.pass{color:var(--green)}.alert{color:var(--yellow)}.fail{color:var(--red)}.chart{width:100%;height:auto;min-height:110px}.empty{color:var(--muted);padding:10px 0}
button{background:#243a47;border:1px solid var(--line);border-radius:7px;color:var(--text);padding:8px 12px;cursor:pointer}
@media(max-width:760px){.grid{grid-template-columns:1fr}.wide{grid-column:auto}header{align-items:flex-start}}
</style>
</head>
<body><main>
<header><div><h1>Crash Platform Quality Monitor</h1><div id="updated" class="muted">Loading monitoring results…</div></div><button id="refresh">Refresh</button></header>
<section id="summary" class="cards"></section>
<section class="grid">
<article class="panel"><h2>Quality checks</h2><div id="quality"></div></article>
<article class="panel"><h2>Automated check results</h2><div id="chart"></div></article>
<article class="panel wide"><h2>Q1 street-name comparison (report only)</h2><div id="streetComparison"></div></article>
<article class="panel wide"><h2>Rows in and out by pipeline stage</h2><div id="stages"></div></article>
<article class="panel wide"><h2>Pipeline stage durations and memory</h2><div id="stageHealth"></div></article>
<article class="panel wide"><h2>Recent end-to-end runs</h2><div id="history"></div></article>
<article class="panel wide"><h2>Consumer lag over time</h2><div id="lagChart"></div></article>
<article class="panel"><h2>Data tables and retained failures</h2><div id="tables"></div></article>
<article class="panel"><h2>Stream and database monitoring</h2><div id="runtime"></div></article>
</section>
</main>
<script>
const esc = value => String(value ?? '—').replace(/[&<>"']/g, char =>
  ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const number = value => Number(value || 0).toLocaleString();
function table(headers, rows) {
  if (!rows.length) return '<div class="empty">No results yet.</div>';
  return '<table><thead><tr>'+headers.map(x=>'<th>'+x+'</th>').join('')+'</tr></thead><tbody>'+
    rows.map(row=>'<tr>'+row.map(x=>'<td>'+x+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
}
function stat(label,value,cls='') {
  return '<article class="card"><div class="label">'+label+'</div><div class="value '+cls+'">'+value+'</div></article>';
}
function drawLagChart(points) {
  const valid=points.filter(p=>Number.isFinite(Number(p.lag)));
  if(valid.length<2)return '<div class="empty">Lag history appears here during a stream run.</div>';
  const max=Math.max(1,...valid.map(p=>Number(p.lag)));
  const coords=valid.map((p,i)=>(i*1000/(valid.length-1))+','+(90-Number(p.lag)*80/max)).join(' ');
  return '<svg class="chart" viewBox="0 0 1000 120" role="img" aria-label="Consumer lag over time">'+
    '<polyline points="'+coords+'" fill="none" stroke="#77bdfb" stroke-width="4"/>'+
    '<text x="0" y="115" fill="#a9bdc4" font-size="18">Latest lag: '+number(valid[valid.length-1].lag)+
    ' · peak observed: '+number(max)+'</text></svg>';
}
function drawChart(counts,total) {
  if (!total) return '<div class="empty">Run the quality pipeline to populate the chart.</div>';
  const colors={pass:'#47d7a0',alert:'#ffc857',fail:'#ff6b6b'};
  let x=0, parts='';
  for (const key of ['pass','alert','fail']) {
    const width=counts[key]/total*1000;
    if (width>0) parts+='<rect x="'+x+'" y="24" width="'+width+'" height="32" fill="'+colors[key]+'"><title>'+key+': '+counts[key]+'</title></rect>';
    x+=width;
  }
  return '<svg class="chart" viewBox="0 0 1000 100" role="img" aria-label="Quality test pass, alert and fail counts">'+parts+
    '<text x="0" y="82" fill="#a9bdc4" font-size="24">'+counts.pass+' passed · '+counts.alert+
    ' alerts · '+counts.fail+' failed</text></svg>';
}
async function refresh() {
  const response=await fetch('/api/status',{cache:'no-store'});
  const data=await response.json();
  document.querySelector('#updated').textContent='Updated '+(data.updated_at||'—')+' · '+(data.run_id||'no run recorded');
  const rules=data.quality?.rules||[];
  const extraChecks=(data.quality?.completeness_checks||[]).map(c=>({
    id:'missing_share:'+c.column,
    description:'Missing-value share '+(Number(c.missing_share)*100).toFixed(1)+'% (baseline '+
      (Number(c.baseline_share)*100).toFixed(1)+'%)',
    result:c.result,
    failed_rows:c.missing_rows,
    threshold_rows:Number(c.warn_share_range[1])*Number(data.quality?.rows_in||0)
  }));
  const checks=rules.concat(extraChecks);
  const counts={pass:0,alert:0,fail:0};
  for(const rule of checks) counts[rule.result]=(counts[rule.result]||0)+1;
  const status=data.quality?.overall_status||'pending';
  document.querySelector('#summary').innerHTML=
    stat('Overall quality',status.toUpperCase(),status)+
    stat('Checks passed',counts.pass,'pass')+
    stat('Alerts',counts.alert,'alert')+
    stat('Failed checks',counts.fail,'fail')+
    stat('Input rows',number(data.quality?.rows_in))+
    stat('Rows quarantined',number(data.quality?.rows_quarantined));
  document.querySelector('#quality').innerHTML=table(
    ['Rule','Result','Rows failed','Threshold'],
    checks.map(r=>['<strong>'+esc(r.id)+'</strong><br><span class="muted">'+esc(r.description)+'</span>',
      '<span class="'+esc(r.result)+'">'+esc(r.result).toUpperCase()+'</span>',
      number(r.failed_rows),number(Math.floor(r.threshold_rows))]));
  const castErrors=Object.entries(data.quality?.invalid_casts_by_column||{})
    .map(([column,count])=>[esc(column),number(count)]);
  if(castErrors.some(row=>row[1]!=='0')) {
    document.querySelector('#quality').innerHTML+=
      '<h3>Invalid casts by count column</h3>'+table(['Column','Invalid casts'],castErrors);
  }
  if(data.quality?.volume_check) {
    const volume=data.quality.volume_check;
    document.querySelector('#quality').innerHTML+=table(
      ['Volume check','Result','Rows','Threshold'],[
        ['Minimum source size','<span class="'+esc(volume.minimum_result)+'">'+esc(volume.minimum_result).toUpperCase()+'</span>',
          number(volume.rows_in),number(volume.minimum_rows)],
        ['Change from previous run',
          '<span class="'+(volume.change_result==='fail'?'fail':volume.change_result==='alert'?'alert':'pass')+'">'+
          esc(volume.change_result).toUpperCase()+'</span>',
          volume.change_rate==null?'—':(Number(volume.change_rate)*100).toFixed(2)+'%',
          'warn 2% / fail 5%']
      ]);
  }
  document.querySelector('#chart').innerHTML=drawChart(counts,checks.length);
  const street=data.quality?.street_name_comparison;
  document.querySelector('#streetComparison').innerHTML=street?table(
    ['Ranking','Top ten','Changed?'],[
      ['Raw names',street.raw_top_10.map(x=>esc(x.street)+' ('+number(x.crashes)+')').join('<br>'),'—'],
      ['Standardized names',street.standardized_top_10.map(x=>esc(x.street)+' ('+number(x.crashes)+')').join('<br>'),
        street.ranking_changed?'Yes':'No'],
      ['Canonical Q1 answer','Unchanged','Report only']
    ]):'<div class="empty">Run Part 1 to generate the comparison.</div>';
  const stages=data.stages||[];
  document.querySelector('#stages').innerHTML=table(['Stage','Rows in','Rows out','Difference'],
    stages.map(s=>[esc(s.stage),number(s.rows_in),number(s.rows_out),
      number(Number(s.rows_out||0)-Number(s.rows_in||0))]));
  document.querySelector('#stageHealth').innerHTML=table(
    ['Stage','Seconds','Container peak / limit'],
    stages.map(s=>[
      esc(s.stage),
      s.seconds==null?'—':esc(s.seconds),
      s.container_memory_peak_mb==null?'—':
        esc(s.container_memory_peak_mb)+' / '+esc(s.container_memory_limit_mb)+' MB'
    ]));
  document.querySelector('#history').innerHTML=table(
    ['Run','Part','Status','Rows in','Rows out','Seconds','Peak MB'],
    (data.pipeline_history||[]).map(r=>[
      esc(r.run_id).slice(0,8),esc(r.part),
      '<span class="'+esc(r.status)+'">'+esc(r.status).toUpperCase()+'</span>',
      number(r.rows_in),number(r.rows_out),esc(r.seconds),esc(r.peak_mem_mb)
    ]));
  document.querySelector('#lagChart').innerHTML=drawLagChart(data.lag_history||[]);
  document.querySelector('#tables').innerHTML=table(['Dataset','Rows / details'],[
    ['Parquet answers','Q1 top streets; Q2 injuries by hour'],
    ['Stream answers','Q3 borough-by-hour; latest hour'],
    ['Quarantine table','data/quarantine/quality_failures · '+number(data.quality?.rows_quarantined)+' rows'],
    ['Validated data','data/lake/quality_validated · '+number(data.quality?.rows_out)+' rows'],
    ['Quality report','results/quality_report.json'],
  ]);
  const runtime=data.runtime||{};
  const db=runtime.database_error?'<br><span class="alert">Database unavailable: '+esc(runtime.database_error)+'</span>':'';
  document.querySelector('#runtime').innerHTML=table(['Signal','Latest value'],[
    ['Consumer lag',runtime.lag==null?'—':number(runtime.lag)+' messages'],
    ['Messages consumed',number(runtime.messages_consumed)],
    ['Consumer peak memory',runtime.consumer_memory==null?'—':esc(runtime.consumer_memory)+' MB'],
    ['Producer peak memory',runtime.producer_memory==null?'—':esc(runtime.producer_memory)+' MB'],
    ['Last database load',esc(runtime.last_loaded_at)],
    ['Database age',runtime.database_age_minutes==null?'—':esc(runtime.database_age_minutes)+' minutes'],
    ['Container memory samples',number(runtime.container_samples)],
    ['Latest container usage',esc(runtime.container_latest)],
  ])+db;
  if((runtime.alerts||[]).length) {
    document.querySelector('#runtime').innerHTML+='<p class="alert"><strong>Alerts</strong><br>'+
      runtime.alerts.map(esc).join('<br>')+'</p>';
  }
}
document.querySelector('#refresh').addEventListener('click',()=>refresh().catch(showError));
function showError(error){document.querySelector('#updated').textContent='Could not load monitoring data: '+error.message;}
refresh().catch(showError);
setInterval(()=>refresh().catch(showError),15000);
</script></body></html>"""


def read_json(name):
    path = Path(RESULTS_DIR) / name
    if not path.is_file():
        return None
    with path.open() as f:
        return json.load(f)


def read_lag_history():
    path = Path(RESULTS_DIR) / 'stream_lag.csv'
    if not path.is_file():
        return []
    with path.open(newline='') as f:
        import csv
        return list(csv.DictReader(f))[-100:]


def read_container_stats():
    path = Path(RESULTS_DIR) / 'container_memory.csv'
    if not path.is_file():
        return []
    with path.open(newline='') as f:
        import csv
        return list(csv.DictReader(f))[-50:]


def collect_status():
    quality = read_json('quality_report.json')
    stages_file = read_json('stage_metrics.json') or {}
    stages = stages_file.get('stages', [])
    for metric_name, stage_name in (
        ('stream_producer_metrics.json', 'stream_producer'),
        ('stream_consumer_metrics.json', 'stream_consumer'),
    ):
        metrics = read_json(metric_name)
        if metrics:
            if stage_name == 'stream_producer':
                rows_in = metrics.get('rows_read', 0)
                rows_out = metrics.get('rows_sent', 0)
                seconds = metrics.get('seconds')
                memory_peak = metrics.get('container_memory_peak_mb')
                memory_limit = metrics.get('container_memory_limit_mb')
            else:
                rows_in = metrics.get('messages_this_run', 0)
                rows_out = metrics.get('messages_consumed', 0)
                seconds = metrics.get('seconds_this_run')
                memory_peak = metrics.get('container_memory_peak_mb')
                memory_limit = metrics.get('container_memory_limit_mb')
            stages.append({
                'stage': stage_name,
                'rows_in': rows_in,
                'rows_out': rows_out,
                'seconds': seconds,
                'container_memory_peak_mb': memory_peak,
                'container_memory_limit_mb': memory_limit,
            })

    producer = read_json('stream_producer_metrics.json') or {}
    consumer = read_json('stream_consumer_metrics.json') or {}
    container_stats = read_container_stats()
    alerts = []
    for stage in stages:
        peak = stage.get('container_memory_peak_mb')
        limit = stage.get('container_memory_limit_mb')
        if peak is not None and limit:
            share = peak / limit
            if share >= 0.8:
                alerts.append(
                    f"{stage['stage']} used {share:.1%} of its container memory limit "
                    f"(80% warning threshold)"
                )
    lag = consumer.get('lag')
    if lag is not None and lag > 1000:
        alerts.append(f'Consumer lag is {lag:,}; fail threshold is 1,000')
    elif lag is not None and lag > 0:
        alerts.append(f'Consumer lag is {lag:,}; expected 0 at completion')
    bad_messages = consumer.get('bad_messages', 0)
    message_count = consumer.get('messages_this_run', 0)
    bad_share = bad_messages / message_count if message_count else 0
    if bad_share > 0.01:
        alerts.append(f'Bad stream messages are {bad_share:.1%}; fail threshold is 1%')
    elif bad_messages:
        alerts.append(f'{bad_messages:,} bad stream messages were observed')
    if quality:
        change = quality.get('volume_check', {}).get('change_rate')
        if change is not None and abs(change) > 0.05:
            alerts.append(f'Row count changed {change:.1%}; fail threshold is 5%')
        elif change is not None and abs(change) > 0.02:
            alerts.append(f'Row count changed {change:.1%}; warn threshold is 2%')
    runtime = {
        'lag': consumer.get('lag'),
        'messages_consumed': consumer.get('messages_consumed'),
        'consumer_memory': consumer.get('peak_memory_mb'),
        'producer_memory': producer.get('peak_memory_mb'),
        'last_loaded_at': None,
        'alerts': alerts,
        'container_samples': len(container_stats),
        'container_latest': (
            f"{container_stats[-1].get('container')} "
            f"{container_stats[-1].get('memory_usage')} "
            f"({container_stats[-1].get('memory_percent')})"
            if container_stats else None
        ),
    }
    if quality:
        for check in quality.get('completeness_checks', []):
            if check['result'] == 'alert':
                alerts.append(
                    f"{check['column']} missing share {check['missing_share']:.1%} "
                    f"is outside {check['warn_share_range'][0]:.1%}–"
                    f"{check['warn_share_range'][1]:.1%}"
                )
    if container_stats:
        latest_memory_percent = container_stats[-1].get('memory_percent', '').rstrip('%')
        try:
            if float(latest_memory_percent) >= 80:
                alerts.append(
                    f"Container {container_stats[-1].get('container')} memory is "
                    f"{latest_memory_percent}%; warning threshold is 80%"
                )
        except ValueError:
            pass
    pipeline_history = []
    try:
        with connect(timeout=2) as conn:
            runtime['last_loaded_at'] = conn.execute(
                'SELECT max(loaded_at) FROM load_log'
            ).fetchone()[0]
            if runtime['last_loaded_at'] is not None:
                loaded_at = runtime['last_loaded_at']
                runtime['database_age_minutes'] = round(
                    (datetime.now(timezone.utc) - loaded_at.astimezone(timezone.utc))
                    .total_seconds() / 60,
                    1,
                )
                runtime['last_loaded_at'] = loaded_at.isoformat()
                if runtime['database_age_minutes'] > 7 * 24 * 60:
                    alerts.append('Database is older than the 7-day fail threshold')
                elif runtime['database_age_minutes'] > 24 * 60:
                    alerts.append('Database is older than the 1-day warning threshold')
            rows = conn.execute(
                'SELECT run_id, part, status, rows_in, rows_out, seconds, peak_mem_mb '
                'FROM pipeline_runs ORDER BY started_at DESC NULLS LAST LIMIT 16'
            ).fetchall()
            pipeline_history = [
                dict(zip(
                    ('run_id', 'part', 'status', 'rows_in', 'rows_out', 'seconds', 'peak_mem_mb'),
                    row,
                ))
                for row in rows
            ]
    except (psycopg.Error, SystemExit) as exc:
        runtime['database_error'] = str(exc)
    return {
        'quality': quality,
        'stages': stages,
        'runtime': runtime,
        'pipeline_history': pipeline_history,
        'lag_history': read_lag_history(),
        'run_id': (quality or {}).get('run_id', stages_file.get('run_id')),
        'updated_at': (quality or {}).get('checked_at', stages_file.get('updated_at')),
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            body = PAGE.encode()
            content_type = 'text/html; charset=utf-8'
        elif self.path == '/api/status':
            try:
                body = json.dumps(collect_status(), default=str).encode()
            except (OSError, ValueError) as exc:
                self.send_error(500, f'Unable to read monitoring results: {exc}')
                return
            content_type = 'application/json; charset=utf-8'
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f'{self.address_string()} - {fmt % args}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8050)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f'Quality dashboard available at http://localhost:{args.port}')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('Stopping dashboard.')
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
