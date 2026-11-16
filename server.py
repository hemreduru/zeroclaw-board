#!/usr/bin/env python3
"""Read-only ZeroClaw activity board. No writes to ZeroClaw state."""
import datetime as dt
import hashlib,json,os,re,sqlite3,time,tomllib
from pathlib import Path

ROOT=Path(os.environ.get('ZEROCLAW_ROOT',str(Path.home()/'.zeroclaw')))
HERE=Path(__file__).resolve().parent
TOOLS={'shell':'Komut çalıştırıyor','file_read':'Dosya okuyor','file_write':'Dosya yazıyor','file_edit':'Dosya düzenliyor','delegate':'Ajanlara iş devrediyor','http_request':'Web isteği yapıyor','web_search':'Araştırıyor','cron_add':'Devam görevini planlıyor','memory_recall':'Hafızayı kontrol ediyor'}
# Retain only safe lifecycle metadata across the upstream rolling trace window.
TRACKED={}
TRACKED_ROOT=None
def clean(s,n=130):
 s=re.sub(r'https?://\S+','[bağlantı]',str(s or ''))
 s=re.sub(r'(?i)(bearer\s+\S+|(?:sk-|ghp_|github_pat_)[\w-]+|(?:password|token|secret|api_key)\s*[:=]\s*\S+)','[gizli]',s)
 return re.sub(r'[\x00-\x1f]+',' ',s)[:n]
def read_db(path,sql):
 with sqlite3.connect('file:'+str(ROOT/path)+'?mode=ro',uri=True,timeout=.3) as c:
  c.row_factory=sqlite3.Row
  c.execute('pragma query_only=ON')
  return [dict(r) for r in c.execute(sql)]
def age(t):
 try:return max(0,time.time()-dt.datetime.fromisoformat(t.replace('Z','+00:00')).timestamp())
 except:return None
def status(s):
 s=str(s or '').lower()
 return 'done' if s in ('ok','completed','complete','succeeded','success') else 'blocked' if s in ('failed','error','cancelled','canceled','paused','blocked','interrupted','lost','timed_out','timedout','timeout') else 'running' if s in ('running','active','in_progress') else 'queued'
def build():
 global TRACKED_ROOT
 if TRACKED_ROOT!=str(ROOT):TRACKED.clear();TRACKED_ROOT=str(ROOT)
 config=tomllib.loads((ROOT/'config.toml').read_text())
 agents={a:{'id':a,'model':clean(v.get('model_provider','')),'active':0,'activity':'Hazır','last':None} for a,v in config.get('agents',{}).items()}
 main=next(iter(agents),'default')
 cards=[];warnings=[]
 def query(p,q):
  try:return read_db(p,q)
  except (sqlite3.Error,OSError):warnings.append(str(p));return []
 jobs=query(Path('data/cron/jobs.db'),'select id,name,agent_alias,locked_at,last_status,enabled,next_run,last_run,source from cron_jobs')
 jobmap={j['id']:j for j in jobs}
 events=[]
 try:
  with (ROOT/'data/state/runtime-trace.jsonl').open('rb') as f:
   f.seek(0,2);size=f.tell();f.seek(max(0,size-4_000_000));data=f.read().decode('utf-8',errors='replace')
  for line in data.splitlines():
   try:events.append(json.loads(line))
   except ValueError:pass
 except OSError:warnings.append('runtime-trace')
 latest={};trace={}
 for e in events:
  z=e.get('zeroclaw',{});a=e.get('attributes',{});msg=e.get('message');job=z.get('cron_job_id');agent=z.get('agent_alias') or jobmap.get(job,{}).get('agent_alias')
  tid=e.get('trace_id') or a.get('trace_id')
  if not agent:
   matches=[v['agent'] for (t,_),v in trace.items() if t==tid]
   if len(set(matches))==1:agent=matches[0]
  if not agent:continue
  if msg not in ('llm_request','llm_response','tool_call_start','tool_call_result','turn_final_response'):continue
  event={'agent':agent,'time':e.get('@timestamp'),'model':clean(a.get('model') or ''),'activity':'Araç tamamlandı; sonraki adım bekleniyor' if msg=='tool_call_result' else TOOLS.get(a.get('tool'),'Araç: '+clean(a.get('tool'),40)) if msg=='tool_call_start' else 'Model yanıtı bekleniyor' if msg=='llm_request' else 'Yanıt değerlendiriliyor' if msg=='llm_response' else 'Tur tamamlandı','final':msg=='turn_final_response','job':job,'trace':tid,'session':z.get('session_key'),'started':e.get('@timestamp')}
  latest[agent]=event
  if tid:
   key=(tid,agent);previous=trace.get(key) or TRACKED.get(key)
   if previous:event['started']=previous['started']
   trace[key]=event
 TRACKED.update(trace)
 for key in list(TRACKED):
  if (age(TRACKED[key]['time']) or 0)>86400:del TRACKED[key]
 # Bound memory even if an installation creates very many short turns.
 if len(TRACKED)>2000:
  for key in sorted(TRACKED,key=lambda k:TRACKED[k]['time'] or '')[:-2000]:del TRACKED[key]
 for j in jobs:
  if not j['enabled'] and not j['locked_at']:continue
  running=bool(j['locked_at']);s='running' if running else 'blocked' if j['last_status'] and status(j['last_status'])=='blocked' else 'queued'
  cards.append({'id':'job:'+j['id'],'agent':j['agent_alias'] or main,'title':clean(j['name']) or 'Zamanlanmış görev','status':s,'source':'Otomasyon' if j['source']=='declarative' else 'Devam görevi','started':j['locked_at'],'updated':j['last_run'],'next':j['next_run'],'detail':'Çalışma sürüyor' if running else 'Son deneme hata verdi; sonraki çalışma bekleniyor' if s=='blocked' else 'Sonraki çalışma bekleniyor','job':j['id']})
 runs=query(Path('data/cron/jobs.db'),'select id,job_id,started_at,finished_at,status,executing_agent from cron_runs order by id desc limit 30')
 for r in runs:
  if r['job_id'] not in jobmap or (age(r['finished_at']) or 0)>86400:continue
  j=jobmap[r['job_id']]
  cards.append({'id':'run:'+str(r['id']),'agent':r['executing_agent'] or j['agent_alias'] or main,'title':clean(j['name']) or 'Zamanlanmış görev','status':status(r['status']),'source':'Çalışma sonucu','started':r['started_at'],'updated':r['finished_at'],'detail':'Bu çalışma turu tamamlandı; projenin bütünü tamamlanmış olmayabilir.' if status(r['status'])=='done' else 'Çalışma sonucu: '+clean(r['status']),'job':None})
 tasks=query(Path('data/control_plane.db'),'select id,kind,agent,status,parent_id,started_at,finished_at,heartbeat_at from tasks order by started_at desc limit 100')
 for t in tasks:
  if t['finished_at'] and (age(t['finished_at']) or 0)>86400:continue
  cards.append({'id':'task:'+t['id'],'agent':t['agent'],'title':clean(t['kind'])+' görevi','status':status(t['status']),'source':'Ajan görevi','started':t['started_at'],'updated':t['heartbeat_at'] or t['finished_at'],'parent':t['parent_id'],'detail':'Alt görev' if t['parent_id'] else 'Bağımsız görev'})
 sessions=query(Path('data/sessions/sessions.db'),"select session_key,state,agent_alias,turn_started_at,last_activity from session_metadata where state != 'idle'")
 for s in sessions:
  cards.append({'id':'session:'+hashlib.sha256(s['session_key'].encode()).hexdigest()[:12],'agent':s['agent_alias'] or main,'title':'Gelen mesaj üzerinde çalışıyor','status':'running','source':'Sohbet','started':s['turn_started_at'],'updated':s['last_activity'],'detail':'Mesaj işleniyor'})
 # Channel metadata can remain idle during a real turn. Include ALL agents,
 # and key by both trace and agent so a child's final cannot close its parent.
 for (tid,agent),e in TRACKED.items():
  if e['final']:continue
  if any(c['agent']==agent and c['status']=='running' and (c.get('job')==e['job'] if e['job'] else c['source'] in ('Sohbet','Ajan görevi')) for c in cards):continue
  stale=(age(e['time']) or 0)>180
  cards.append({'id':'trace:'+tid+':'+agent,'agent':agent,'title':'Sohbet görevi' if e['session'] else 'Ajan çalışması','status':'blocked' if stale else 'running','source':'Son etkinlik','started':e['started'],'updated':e['time'],'detail':'Çalışmanın bitişi doğrulanamadı; son etkinlik eski.' if stale else e['activity'],'uncertain':True,'stale':stale,'model':e['model'],'job':e['job'],'trace':tid})
 for card in cards:
  a=card['agent'];agents.setdefault(a,{'id':a,'model':'','active':0,'activity':'Hazır','last':None})
  ev=latest.get(a)
  if card['status']=='running':
   agents[a]['active']+=1
   if ev and (ev['trace']==card['trace'] if card.get('trace') else ev['job']==card['job'] if card.get('job') else True):card['detail']=ev['activity'];card['updated']=ev['time'];card['model']=ev['model']
   seconds=age(card.get('updated') or card.get('started'))
   card['stale']=seconds is not None and seconds>180
 for a,v in agents.items():
  if a in latest:
   e=latest[a];v['last']=e['time'];v['model']=e['model'] or v['model'];v['activity']=e['activity'] if v['active'] else 'Hazır'
  active=[c for c in cards if c['agent']==a and c['status']=='running']
  uncertain=[c for c in cards if c['agent']==a and c.get('uncertain') and c.get('stale')]
  v['state']='running' if active else 'unknown' if uncertain else 'idle'
  if active:
   c=max(active,key=lambda c:c.get('updated') or '')
   v['activity']=c['detail'];v['last']=c.get('updated');v['model']=c.get('model') or v['model']
  elif uncertain:v['activity']='Durum doğrulanamadı'
 return {'updated':dt.datetime.now(dt.timezone.utc).isoformat(),'agents':list(agents.values()),'cards':cards,'warnings':warnings}

