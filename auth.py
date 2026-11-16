"""Gateway-backed sessions; credentials never touch disk or browser storage."""
import os,secrets,threading,time,urllib.request,urllib.error
from urllib.parse import urlsplit

GATEWAY=os.environ.get('ZEROCLAW_GATEWAY','http://127.0.0.1:42617').rstrip('/')
parts=urlsplit(GATEWAY)
if parts.scheme not in ('http','https') or not parts.hostname or parts.username or parts.password:
 raise ValueError('Invalid gateway URL')
if parts.scheme=='http' and parts.hostname not in ('127.0.0.1','localhost','::1'):
 raise ValueError('Remote gateway requires HTTPS')
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
client=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
class GatewayUnavailable(Exception):pass
def verify(token):
 if not isinstance(token,str) or not 8<=len(token)<=4096 or any(ord(c)<33 or ord(c)>126 for c in token):return False
 req=urllib.request.Request(GATEWAY+'/api/status',headers={'Authorization':'Bearer '+token})
 try:
  with client.open(req,timeout=4) as r:return r.status==200
 except urllib.error.HTTPError as e:
  if e.code in (401,403):return False
  raise GatewayUnavailable() from None
 except (OSError,urllib.error.URLError):raise GatewayUnavailable() from None
class Sessions:
 def __init__(self):self.data={};self.attempts={};self.lock=threading.Lock()
 def allowed(self,ip):
  with self.lock:
   now=time.monotonic();self.attempts={k:v for k,v in self.attempts.items() if now-v[0]<60}
   start,count=self.attempts.get(ip,(now,0));self.attempts[ip]=(start,count+1)
   return count<10
 def create(self,token):
  if not verify(token):return None
  with self.lock:
   self.data={k:v for k,v in self.data.items() if v[1]>time.time()}
   if len(self.data)>=1000:self.data.pop(next(iter(self.data)))
   sid=secrets.token_urlsafe(32);self.data[sid]=(token,time.time()+8*3600);return sid
 def check(self,sid):
  with self.lock:item=self.data.get(sid)
  if not item:return False
  if item[1]<time.time() or not verify(item[0]):self.remove(sid);return False
  return True
 def remove(self,sid):
  with self.lock:self.data.pop(sid,None)
sessions=Sessions()
