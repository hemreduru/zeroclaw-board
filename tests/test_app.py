import asyncio,json,tempfile,pathlib,sqlite3,time,unittest
from unittest.mock import patch
from aiohttp import CookieJar,WSServerHandshakeError,WSMsgType
from aiohttp.test_utils import TestClient,TestServer
import auth,server,web

class AppTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.state={'updated':'2026-01-01T00:00:00Z','agents':[],'cards':[],'warnings':[]}
  self.valid=True
  self.verify=patch('auth.verify',side_effect=lambda t:self.valid and t=='test-gateway-token');self.verify.start()
  self.build=patch('server.build',side_effect=lambda:dict(self.state));self.build.start()
  auth.sessions=auth.Sessions()
  self.client=TestClient(TestServer(web.make_app()),cookie_jar=CookieJar(unsafe=True));await self.client.start_server()
  self.origin=str(self.client.make_url('/')).rstrip('/')
 async def asyncTearDown(self):
  await self.client.close();self.build.stop();self.verify.stop()
 async def login(self):
  r=await self.client.post('/api/login',json={'token':'test-gateway-token'},headers={'Origin':self.origin});self.assertEqual(r.status,200)
 async def test_login_origin_and_bad_token(self):
  self.assertEqual((await self.client.get('/api/session')).status,401)
  self.assertEqual((await self.client.post('/api/login',json={'token':'test-gateway-token'},headers={'Origin':'http://evil.example'})).status,403)
  self.assertEqual((await self.client.post('/api/login',json={'token':'invalid'},headers={'Origin':self.origin})).status,401)
  await self.login();self.assertEqual((await self.client.get('/api/session')).status,200)
  self.valid=False;self.assertEqual((await self.client.get('/api/session')).status,401)
 async def test_websocket_updates_and_revocation(self):
  await self.login()
  with self.assertRaises(WSServerHandshakeError):await self.client.ws_connect('/ws',headers={'Origin':'http://evil.example'})
  ws=await self.client.ws_connect('/ws',headers={'Origin':self.origin})
  first=await ws.receive_json(timeout=5);self.assertEqual(first['type'],'board')
  with self.assertRaises(asyncio.TimeoutError):await asyncio.wait_for(ws.receive(),3)
  self.state['agents']=[{'id':'new-agent'}]
  changed=await ws.receive_json(timeout=5);self.assertEqual(changed['data']['agents'][0]['id'],'new-agent')
  self.valid=False
  closed=await ws.receive(timeout=12);self.assertEqual(closed.type,WSMsgType.CLOSE);self.assertEqual(ws.close_code,4001)
 async def test_logout_and_token_never_returned(self):
  await self.login();r=await self.client.get('/api/session');self.assertNotIn('test-gateway-token',await r.text())
  self.assertEqual((await self.client.post('/api/logout',headers={'Origin':self.origin})).status,200)
  self.assertEqual((await self.client.get('/api/session')).status,401)

class AdapterTests(unittest.TestCase):
 def test_dynamic_discovery_redaction_and_failure(self):
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);(root/'config.toml').write_text('\n'.join(f'[agents.agent{i}]\nmodel_provider="test.model"' for i in range(10)))
   state=root/'data/state';state.mkdir(parents=True)
   (state/'runtime-trace.jsonl').write_text(json.dumps({'message':'tool_call_start','@timestamp':'2026-01-01T00:00:00Z','zeroclaw':{'agent_alias':'agent9'},'attributes':{'tool':'shell','arguments':'SECRET_ARGUMENT','output':'SECRET_OUTPUT'}}))
   def rows(path,sql):
    if 'from cron_jobs' in sql:return [{'id':'j','name':'Test job','agent_alias':'agent9','locked_at':'2026-01-01T00:00:00Z','last_status':None,'enabled':1,'next_run':None,'last_run':None,'source':'imperative'}]
    return []
   with patch.object(server,'ROOT',root),patch.object(server,'read_db',side_effect=rows):
    data=server.build();self.assertEqual(len(data['agents']),10);self.assertEqual(data['cards'][0]['status'],'running');self.assertNotIn('SECRET_',json.dumps(data))
    with (root/'config.toml').open('a') as f:f.write('\n[agents.agent10]\nmodel_provider="test.new"\n')
    self.assertEqual(len(server.build()['agents']),11)
   self.assertEqual(server.status('timed_out'),'blocked');self.assertEqual(server.clean('token=SECRET'),'[gizli]')
 def test_sqlite_is_readonly(self):
  with tempfile.TemporaryDirectory() as temp:
   p=pathlib.Path(temp);db=sqlite3.connect(p/'test.db');db.execute('create table tasks(id)');db.close()
   with patch.object(server,'ROOT',p):
    with self.assertRaises(sqlite3.OperationalError):server.read_db(pathlib.Path('test.db'),'insert into tasks values(1)')
if __name__=='__main__':unittest.main()
