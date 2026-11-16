import asyncio,contextlib,json,os,time
from pathlib import Path
from aiohttp import web
import auth,server

HERE=Path(__file__).parent
ORIGIN=os.environ.get('BOARD_ORIGIN','')
SECURE=os.environ.get('BOARD_SECURE_COOKIE','0')=='1'
def same_origin(r):return r.headers.get('Origin')==(ORIGIN or f'{r.scheme}://{r.host}')
async def authorized(r):return await asyncio.to_thread(auth.sessions.check,r.cookies.get('zcboard',''))
@web.middleware
async def security(r,handler):
 try:response=await handler(r)
 except auth.GatewayUnavailable:response=web.json_response({'error':'gateway_unavailable'},status=503)
 if not response.prepared:
  response.headers.update({'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer','Content-Security-Policy':"default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"})
 return response
async def login(r):
 if not same_origin(r):return web.json_response({'error':'origin'},status=403)
 if not auth.sessions.allowed(r.remote):return web.json_response({'error':'rate_limit'},status=429)
 try:token=(await r.json()).get('token','')
 except (ValueError,AttributeError):return web.json_response({'error':'invalid_token'},status=400)
 sid=await asyncio.to_thread(auth.sessions.create,token)
 if not sid:return web.json_response({'error':'invalid_token'},status=401)
 response=web.json_response({'ok':True});response.set_cookie('zcboard',sid,httponly=True,samesite='Strict',secure=SECURE,max_age=28800,path='/');return response
async def logout(r):
 if not same_origin(r):return web.Response(status=403)
 auth.sessions.remove(r.cookies.get('zcboard',''));response=web.json_response({'ok':True});response.del_cookie('zcboard',path='/');return response
async def session(r):return web.json_response({'ok':True}) if await authorized(r) else web.json_response({'error':'unauthorized'},status=401)
async def asset(r):
 name=r.match_info.get('asset') or 'index.html'
 if name not in ('index.html','app.js','i18n.js','style.css'):raise web.HTTPNotFound()
 return web.FileResponse(HERE/name)
async def socket(r):
 if not same_origin(r):return web.Response(status=403)
 if not await authorized(r):return web.Response(status=401)
 ws=web.WebSocketResponse(heartbeat=20,max_msg_size=1024);await ws.prepare(r)
 sid=r.cookies.get('zcboard','');previous=None;checked=time.monotonic();generation=-1
 try:
  while not ws.closed:
   # Consume control frames so heartbeat, ping and close behave correctly.
   try:
    msg=await asyncio.wait_for(ws.receive(),timeout=1)
    if msg.type in (web.WSMsgType.CLOSE,web.WSMsgType.CLOSED,web.WSMsgType.ERROR):break
   except asyncio.TimeoutError:pass
   if time.monotonic()-checked>=10:
    try:valid=await asyncio.to_thread(auth.sessions.check,sid)
    except auth.GatewayUnavailable:
     await ws.close(code=1013,message=b'gateway_unavailable');break
    if not valid:await ws.close(code=4001,message=b'unauthorized');break
    checked=time.monotonic()
   state=r.app['state']
   if generation!=state['generation']:
    generation=state['generation']
    await asyncio.wait_for(ws.send_json(state['payload']),timeout=5)
 finally:await ws.close()
 return ws
async def collect(app):
 previous=None
 while True:
  try:
   data=await asyncio.to_thread(server.build)
   canonical=json.dumps({k:v for k,v in data.items() if k!='updated'},sort_keys=True)
   payload={'type':'board','data':data}
  except Exception:canonical='unavailable';payload={'type':'error','error':'data_unavailable'}
  if canonical!=previous:
   app['state']['generation']+=1;app['state']['payload']=payload;previous=canonical
  await asyncio.sleep(2)
async def lifecycle(app):
 task=asyncio.create_task(collect(app))
 yield
 task.cancel()
 with contextlib.suppress(asyncio.CancelledError):await task
def make_app():
 app=web.Application(middlewares=[security],client_max_size=8192)
 app['state']={'generation':0,'payload':{'type':'loading'}}
 app.cleanup_ctx.append(lifecycle)
 app.add_routes([web.post('/api/login',login),web.post('/api/logout',logout),web.get('/api/session',session),web.get('/ws',socket),web.get('/',asset),web.get('/{asset}',asset)])
 return app
if __name__=='__main__':web.run_app(make_app(),host=os.environ.get('BOARD_HOST','127.0.0.1'),port=int(os.environ.get('BOARD_PORT','42618')),access_log=None,print=None)
