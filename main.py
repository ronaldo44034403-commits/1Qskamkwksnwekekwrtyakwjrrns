"""
Мессенджер-сервер на Python (FastAPI + SQLite + WebSocket).

Установка:   pip install fastapi uvicorn
Запуск:      uvicorn main:app --host 0.0.0.0 --port 8000   (или просто: python main.py)
Render:      Start command = uvicorn main:app --host 0.0.0.0 --port $PORT

Переменные окружения (необязательно):
  ADMIN_PHONE  номер админ-аккаунта (по умолчанию +000)
  LOGIN_CODE   код входа для всех (по умолчанию 1234)
  DB_PATH      путь к базе (по умолчанию server.db)

API:
  POST /auth/login          {"phone": "+123", "code": "1234", "name": "Вася"} -> token
  GET  /me                  Authorization: Bearer <token>
  GET  /users
  GET  /messages/{chat_id}?limit=50      chat_id: "global" или "dm:<id1>:<id2>"
  POST /send                {"chat_id": "global", "text": "привет"}
  WS   /ws?token=<token>    приходят новые сообщения, можно слать {"chat_id","text"}

Админ-команды (пишет админ в любой чат, начинаются с точки):
  .помощь
  .юзеры
  .бан <id|номер>        .разбан <id|номер>
  .кик <id|номер>
  .премиум <id|номер> [вкл|выкл]
  .звезды <id|номер> <кол-во>      (можно отрицательное)
  .админ <id|номер> [вкл|выкл]
"""
import os
import sqlite3
import secrets
import time
from contextlib import closing
from typing import Dict, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "server.db")
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "+000")
LOGIN_CODE = os.environ.get("LOGIN_CODE", "1234")

app = FastAPI(title="MyMessenger server")


# ---------- База ----------
def _db():
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=10000")
    return c


def run(sql, args=()):
    with closing(_db()) as c:
        cur = c.execute(sql, args)
        c.commit()
        return cur.lastrowid


def one(sql, args=()):
    with closing(_db()) as c:
        return c.execute(sql, args).fetchone()


def many(sql, args=()):
    with closing(_db()) as c:
        return c.execute(sql, args).fetchall()


def init_db():
    run("""CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL,
        token TEXT UNIQUE NOT NULL,
        is_admin INTEGER DEFAULT 0,
        premium INTEGER DEFAULT 0,
        stars INTEGER DEFAULT 0,
        banned INTEGER DEFAULT 0,
        created INTEGER)""")
    run("""CREATE TABLE IF NOT EXISTS messages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT NOT NULL,
        sender_id INTEGER NOT NULL,
        text TEXT NOT NULL,
        ts INTEGER)""")
    run("CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id, id)")


init_db()

if ADMIN_PHONE == "+000" or LOGIN_CODE == "1234":
    print("ВНИМАНИЕ: стоят ADMIN_PHONE/LOGIN_CODE по умолчанию. "
          "Любой, кто знает +000 и 1234, станет админом. Задай свои в переменных окружения.")


def pub(u):
    return {
        "id": u["id"], "phone": u["phone"], "name": u["name"],
        "is_admin": bool(u["is_admin"]), "premium": bool(u["premium"]),
        "stars": u["stars"], "banned": bool(u["banned"]),
    }


# ---------- Подключения ----------
class Hub:
    def __init__(self):
        self.conns: Dict[int, Set[WebSocket]] = {}

    async def add(self, uid, ws):
        self.conns.setdefault(uid, set()).add(ws)

    def remove(self, uid, ws):
        s = self.conns.get(uid)
        if s:
            s.discard(ws)
            if not s:
                self.conns.pop(uid, None)

    async def send_to(self, uid, payload):
        for ws in list(self.conns.get(uid, ())):
            try:
                await ws.send_json(payload)
            except Exception:
                self.remove(uid, ws)

    async def kick(self, uid, reason="kicked"):
        for ws in list(self.conns.get(uid, ())):
            try:
                await ws.send_json({"type": "kicked", "reason": reason})
                await ws.close()
            except Exception:
                pass
        self.conns.pop(uid, None)

    async def broadcast_chat(self, chat_id, payload):
        if chat_id == "global":
            for uid in list(self.conns):
                await self.send_to(uid, payload)
        else:
            _, a, b = chat_id.split(":")
            for uid in {int(a), int(b)}:
                await self.send_to(uid, payload)


hub = Hub()


# ---------- Авторизация / чаты ----------
def user_by_token(token):
    if not token:
        raise HTTPException(401, "no token")
    u = one("SELECT * FROM users WHERE token=?", (token,))
    if not u:
        raise HTTPException(401, "bad token")
    if u["banned"]:
        raise HTTPException(403, "banned")
    return u


def bearer(authorization):
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return None


def check_chat(user, chat_id):
    if chat_id == "global":
        return
    parts = chat_id.split(":")
    if len(parts) != 3 or parts[0] != "dm":
        raise HTTPException(400, "bad chat_id")
    try:
        a, b = int(parts[1]), int(parts[2])
    except ValueError:
        raise HTTPException(400, "bad chat_id")
    if user["id"] not in (a, b) or a == b:
        raise HTTPException(403, "no access")
    if not one("SELECT 1 FROM users WHERE id=?", (b if user["id"] == a else a,)):
        raise HTTPException(404, "no such user")


# ---------- Админ-команды ----------
def find_target(arg):
    if arg.isdigit():
        return one("SELECT * FROM users WHERE id=?", (int(arg),))
    return one("SELECT * FROM users WHERE phone=?", (arg,))


def flag(args, idx=1, default=1):
    if len(args) > idx:
        return 0 if args[idx].lower() in ("выкл", "off", "0", "нет") else 1
    return default


async def run_command(admin, text):
    parts = text[1:].split()
    if not parts:
        return "Напиши .помощь"
    cmd, args = parts[0].lower(), parts[1:]

    if cmd in ("помощь", "help"):
        return ("Команды:\n.юзеры\n.бан <id|номер>\n.разбан <id|номер>\n.кик <id|номер>\n"
                ".премиум <id|номер> [вкл|выкл]\n.звезды <id|номер> <кол-во>\n"
                ".админ <id|номер> [вкл|выкл]")

    if cmd in ("юзеры", "users"):
        rows = many("SELECT * FROM users ORDER BY id LIMIT 100")
        return "\n".join(
            f"#{r['id']} {r['name']} {r['phone']}"
            f"{' [админ]' if r['is_admin'] else ''}{' [премиум]' if r['premium'] else ''}"
            f"{' [бан]' if r['banned'] else ''} ★{r['stars']}" for r in rows) or "Пусто"

    if not args:
        return "Укажи пользователя: id или номер"
    t = find_target(args[0])
    if not t:
        return "Пользователь не найден"
    tid = t["id"]

    if cmd in ("бан", "ban"):
        if t["is_admin"]:
            return "Нельзя банить админа"
        run("UPDATE users SET banned=1 WHERE id=?", (tid,))
        await hub.kick(tid, "banned")
        return f"{t['name']} забанен"

    if cmd in ("разбан", "unban"):
        run("UPDATE users SET banned=0 WHERE id=?", (tid,))
        return f"{t['name']} разбанен"

    if cmd in ("кик", "kick"):
        await hub.kick(tid, "kicked")
        return f"{t['name']} отключён (может зайти снова)"

    if cmd in ("премиум", "premium"):
        v = flag(args)
        run("UPDATE users SET premium=? WHERE id=?", (v, tid))
        return f"Премиум у {t['name']}: {'вкл' if v else 'выкл'}"

    if cmd in ("звезды", "звёзды", "stars"):
        if len(args) < 2:
            return "Формат: .звезды <id|номер> <кол-во>"
        try:
            n = int(args[1])
        except ValueError:
            return "Кол-во должно быть числом"
        run("UPDATE users SET stars=MAX(0, stars+?) WHERE id=?", (n, tid))
        new = one("SELECT stars FROM users WHERE id=?", (tid,))["stars"]
        return f"У {t['name']} теперь ★{new}"

    if cmd in ("админ", "admin"):
        v = flag(args)
        run("UPDATE users SET is_admin=? WHERE id=?", (v, tid))
        return f"Админ у {t['name']}: {'вкл' if v else 'выкл'}"

    return "Неизвестная команда. .помощь"


# ---------- Отправка ----------
async def handle_send(user, chat_id, text):
    text = (text or "").strip()
    if not text:
        raise HTTPException(400, "empty")
    if len(text) > 4000:
        raise HTTPException(400, "too long")
    check_chat(user, chat_id)

    if text.startswith(".") and user["is_admin"]:
        reply = await run_command(user, text)
        sys_msg = {"type": "system", "chat_id": chat_id, "text": reply, "ts": int(time.time())}
        await hub.send_to(user["id"], sys_msg)
        return sys_msg

    ts = int(time.time())
    mid = run("INSERT INTO messages(chat_id,sender_id,text,ts) VALUES(?,?,?,?)",
              (chat_id, user["id"], text, ts))
    msg = {"type": "message", "id": mid, "chat_id": chat_id, "text": text, "ts": ts,
           "sender": pub(user)}
    await hub.broadcast_chat(chat_id, msg)
    return msg


# ---------- HTTP ----------
class LoginIn(BaseModel):
    phone: str
    code: str
    name: str = ""


class SendIn(BaseModel):
    chat_id: str = "global"
    text: str


PAGE = r"""<!DOCTYPE html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MyMessenger</title>
<style>
:root{--bg:#0e1621;--panel:#17212b;--line:#0b121a;--txt:#e8eef5;--mut:#7f91a4;--acc:#2b95e6;--me:#2b5278}
*{box-sizing:border-box}[hidden]{display:none!important}
html,body{height:100%;margin:0;background:var(--bg);color:var(--txt);font-family:system-ui,sans-serif;font-size:16px}
#app{display:flex;flex-direction:column;height:100dvh;max-width:720px;margin:auto}
input,select,button{font:inherit;color:var(--txt);background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:11px}
button{background:var(--acc);border:0;cursor:pointer}button.g{background:var(--panel);border:1px solid #2a3947}
button.r{background:#b3402f}
#login{margin:auto;width:92%;max-width:360px;display:flex;flex-direction:column;gap:10px}
#login h2{text-align:center;margin:0 0 6px}#err{color:#ff7b6b;min-height:1.2em;text-align:center}
#main{display:flex;flex-direction:column;flex:1;min-height:0}
header{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;background:var(--panel)}
header span{color:var(--mut);font-size:14px}
nav{display:flex;background:var(--panel);border-bottom:1px solid var(--line)}
nav button{flex:1;background:none;border-radius:0;color:var(--mut);border-bottom:2px solid transparent}
nav button.on{color:var(--acc);border-bottom-color:var(--acc)}
.pane{flex:1;min-height:0;display:flex;flex-direction:column}
#chatTitle{padding:8px 14px;color:var(--mut);font-size:14px}
#msgs{flex:1;overflow-y:auto;padding:8px 12px;display:flex;flex-direction:column;gap:6px}
.m{background:var(--panel);padding:8px 11px;border-radius:12px;max-width:82%;align-self:flex-start;word-break:break-word}
.m.me{background:var(--me);align-self:flex-end}.n{font-size:12px;color:var(--acc);margin-bottom:2px}
.ts{font-size:11px;color:var(--mut);text-align:right;margin-top:2px}
.s{align-self:center;background:#243447;color:#cfe3f7;padding:8px 12px;border-radius:12px;white-space:pre-wrap;font-size:14px}
#bar{display:flex;gap:8px;padding:8px;background:var(--panel)}#inp{flex:1}
.list{overflow-y:auto;padding:8px}.u{padding:13px;border-bottom:1px solid var(--line)}
#admin{padding:12px;overflow-y:auto;gap:10px}
.row{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0}
#alog{white-space:pre-wrap;background:var(--panel);border-radius:10px;padding:10px;font-size:14px;min-height:80px;margin-top:8px}
#toast{position:fixed;top:10px;left:50%;transform:translateX(-50%);background:#243447;padding:10px 16px;border-radius:12px;max-width:90%}
</style></head><body><div id="app">
<div id="login" hidden>
<h2>MyMessenger</h2>
<input id="phone" placeholder="Номер (любой, например +000)" inputmode="tel">
<input id="name" placeholder="Имя (для нового аккаунта)">
<input id="code" placeholder="Код (1234)" inputmode="numeric">
<button id="goLogin">Войти</button><div id="err"></div></div>
<div id="main" hidden>
<header><b id="who"></b><button class="g" id="out">Выйти</button></header>
<nav><button data-t="chat" class="on">Чат</button><button data-t="people">Люди</button><button data-t="admin" id="adminTab" hidden>Админ</button></nav>
<div class="pane" id="p-chat"><div id="chatTitle"></div><div id="msgs"></div>
<div id="bar"><input id="inp" placeholder="Сообщение..." autocomplete="off"><button id="send">➤</button></div></div>
<div class="pane list" id="p-people" hidden><div id="ulist"></div></div>
<div class="pane" id="admin" hidden>
<select id="sel"></select>
<div class="row"><button class="r" data-c="бан">Бан</button><button class="g" data-c="разбан">Разбан</button><button class="g" data-c="кик">Кик</button></div>
<div class="row"><button data-c="премиум" data-x="вкл">Премиум вкл</button><button class="g" data-c="премиум" data-x="выкл">Премиум выкл</button></div>
<div class="row"><button data-c="админ" data-x="вкл">Админ вкл</button><button class="g" data-c="админ" data-x="выкл">Админ выкл</button></div>
<div class="row"><input id="stars" type="number" value="100" style="width:110px"><button id="giveStars">Выдать ★</button></div>
<div class="row"><input id="raw" placeholder=".команда" style="flex:1"><button id="runRaw">OK</button></div>
<div class="row"><button class="g" id="hUsers">.юзеры</button><button class="g" id="hHelp">.помощь</button></div>
<div id="alog"></div></div>
</div></div><div id="toast" hidden></div>
<script>
const $=s=>document.querySelector(s);
let token=localStorage.getItem('token'),me=null,chat='global',ws=null,users=[],kicked=false,opened=false,seen=new Set();
async function api(path,body){
  const o={headers:{'Content-Type':'application/json'}};
  if(token)o.headers.Authorization='Bearer '+token;
  if(body){o.method='POST';o.body=JSON.stringify(body)}
  const r=await fetch(path,o);let d=null;try{d=await r.json()}catch(e){}
  if(!r.ok)throw new Error(d&&typeof d.detail==='string'?d.detail:'HTTP '+r.status);
  return d;
}
function toast(t){const e=$('#toast');e.textContent=t;e.hidden=false;clearTimeout(toast.t);toast.t=setTimeout(()=>e.hidden=true,3500)}
async function doLogin(){
  $('#err').textContent='';
  try{
    token=null;
    const d=await api('/auth/login',{phone:$('#phone').value.trim(),code:$('#code').value.trim(),name:$('#name').value.trim()});
    token=d.token;localStorage.setItem('token',token);await boot();
  }catch(e){$('#err').textContent=e.message}
}
function logout(){
  token=null;me=null;kicked=false;opened=false;localStorage.removeItem('token');
  if(ws){try{ws.close()}catch(e){}ws=null}
  $('#main').hidden=true;$('#login').hidden=false;
}
function showMe(){$('#who').textContent=me.name+(me.is_admin?' 👑':'')+(me.premium?' ⭐':'')+'  ★'+me.stars}
async function boot(){
  try{me=await api('/me')}catch(e){logout();return}
  $('#login').hidden=true;$('#main').hidden=false;
  $('#adminTab').hidden=!me.is_admin;showMe();
  await refreshUsers();await openChat('global','Общий чат');connect();
}
function tab(n){
  document.querySelectorAll('nav button').forEach(b=>b.classList.toggle('on',b.dataset.t===n));
  $('#p-chat').hidden=n!=='chat';$('#p-people').hidden=n!=='people';$('#admin').hidden=n!=='admin';
  if(n==='people'||n==='admin')refreshUsers().catch(()=>{});
}
function addMsg(m){
  if(seen.has(m.id))return;seen.add(m.id);
  const b=$('#msgs'),d=document.createElement('div');d.className='m'+(m.sender.id===me.id?' me':'');
  const n=document.createElement('div');n.className='n';
  n.textContent=m.sender.name+(m.sender.is_admin?' 👑':'')+(m.sender.premium?' ⭐':'');
  const t=document.createElement('div');t.textContent=m.text;
  const ts=document.createElement('div');ts.className='ts';
  ts.textContent=new Date(m.ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  d.append(n,t,ts);b.append(d);b.scrollTop=b.scrollHeight;
}
function addSys(txt){const b=$('#msgs'),d=document.createElement('div');d.className='s';d.textContent=txt;b.append(d);b.scrollTop=b.scrollHeight}
async function loadMsgs(){
  const box=$('#msgs');box.textContent='';seen=new Set();
  try{(await api('/messages/'+chat+'?limit=100')).forEach(addMsg)}catch(e){addSys(e.message)}
}
async function openChat(id,title){chat=id;$('#chatTitle').textContent=title;await loadMsgs();tab('chat')}
async function send(){
  const i=$('#inp'),text=i.value.trim();if(!text)return;i.value='';
  try{const r=await api('/send',{chat_id:chat,text});if(r.type==='system')addSys(r.text);else addMsg(r)}
  catch(e){addSys(e.message)}
}
async function refreshUsers(){
  users=await api('/users');
  const l=$('#ulist');l.textContent='';
  const g=document.createElement('div');g.className='u';g.textContent='💬 Общий чат';g.onclick=()=>openChat('global','Общий чат');l.append(g);
  users.filter(u=>u.id!==me.id).forEach(u=>{
    const d=document.createElement('div');d.className='u';
    d.textContent=u.name+'  '+u.phone+(u.is_admin?' 👑':'')+(u.premium?' ⭐':'')+(u.banned?' 🚫':'')+'  ★'+u.stars;
    d.onclick=()=>{const a=Math.min(me.id,u.id),b=Math.max(me.id,u.id);openChat('dm:'+a+':'+b,u.name)};
    l.append(d);
  });
  const s=$('#sel'),old=s.value;s.textContent='';
  users.forEach(u=>{const o=document.createElement('option');o.value=u.id;o.textContent='#'+u.id+' '+u.name+' '+u.phone;s.append(o)});
  if(old)s.value=old;
}
async function runAdmin(text){
  try{
    const r=await api('/send',{chat_id:'global',text});
    $('#alog').textContent=text+'\n'+r.text+'\n\n'+$('#alog').textContent;
    me=await api('/me');showMe();await refreshUsers();
  }catch(e){$('#alog').textContent='Ошибка: '+e.message+'\n\n'+$('#alog').textContent}
}
function cmd(c,x){const id=$('#sel').value;if(!id)return;runAdmin('.'+c+' '+id+(x?' '+x:''))}
function connect(){
  if(ws){try{ws.close()}catch(e){}}
  const p=location.protocol==='https:'?'wss:':'ws:';
  ws=new WebSocket(p+'//'+location.host+'/ws?token='+token);
  ws.onopen=()=>{if(opened)loadMsgs();opened=true};
  ws.onmessage=e=>{
    const m=JSON.parse(e.data);
    if(m.type==='message'){if(m.chat_id===chat)addMsg(m);else toast(m.sender.name+': '+m.text)}
    else if(m.type==='kicked'){kicked=true;toast('Вас отключили: '+m.reason);if(m.reason==='banned')logout()}
    else if(m.type==='error')addSys(m.detail);
  };
  ws.onclose=()=>{if(token&&!kicked)setTimeout(connect,3000)};
}
$('#goLogin').onclick=doLogin;
['phone','name','code'].forEach(i=>$('#'+i).addEventListener('keydown',e=>{if(e.key==='Enter')doLogin()}));
$('#out').onclick=logout;$('#send').onclick=send;
$('#inp').addEventListener('keydown',e=>{if(e.key==='Enter')send()});
document.querySelectorAll('nav button').forEach(b=>b.onclick=()=>tab(b.dataset.t));
document.querySelectorAll('#admin [data-c]').forEach(b=>b.onclick=()=>cmd(b.dataset.c,b.dataset.x));
$('#giveStars').onclick=()=>cmd('звезды',$('#stars').value);
$('#runRaw').onclick=()=>{const t=$('#raw').value.trim();if(t)runAdmin(t)};
$('#hUsers').onclick=()=>runAdmin('.юзеры');$('#hHelp').onclick=()=>runAdmin('.помощь');
if(token)boot();else $('#login').hidden=false;
</script></body></html>
"""


@app.get("/health")
def health():
    return {"ok": True, "service": "MyMessenger"}


@app.get("/", response_class=HTMLResponse)
def root():
    return PAGE


@app.post("/auth/login")
def login(data: LoginIn):
    phone = data.phone.strip()
    if not phone or len(phone) > 20:
        raise HTTPException(400, "bad phone")
    if data.code != LOGIN_CODE:
        raise HTTPException(403, "wrong code")
    u = one("SELECT * FROM users WHERE phone=?", (phone,))
    if u and u["banned"]:
        raise HTTPException(403, "banned")
    if not u:
        run("INSERT INTO users(phone,name,token,is_admin,created) VALUES(?,?,?,?,?)",
            (phone, (data.name or phone)[:40], secrets.token_hex(24),
             1 if phone == ADMIN_PHONE else 0, int(time.time())))
        u = one("SELECT * FROM users WHERE phone=?", (phone,))
    elif phone == ADMIN_PHONE and not u["is_admin"]:
        run("UPDATE users SET is_admin=1 WHERE id=?", (u["id"],))
        u = one("SELECT * FROM users WHERE id=?", (u["id"],))
    return {"token": u["token"], "user": pub(u)}


@app.get("/me")
def me(authorization: str = Header(None)):
    return pub(user_by_token(bearer(authorization)))


@app.get("/users")
def users(authorization: str = Header(None)):
    user_by_token(bearer(authorization))
    return [pub(u) for u in many("SELECT * FROM users ORDER BY id")]


@app.get("/messages/{chat_id}")
def messages(chat_id: str, limit: int = 50, authorization: str = Header(None)):
    u = user_by_token(bearer(authorization))
    check_chat(u, chat_id)
    limit = max(1, min(limit, 200))
    rows = many("""SELECT m.id, m.chat_id, m.text, m.ts, u.id AS uid, u.name, u.premium, u.is_admin
                   FROM messages m JOIN users u ON u.id=m.sender_id
                   WHERE m.chat_id=? ORDER BY m.id DESC LIMIT ?""", (chat_id, limit))
    return [{"id": r["id"], "chat_id": r["chat_id"], "text": r["text"], "ts": r["ts"],
             "sender": {"id": r["uid"], "name": r["name"],
                        "premium": bool(r["premium"]), "is_admin": bool(r["is_admin"])}}
            for r in reversed(rows)]


@app.post("/send")
async def send(data: SendIn, authorization: str = Header(None)):
    u = user_by_token(bearer(authorization))
    return await handle_send(u, data.chat_id, data.text)


# ---------- WebSocket ----------
@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket, token: str = ""):
    u = one("SELECT * FROM users WHERE token=?", (token,))
    if not u or u["banned"]:
        await ws.close(code=4401)
        return
    await ws.accept()
    uid = u["id"]
    await hub.add(uid, ws)
    try:
        while True:
            data = await ws.receive_json()
            u = one("SELECT * FROM users WHERE id=?", (uid,))
            if not u or u["banned"]:
                await ws.close(code=4403)
                break
            if not isinstance(data, dict):
                await ws.send_json({"type": "error", "detail": "bad payload"})
                continue
            try:
                await handle_send(u, str(data.get("chat_id", "global")), str(data.get("text", "")))
            except HTTPException as e:
                await ws.send_json({"type": "error", "detail": e.detail})
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        hub.remove(uid, ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
