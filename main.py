"""
Мессенджер-сервер на Python (FastAPI + SQLite + WebSocket).

Установка:   pip install fastapi uvicorn
Запуск:      uvicorn server:app --host 0.0.0.0 --port 8000
Render:      Start command = uvicorn server:app --host 0.0.0.0 --port $PORT

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
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "server.db")
ADMIN_PHONE = os.environ.get("ADMIN_PHONE", "+000")
LOGIN_CODE = os.environ.get("LOGIN_CODE", "1234")

app = FastAPI(title="MyMessenger server")


# ---------- База ----------
def _db():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
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


@app.get("/")
def root():
    return {"ok": True, "service": "MyMessenger"}


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
                break
            try:
                await handle_send(u, data.get("chat_id", "global"), data.get("text", ""))
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
