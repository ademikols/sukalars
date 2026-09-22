import os
import json
import hmac
import hashlib
from datetime import datetime
from typing import Dict, Set, Optional
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart
from aiogram.enums import ParseMode
from dotenv import load_dotenv
import uvicorn

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "8000"))
DOMAIN = os.getenv("DOMAIN", "http://localhost:8000")

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен")

bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()

# Хранилище комнат в памяти
# rooms = {room_code: {"users": {user_id: {"name": str, "ws": WebSocket}}, "video": {...}}}
rooms: Dict[str, Dict] = {}
room_connections: Dict[str, Set[WebSocket]] = {}


def verify_telegram_init_data(init_data: str) -> Optional[dict]:
    """Проверяет подпись initData от Telegram"""
    try:
        params = {}
        for pair in init_data.split("&"):
            key, value = pair.split("=", 1)
            params[key] = value
        
        received_hash = params.pop("hash", "")
        
        # Создаём данные для проверки
        data_check_string = "\n".join(
            f"{k}={v}" for k, v in sorted(params.items())
        )
        
        # Вычисляем подпись
        secret_key = hmac.new(
            b"WebAppData",
            BOT_TOKEN.encode(),
            hashlib.sha256
        ).digest()
        
        computed_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        if computed_hash == received_hash:
            # Парсим user из params
            if "user" in params:
                user_data = json.loads(params["user"])
                return user_data
            return {"id": 0}
        return None
    except Exception as e:
        print(f"Ошибка при проверке initData: {e}")
        return None


def generate_room_code() -> str:
    """Генерирует код комнаты"""
    import random
    import string
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


@dp.message(CommandStart())
async def start_handler(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    user_name = message.from_user.full_name or "Аноним"
    
    # Создаём кнопку WebApp
    webapp_url = f"{DOMAIN}?user_id={user_id}&username={user_name}"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text="📹 Открыть просмотр",
                web_app=WebAppInfo(url=webapp_url)
            )]
        ]
    )
    
    # Если админ, добавляем кнопку админки
    if user_id == ADMIN_ID:
        keyboard.inline_keyboard.append([
            InlineKeyboardButton(
                text="⚙️ Админка",
                web_app=WebAppInfo(url=f"{DOMAIN}?admin=1&user_id={user_id}")
            )
        ])
    
    await message.answer(
        "👋 Добро пожаловать! Нажми кнопку ниже, чтобы начать совместный просмотр видео.",
        reply_markup=keyboard
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_webhook(f"{DOMAIN}/webhook")
    
    # Запускаем polling в фоне
    dispatcher_task = asyncio.create_task(dp.feed_update(None, {}))
    
    yield
    
    # Shutdown
    await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def get_index():
    """Отдаёт главную страницу"""
    return FileResponse("index.html", media_type="text/html")


@app.post("/webhook")
async def webhook(request):
    """Вебхук от Telegram"""
    try:
        update_data = await request.json()
        await dp.feed_update(bot, update_data)
    except Exception as e:
        print(f"Ошибка в вебхуке: {e}")
    return {"ok": True}


@app.get("/create-room")
async def create_room(user_id: int, username: str):
    """Создаёт новую комнату"""
    room_code = generate_room_code()
    
    rooms[room_code] = {
        "created_at": datetime.now().isoformat(),
        "creator_id": user_id,
        "creator_name": username,
        "users": {},
        "current_video": None,
        "playback": {"playing": False, "time": 0}
    }
    room_connections[room_code] = set()
    
    return {"room_code": room_code, "ws_url": f"ws://{DOMAIN.split('://')[-1]}/ws/{room_code}"}


@app.get("/room-info/{room_code}")
async def get_room_info(room_code: str):
    """Получает информацию о комнате"""
    if room_code not in rooms:
        raise HTTPException(status_code=404, detail="Комната не найдена")
    
    room = rooms[room_code]
    return {
        "users_count": len(room["users"]),
        "current_video": room.get("current_video"),
        "playback": room.get("playback"),
        "users": [{"id": uid, "name": u.get("name")} for uid, u in room["users"].items()]
    }


@app.websocket("/ws/{room_code}")
async def websocket_endpoint(websocket: WebSocket, room_code: str):
    """WebSocket для синхронизации видео"""
    init_data = websocket.query_params.get("init_data")
    
    if not init_data:
        await websocket.close(code=1008, reason="init_data required")
        return
    
    user_data = verify_telegram_init_data(init_data)
    if not user_data:
        await websocket.close(code=1008, reason="Invalid signature")
        return
    
    user_id = user_data.get("id")
    user_name = user_data.get("first_name", "Аноним")
    
    if room_code not in rooms:
        await websocket.close(code=1008, reason="Room not found")
        return
    
    await websocket.accept()
    room_connections[room_code].add(websocket)
    
    # Добавляем пользователя в комнату
    rooms[room_code]["users"][user_id] = {
        "name": user_name,
        "joined_at": datetime.now().isoformat()
    }
    
    # Отправляем текущее состояние
    await websocket.send_json({
        "type": "room_state",
        "users": [
            {"id": uid, "name": u.get("name")} 
            for uid, u in rooms[room_code]["users"].items()
        ],
        "current_video": rooms[room_code].get("current_video"),
        "playback": rooms[room_code].get("playback")
    })
    
    # Уведомляем остальных о присоединении
    await broadcast_to_room(room_code, {
        "type": "user_joined",
        "user_id": user_id,
        "user_name": user_name,
        "users_count": len(rooms[room_code]["users"])
    }, exclude_ws=websocket)
    
    try:
        while True:
            data = await websocket.receive_json()
            
            if data.get("type") == "video_change":
                rooms[room_code]["current_video"] = {
                    "url": data.get("url"),
                    "type": data.get("video_type"),  # youtube или vk
                    "changed_by": user_id
                }
                rooms[room_code]["playback"] = {"playing": False, "time": 0}
                
                await broadcast_to_room(room_code, {
                    "type": "video_changed",
                    "url": data.get("url"),
                    "video_type": data.get("video_type"),
                    "changed_by": user_name
                })
            
            elif data.get("type") == "play":
                rooms[room_code]["playback"]["playing"] = True
                rooms[room_code]["playback"]["time"] = data.get("time", 0)
                
                await broadcast_to_room(room_code, {
                    "type": "play",
                    "time": data.get("time", 0),
                    "user": user_name
                }, exclude_ws=websocket)
            
            elif data.get("type") == "pause":
                rooms[room_code]["playback"]["playing"] = False
                rooms[room_code]["playback"]["time"] = data.get("time", 0)
                
                await broadcast_to_room(room_code, {
                    "type": "pause",
                    "time": data.get("time", 0),
                    "user": user_name
                }, exclude_ws=websocket)
            
            elif data.get("type") == "seek":
                rooms[room_code]["playback"]["time"] = data.get("time", 0)
                
                await broadcast_to_room(room_code, {
                    "type": "seek",
                    "time": data.get("time", 0),
                    "user": user_name
                }, exclude_ws=websocket)
    
    except WebSocketDisconnect:
        pass
    finally:
        room_connections[room_code].discard(websocket)
        del rooms[room_code]["users"][user_id]
        
        if not rooms[room_code]["users"]:
            del rooms[room_code]
            del room_connections[room_code]
        else:
            await broadcast_to_room(room_code, {
                "type": "user_left",
                "user_id": user_id,
                "user_name": user_name,
                "users_count": len(rooms[room_code]["users"])
            })


@app.get("/admin/rooms")
async def admin_get_rooms(user_id: int):
    """Админ-панель: список комнат"""
    if user_id != ADMIN_ID:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return {
        "rooms": [
            {
                "code": code,
                "creator": room["creator_name"],
                "users_count": len(room["users"]),
                "created_at": room["created_at"],
                "current_video": room.get("current_video")
            }
            for code, room in rooms.items()
        ]
    }


@app.post("/admin/close-room/{room_code}")
async def admin_close_room(room_code: str, user_id: int):
    """Админ-панель: закрыть комнату"""
    if user_id != ADMIN_ID:
        raise HTTPException(status_code=403, detail="Access denied")
    
    if room_code not in rooms:
        raise HTTPException(status_code=404, detail="Room not found")
    
    # Закрываем все подключения
    for ws in room_connections[room_code]:
        await ws.close(code=1000, reason="Room closed by admin")
    
    del rooms[room_code]
    del room_connections[room_code]
    
    return {"status": "closed"}


async def broadcast_to_room(room_code: str, message: dict, exclude_ws: Optional[WebSocket] = None):
    """Отправляет сообщение всем в комнате"""
    if room_code not in room_connections:
        return
    
    disconnected = set()
    for ws in room_connections[room_code]:
        if exclude_ws and ws == exclude_ws:
            continue
        try:
            await ws.send_json(message)
        except Exception:
            disconnected.add(ws)
    
    # Удаляем отключённые WebSocket'ы
    for ws in disconnected:
        room_connections[room_code].discard(ws)


async def run_bot():
    """Запускает бота"""
    await dp.start_polling(bot)


async def run_server():
    """Запускает FastAPI сервер"""
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info"
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Главная функция"""
    await asyncio.gather(
        run_bot(),
        run_server()
    )


if __name__ == "__main__":
    asyncio.run(main())
