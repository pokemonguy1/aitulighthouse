# -*- coding: utf-8 -*-
import time
import asyncio
import json
import logging
from datetime import datetime, timedelta, time as dt_time # Alias time from datetime
import pytz
import uuid # For generating unique IDs for custom lessons
import re # For input validation (time, room)

from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery # For keyboards
from aiogram.filters import StateFilter # To handle /cancel command during FSM


# --- Configuration ---

BOT_TOKEN = "x" # Replace with your actual Bot Token
ADMIN_ID = x # Replace with your Admin User ID

# Constants
TIMETABLE_FILE = 'small_timetable.json'
ROOM_LINKS_FILE = 'testAITULight_room_file_ids.json'
USER_DATA_FILE = 'test_user_data.json'
# DEFAULT notification offset used for new users or if not set
DEFAULT_NOTIFICATION_OFFSET_MINUTES = 10
MIN_OFFSET_MINUTES = 1
MAX_OFFSET_MINUTES = 120
CHECK_INTERVAL_SECONDS = 60 # Check every 60 seconds
TIMEZONE = pytz.timezone('Asia/Almaty')
RATE_LIMIT_DELAY = 0.05 # Delay between messages in loops
BROADCAST_RATE_LIMIT_DELAY = 0.1 # Delay for broadcast
LEARN_NOTIFICATION_DELAY = 0.05 # Delay between sending learn notifications

MAX_CUSTOM_LESSONS = 12 # Maximum custom lessons per user
DAYS_OF_WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
TIME_FORMAT_REGEX = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$") # HH:MM format

# Static Learn Notification Text
LEARN_NOTIFICATION_TEXT = "Do not forget to complete quizzes on https://learn.astanait.edu.kz/ ! :)"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Data ---
def load_json_data(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: {filename} not found.")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from {filename}.")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error loading {filename}: {e}", exc_info=True)
        return {}

timetable_data = load_json_data(TIMETABLE_FILE)
room_links_data = load_json_data(ROOM_LINKS_FILE)

if not timetable_data:
    logging.warning(f"Timetable data ({TIMETABLE_FILE}) not found or invalid. Official schedule features may not work.")
if not room_links_data:
    logging.warning(f"Room links data ({ROOM_LINKS_FILE}) not found or invalid. Maps may not be available.")

# --- User Data Persistence ---
# load_user_data and save_user_data remain unchanged
def load_user_data():
    """Loads user data, adding defaults including custom_lessons if necessary."""
    try:
        with open(USER_DATA_FILE, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        processed_data = {}
        for k, v in raw_data.items():
            try:
                user_id = int(k)
                default_user_struct = {
                    "group": None,
                    "learn_notify": False,
                    "notification_offset": DEFAULT_NOTIFICATION_OFFSET_MINUTES,
                    "custom_lessons": []
                }
                if isinstance(v, str):
                    processed_data[user_id] = default_user_struct.copy()
                    processed_data[user_id]["group"] = v
                    logging.info(f"Converted user {user_id} data from old string format.")
                elif isinstance(v, dict):
                    processed_data[user_id] = default_user_struct.copy()
                    processed_data[user_id].update({
                        "group": v.get("group"),
                        "learn_notify": v.get("learn_notify", False),
                        "notification_offset": v.get("notification_offset", DEFAULT_NOTIFICATION_OFFSET_MINUTES),
                        "custom_lessons": v.get("custom_lessons", [])
                    })
                    if not isinstance(processed_data[user_id]["custom_lessons"], list):
                         logging.warning(f"Corrected non-list custom_lessons for user {user_id}.")
                         processed_data[user_id]["custom_lessons"] = []
                    if len(processed_data[user_id]["custom_lessons"]) > MAX_CUSTOM_LESSONS:
                        logging.warning(f"User {user_id} had > {MAX_CUSTOM_LESSONS} custom lessons. Truncating.")
                        processed_data[user_id]["custom_lessons"] = processed_data[user_id]["custom_lessons"][:MAX_CUSTOM_LESSONS]
                else:
                    logging.warning(f"Skipping invalid data type for user {user_id}: {type(v)}")
            except ValueError:
                logging.error(f"Skipping invalid user ID key: {k}")
        return processed_data
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning(f"{USER_DATA_FILE} not found or invalid. Starting with empty user data.")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error loading user data: {e}", exc_info=True)
        return {}

def save_user_data(data):
    """Saves user data to the JSON file."""
    try:
        data_to_save = {str(k): v for k, v in data.items()}
        with open(USER_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4)
    except IOError as e:
        logging.error(f"Error saving user data to {USER_DATA_FILE}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error saving user data: {e}", exc_info=True)


# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- State Definitions ---
class Registration(StatesGroup):
    waiting_for_group = State()

class Broadcasting(StatesGroup):
    waiting_for_message = State()

class NotificationSettings(StatesGroup):
    waiting_for_minutes = State()

class AddCustomLesson(StatesGroup):
    waiting_for_day = State()
    waiting_for_subject = State()
    waiting_for_start_time = State()
    waiting_for_end_time = State()
    waiting_for_room = State()


# --- Global Data ---
user_groups: dict[int, dict] = load_user_data()
notified_lessons = set()
timetable_usage = {}
find_usage = {}
last_learn_notify_sent_key = None
# -----------------------------------

# --- Helper Functions ---
def get_current_day_of_week():
    now = datetime.now(TIMEZONE)
    return now.strftime('%A')

def clean_room_number(raw_room):
    """Cleans room number for map lookup. Returns cleaned string or None."""
    if not raw_room or not isinstance(raw_room, str):
        return None
    room = raw_room.strip().upper()
    if room == "ONLINE":
        return None
    room = room.split('(')[0].strip()
    room = room.split('\n')[0].strip()
    if len(room) > 1 and room[-1].isalpha():
        if not room[-2].isalpha():
             # logging.debug(f"Stripping trailing letter '{room[-1]}' from room '{room}'") # Optional debug
             room = room[:-1]
    room = room.strip()
    return room if room else None

def is_valid_time_format(time_str):
    """Checks if a string is in HH:MM format."""
    return bool(TIME_FORMAT_REGEX.match(time_str))

# --- Generic Cancel Handler for FSM ---
@dp.message(Command("cancel"), StateFilter("*"))
async def cancel_handler(message: types.Message, state: FSMContext):
    """Allows users to cancel the current operation state."""
    current_state = await state.get_state()
    if current_state is None:
        await message.reply("Nothing to cancel.")
        return
    logging.info(f"User {message.from_user.id} cancelled state {current_state}")
    await state.clear()
    await message.reply("✅ Action cancelled.")

# --- Command Handlers ---
# These handlers are now registered SOLELY via decorators
# Order matters less here as filters are specific (Command vs StateFilter(None))

@dp.message(CommandStart())
async def send_welcome(message: types.Message, state: FSMContext):
    logging.info(f"User {message.from_user.id} started the bot.")
    await state.clear()
    await message.reply("Welcome! 👋 Please enter your group number (e.g., EE-2401 or IoT-2401) to get started, or use /help.")
    await state.set_state(Registration.waiting_for_group)

@dp.message(Registration.waiting_for_group, F.text)
async def process_group_number(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    matched_group_key = None
    if timetable_data:
        for key in timetable_data.keys():
            if key.upper() == user_input.upper():
                matched_group_key = key
                break
    user_id = message.from_user.id
    if matched_group_key:
        existing_data = user_groups.get(user_id, {})
        user_groups[user_id] = {
            "group": matched_group_key,
            "learn_notify": existing_data.get("learn_notify", False),
            "notification_offset": existing_data.get("notification_offset", DEFAULT_NOTIFICATION_OFFSET_MINUTES),
            "custom_lessons": existing_data.get("custom_lessons", [])
        }
        save_user_data(user_groups)
        current_offset = user_groups[user_id]["notification_offset"]
        await message.reply(f"✅ Great! Your group '{matched_group_key}' is registered. "
                          f"I will notify you <b>{current_offset} minutes</b> before your lessons.\n"
                          f"Use /timetable for today's official schedule.\n"
                          f"Use /add_lesson to add your own reminders.\n"
                          f"See all commands with /help.")
        logging.info(f"User {user_id} registered/updated group: {matched_group_key}, offset: {current_offset}")
        await state.clear()
    else:
        existing_data = user_groups.get(user_id, {})
        if not existing_data:
             user_groups[user_id] = {
                "group": None, "learn_notify": False,
                "notification_offset": DEFAULT_NOTIFICATION_OFFSET_MINUTES,
                "custom_lessons": []
             }
             save_user_data(user_groups)
             await message.reply(f"⚠️ Couldn't find group '{user_input}' in the official timetable. "
                                f"I've registered you, but official schedule features won't work.\n"
                                f"You can still use /add_lesson for custom reminders.\n"
                                f"If '{user_input}' was a typo, use /start again.\n"
                                f"See commands with /help.")
             logging.warning(f"User {message.from_user.id} entered group '{user_input}' (not found). Registered without official group.")
             await state.clear()
        else:
             await message.reply("❌ Sorry, I couldn't find that group number in the timetable. "
                                 "Please check the format (e.g., EE-2401) and try again, or use /cancel.")
             logging.warning(f"User {message.from_user.id} entered group '{user_input}', which was not found. User already existed.")

@dp.message(Command("timetable"))
async def show_daily_timetable(message: types.Message):
    user_id = message.from_user.id
    current_time = time.time()
    cooldown_period = 30
    last_used = timetable_usage.get(user_id, 0)
    if current_time - last_used < cooldown_period:
        time_remaining = int(cooldown_period - (current_time - last_used))
        await message.reply(f"⏳ Please wait {time_remaining}s before using /timetable again.")
        logging.warning(f"User {user_id} triggered /timetable cooldown ({time_remaining:.1f}s remaining).")
        return
    user_data = user_groups.get(user_id)
    if not user_data:
        await message.reply("I don't know you yet. Please use /start to register.")
        return
    if not user_data.get("group"):
        await message.reply("Your group isn't set or wasn't found in the official schedule. Use /start to set it, or /view_lessons for your custom schedule.")
        return
    if not timetable_data:
         await message.reply("The official timetable data is currently unavailable. Please try again later.")
         return
    group_number = user_data["group"]
    current_day = get_current_day_of_week()
    logging.info(f"User {user_id} ({group_number}) requested timetable for {current_day}.")
    group_schedule = timetable_data.get(group_number, {})
    day_schedule = group_schedule.get(current_day)
    timetable_usage[user_id] = current_time
    if not day_schedule:
        await message.reply(f"🎉 No official lessons scheduled for your group ({group_number}) today ({current_day})! Check /view_lessons for custom ones.")
        logging.info(f"No official lessons found for {user_id} ({group_number}) on {current_day}.")
        return
    await message.reply(f"📅 <b>Official Timetable for {current_day} ({group_number}):</b>")
    sorted_lesson_keys = sorted(day_schedule.keys(), key=int)
    sent_lesson = False
    for lesson_key in sorted_lesson_keys:
        try:
            lesson_details = day_schedule[lesson_key]
            time_range = lesson_details.get("time", "N/A")
            subject = lesson_details.get("subject", "N/A")
            room_raw = lesson_details.get("room", "N/A")
            lesson_type = lesson_details.get("type", "N/A").capitalize()
            lecturer = lesson_details.get("lecturer", "N/A")
            lesson_info_text = (
                f"<b>{lesson_key}. {subject}</b> ({lesson_type})\n"
                f"🕒 Time: {time_range}\n"
                f"👨‍🏫 Lecturer: {lecturer}\n"
                f"🚪 Room: {room_raw if isinstance(room_raw, str) else 'N/A'}"
            )
            is_online = isinstance(room_raw, str) and room_raw.strip().upper() == "ONLINE"
            room_cleaned = None
            if not is_online:
                room_cleaned = clean_room_number(room_raw)
            if is_online:
                await bot.send_message(chat_id=user_id, text=lesson_info_text)
            elif room_cleaned and room_links_data:
                photo_file_id = room_links_data.get(room_cleaned)
                if photo_file_id:
                    caption_text = f"{lesson_info_text}\n\n📍 Location Map ({room_cleaned})"
                    try:
                        await bot.send_photo(chat_id=user_id, photo=photo_file_id, caption=caption_text)
                    except TelegramAPIError as e_photo:
                        logging.error(f"Timetable: Failed to send photo {photo_file_id} for room {room_cleaned} (raw: {room_raw}) to {user_id}: {e_photo}")
                        fallback_text = f"{lesson_info_text}\n\n⚠️ Couldn't send map photo ({e_photo})."
                        await bot.send_message(user_id, fallback_text)
                        if "blocked" in str(e_photo).lower() or "deactivated" in str(e_photo).lower(): raise e_photo
                else:
                    text_with_note = f"{lesson_info_text}\n\nℹ️ Map photo for room '{room_cleaned}' is not available."
                    await bot.send_message(chat_id=user_id, text=text_with_note)
            else:
                 text_with_note = f"{lesson_info_text}\n\nℹ️ Room location unknown or map data missing."
                 await bot.send_message(chat_id=user_id, text=text_with_note)
            sent_lesson = True
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except TelegramAPIError as e:
            logging.error(f"Telegram API Error processing lesson {lesson_key} for {user_id}: {e}")
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "user is deactivated" in str(e).lower() or "chat not found" in str(e).lower():
                logging.warning(f"User {user_id} blocked/deactivated during timetable request. Removing.")
                if user_groups.pop(user_id, None) is not None:
                    global notified_lessons
                    notified_lessons = {n for n in notified_lessons if n[0] != user_id}
                    save_user_data(user_groups)
                timetable_usage.pop(user_id, None)
                find_usage.pop(user_id, None)
                break
            else:
                 logging.warning(f"Continuing timetable processing for {user_id} despite non-blocking API error: {e}")
                 await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"Unexpected error processing lesson {lesson_key} for {user_id}: {e}", exc_info=True)
            try: await bot.send_message(user_id, "An error occurred while fetching part of the timetable.")
            except Exception: pass
            break
    if not sent_lesson and not day_schedule:
         await message.reply(f"🎉 No official lessons scheduled for your group ({group_number}) today ({current_day})! Check /view_lessons for custom ones.")

@dp.message(Command("find"))
async def handle_find_room(message: types.Message):
    user_id = message.from_user.id
    current_time = time.time()
    cooldown_period = 10
    last_used = find_usage.get(user_id, 0)
    if current_time - last_used < cooldown_period:
        time_remaining = int(cooldown_period - (current_time - last_used))
        await message.reply(f"⏳ Please wait {time_remaining}s before using /find again.")
        logging.warning(f"User {user_id} triggered /find cooldown ({time_remaining:.1f}s remaining).")
        return
    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) < 2 or not command_parts[1].strip():
        await message.reply("❓ Please specify the room number after the command.\nExample: <code>/find C1.3.122</code> or <code>/find C1.1.256P</code>")
        return
    room_query = command_parts[1].strip()
    logging.info(f"User {user_id} requested to find room: '{room_query}'")
    room_cleaned = clean_room_number(room_query)
    if not room_cleaned:
        await message.reply(f"❌ Sorry, '{room_query}' doesn't look like a valid physical room number I can search for (after cleaning).")
        logging.warning(f"Invalid room format provided by {user_id} for /find: '{room_query}' -> cleaned to None")
        return
    if not room_links_data:
         await message.reply("Room map data is currently unavailable.")
         return
    find_usage[user_id] = current_time
    photo_file_id = room_links_data.get(room_cleaned)
    if photo_file_id:
        logging.info(f"Found map for room '{room_cleaned}' (cleaned from '{room_query}') for user {user_id}. Sending photo.")
        try:
            await bot.send_photo(chat_id=user_id, photo=photo_file_id, caption=f"📍 Location map for room {room_cleaned}")
        except TelegramAPIError as e:
            logging.error(f"Failed to send photo {photo_file_id} for room '{room_cleaned}' to {user_id} via /find: {e}")
            if "FILE_ID_INVALID" in str(e).upper() or "invalid file identifier" in str(e): await message.reply(f"ℹ️ The map data for room '{room_cleaned}' seems to be invalid or corrupted.")
            elif "blocked" in str(e).lower() or "deactivated" in str(e).lower():
                 logging.warning(f"User {user_id} blocked bot during /find request. Removing from active usage tracking.")
                 timetable_usage.pop(user_id, None)
                 find_usage.pop(user_id, None)
            else: await message.reply(f"❌ An error occurred while sending the map for '{room_cleaned}'.")
        except Exception as e:
            logging.error(f"Unexpected error sending photo for room '{room_cleaned}' to {user_id} via /find: {e}", exc_info=True)
            await message.reply("An unexpected error occurred while sending the map.")
    else:
        logging.warning(f"Map not found for cleaned room '{room_cleaned}' (query: '{room_query}') requested by {user_id}.")
        await message.reply(f"❌ Sorry, I couldn't find a map for room '{room_cleaned}'. Check the room number or it might not be in my database.")

@dp.message(Command("minutes"))
async def handle_minutes_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_groups:
        await message.reply("I need to know you first. Please use /start to register.")
        return
    current_offset = user_groups[user_id].get("notification_offset", DEFAULT_NOTIFICATION_OFFSET_MINUTES)
    await message.reply(
        f"Your current notification offset is <b>{current_offset} minutes</b> before the lesson.\n\n"
        f"Please enter the new number of minutes you want (from {MIN_OFFSET_MINUTES} to {MAX_OFFSET_MINUTES}), or /cancel:"
    )
    await state.set_state(NotificationSettings.waiting_for_minutes)
    logging.info(f"User {user_id} initiated /minutes command. Current offset: {current_offset}")

@dp.message(NotificationSettings.waiting_for_minutes, F.text)
async def process_minutes_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        minutes_input = int(message.text.strip())
        if MIN_OFFSET_MINUTES <= minutes_input <= MAX_OFFSET_MINUTES:
            if user_id in user_groups:
                user_groups[user_id]["notification_offset"] = minutes_input
                save_user_data(user_groups)
                await message.reply(f"✅ Okay! Your notification offset has been updated to <b>{minutes_input} minutes</b> before each lesson.")
                logging.info(f"User {user_id} successfully set notification offset to {minutes_input} minutes.")
                await state.clear()
            else:
                await message.reply("Something went wrong, couldn't find your user data. Please try /start again.")
                logging.error(f"User {user_id} was in state waiting_for_minutes, but user_data was missing.")
                await state.clear()
        else:
            await message.reply(f"❌ Invalid number. Please enter a value between {MIN_OFFSET_MINUTES} and {MAX_OFFSET_MINUTES}, or /cancel.")
            logging.warning(f"User {user_id} entered invalid offset: {minutes_input}. Prompting again.")
    except ValueError:
        await message.reply(f"❌ That doesn't look like a valid number. Please enter a whole number between {MIN_OFFSET_MINUTES} and {MAX_OFFSET_MINUTES}, or /cancel.")
        logging.warning(f"User {user_id} entered non-numeric value for offset: '{message.text}'. Prompting again.")
    except Exception as e:
        logging.error(f"Error processing minutes input for user {user_id}: {e}", exc_info=True)
        await message.reply("An unexpected error occurred. Please try again later or use /cancel.")
        await state.clear()

@dp.message(Command("learn"))
async def handle_learn_command(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_groups:
        await message.reply("I need to know you first. Please use /start to register.")
        return
    current_state = user_groups[user_id].get("learn_notify", False)
    new_state = not current_state
    user_groups[user_id]["learn_notify"] = new_state
    save_user_data(user_groups)
    status_message = "ON" if new_state else "OFF"
    await message.reply(f"✅ Learn platform notifications turned {status_message}.")
    logging.info(f"User {user_id} toggled learn notifications to {status_message}.")

@dp.message(Command("broadcast"))
async def start_broadcast(message: types.Message, state: FSMContext):
    # Admin check and broadcast logic remains unchanged
    if message.from_user.id != ADMIN_ID: return
    await state.clear()
    await message.reply("Okay, Admin! Send me the message you want to broadcast, or /cancel.")
    await state.set_state(Broadcasting.waiting_for_message)
    logging.info(f"Admin {ADMIN_ID} initiated broadcast sequence.")

@dp.message(Broadcasting.waiting_for_message, F.from_user.id == ADMIN_ID)
async def handle_broadcast_content(message: types.Message, state: FSMContext):
    # Admin check and broadcast logic remains unchanged
    await state.clear()
    if not user_groups:
        await message.reply("No registered users to broadcast to.")
        return
    logging.info(f"Admin {ADMIN_ID} provided broadcast content. Starting broadcast.")
    await message.reply(f"Starting broadcast to {len(user_groups)} users...")
    success_count, fail_count, blocked_users = 0, 0, []
    users_to_broadcast = list(user_groups.keys()) # Copy keys
    for chat_id in users_to_broadcast:
        if chat_id not in user_groups: continue
        try:
            await message.copy_to(chat_id=chat_id)
            success_count += 1
        except TelegramAPIError as e:
            fail_count += 1
            logging.error(f"Failed broadcast to {chat_id}: {e}")
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "chat not found" in str(e).lower() or "user is deactivated" in str(e).lower():
                blocked_users.append(chat_id)
        except Exception as e:
            fail_count += 1
            logging.error(f"Unexpected broadcast error to {chat_id}: {e}")
        await asyncio.sleep(BROADCAST_RATE_LIMIT_DELAY)
    if blocked_users:
        updated = False
        for user_id in blocked_users:
            if user_groups.pop(user_id, None) is not None:
                updated = True
                logging.info(f"Removed blocked user {user_id} after broadcast attempt.")
                global notified_lessons
                notified_lessons = {n for n in notified_lessons if n[0] != user_id}
                timetable_usage.pop(user_id, None)
                find_usage.pop(user_id, None)
        if updated: save_user_data(user_groups)
    summary = f"Broadcast finished.\nSuccess: {success_count}\nFailed: {fail_count}"
    if blocked_users: summary += f"\nRemoved {len(blocked_users)} blocked/deactivated users."
    await message.reply(summary)
    logging.info(f"Broadcast summary: {summary}")

@dp.message(Command("help"))
async def send_help_message(message: types.Message):
    user_id = message.from_user.id
    is_admin = user_id == ADMIN_ID
    help_text = "<b>Available Commands:</b>\n\n"
    help_text += "/start - Register or change your group\n"
    help_text += "/timetable - Show today's official schedule\n"
    help_text += "/find <code>[room]</code> - Find a room map (e.g., /find C1.3.122 or /find C1.1.256P)\n"
    help_text += "/minutes - Set lesson notification time (before lesson starts)\n"
    help_text += "/learn - Toggle Learn platform reminders (Mon, Wed, Fri at 19:40)\n"
    help_text += "\n<b>Custom Lessons:</b>\n"
    help_text += "/add_lesson - Add a custom lesson/reminder\n"
    help_text += "/view_lessons - View your added custom lessons\n"
    help_text += "/delete_lesson - Delete a custom lesson\n"
    help_text += "\n/help - Show this help message\n"
    help_text += "/cancel - Cancel current operation (like adding a lesson)\n"
    if is_admin:
        help_text += "\n<b>Admin Commands:</b>\n"
        help_text += "/broadcast - Send a message to all users\n"
    await message.reply(help_text)

# --- Custom Lesson Commands ---
# These should now be correctly triggered via decorators

@dp.message(Command("add_lesson"))
async def add_custom_lesson_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in user_groups:
        await message.reply("Please register using /start before adding custom lessons.")
        return
    if len(user_groups[user_id].get("custom_lessons", [])) >= MAX_CUSTOM_LESSONS:
        await message.reply(f"❌ You have reached the maximum limit of {MAX_CUSTOM_LESSONS} custom lessons. Use /delete_lesson to remove old ones first.")
        return
    buttons = [ [InlineKeyboardButton(text=day, callback_data=f"add_day_{day}")] for day in DAYS_OF_WEEK ]
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.reply("Let's add a custom lesson. First, select the day of the week:", reply_markup=keyboard)
    await state.set_state(AddCustomLesson.waiting_for_day)
    logging.info(f"User {user_id} initiated /add_lesson.")

@dp.callback_query(AddCustomLesson.waiting_for_day, F.data.startswith("add_day_"))
async def process_custom_lesson_day(callback_query: CallbackQuery, state: FSMContext):
    selected_day = callback_query.data.split("_", 2)[-1]
    await state.update_data(day=selected_day)
    await callback_query.message.edit_text(f"✅ Day set to: <b>{selected_day}</b>.\n\n"
                                           f"Now, please enter a name or subject for this lesson (e.g., 'Study Group', 'AI Club Meeting'), or /cancel:")
    await callback_query.answer()
    await state.set_state(AddCustomLesson.waiting_for_subject)
    logging.info(f"User {callback_query.from_user.id} selected day '{selected_day}' for custom lesson.")

@dp.message(AddCustomLesson.waiting_for_subject, F.text)
async def process_custom_lesson_subject(message: types.Message, state: FSMContext):
    subject = message.text.strip()
    if not subject:
        await message.reply("Subject cannot be empty. Please enter a name, or /cancel.")
        return
    if len(subject) > 100:
         await message.reply("Subject name is too long (max 100 chars). Please enter a shorter name, or /cancel.")
         return
    await state.update_data(subject=subject)
    await message.reply(f"✅ Subject set to: <b>{subject}</b>.\n\n"
                      f"Now, enter the <b>start time</b> in HH:MM format (e.g., 09:00, 14:30), or /cancel:")
    await state.set_state(AddCustomLesson.waiting_for_start_time)
    logging.info(f"User {message.from_user.id} entered subject '{subject}' for custom lesson.")

@dp.message(AddCustomLesson.waiting_for_start_time, F.text)
async def process_custom_lesson_start_time(message: types.Message, state: FSMContext):
    start_time = message.text.strip()
    if not is_valid_time_format(start_time):
        await message.reply("❌ Invalid time format. Please use HH:MM (e.g., 09:00, 14:30), or /cancel.")
        return
    await state.update_data(start_time=start_time)
    await message.reply(f"✅ Start time set to: <b>{start_time}</b>.\n\n"
                      f"Now, enter the <b>end time</b> in HH:MM format (e.g., 10:30, 16:00), or /cancel:")
    await state.set_state(AddCustomLesson.waiting_for_end_time)
    logging.info(f"User {message.from_user.id} entered start time '{start_time}' for custom lesson.")

@dp.message(AddCustomLesson.waiting_for_end_time, F.text)
async def process_custom_lesson_end_time(message: types.Message, state: FSMContext):
    end_time = message.text.strip()
    if not is_valid_time_format(end_time):
        await message.reply("❌ Invalid time format. Please use HH:MM (e.g., 10:30, 16:00), or /cancel.")
        return
    user_data = await state.get_data()
    start_time = user_data.get("start_time")
    try:
        start_dt = datetime.strptime(start_time, '%H:%M')
        end_dt = datetime.strptime(end_time, '%H:%M')
        if end_dt <= start_dt:
            await message.reply(f"❌ End time ({end_time}) must be after the start time ({start_time}). Please enter a valid end time, or /cancel.")
            return
    except ValueError:
        await message.reply("Error comparing times. Please try again or /cancel.")
        logging.error(f"Error comparing times {start_time} and {end_time} for user {message.from_user.id}")
        return
    await state.update_data(end_time=end_time)
    await message.reply(f"✅ End time set to: <b>{end_time}</b>.\n\n"
                      f"Finally, enter the <b>room number</b> (e.g., C1.3.122 or C1.1.256P) (if it is Physical Education type anything, e.g. <b>Gym</b>), or type 'ONLINE', or /cancel:")
    await state.set_state(AddCustomLesson.waiting_for_room)
    logging.info(f"User {message.from_user.id} entered end time '{end_time}' for custom lesson.")

@dp.message(AddCustomLesson.waiting_for_room, F.text)
async def process_custom_lesson_room(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    room_input = message.text.strip()
    if not room_input:
        await message.reply("Room cannot be empty. Please enter a room number (e.g., C1.3.122) or 'ONLINE', or /cancel.")
        return
    room_to_store = room_input
    room_cleaned_for_lookup = None
    is_online = False
    if room_input.upper() == "ONLINE":
        is_online = True
        room_to_store = "ONLINE"
    else:
        room_cleaned_for_lookup = clean_room_number(room_input)
        if not room_cleaned_for_lookup:
             await message.reply(f"❌ Invalid room format '{room_input}'. Please use a format like C1.3.122, C1.1.256P, or type 'ONLINE', or /cancel.")
             return
        if room_links_data and room_cleaned_for_lookup not in room_links_data:
            await message.reply(f"⚠️ Room '{room_input}' entered. I will look for a map for '{room_cleaned_for_lookup}', but I don't seem to have one. "
                                f"The lesson will be saved, but map notifications might not work. You can continue or /cancel.")
    lesson_data = await state.get_data()
    lesson_data['room'] = room_to_store
    lesson_data['id'] = str(uuid.uuid4())
    if user_id in user_groups:
        if len(user_groups[user_id].get("custom_lessons", [])) < MAX_CUSTOM_LESSONS:
            user_groups[user_id].setdefault("custom_lessons", []).append(lesson_data)
            save_user_data(user_groups)
            await message.reply(
                f"✅ <b>Custom lesson added!</b>\n\n"
                f"📌 Subject: {lesson_data['subject']}\n"
                f"📅 Day: {lesson_data['day']}\n"
                f"🕒 Time: {lesson_data['start_time']} - {lesson_data['end_time']}\n"
                f"🚪 Room: {lesson_data['room']} "
                f"{f'(Map lookup: {room_cleaned_for_lookup})' if not is_online and room_cleaned_for_lookup else ''}\n\n"
                f"Use /view_lessons to see all your custom lessons."
            )
            logging.info(f"User {user_id} successfully added custom lesson: {lesson_data}")
            await state.clear()
        else:
            await message.reply(f"❌ Could not save. You have reached the maximum limit of {MAX_CUSTOM_LESSONS} custom lessons.")
            logging.warning(f"User {user_id} reached max custom lessons during final save step.")
            await state.clear()
    else:
        await message.reply("Error: Could not find your user data. Please try /start again.")
        logging.error(f"User {user_id} was in state waiting_for_room, but user_data was missing during save.")
        await state.clear()

@dp.message(Command("view_lessons"))
async def view_custom_lessons(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_groups or not user_groups[user_id].get("custom_lessons"):
        await message.reply("You haven't added any custom lessons yet. Use /add_lesson to create one.")
        return
    custom_lessons = user_groups[user_id]["custom_lessons"]
    response_text = "📅 <b>Your Custom Lessons:</b>\n\n"
    if not custom_lessons:
         await message.reply("You haven't added any custom lessons yet. Use /add_lesson to create one.")
         return
    lessons_sorted = sorted(custom_lessons, key=lambda x: (DAYS_OF_WEEK.index(x.get('day', 'Sunday')), x.get('start_time', '23:59')))
    for i, lesson in enumerate(lessons_sorted):
        room_display = lesson.get('room', 'N/A')
        response_text += (
            f"<b>{i+1}. {lesson.get('subject', 'N/A')}</b>\n"
            f"   - Day: {lesson.get('day', 'N/A')}\n"
            f"   - Time: {lesson.get('start_time', 'N/A')} - {lesson.get('end_time', 'N/A')}\n"
            f"   - Room: {room_display}\n\n"
        )
    response_text += "Use /delete_lesson to remove lessons."
    await message.reply(response_text)
    logging.info(f"User {user_id} viewed their {len(custom_lessons)} custom lessons.")

@dp.message(Command("delete_lesson"))
async def delete_custom_lesson_start(message: types.Message):
    user_id = message.from_user.id
    if user_id not in user_groups or not user_groups[user_id].get("custom_lessons"):
        await message.reply("You don't have any custom lessons to delete. Use /add_lesson first.")
        return
    custom_lessons = user_groups[user_id]["custom_lessons"]
    if not custom_lessons:
        await message.reply("You don't have any custom lessons to delete. Use /add_lesson first.")
        return
    buttons = []
    lessons_sorted = sorted(custom_lessons, key=lambda x: (DAYS_OF_WEEK.index(x.get('day', 'Sunday')), x.get('start_time', '23:59')))
    for lesson in lessons_sorted:
        label = f"{lesson.get('day', '?')[:3]} {lesson.get('start_time', '?:??')} - {lesson.get('subject', 'N/A')[:20]}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"delete_lesson_{lesson.get('id')}")])
    buttons.append([InlineKeyboardButton(text="❌ Cancel Deletion", callback_data="delete_lesson_cancel")])
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    await message.reply("Select the custom lesson you want to delete:", reply_markup=keyboard)
    logging.info(f"User {user_id} initiated /delete_lesson.")

@dp.callback_query(F.data.startswith("delete_lesson_"))
async def process_custom_lesson_delete(callback_query: CallbackQuery):
    user_id = callback_query.from_user.id
    data = callback_query.data
    if data == "delete_lesson_cancel":
        await callback_query.message.edit_text("Deletion cancelled.")
        await callback_query.answer()
        logging.info(f"User {user_id} cancelled lesson deletion.")
        return
    lesson_id_to_delete = data.split("_", 2)[-1]
    if user_id not in user_groups or "custom_lessons" not in user_groups[user_id]:
        await callback_query.message.edit_text("Error: Could not find your lesson data.")
        await callback_query.answer("Error", show_alert=True)
        logging.error(f"User {user_id} tried to delete lesson {lesson_id_to_delete}, but user data/lessons missing.")
        return
    initial_lesson_count = len(user_groups[user_id]["custom_lessons"])
    user_groups[user_id]["custom_lessons"] = [
        lesson for lesson in user_groups[user_id]["custom_lessons"] if lesson.get("id") != lesson_id_to_delete
    ]
    if len(user_groups[user_id]["custom_lessons"]) < initial_lesson_count:
        save_user_data(user_groups)
        await callback_query.message.edit_text(f"✅ Custom lesson deleted successfully!")
        await callback_query.answer("Lesson deleted")
        logging.info(f"User {user_id} deleted custom lesson with ID {lesson_id_to_delete}.")
    else:
        await callback_query.message.edit_text("Could not find the selected lesson to delete. It might have already been removed.")
        await callback_query.answer("Lesson not found", show_alert=True)
        logging.warning(f"User {user_id} tried to delete lesson {lesson_id_to_delete}, but it was not found in their list.")

# --- Default Handler ---
# MUST be registered last (implicitly via decorator order)
@dp.message(StateFilter(None))
async def handle_other_messages(message: types.Message):
    """Handles unrecognized commands or text when no state is active."""
    user_id = message.from_user.id
    logging.info(f"Received unrecognized message from {user_id}: '{message.text}'")
    user_data = user_groups.get(user_id)
    if user_data:
        group_num = user_data.get("group", "Not Set")
        offset = user_data.get("notification_offset", DEFAULT_NOTIFICATION_OFFSET_MINUTES)
        await message.reply(f"Hi! Your group: {group_num}\nNotify {offset} min before lessons.\n\n"
                          f"I didn't understand that. Use /help to see available commands.")
    else:
        await message.reply("Hello! I didn't understand that. Use /start to register or /help to see commands.")

# --- Notification Logic (check_schedule remains unchanged) ---
async def check_schedule():
    global notified_lessons, last_learn_notify_sent_key, user_groups
    while True:
        removed_users_in_check = set()
        try:
            now = datetime.now(TIMEZONE)
            current_day_name = now.strftime('%A')
            current_weekday = now.weekday()
            today_iso = now.date().isoformat()
            current_time_hm = now.strftime("%H:%M")
            current_minute_key = f"{today_iso}-{current_time_hm}"

            # --- Daily Cleanup ---
            if now.time() < dt_time(0, 5):
                if not hasattr(check_schedule, 'last_cleared_date') or check_schedule.last_cleared_date != today_iso:
                    logging.info(f"Performing daily cleanup for {today_iso}. Current notified_lessons count: {len(notified_lessons)}")
                    notified_lessons_before = len(notified_lessons)
                    notified_lessons = {n for n in notified_lessons if n[1] >= today_iso}
                    check_schedule.last_cleared_date = today_iso
                    logging.info(f"Notified lessons cleaned up. Count before: {notified_lessons_before}, after: {len(notified_lessons)}")

            # --- 1. Learn Platform Notification Check ---
            if current_weekday in [0, 2, 4] and current_time_hm == "19:40":
                if last_learn_notify_sent_key != current_minute_key:
                    logging.info(f"Time matched for Learn notification ({current_minute_key}). Checking users.")
                    users_to_notify_learn = []
                    current_users_copy = dict(user_groups)
                    for user_id, user_data in current_users_copy.items():
                        if user_data.get("learn_notify", False): users_to_notify_learn.append(user_id)
                    if users_to_notify_learn:
                        logging.info(f"Sending Learn notification to {len(users_to_notify_learn)} users.")
                        sent_count = 0
                        for chat_id in users_to_notify_learn:
                             if chat_id in user_groups and user_groups[chat_id].get("learn_notify"):
                                try:
                                    await bot.send_message(chat_id, LEARN_NOTIFICATION_TEXT)
                                    sent_count += 1; await asyncio.sleep(LEARN_NOTIFICATION_DELAY)
                                except TelegramAPIError as e:
                                    logging.error(f"Failed to send Learn notification to {chat_id}: {e}")
                                    if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "chat not found" in str(e).lower() or "user is deactivated" in str(e).lower():
                                        logging.warning(f"Removing user {chat_id} due to error during Learn notification.")
                                        if user_groups.pop(chat_id, None):
                                            save_user_data(user_groups); notified_lessons = {n for n in notified_lessons if n[0] != chat_id}
                                            timetable_usage.pop(chat_id, None); find_usage.pop(chat_id, None)
                                            removed_users_in_check.add(chat_id)
                                except Exception as e: logging.error(f"Unexpected error sending Learn notification to {chat_id}: {e}", exc_info=True)
                        logging.info(f"Finished sending Learn notifications. Sent: {sent_count}")
                    else: logging.info("No users opted-in for Learn notifications at this time.")
                    last_learn_notify_sent_key = current_minute_key

            # --- 2. Lesson Reminder Notification Check (Official & Custom) ---
            users_data_list = list(user_groups.items())
            for chat_id, user_data in users_data_list:
                if chat_id in removed_users_in_check or chat_id not in user_groups: continue
                user_notification_offset = user_data.get("notification_offset", DEFAULT_NOTIFICATION_OFFSET_MINUTES)
                notify_time_target = now + timedelta(minutes=user_notification_offset)
                notify_target_hour = notify_time_target.hour; notify_target_minute = notify_time_target.minute

                # --- A. Check Official Schedule ---
                group_number = user_data.get("group")
                if group_number and timetable_data and group_number in timetable_data:
                    group_schedule = timetable_data.get(group_number, {}); day_schedule = group_schedule.get(current_day_name, {})
                    if day_schedule:
                        for lesson_key, lesson_details in day_schedule.items():
                            if chat_id not in user_groups or chat_id in removed_users_in_check: break
                            try:
                                time_range = lesson_details.get("time"); room_raw = lesson_details.get("room")
                                subject = lesson_details.get("subject", "N/A"); lesson_type = lesson_details.get("type", "N/A").capitalize()
                                lecturer = lesson_details.get("lecturer", "N/A")
                                if not time_range or not isinstance(time_range, str): continue
                                start_time_str = time_range.split('-')[0].strip()
                                if not is_valid_time_format(start_time_str): continue
                                lesson_hour, lesson_minute = map(int, start_time_str.split(':'))
                                if lesson_hour == notify_target_hour and lesson_minute == notify_target_minute:
                                    notification_id = (chat_id, today_iso, f"official_{group_number}_{current_day_name}_{lesson_key}")
                                    if notification_id in notified_lessons: continue
                                    logging.info(f"Match found: Sending OFFICIAL lesson notification {lesson_key} ({subject}) to {chat_id} ({group_number}) at {user_notification_offset} min offset.")
                                    is_online = isinstance(room_raw, str) and room_raw.strip().upper() == "ONLINE"; room_cleaned = None
                                    if not is_online: room_cleaned = clean_room_number(room_raw)
                                    base_message = (f"🔔 <b>Lesson Reminder! ({user_notification_offset} min)</b>\n\n"
                                                    f"<b>{lesson_key}. {subject}</b> ({lesson_type})\n"
                                                    f"🕒 Starts at: {start_time_str}\n"
                                                    f"👨‍🏫 Lecturer: {lecturer}\n"
                                                    f"🚪 Room: {room_raw if isinstance(room_raw, str) else 'N/A'}")
                                    try:
                                        await bot.send_message(chat_id, base_message); notified_lessons.add(notification_id); await asyncio.sleep(RATE_LIMIT_DELAY)
                                        if not is_online and room_cleaned and room_links_data:
                                            photo_file_id = room_links_data.get(room_cleaned)
                                            if photo_file_id:
                                                try: await bot.send_photo(chat_id=chat_id, photo=photo_file_id, caption=f"📍 Location map for room {room_cleaned}"); await asyncio.sleep(RATE_LIMIT_DELAY)
                                                except TelegramAPIError as e_photo: logging.error(f"Notify: Failed to send OFFICIAL map photo {photo_file_id} for room {room_cleaned} (raw: {room_raw}) to {chat_id}: {e_photo}")
                                            else: logging.warning(f"Notify: No map photo found for OFFICIAL cleaned room '{room_cleaned}' (raw: '{room_raw}') for user {chat_id}.")
                                        logging.info(f"Successfully sent OFFICIAL lesson notification {notification_id}")
                                    except TelegramAPIError as e:
                                        logging.error(f"API Error sending OFFICIAL lesson notification part to {chat_id} (ID: {notification_id}): {e}"); notified_lessons.discard(notification_id)
                                        if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "chat not found" in str(e).lower() or "user is deactivated" in str(e).lower():
                                            logging.warning(f"Removing user {chat_id} due to error during OFFICIAL lesson notification.")
                                            if user_groups.pop(chat_id, None):
                                                save_user_data(user_groups); notified_lessons = {n for n in notified_lessons if n[0] != chat_id}
                                                timetable_usage.pop(chat_id, None); find_usage.pop(chat_id, None); removed_users_in_check.add(chat_id); break
                                    except Exception as e: notified_lessons.discard(notification_id); logging.error(f"Unexpected error sending OFFICIAL lesson notification part for {chat_id} / lesson {lesson_key}: {e}", exc_info=True)
                            except Exception as e: logging.error(f"Error processing inner loop for OFFICIAL lesson {lesson_key}, user {chat_id}, group {group_number}: {e}", exc_info=True)

                # --- B. Check Custom Schedule ---
                custom_lessons = user_data.get("custom_lessons", [])
                if custom_lessons:
                    for lesson in custom_lessons:
                        if chat_id not in user_groups or chat_id in removed_users_in_check: break
                        try:
                            lesson_day = lesson.get("day"); start_time_str = lesson.get("start_time"); lesson_id = lesson.get("id")
                            subject = lesson.get("subject", "N/A"); room_stored = lesson.get("room", "N/A"); end_time_str = lesson.get("end_time", "N/A")
                            if not lesson_day or lesson_day != current_day_name or not start_time_str or not lesson_id or not is_valid_time_format(start_time_str): continue
                            lesson_hour, lesson_minute = map(int, start_time_str.split(':'))
                            if lesson_hour == notify_target_hour and lesson_minute == notify_target_minute:
                                notification_id = (chat_id, today_iso, f"custom_{lesson_id}")
                                if notification_id in notified_lessons: continue
                                logging.info(f"Match found: Sending CUSTOM lesson notification '{subject}' (ID: {lesson_id}) to {chat_id} at {user_notification_offset} min offset.")
                                is_online = room_stored.upper() == "ONLINE"; room_cleaned = None
                                if not is_online: room_cleaned = clean_room_number(room_stored)
                                base_message = (f"🔔 <b>Custom Reminder! ({user_notification_offset} min)</b>\n\n"
                                                f"📌 Subject: <b>{subject}</b>\n"
                                                f"🕒 Starts at: {start_time_str} (Ends: {end_time_str})\n"
                                                f"🚪 Room: {room_stored}")
                                try:
                                    await bot.send_message(chat_id, base_message); notified_lessons.add(notification_id); await asyncio.sleep(RATE_LIMIT_DELAY)
                                    if not is_online and room_cleaned and room_links_data:
                                        photo_file_id = room_links_data.get(room_cleaned)
                                        if photo_file_id:
                                            try: await bot.send_photo(chat_id=chat_id, photo=photo_file_id, caption=f"📍 Location map for room {room_cleaned}"); await asyncio.sleep(RATE_LIMIT_DELAY)
                                            except TelegramAPIError as e_photo: logging.error(f"Notify: Failed to send CUSTOM map photo {photo_file_id} for room {room_cleaned} (stored: {room_stored}) to {chat_id}: {e_photo}")
                                        else: logging.warning(f"Notify: No map photo found for CUSTOM cleaned room '{room_cleaned}' (stored: '{room_stored}') for user {chat_id}.")
                                    logging.info(f"Successfully sent CUSTOM lesson notification {notification_id}")
                                except TelegramAPIError as e:
                                    logging.error(f"API Error sending CUSTOM lesson notification part to {chat_id} (ID: {notification_id}): {e}"); notified_lessons.discard(notification_id)
                                    if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "chat not found" in str(e).lower() or "user is deactivated" in str(e).lower():
                                        logging.warning(f"Removing user {chat_id} due to error during CUSTOM lesson notification.")
                                        if user_groups.pop(chat_id, None):
                                            save_user_data(user_groups); notified_lessons = {n for n in notified_lessons if n[0] != chat_id}
                                            timetable_usage.pop(chat_id, None); find_usage.pop(chat_id, None); removed_users_in_check.add(chat_id); break
                                except Exception as e: notified_lessons.discard(notification_id); logging.error(f"Unexpected error sending CUSTOM lesson notification part for {chat_id} / lesson ID {lesson_id}: {e}", exc_info=True)
                        except Exception as e: logging.error(f"Error processing inner loop for CUSTOM lesson (ID: {lesson.get('id', 'UNKNOWN')}), user {chat_id}: {e}", exc_info=True)

        except Exception as e:
            logging.critical(f"CRITICAL ERROR in check_schedule main loop: {e}", exc_info=True)
            await asyncio.sleep(CHECK_INTERVAL_SECONDS * 2)
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

# --- Main Execution ---
async def main():
    global user_groups
    user_groups = load_user_data()
    logging.info(f"Loaded {len(user_groups)} users from {USER_DATA_FILE}")

    # REMOVED explicit handler registration from here.
    # Relying solely on decorators placed above each handler function.

    scheduler_task = asyncio.create_task(check_schedule(), name="ScheduleChecker")
    logging.info("Background scheduler task created.")
    logging.info("Starting bot polling...")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        logging.info("Shutting down bot...")
        scheduler_task.cancel()
        try: await scheduler_task
        except asyncio.CancelledError: logging.info("Scheduler task cancelled successfully.")
        except Exception as e: logging.error(f"Error during scheduler task cancellation: {e}", exc_info=True)
        save_user_data(user_groups)
        logging.info(f"Final user data saved. Bot polling stopped.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Shutdown requested via KeyboardInterrupt.")
    except Exception as e:
        logging.error(f"Unhandled exception in main execution scope: {e}", exc_info=True)