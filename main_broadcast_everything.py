import asyncio
import json
import logging
from datetime import datetime, timedelta, time
import pytz

from aiogram import Bot, Dispatcher, types, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError

# --- Configuration ---

BOT_TOKEN = "xxxxx"
ADMIN_ID = 1111111111

# Constants
TIMETABLE_FILE = 'final_timetable.json'
ROOM_LINKS_FILE = 'photo_file_ids.json'
USER_DATA_FILE = 'user_data.json'
NOTIFICATION_OFFSET_MINUTES = 10
CHECK_INTERVAL_SECONDS = 60
TIMEZONE = pytz.timezone('Asia/Almaty')
RATE_LIMIT_DELAY = 0.05
BROADCAST_RATE_LIMIT_DELAY = 0.1

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Load Data ---
def load_json_data(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: {filename} not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from {filename}.")
        return None

timetable_data = load_json_data(TIMETABLE_FILE)
room_links_data = load_json_data(ROOM_LINKS_FILE)

if not timetable_data or not room_links_data:
    logging.error("Failed to load necessary data files. Exiting.")
    exit()

# --- User Data Persistence ---
def load_user_data():
    try:
        with open(USER_DATA_FILE, 'r', encoding='utf-8') as f:
            return {int(k): v for k, v in json.load(f).items()}
    except (FileNotFoundError, json.JSONDecodeError):
        logging.warning(f"{USER_DATA_FILE} not found or invalid. Starting with empty user data.")
        return {}

def save_user_data(data):
    try:
        with open(USER_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except IOError as e:
        logging.error(f"Error saving user data to {USER_DATA_FILE}: {e}")

# --- Bot Setup ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- State Definitions ---
class Registration(StatesGroup):
    waiting_for_group = State()

class Broadcasting(StatesGroup):
    waiting_for_message = State()

# --- Global Data ---
user_groups = load_user_data()
notified_lessons = set()

# --- Helper Functions ---
def get_current_day_of_week():
    now = datetime.now(TIMEZONE)
    return now.strftime('%A')

def clean_room_number(raw_room):
    if not raw_room or raw_room.lower() == "online":
        return None
    room = raw_room.split('\n')[0].split('(')[0].strip()
    if room and room[-1].isalpha():
        room = room[:-1]
    return room.strip()

# --- Command Handlers ---
@dp.message(CommandStart())
async def send_welcome(message: types.Message, state: FSMContext):
    logging.info(f"User {message.from_user.id} started the bot.")
    # Cancel any pending broadcast state if user restarts
    await state.clear()
    await message.reply("Welcome! Please enter your group number (e.g., EE-2401):")
    await state.set_state(Registration.waiting_for_group)

@dp.message(Registration.waiting_for_group, F.text)
async def process_group_number(message: types.Message, state: FSMContext):
    user_input = message.text.strip()
    user_input_upper = user_input.upper() # Use this for comparison
    matched_group_key = None

    # Iterate through the actual keys in your timetable data
    for key in timetable_data.keys():
        # Compare the uppercase version of the user input with the uppercase version of the key
        if key.upper() == user_input_upper:
            matched_group_key = key  # Store the original key with correct casing
            break # Found a match, no need to check further

    if matched_group_key:
        user_id = message.from_user.id
        # Store the correctly cased key from the timetable data
        user_groups[user_id] = matched_group_key
        save_user_data(user_groups)
        await message.reply(f"Great! Your group '{matched_group_key}' is registered. " # Use matched_group_key here
                          f"I will notify you {NOTIFICATION_OFFSET_MINUTES} minutes before your lessons.")
        logging.info(f"User {user_id} registered group: {matched_group_key}") # Log the correct key
        await state.clear()
    else:
        await message.reply("Sorry, I couldn't find that group number in the timetable. "
                          "Please check the format (e.g., EE-2401 or IoT-2401) and try again.")
        logging.warning(f"User {message.from_user.id} entered group '{user_input}', which was not found.")

# --- Admin Broadcast Command ---
@dp.message(Command("broadcast"))
async def start_broadcast(message: types.Message, state: FSMContext):
    """Initiates the broadcast process by asking for the message."""
    if message.from_user.id != ADMIN_ID:
        logging.warning(f"Unauthorized broadcast attempt by user {message.from_user.id}")
        return # Silently ignore if not admin

    # Cancel previous state just in case
    await state.clear()

    await message.reply("Aight, send me the message you want to broadcast (text, photo, video, etc.).")
    await state.set_state(Broadcasting.waiting_for_message)
    logging.info(f"Admin {ADMIN_ID} initiated broadcast sequence.")

# --- Handler for receiving the message TO broadcast ---
@dp.message(Broadcasting.waiting_for_message, F.from_user.id == ADMIN_ID)
async def handle_broadcast_content(message: types.Message, state: FSMContext):
    """Receives the message from the admin and performs the broadcast."""
    await state.clear() # Clear state immediately

    if not user_groups:
        await message.reply("There are currently no registered users to broadcast to.")
        logging.warning(f"Admin {ADMIN_ID} tried to broadcast, but no users are registered.")
        return

    logging.info(f"Admin {ADMIN_ID} provided broadcast content (message_id: {message.message_id}). Starting broadcast.")
    await message.reply(f"Got it! Starting broadcast to {len(user_groups)} users...")

    success_count = 0
    fail_count = 0
    blocked_users = []
    users_to_broadcast = list(user_groups.keys())

    for chat_id in users_to_broadcast:
        # --- Skip if user got removed concurrently ---
        if chat_id not in user_groups:
            continue
        # -----------------------------------------
        try:
            await message.copy_to(chat_id=chat_id)
            success_count += 1
            logging.debug(f"Broadcast message copied successfully to {chat_id}")
        except TelegramAPIError as e:
            fail_count += 1
            logging.error(f"Failed to copy broadcast message to {chat_id}: {e}")
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "user is deactivated" in str(e).lower():
                blocked_users.append(chat_id)
                logging.warning(f"User {chat_id} blocked the bot or is deactivated. Marked for removal.")
        except Exception as e:
             fail_count += 1
             logging.error(f"Unexpected error sending broadcast to {chat_id}: {e}")

        await asyncio.sleep(BROADCAST_RATE_LIMIT_DELAY)

    if blocked_users:
        updated = False
        for user_id in blocked_users:
            if user_groups.pop(user_id, None) is not None:
                updated = True
                logging.info(f"Removed blocked/deactivated user {user_id} from user_groups.")
        if updated:
            save_user_data(user_groups)

    summary_message = (
        f"Broadcast finished.\n\n"
        f"Successfully sent: {success_count}\n"
        f"Failed: {fail_count}"
    )
    if blocked_users:
        summary_message += f"\nRemoved {len(blocked_users)} blocked/deactivated users."

    await message.reply(summary_message)
    logging.info(f"Broadcast summary: Success={success_count}, Fail={fail_count}, Removed={len(blocked_users)}")


# --- Default Handler ---
@dp.message()
async def handle_other_messages(message: types.Message, state: FSMContext):
    """Handles any other message when not in a specific state or workflow."""
    current_state = await state.get_state()
    # Check if it's the admin but not in the broadcast waiting state
    if message.from_user.id == ADMIN_ID and current_state is None:
         await message.reply("I understand /start and /broadcast.")
    # Check if it's a non-admin user not in the registration state
    elif current_state is None:
        await message.reply("I understand /start. If you want to change your group, use /start again.")
    # Add more specific handling here if needed for other states or unexpected messages

# --- Notification Logic (check_schedule - unchanged from previous version) ---
async def check_schedule():
    """Periodically checks the schedule and sends notifications."""
    global notified_lessons
    while True:
        now = datetime.now(TIMEZONE)
        current_day = now.strftime('%A')
        if now.time() < time(0, 1):
             today_iso = now.date().isoformat()
             if not any(n[1] == today_iso for n in notified_lessons):
                 logging.info(f"Clearing notified lessons for new day: {today_iso}")
                 yesterday = (now - timedelta(days=1)).date().isoformat()
                 notified_lessons = {n for n in notified_lessons if n[1] >= yesterday}

        notify_time_target = now + timedelta(minutes=NOTIFICATION_OFFSET_MINUTES)
        logging.debug(f"Checking schedule for notifications around {notify_time_target.strftime('%H:%M')} on {current_day}")

        users_to_notify = list(user_groups.items())
        for chat_id, group_number in users_to_notify:
            if chat_id not in user_groups: continue # User removed check

            if group_number not in timetable_data:
                logging.warning(f"User {chat_id}'s group '{group_number}' not found in timetable, skipping.")
                continue

            group_schedule = timetable_data.get(group_number, {})
            day_schedule = group_schedule.get(current_day, {})
            if not day_schedule: continue

            for lesson_key, lesson_details in day_schedule.items():
                if chat_id not in user_groups: break # User removed check

                try:
                    time_range = lesson_details.get("time")
                    room_raw = lesson_details.get("room")
                    subject = lesson_details.get("subject", "N/A")
                    if not time_range or not room_raw: continue

                    start_time_str = time_range.split('-')[0].strip()
                    lesson_hour, lesson_minute = map(int, start_time_str.split(':'))
                    lesson_start_time_today = TIMEZONE.localize(datetime.combine(now.date(), time(lesson_hour, lesson_minute)))

                    if (lesson_start_time_today.hour == notify_time_target.hour and
                        lesson_start_time_today.minute == notify_time_target.minute):

                        notification_id = (chat_id, now.date().isoformat(), f"{group_number}_{current_day}_{lesson_key}")
                        if notification_id in notified_lessons: continue

                        room_cleaned = clean_room_number(room_raw)

                        try:
                            if room_cleaned:
                                photo_file_id = room_links_data.get(room_cleaned)
                                message_text = (
                                    f"🔔 Lesson Reminder!\n\n"
                                    f"<b>Subject:</b> {subject}\n"
                                    f"<b>Time:</b> {time_range}\n"
                                    f"<b>Room:</b> {room_raw.split(chr(10))[0]} "
                                    f"(in {NOTIFICATION_OFFSET_MINUTES} minutes)"
                                )
                                await bot.send_message(chat_id, message_text)
                                await asyncio.sleep(RATE_LIMIT_DELAY)

                                if photo_file_id:
                                    logging.info(f"Sending map photo with file_id for room {room_cleaned} to {chat_id}")
                                    try:
                                        await bot.send_photo(chat_id=chat_id, photo=photo_file_id, caption=f"📍 Location map for room {room_cleaned}")
                                        await asyncio.sleep(RATE_LIMIT_DELAY)
                                    except TelegramAPIError as e_photo:
                                        logging.error(f"Failed to send photo {photo_file_id} to {chat_id}: {e_photo}")
                                        if "FILE_ID_INVALID" in str(e_photo).upper() or "invalid file identifier" in str(e_photo):
                                             await bot.send_message(chat_id, f"ℹ️ Map for room '{room_cleaned}' could not be sent (invalid map data).")
                                        elif "blocked" not in str(e_photo).lower() and "deactivated" not in str(e_photo).lower():
                                            await bot.send_message(chat_id, f"ℹ️ Couldn't send map for room '{room_cleaned}'.")
                                        else: raise e_photo
                                        await asyncio.sleep(RATE_LIMIT_DELAY)
                                else:
                                    logging.warning(f"No map photo file_id found for room '{room_cleaned}'")
                                    await bot.send_message(chat_id, f"ℹ️ Map photo for room '{room_cleaned}' is not available.")
                                    await asyncio.sleep(RATE_LIMIT_DELAY)

                                notified_lessons.add(notification_id)
                                logging.info(f"Sent notification to {chat_id} for {group_number} lesson {lesson_key}")

                            elif room_raw and room_raw.lower() == "online":
                                message_text = (
                                    f"💻 Lesson Reminder!\n\n"
                                    f"<b>Subject:</b> {subject}\n"
                                    f"<b>Time:</b> {time_range}\n"
                                    f"<b>Location:</b> Online\n"
                                    f"(in {NOTIFICATION_OFFSET_MINUTES} minutes)"
                                )
                                await bot.send_message(chat_id, message_text)
                                await asyncio.sleep(RATE_LIMIT_DELAY)
                                notified_lessons.add(notification_id)
                                logging.info(f"Sent online notification to {chat_id} for {group_number} lesson {lesson_key}")
                            else:
                                logging.warning(f"Skipping notification for {group_number} lesson {lesson_key} due to unhandled room: {room_raw}")

                        except TelegramAPIError as e:
                            logging.error(f"Telegram API Error sending notification to {chat_id}: {e}")
                            if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "user is deactivated" in str(e).lower():
                                logging.warning(f"User {chat_id} blocked/deactivated during notification. Removing.")
                                if user_groups.pop(chat_id, None) is not None:
                                    notified_lessons = {n for n in notified_lessons if n[0] != chat_id}
                                    save_user_data(user_groups)
                        except Exception as e:
                            logging.error(f"Unexpected error processing notification for {chat_id} / lesson {lesson_key}: {e}")
                except Exception as e:
                    logging.error(f"Error processing lesson {lesson_key} for {group_number} before sending: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# --- Main Execution ---
async def main():
    if not timetable_data or not room_links_data: return

    global user_groups
    user_groups = load_user_data()
    logging.info(f"Loaded {len(user_groups)} users from {USER_DATA_FILE}")

    asyncio.create_task(check_schedule())
    logging.info("Background scheduler started.")

    logging.info("Starting bot polling...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())