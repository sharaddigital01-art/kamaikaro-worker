"""
Run ONCE on your own machine to create a Telethon StringSession.

    python generate_session.py

It asks for your phone number and the OTP Telegram sends you (and your 2FA
password if enabled), then prints TELETHON_SESSION_STRING. Put that string in
your .env / cloud env vars so the server never needs an interactive login.

Treat the string like a password: anyone holding it can act as your account.
"""
import asyncio
import os
import re

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")


async def main() -> None:
    if not API_ID or not API_HASH:
        raise SystemExit("Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env first.")

    phone = input("Phone number with country code (e.g. +919876543210): ").strip()
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    await client.connect()
    try:
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            code = input("Enter the login code Telegram sent you: ").strip().replace(" ", "")
            try:
                await client.sign_in(phone, code)
            except SessionPasswordNeededError:
                pw = input("2FA password: ")
                await client.sign_in(password=pw)

        session_string = client.session.save()
    finally:
        await client.disconnect()

    # Automatically save into .env
    env_file = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_file):
        with open(env_file, "r", encoding="utf-8") as f:
            content = f.read()
        if "TELETHON_SESSION_STRING=" in content:
            content = re.sub(r'TELETHON_SESSION_STRING=.*', f'TELETHON_SESSION_STRING="{session_string}"', content)
        else:
            content = content.rstrip() + f'\nTELETHON_SESSION_STRING="{session_string}"\n'
        with open(env_file, "w", encoding="utf-8") as f:
            f.write(content)
        print("\n[+] TELETHON_SESSION_STRING automatically saved to .env")

    print("\n" + "=" * 70)
    print("SUCCESS. Your session string:\n")
    print(f"TELETHON_SESSION_STRING={session_string}")
    print("\n" + "=" * 70)
    print(
        "\nNext steps:\n"
        "  1. Local: paste the line above into telegram-worker/.env\n"
        "  2. Cloud (Render/Railway): add TELETHON_SESSION_STRING as an environment\n"
        "     variable in the dashboard (do NOT commit it to git).\n"
        "  3. Make sure this account has JOINED every source channel it should monitor.\n"
        "  4. Never share this string; revoke it via Telegram > Settings > Devices if leaked.\n"
    )


if __name__ == "__main__":
    asyncio.run(main())
