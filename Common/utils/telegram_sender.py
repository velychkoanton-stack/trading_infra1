from __future__ import annotations

import time
from typing import Optional

import requests

from Common.config.api_loader import load_api_file
from Common.config.path_config import get_api_file_path


API_URL = "https://api.telegram.org/bot{token}/sendMessage"


def _load_telegram_config(api_file_name: str = "api_telegram_main.txt") -> tuple[str, str]:
    file_path = get_api_file_path(api_file_name)
    config = load_api_file(file_path)

    token = (
        config.get("token")
        or config.get("TOKEN")
        or config.get("TELEGRAM_BOT_TOKEN")
        or config.get("BOT_TOKEN")
    )
    chat_id = (
        config.get("chat_id")
        or config.get("CHAT_ID")
        or config.get("TELEGRAM_CHAT_ID")
        or config.get("TG_CHAT_ID")
    )

    if not token:
        raise ValueError(f"Telegram token not found in API file: {api_file_name}")

    if not chat_id:
        raise ValueError(f"Telegram chat_id not found in API file: {api_file_name}")

    return str(token).strip(), str(chat_id).strip()


def send_tg_message(
    text: str,
    parse_mode: Optional[str] = None,
    disable_web_page_preview: bool = True,
    api_file_name: str = "api_telegram_main.txt",
) -> bool:
    token, chat_id = _load_telegram_config(api_file_name=api_file_name)
    url = API_URL.format(token=token)

    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }

    if parse_mode:
        payload["parse_mode"] = parse_mode

    for attempt in range(1, 4):
        try:
            response = requests.post(url, json=payload, timeout=10)

            if response.ok:
                return True

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(min(2 * attempt, 5))
                continue

            return False

        except Exception:
            time.sleep(min(2 * attempt, 5))

    return False