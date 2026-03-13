import time
import requests
from pathlib import Path

from Common.utils.api_file_loader import load_api_file


API_FILE = "api_telegram_main.txt"
API_URL = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramSender:

    def __init__(self):
        api = load_api_file(API_FILE)

        self.token = api["token"]
        self.chat_id = api["chat_id"]

        self.url = API_URL.format(token=self.token)

    def send(self, text: str, parse_mode: str | None = None) -> bool:

        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }

        if parse_mode:
            payload["parse_mode"] = parse_mode

        for attempt in range(1, 4):
            try:
                r = requests.post(self.url, json=payload, timeout=10)

                if r.ok:
                    return True

                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 * attempt, 5))
                    continue

                return False

            except Exception:
                time.sleep(min(2 * attempt, 5))

        return False


tg = TelegramSender()


def send_tg_message(text: str, parse_mode: str | None = None) -> bool:
    return tg.send(text, parse_mode=parse_mode)