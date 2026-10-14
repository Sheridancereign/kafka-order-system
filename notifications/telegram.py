import httpx
from django.conf import settings


class TelegramError(Exception):
    pass


def send_message(text: str) -> None:
    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = httpx.post(
            url,
            json={"chat_id": settings.TELEGRAM_CHAT_ID, "text": text},
            timeout=10.0,
        )
    except httpx.HTTPError as exc:
        raise TelegramError(f"Network error: {type(exc).__name__}") from None

    if response.status_code != 200:
        raise TelegramError(f"Telegram API returned {response.status_code}")