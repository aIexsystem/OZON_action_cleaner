"""
ozon_promo_bot.py

Скрипт последовательно обходит кабинеты Ozon Seller API, оценивает
акции по средней скидке относительно action_price (на первой сотне товаров),
а для убыточных акций собирает список участвующих товаров через 5 минут после
старта и деактивирует их через 10 минут. Все этапы сопровождаются
уведомлениями в Telegram и логированием.
"""

import asyncio
import aiohttp
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Set, Tuple

# ----------------------------------------------------------------------------- 
# Конфигурация
ACCOUNTS = [
    {
        "name": "OZON shop1",
        "client_id": "",
        "api_key": "",
    },
    {
        "name": "OZON shop2",
        "client_id": "",
        "api_key": "",
    }
#### add as many accounts as you need
#### before using it make sure that you gave to API token required role (in this case it should be "Actions" inside the ozon API integrations has to be granted)
]

DISCOUNT_THRESHOLD = 35.0  # порог средней скидки
LIMIT_CANDIDATES = 100     # сколько кандидатов берем для оценки
POLL_INTERVAL = 1200      # секунд между проходами по всем кабинетам

TELEGRAM_BOT_TOKEN: str = "" ## api token for TG bot
TELEGRAM_CHAT_ID: str = "" #### TG chat ID 


######
######
# This excluded action names implies that these particular actions (promotions, sales whatever you want to call them) won't be deleted, you just need to type their ID and names :D
######
######
EXCLUDED_ACTION_NAMES: Set[str] = {
    "Бустинг 25% (ранее — «Бустинг х4»)",
    "Бустинг 15% (ранее — «Бустинг х3»)",
}
EXCLUDED_ACTION_IDS: Set[int] = {1177259, 1177179}
#####
#####

OZON_API_URL = "https://api-seller.ozon.ru"

# Временные зоны
TIMEZONE_UTC = timezone.utc
TIMEZONE_ACTION = timezone(timedelta(hours=3))  # Москва: UTC+3

logging.basicConfig(
    filename="ozon_promo_bot.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Хранилище собранных товаров для запланированных акций
pending_products: Dict[int, List[int]] = {}


@dataclass
class Promotion:
    action_id: int
    title: str
    start_at: datetime
    end_at: datetime
    potential_count: int
    excluded: bool = False

    @classmethod
    def from_api(cls, data: Dict) -> "Promotion":
        return cls(
            action_id=int(data["id"]),
            title=data.get("title", ""),
            start_at=datetime.fromisoformat(data["date_start"].replace("Z", "+00:00")).astimezone(TIMEZONE_UTC),
            end_at=datetime.fromisoformat(data["date_end"].replace("Z", "+00:00")).astimezone(TIMEZONE_UTC),
            potential_count=int(data.get("potential_products_count", 0)),
        )


class OzonClient:
    def __init__(self, client_id: str, api_key: str, session: aiohttp.ClientSession):
        self.client_id = client_id
        self.api_key = api_key
        self.session = session

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    async def get_actions(self) -> List[Promotion]:
        url = f"{OZON_API_URL}/v1/actions"
        async with self.session.get(url, headers=self.headers) as resp:
            if resp.status != 200:
                logger.error("Failed to fetch actions (status %s)", resp.status)
                return []
            data = await resp.json()
        promotions = []
        for entry in data.get("result", []):
            promo = Promotion.from_api(entry)
            if promo.title in EXCLUDED_ACTION_NAMES or promo.action_id in EXCLUDED_ACTION_IDS:
                promo.excluded = True
            promotions.append(promo)
        return promotions

    async def get_candidates_page(self, action_id: int, limit: int) -> Tuple[List[int], float]:
        """
        Возвращает не более `limit` товаров и среднее значение скидки 
        (price vs action_price) для этих товаров.
        """
        payload = {"action_id": action_id, "limit": limit}
        url = f"{OZON_API_URL}/v1/actions/candidates"
        async with self.session.post(url, headers=self.headers, json=payload) as resp:
            if resp.status != 200:
                logger.error("Failed to fetch candidates (status %s)", resp.status)
                return [], 0.0
            data = await resp.json()
        result = data.get("result", {})
        products = result.get("products", [])
        total_discount = 0.0
        count = 0
        ids: List[int] = []
        for item in products:
            price = item.get("price")
            action_price = item.get("action_price")
            try:
                price_val = float(str(price).replace(" ", "").replace(",", "."))
                action_price_val = float(str(action_price).replace(" ", "").replace(",", "."))
            except (TypeError, ValueError):
                continue
            if action_price_val <= 0:
                continue
            discount_percent = (price_val - action_price_val) / action_price_val * 100
            total_discount += discount_percent
            count += 1
            pid = item.get("id")
            if pid is not None:
                ids.append(int(pid))
        avg_discount = total_discount / count if count else 0.0
        return ids, avg_discount

    async def get_action_products(self, action_id: int, total_expected: int) -> List[int]:
        """
        Получает все товары, участвующие в акции, используя limit и last_id для пагинации.
        total_expected — ожидаемое количество товаров из potential_count (для ограничения выборок).
        """
        product_ids: List[int] = []
        limit = 1000
        last_id: Optional[str] = None
        remaining = total_expected
        while remaining > 0:
            fetch_limit = min(limit, remaining)
            payload = {"action_id": action_id, "limit": fetch_limit}
            if last_id:
                payload["last_id"] = last_id
            url = f"{OZON_API_URL}/v1/actions/products"
            async with self.session.post(url, headers=self.headers, json=payload) as resp:
                if resp.status != 200:
                    logger.error("Failed to fetch products for action %s (status %s)", action_id, resp.status)
                    break
                data = await resp.json()
            result = data.get("result", {})
            products = result.get("products", [])
            for item in products:
                pid = item.get("product_id") or item.get("id")
                if pid is not None:
                    product_ids.append(int(pid))
            last_id = result.get("last_id")
            if not last_id:
                break
            remaining -= fetch_limit
        return product_ids

    async def deactivate_products(self, action_id: int, product_ids: List[int]) -> bool:
        if not product_ids:
            return True
        payload = {"action_id": action_id, "product_ids": product_ids}
        url = f"{OZON_API_URL}/v1/actions/products/deactivate"
        async with self.session.post(url, headers=self.headers, json=payload) as resp:
            return resp.status == 200


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.token = token
        self.chat_id = chat_id
        self.session = session

    async def send_message(self, message: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": message, "parse_mode": "HTML"}
        async with self.session.post(url, json=payload) as resp:
            if resp.status != 200:
                logger.error("Telegram send error: HTTP %s", resp.status)


async def fetch_products_task(
    name: str,
    promo: Promotion,
    client: OzonClient,
    notifier: TelegramNotifier,
    delay_seconds: float,
) -> None:
    """Ждёт delay_seconds, затем собирает товары акции."""
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    # Получаем все товары акции через /v1/actions/products
    product_ids = await client.get_action_products(promo.action_id, promo.potential_count)
    pending_products[promo.action_id] = product_ids
    logger.info("[%s] Собрано %d товаров для акции '%s' (ID %s).",
                name, len(product_ids), promo.title, promo.action_id)
    await notifier.send_message(
        f"[{name}] Собрали {len(product_ids)} товаров для акции '{promo.title}' (ID {promo.action_id})."
    )


async def deactivate_task(
    name: str,
    promo: Promotion,
    client: OzonClient,
    notifier: TelegramNotifier,
    delay_seconds: float,
) -> None:
    """Ждёт delay_seconds и деактивирует товары акции."""
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    # Берём собранные товары (если fetch успел отработать)
    product_ids = pending_products.pop(promo.action_id, None)
    if product_ids is None:
        # На всякий случай получаем товары сейчас
        product_ids = await client.get_action_products(promo.action_id, promo.potential_count)
    success = await client.deactivate_products(promo.action_id, product_ids)
    if success:
        logger.info("[%s] Акция '%s' истреблена! %d товаров деактивировано.", 
                    name, promo.title, len(product_ids))
        await notifier.send_message(
            f"[{name}] Акция '{promo.title}' завершена: {len(product_ids)} товаров деактивировано."
        )
    else:
        logger.error("[%s] Не удалось удалить товары из акции '%s' (ID %s)", 
                     name, promo.title, promo.action_id)
        await notifier.send_message(
            f"[{name}] Не удалось удалить товары из акции '{promo.title}' (ID {promo.action_id}). Проверьте логи."
        )


async def monitor_once(session: aiohttp.ClientSession) -> None:
    """Обходит все кабинеты последовательно и анализирует акции."""
    for account in ACCOUNTS:
        name = account["name"]
        client = OzonClient(account["client_id"], account["api_key"], session)
        notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, session)
        actions = await client.get_actions()
        for promo in actions:
            if promo.excluded:
                continue
            logger.info("[%s] Проверка акции '%s' (ID %s)", name, promo.title, promo.action_id)
            ids, avg_discount = await client.get_candidates_page(promo.action_id, LIMIT_CANDIDATES)
            if avg_discount > DISCOUNT_THRESHOLD:
                # Рассчитываем, когда собирать товары (5 минут после старта) и когда удалять (10 минут после старта)
                now_msk = datetime.now(tz=TIMEZONE_ACTION)
                start_msk = promo.start_at.astimezone(TIMEZONE_ACTION)
                fetch_time_msk = start_msk + timedelta(minutes=5)
                delete_time_msk = start_msk + timedelta(minutes=10)
                fetch_delay = max((fetch_time_msk - now_msk).total_seconds(), 0)
                delete_delay = max((delete_time_msk - now_msk).total_seconds(), 0)

                # Отправляем уведомление о неприбыльной акции
                await notifier.send_message(
                    f"[{name}] Найдена неприбыльная акция '{promo.title}' (ID {promo.action_id}). "
                    f"Средняя скидка по первым {LIMIT_CANDIDATES} товарам: {avg_discount:.2f}% (> {DISCOUNT_THRESHOLD}%). "
                    f"Соберём список товаров в {fetch_time_msk.strftime('%Y-%m-%d %H:%M:%S %Z')} "
                    f"и удалим их в {delete_time_msk.strftime('%Y-%m-%d %H:%M:%S %Z')}."
                )

                logger.info(
                    "[%s] Акция '%s' неприбыльна: средняя скидка %.2f%% > %.1f%%. "
                    "Собираем товары через %.0f секунд, удаляем через %.0f секунд.",
                    name, promo.title, avg_discount, DISCOUNT_THRESHOLD,
                    fetch_delay, delete_delay
                )

                # Планируем сбор товаров через 5 минут
                asyncio.create_task(
                    fetch_products_task(name, promo, client, notifier, fetch_delay)
                )
                # Планируем удаление товаров через 10 минут
                asyncio.create_task(
                    deactivate_task(name, promo, client, notifier, delete_delay)
                )
            else:
                logger.info(
                    "[%s] Акция '%s' не подходит: средняя скидка %.2f%% ≤ %.1f%%.",
                    name, promo.title, avg_discount, DISCOUNT_THRESHOLD
                )


async def main() -> None:
    """Запускает мониторинг по всем кабинетам в цикле."""
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await monitor_once(session)
            except Exception as exc:
                logger.exception("Unexpected error during monitoring: %s", exc)
            await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("ozon_promo_bot terminated by user")



#### made by Aleksandr Bogdanov (aka Alex System)
