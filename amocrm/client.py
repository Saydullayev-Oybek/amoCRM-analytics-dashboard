"""Qayta ishlatiladigan HTTP so'rov qatlami: auth + rate-limit + 429 retry + pagination.

Barcha entity'lar shu bitta qatlamdan foydalanadi — so'rov/pagination/rate-limit/auth
logikasi takrorlanmaydi.
"""

from __future__ import annotations

import logging      # log xabarlarini chiqarish uchun standart kutubxona
import threading    # bir nechta oqim (thread) bir vaqtda ishlaganda qulf (lock) uchun
import time         # kutish (sleep) va vaqtni o'lchash (monotonic) uchun

import requests     # HTTP so'rovlar yuboradigan asosiy kutubxona
from dlt.sources.helpers.rest_client import RESTClient                    # dlt'ning tayyor REST klienti
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth         # Bearer token bilan avtorizatsiya
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator # HAL havolalari bo'yicha sahifalash

from .config import base_url  # subdomen'dan bazaviy URL yasaydigan yordamchi

log = logging.getLogger("amocrm")  # "amocrm" nomli logger — barcha modul shu nom bilan yozadi

# amoCRM limiti: 7 so'rov/sekund (bitta integratsiya uchun). Zaxira uchun 6.
DEFAULT_REQUESTS_PER_SECOND = 6
# HAL javobidagi keyingi sahifa havolasi.
NEXT_URL_PATH = "_links.next.href"


class RateLimiter:
    """Thread-safe throttle: so'rovlar orasida minimal interval saqlaydi."""

    def __init__(self, max_per_second: float) -> None:
        # max_per_second: sekundiga ruxsat etilgan maksimal so'rov soni.
        if max_per_second <= 0:                                  # noto'g'ri qiymatdan himoya
            raise ValueError("max_per_second musbat bo'lishi kerak")
        self._min_interval = 1.0 / max_per_second                # ikki so'rov orasidagi eng kichik vaqt (sekund)
        self._lock = threading.Lock()                            # bir vaqtda faqat bitta oqim o'tishi uchun qulf
        self._last = 0.0                                         # oxirgi so'rov yuborilgan vaqt (monotonic)

    def wait(self) -> None:
        # Har so'rovdan oldin chaqiriladi; kerak bo'lsa biroz kutadi.
        with self._lock:                                         # qulfni olamiz — hisob-kitob atomik bo'lsin
            now = time.monotonic()                               # hozirgi vaqt (orqaga qaytmaydigan soat)
            delta = now - self._last                             # oxirgi so'rovdan beri o'tgan vaqt
            if delta < self._min_interval:                       # juda tez kelgan bo'lsak
                time.sleep(self._min_interval - delta)           # yetishmagan vaqtcha kutamiz
            self._last = time.monotonic()                        # so'rov vaqtini yangilaymiz


class ThrottledSession(requests.Session):
    """Har bir so'rovni rate-limiter orqali o'tkazadi va 429 da Retry-After'ni kutadi.

    amoCRM ro'yxat endpointlari ma'lumot tugaganda HTTP 204 (bo'sh tana) qaytaradi;
    bunday holatda paginator to'xtashi uchun tanani bo'sh JSON obyektiga aylantiramiz.
    """

    def __init__(self, limiter: RateLimiter, max_retries: int = 5) -> None:
        super().__init__()                    # requests.Session'ning o'z sozlashini bajaramiz
        self._limiter = limiter               # rate-limiter'ni saqlab qo'yamiz
        self._max_retries = max_retries       # xatoda necha marta qayta urinish

    def send(self, request, **kwargs):  # type: ignore[override]
        # requests har bir tayyor so'rovni shu metod orqali yuboradi — biz uni "ushlab" qoldik.
        resp = None                                          # javob (response) — hozircha bo'sh
        for attempt in range(self._max_retries + 1):         # 0-urinishdan boshlab, retry'lar bilan
            self._limiter.wait()                             # rate-limitni hurmat qilib, kerak bo'lsa kutamiz
            # dlt RESTClient.paginate javobga raise_for_status hook o'rnatadi,
            # shu sabab 429 super().send() ichida HTTPError ko'tarishi mumkin.
            # Uni ushlaymiz va 429 bo'lsa qayta urinamiz.
            try:
                resp = super().send(request, **kwargs)       # haqiqiy HTTP so'rovni yuboramiz
            except requests.exceptions.HTTPError as exc:     # 4xx/5xx hook orqali xato ko'tarsa
                resp = exc.response                          # xato ichidagi javobni olamiz
                if self._should_retry(resp, attempt):        # bu 429 va urinish qolgan bo'lsa
                    self._sleep_for_retry(resp, attempt)     # Retry-After'cha kutamiz
                    continue                                 # keyingi urinishga o'tamiz
                raise                                        # aks holda xatoni yuqoriga uzatamiz
            except (
                requests.exceptions.ConnectionError,          # ulanish uzilishi
                requests.exceptions.ChunkedEncodingError,     # yarim kelgan (buzilgan) javob
            ) as exc:
                # amoCRM uzoq paginatsiyada (masalan events backfill'ida) ba'zan
                # ulanishni javobsiz uzadi (RemoteDisconnected). Bu vaqtinchalik —
                # backoff bilan qayta urinamiz, aks holda butun pipeline yiqiladi.
                if attempt < self._max_retries:               # urinish qolgan bo'lsa
                    self._sleep_for_connection_retry(exc, attempt)  # backoff bilan kutamiz
                    continue                                  # qayta urinamiz
                raise                                         # urinishlar tugadi — xatoni uzatamiz

            if self._should_retry(resp, attempt):             # hook ishlamagan holatda ham 429'ni tekshiramiz
                self._sleep_for_retry(resp, attempt)          # kutamiz
                continue                                      # qayta urinamiz
            break                                             # javob muvaffaqiyatli — sikldan chiqamiz

        if resp is not None and resp.status_code == 204:      # 204 = bo'sh sahifa (ma'lumot tugadi)
            # Bo'sh sahifa: paginator keyingi havolani topmaydi va to'xtaydi.
            resp._content = b"{}"                             # tanani bo'sh JSON'ga almashtiramiz
        return resp                                           # yakuniy javobni qaytaramiz

    def _should_retry(self, resp, attempt: int) -> bool:
        # 429 (Too Many Requests) bo'lsa VA urinishlar qolgan bo'lsa — True qaytaradi.
        return (
            resp is not None                                  # javob mavjud bo'lsa
            and resp.status_code == 429                        # va u 429 bo'lsa
            and attempt < self._max_retries                    # va yana urinish mumkin bo'lsa
        )

    def _sleep_for_retry(self, resp, attempt: int) -> None:
        # 429 dan keyin qancha kutishni Retry-After sarlavhasidan hisoblab kutadi.
        retry_after = _parse_retry_after(resp)                 # kutish vaqtini olamiz (sekund)
        log.warning(                                           # ogohlantirish yozamiz
            "429 Too Many Requests — %.1fs kutilyapti (urinish %d/%d)",
            retry_after,
            attempt + 1,
            self._max_retries,
        )
        time.sleep(retry_after)                                # belgilangan vaqtcha kutamiz

    def _sleep_for_connection_retry(self, exc: Exception, attempt: int) -> None:
        # Eksponensial backoff (2, 4, 8, ...), lekin 30s dan oshmasin.
        backoff = min(2.0 ** attempt, 30.0)                    # 2^urinish, lekin maksimum 30 sekund
        log.warning(                                           # nima bo'lganini logga yozamiz
            "Ulanish uzildi (%s) — %.1fs kutib qayta urinamiz (urinish %d/%d)",
            type(exc).__name__,
            backoff,
            attempt + 1,
            self._max_retries,
        )
        time.sleep(backoff)                                    # backoff vaqtcha kutamiz


def _parse_retry_after(resp: requests.Response) -> float:
    # Retry-After sarlavhasini o'qib, sekundlarda son qaytaradi.
    value = resp.headers.get("Retry-After")                    # sarlavhani olamiz (bo'lmasligi mumkin)
    if not value:                                              # sarlavha yo'q bo'lsa
        return 1.0                                             # standart 1 sekund
    try:
        return max(0.0, float(value))                          # sonli qiymat (manfiy bo'lmasin)
    except ValueError:                                         # son emas (masalan sana formati) bo'lsa
        return 1.0                                             # xavfsiz standart qiymat


def build_client(
    auth: dict, requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND
) -> RESTClient:
    """auth.json ma'lumotlaridan sozlangan RESTClient yaratadi."""
    limiter = RateLimiter(requests_per_second)                 # rate-limiter yaratamiz (6 req/s)
    session = ThrottledSession(limiter)                        # o'zimizning maxsus session (retry+throttle)
    return RESTClient(                                         # dlt REST klientini sozlab qaytaramiz
        base_url=base_url(auth["subdomain"]),                  # https://<subdomain>.amocrm.ru
        auth=BearerTokenAuth(auth["access_token"]),            # har so'rovga Bearer token qo'shadi
        session=session,                                       # bizning throttled session'ni beramiz
        headers={"Accept": "application/json"},                # javobni JSON ko'rinishida so'raymiz
    )


def paginate(client: RESTClient, path: str, params: dict, data_selector: str):
    """`_links.next.href` bo'yicha barcha sahifalarni kezib, yozuvlarni yield qiladi."""
    pages = client.paginate(                                   # dlt sahifalarni birma-bir qaytaradigan generator
        path=path,                                             # endpoint yo'li, masalan /api/v4/leads
        params=params,                                         # so'rov parametrlari (limit, filter, order...)
        data_selector=data_selector,                           # JSON ichida ma'lumot qayerda (_embedded.leads)
        paginator=JSONLinkPaginator(next_url_path=NEXT_URL_PATH),  # keyingi sahifa havolasi qayerda
    )
    for page in pages:                                         # har bir sahifa ustidan yuramiz
        yield from page                                        # sahifadagi har bir yozuvni tashqariga chiqaramiz
