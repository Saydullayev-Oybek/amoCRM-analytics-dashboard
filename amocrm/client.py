"""Qayta ishlatiladigan HTTP so'rov qatlami: auth + rate-limit + 429 retry + pagination.

Barcha entity'lar shu bitta qatlamdan foydalanadi — so'rov/pagination/rate-limit/auth
logikasi takrorlanmaydi.
"""

from __future__ import annotations

import logging
import threading
import time

import requests
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
from dlt.sources.helpers.rest_client.paginators import JSONLinkPaginator

from .config import base_url

log = logging.getLogger("amocrm")

# amoCRM limiti: 7 so'rov/sekund (bitta integratsiya uchun). Zaxira uchun 6.
DEFAULT_REQUESTS_PER_SECOND = 6
# HAL javobidagi keyingi sahifa havolasi.
NEXT_URL_PATH = "_links.next.href"


class RateLimiter:
    """Thread-safe throttle: so'rovlar orasida minimal interval saqlaydi."""

    def __init__(self, max_per_second: float) -> None:
        if max_per_second <= 0:
            raise ValueError("max_per_second musbat bo'lishi kerak")
        self._min_interval = 1.0 / max_per_second
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delta = now - self._last
            if delta < self._min_interval:
                time.sleep(self._min_interval - delta)
            self._last = time.monotonic()


class ThrottledSession(requests.Session):
    """Har bir so'rovni rate-limiter orqali o'tkazadi va 429 da Retry-After'ni kutadi.

    amoCRM ro'yxat endpointlari ma'lumot tugaganda HTTP 204 (bo'sh tana) qaytaradi;
    bunday holatda paginator to'xtashi uchun tanani bo'sh JSON obyektiga aylantiramiz.
    """

    def __init__(self, limiter: RateLimiter, max_retries: int = 5) -> None:
        super().__init__()
        self._limiter = limiter
        self._max_retries = max_retries

    def send(self, request, **kwargs):  # type: ignore[override]
        resp = None
        for attempt in range(self._max_retries + 1):
            self._limiter.wait()
            resp = super().send(request, **kwargs)

            if resp.status_code == 429 and attempt < self._max_retries:
                retry_after = _parse_retry_after(resp)
                log.warning(
                    "429 Too Many Requests — %.1fs kutilyapti (urinish %d/%d)",
                    retry_after,
                    attempt + 1,
                    self._max_retries,
                )
                time.sleep(retry_after)
                continue
            break

        if resp is not None and resp.status_code == 204:
            # Bo'sh sahifa: paginator keyingi havolani topmaydi va to'xtaydi.
            resp._content = b"{}"
        return resp


def _parse_retry_after(resp: requests.Response) -> float:
    value = resp.headers.get("Retry-After")
    if not value:
        return 1.0
    try:
        return max(0.0, float(value))
    except ValueError:
        return 1.0


def build_client(
    auth: dict, requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND
) -> RESTClient:
    """auth.json ma'lumotlaridan sozlangan RESTClient yaratadi."""
    limiter = RateLimiter(requests_per_second)
    session = ThrottledSession(limiter)
    return RESTClient(
        base_url=base_url(auth["subdomain"]),
        auth=BearerTokenAuth(auth["access_token"]),
        session=session,
        headers={"Accept": "application/json"},
    )


def paginate(client: RESTClient, path: str, params: dict, data_selector: str):
    """`_links.next.href` bo'yicha barcha sahifalarni kezib, yozuvlarni yield qiladi."""
    pages = client.paginate(
        path=path,
        params=params,
        data_selector=data_selector,
        paginator=JSONLinkPaginator(next_url_path=NEXT_URL_PATH),
    )
    for page in pages:
        yield from page
