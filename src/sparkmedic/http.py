"""Tiny dependency-free JSON-over-HTTP client for the Spark monitoring REST API."""

from __future__ import annotations

import json
import random
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

from sparkmedic.auth import AuthProvider, NoAuth
from sparkmedic.version import __version__

RETRY_STATUS = {429, 502, 503, 504}


class SparkApiError(RuntimeError):
    def __init__(self, message: str, status: Optional[int] = None, url: str = ""):
        super().__init__(message)
        self.status = status
        self.url = url


class _SameHostRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects but never forward credentials to a different host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        new = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new is not None:
            old_host = urllib.parse.urlparse(req.full_url).netloc
            new_host = urllib.parse.urlparse(newurl).netloc
            if old_host != new_host:
                for h in list(new.headers):
                    if h.lower() == "authorization":
                        del new.headers[h]
                new.unredirected_hdrs.pop("Authorization", None)
        return new


class HttpClient:
    """GET JSON from ``base_url`` with retries, TLS verification and pluggable auth."""

    def __init__(
        self,
        base_url: str,
        auth: Optional[AuthProvider] = None,
        timeout: float = 60.0,
        verify_tls: bool = True,
        ca_bundle: Optional[str] = None,
        retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/") + "/"
        self.auth = auth or NoAuth()
        self.timeout = timeout
        self.retries = retries
        if ca_bundle:
            ctx = ssl.create_default_context(cafile=ca_bundle)
        else:
            ctx = ssl.create_default_context()
        if not verify_tls:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=ctx), _SameHostRedirectHandler()
        )

    def url(self, path: str, params: Optional[dict[str, Any]] = None) -> str:
        u = urllib.parse.urljoin(self.base_url, path.lstrip("/"))
        if params:
            clean = {k: v for k, v in params.items() if v is not None}
            u += "?" + urllib.parse.urlencode(clean, doseq=True)
        return u

    def get_json(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        url = self.url(path, params)
        last_exc: Optional[Exception] = None
        for attempt in range(self.retries + 1):
            req = urllib.request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
            req.add_header("User-Agent", f"sparkmedic/{__version__}")
            for k, v in self.auth.headers().items():
                req.add_header(k, v)
            try:
                with self._opener.open(req, timeout=self.timeout) as resp:
                    body = resp.read()
                    ctype = resp.headers.get("Content-Type", "")
                return self._decode(body, ctype, url)
            except urllib.error.HTTPError as exc:
                status = exc.code
                snippet = _snippet(exc.read())
                if status in RETRY_STATUS and attempt < self.retries:
                    last_exc = exc
                    self._sleep(attempt, exc.headers.get("Retry-After"))
                    continue
                hint = ""
                if status in (401, 403):
                    hint = " (authentication failed or insufficient permissions; check your token/profile)"
                elif status == 404:
                    hint = " (endpoint or application not found)"
                raise SparkApiError(f"HTTP {status} for {_safe(url)}{hint}: {snippet}", status, url) from None
            except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                last_exc = exc
                if attempt < self.retries:
                    self._sleep(attempt, None)
                    continue
                raise SparkApiError(f"Cannot reach {_safe(url)}: {exc}", None, url) from None
        raise SparkApiError(f"Request failed for {_safe(url)}: {last_exc}", None, url)

    @staticmethod
    def _decode(body: bytes, ctype: str, url: str) -> Any:
        try:
            return json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            looks_html = b"<html" in body[:500].lower() or "html" in ctype
            hint = (
                "Got an HTML page instead of JSON. This usually means a login redirect "
                "(missing/expired credentials) or a URL that is not a Spark REST endpoint."
                if looks_html
                else "Response was not valid JSON."
            )
            raise SparkApiError(f"{hint} URL: {_safe(url)}", None, url) from None

    @staticmethod
    def _sleep(attempt: int, retry_after: Optional[str]) -> None:
        if retry_after and retry_after.isdigit():
            time.sleep(min(float(retry_after), 30.0))
            return
        time.sleep(min(2**attempt + random.random(), 20.0))


def _snippet(body: bytes, n: int = 300) -> str:
    text = body.decode("utf-8", errors="replace").strip().replace("\n", " ")
    return text[:n]


def _safe(url: str) -> str:
    """Strip query strings that could carry secrets from error messages."""
    parts = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
