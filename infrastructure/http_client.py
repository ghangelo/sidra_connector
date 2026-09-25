# -*- coding: utf-8 -*-

import json
import random
import time

from qgis.PyQt.QtCore import QByteArray, QUrl
from qgis.PyQt.QtNetwork import QNetworkRequest
from qgis.core import QgsBlockingNetworkRequest, QgsMessageLog, Qgis

from ..constants import (
    HTTP_MAX_RETRIES,
    HTTP_RETRY_BASE_DELAY_SECONDS,
    HTTP_RETRY_STATUS_CODES,
    MAX_RESPONSE_BYTES,
    PLUGIN_USER_AGENT,
)
from ..exceptions import (
    SidraBrowserChallengeError,
    SidraHttpError,
    SidraInvalidResponseError,
    SidraNetworkError,
)

LOG_TAG = "SIDRA Connector"


class SidraHttpClient:
    def __init__(self, max_retries=HTTP_MAX_RETRIES, retry_base_delay=HTTP_RETRY_BASE_DELAY_SECONDS):
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay

    def get_json(self, url, feedback=None, headers=None):
        last_error = None

        for attempt in range(self.max_retries + 1):
            if feedback is not None and feedback.isCanceled():
                raise SidraNetworkError("Operação cancelada pelo usuário.")

            try:
                return self._execute_get_json(url=url, feedback=feedback, headers=headers)
            except SidraHttpError as exc:
                last_error = exc
                if exc.status_code not in HTTP_RETRY_STATUS_CODES:
                    raise
                if attempt >= self.max_retries:
                    raise
                self._wait_before_retry(attempt)
            except SidraNetworkError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                self._wait_before_retry(attempt)

        if last_error is not None:
            raise last_error

        raise SidraNetworkError("Falha de rede sem diagnóstico.")

    def _execute_get_json(self, url, feedback=None, headers=None):
        request = QNetworkRequest(QUrl(url))

        request.setRawHeader(QByteArray(b"Accept"), QByteArray(b"application/json"))
        request.setRawHeader(QByteArray(b"User-Agent"), QByteArray(PLUGIN_USER_AGENT.encode("utf-8")))

        for name, value in (headers or {}).items():
            request.setRawHeader(QByteArray(str(name).encode("utf-8")), QByteArray(str(value).encode("utf-8")))

        blocking_request = QgsBlockingNetworkRequest()
        error_code = blocking_request.get(request, forceRefresh=True, feedback=feedback)
        reply = blocking_request.reply()

        if error_code != QgsBlockingNetworkRequest.NoError:
            message = blocking_request.errorMessage()
            raise SidraNetworkError(f"Falha ao acessar o serviço do IBGE: {message}")

        status_code = self._read_status_code(reply)
        content_type = self._read_content_type(reply)
        raw_content = bytes(reply.content())

        if len(raw_content) > MAX_RESPONSE_BYTES:
            raise SidraInvalidResponseError("A resposta excedeu o limite de segurança configurado.")

        text = raw_content.decode("utf-8-sig", errors="replace")

        if status_code < 200 or status_code >= 300:
            message = self._extract_server_message(text)
            raise SidraHttpError(
                message=f"O serviço do IBGE respondeu com HTTP {status_code}: {message}",
                status_code=status_code,
                url=url,
            )

        if self._looks_like_browser_challenge(content_type=content_type, text=text):
            raise SidraBrowserChallengeError("O endpoint retornou uma página de verificação de navegador em vez de JSON.")

        try:
            return json.loads(text)
        except (TypeError, ValueError) as exc:
            raise SidraInvalidResponseError("O serviço respondeu com conteúdo que não é JSON válido.") from exc

    @staticmethod
    def _read_status_code(reply):
        attribute = QNetworkRequest.HttpStatusCodeAttribute
        value = reply.attribute(attribute)

        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _read_content_type(reply):
        value = reply.rawHeader(b"Content-Type")

        if hasattr(value, "data"):
            value = value.data()

        if isinstance(value, bytes):
            return value.decode("latin-1", errors="replace").lower()

        return str(value or "").lower()

    @staticmethod
    def _looks_like_browser_challenge(content_type, text):
        normalized = (text or "").strip().lower()

        if "text/html" in content_type:
            return True
        if normalized.startswith("<!doctype html"):
            return True
        if normalized.startswith("<html"):
            return True

        indicators = (
            "checking your browser",
            "verify you are human",
            "browser challenge",
            "enable javascript",
            "access denied",
            "just a moment",
            "cf-chl-",
            "cloudflare",
        )

        return any(indicator in normalized for indicator in indicators)

    @staticmethod
    def _extract_server_message(text):
        normalized = " ".join((text or "").split())
        if not normalized:
            return "resposta vazia"
        return normalized[:500]

    def _wait_before_retry(self, attempt):
        delay = self.retry_base_delay * (2 ** attempt)
        jitter = random.uniform(0, 0.5)
        total_delay = delay + jitter

        QgsMessageLog.logMessage(f"Nova tentativa de acesso em {total_delay:.1f} segundos.", LOG_TAG, Qgis.Warning)
        time.sleep(total_delay)
