# -*- coding: utf-8 -*-

import json
import os
import time

import requests

from ..constants import (
    HTTP_CONNECT_TIMEOUT_SECONDS,
    HTTP_MAX_RETRIES,
    HTTP_READ_TIMEOUT_SECONDS,
    HTTP_RETRY_BASE_DELAY_SECONDS,
    HTTP_RETRY_STATUS_CODES,
    MAX_RESPONSE_BYTES,
    PLUGIN_USER_AGENT,
)
from ..exceptions import (
    SidraBrowserChallengeError,
    SidraCancelledError,
    SidraHttpError,
    SidraInvalidResponseError,
    SidraNetworkError,
)

try:
    from qgis.PyQt.QtCore import QByteArray, QUrl
    from qgis.PyQt.QtNetwork import QNetworkRequest
    from qgis.core import QgsApplication, QgsBlockingNetworkRequest, QgsMessageLog, Qgis

    QGIS_AVAILABLE = True
    HTTP_STATUS_ATTR = QNetworkRequest.Attribute.HttpStatusCodeAttribute
    WARN_LEVEL = Qgis.MessageLevel.Warning
except ImportError:
    QGIS_AVAILABLE = False
    QgsBlockingNetworkRequest = None

LOG_TAG = "SIDRA Connector"


class SidraHttpClient:
    def __init__(self, max_retries=HTTP_MAX_RETRIES, retry_base_delay=HTTP_RETRY_BASE_DELAY_SECONDS):
        self.max_retries = max_retries
        self.retry_base_delay = retry_base_delay

    def get_json(self, url, feedback=None, headers=None):
        last_error = None

        for attempt in range(self.max_retries + 1):
            if feedback is not None and feedback.isCanceled():
                raise SidraCancelledError("Operação cancelada pelo usuário.")

            try:
                if self._can_use_qgis_network():
                    return self._execute_qgis_get_json(url=url, feedback=feedback, headers=headers)
                return self._execute_requests_get_json(url=url, headers=headers)
            except SidraCancelledError:
                raise
            except SidraHttpError as exc:
                last_error = exc
                if exc.status_code not in HTTP_RETRY_STATUS_CODES or attempt >= self.max_retries:
                    raise
                self._wait_before_retry(attempt, feedback=feedback)
            except SidraNetworkError as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                self._wait_before_retry(attempt, feedback=feedback)

        if last_error is not None:
            raise last_error

        raise SidraNetworkError("Falha de rede sem diagnóstico.")

    @staticmethod
    def _can_use_qgis_network():
        if not QGIS_AVAILABLE or QgsBlockingNetworkRequest is None:
            return False
        # Se QgsBlockingNetworkRequest foi substituído por mock em teste, usa ele;
        # caso contrário exige QgsApplication inicializado.
        if getattr(QgsBlockingNetworkRequest, "__module__", "").startswith("qgis"):
            return QgsApplication.instance() is not None
        return True

    def _execute_qgis_get_json(self, url, feedback=None, headers=None):
        request = QNetworkRequest(QUrl(url))
        request.setRawHeader(QByteArray(b"Accept"), QByteArray(b"application/json"))
        request.setRawHeader(QByteArray(b"User-Agent"), QByteArray(PLUGIN_USER_AGENT.encode("utf-8")))

        for name, value in (headers or {}).items():
            request.setRawHeader(
                QByteArray(str(name).encode("utf-8")),
                QByteArray(str(value).encode("utf-8")),
            )

        blocking_request = QgsBlockingNetworkRequest()
        error_code = blocking_request.get(request, forceRefresh=True, feedback=feedback)

        if feedback is not None and feedback.isCanceled():
            raise SidraCancelledError("Operação cancelada pelo usuário.")

        reply = blocking_request.reply()
        status_code = self._read_status_code(reply)
        content_type = self._read_content_type(reply)
        raw_content = bytes(reply.content()) if reply is not None else b""

        if len(raw_content) > MAX_RESPONSE_BYTES:
            raise SidraInvalidResponseError("A resposta excedeu o limite de segurança configurado.")

        text = raw_content.decode("utf-8-sig", errors="replace")

        # Avalia código HTTP antes de error_code para não mascarar HTTP 4xx/5xx como falha de rede
        if status_code > 0 and (status_code < 200 or status_code >= 300):
            message = self._extract_server_message(text)
            raise SidraHttpError(
                message=f"O serviço do IBGE respondeu com HTTP {status_code}: {message}",
                status_code=status_code,
                url=url,
            )

        no_error = getattr(
            getattr(blocking_request, "ErrorCode", blocking_request),
            "NoError",
            0,
        )
        if error_code != no_error:
            message = blocking_request.errorMessage()
            raise SidraNetworkError(f"Falha ao acessar o serviço do IBGE: {message}")

        return self._parse_validated_json(content_type=content_type, text=text)

    def _execute_requests_get_json(self, url, headers=None):
        req_headers = {
            "Accept": "application/json",
            "User-Agent": PLUGIN_USER_AGENT,
        }
        if headers:
            req_headers.update(headers)

        try:
            response = requests.get(
                url,
                headers=req_headers,
                timeout=(HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS),
            )
        except requests.exceptions.RequestException as exc:
            raise SidraNetworkError(f"Falha ao acessar o serviço do IBGE: {exc}") from exc

        raw_content = response.content or b""
        if len(raw_content) > MAX_RESPONSE_BYTES:
            raise SidraInvalidResponseError("A resposta excedeu o limite de segurança configurado.")

        text = raw_content.decode("utf-8-sig", errors="replace")
        status_code = response.status_code
        content_type = (response.headers.get("Content-Type", "") or "").lower()

        if status_code < 200 or status_code >= 300:
            message = self._extract_server_message(text)
            raise SidraHttpError(
                message=f"O serviço do IBGE respondeu com HTTP {status_code}: {message}",
                status_code=status_code,
                url=url,
            )

        return self._parse_validated_json(content_type=content_type, text=text)

    def _parse_validated_json(self, content_type, text):
        if self._looks_like_browser_challenge(content_type=content_type, text=text):
            raise SidraBrowserChallengeError(
                "O endpoint retornou uma página de verificação de navegador em vez de JSON."
            )

        try:
            return json.loads(text)
        except (TypeError, ValueError) as exc:
            raise SidraInvalidResponseError(
                "O serviço respondeu com conteúdo que não é JSON válido."
            ) from exc

    @staticmethod
    def _read_status_code(reply):
        if reply is None:
            return 0
        value = reply.attribute(HTTP_STATUS_ATTR)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _read_content_type(reply):
        if reply is None:
            return ""
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

    def _wait_before_retry(self, attempt, feedback=None):
        delay = self.retry_base_delay * (2 ** attempt)
        jitter = (os.urandom(1)[0] / 255.0) * 0.5
        total_delay = delay + jitter

        if QGIS_AVAILABLE:
            QgsMessageLog.logMessage(
                f"Nova tentativa de acesso em {total_delay:.1f} segundos.",
                LOG_TAG,
                WARN_LEVEL,
            )

        deadline = time.monotonic() + total_delay
        while time.monotonic() < deadline:
            if feedback is not None and feedback.isCanceled():
                raise SidraCancelledError("Operação cancelada pelo usuário.")
            time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
