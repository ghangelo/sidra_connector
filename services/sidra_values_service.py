# -*- coding: utf-8 -*-

import re
from urllib.parse import urlsplit, urlunsplit

from ..exceptions import SidraHttpError, SidraInvalidResponseError, SidraQueryTooLargeError


class SidraValuesService:
    PERIOD_TOKEN = "/p/"

    def __init__(self, http_client, checkpoint_store=None):
        self.http_client = http_client
        self.checkpoint_store = checkpoint_store

    def collect(self, query_url, periods, batch_size=12, feedback=None, progress_callback=None):
        periods = [str(period).strip() for period in periods]
        periods = [period for period in periods if period]

        if not periods:
            raise ValueError("É necessário informar pelo menos um período.")

        batches = self._split_batches(periods, batch_size)
        all_rows = []

        for index, batch in enumerate(batches):
            if feedback is not None and feedback.isCanceled():
                break

            batch_key = self._batch_key(query_url, batch)
            cached = self._read_checkpoint(batch_key)

            if cached is not None:
                rows = cached
            else:
                rows = self._download_adaptive(query_url=query_url, periods=batch, feedback=feedback)
                self._write_checkpoint(batch_key, rows)

            all_rows.extend(rows)

            if progress_callback is not None:
                progress_callback(index + 1, len(batches), batch)

        return self._merge_rows(all_rows)

    def _download_adaptive(self, query_url, periods, feedback=None):
        batch_url = self.replace_periods(query_url, periods)

        try:
            payload = self.http_client.get_json(batch_url, feedback=feedback)
            return self._validate_values_payload(payload)
        except Exception as exc:
            if not self._is_query_too_large(exc):
                raise

            if len(periods) <= 1:
                raise SidraQueryTooLargeError(
                    "Mesmo a consulta de um único período excedeu o limite aceito pelo SIDRA. Reduza também as variáveis, localidades ou categorias."
                ) from exc

            middle = len(periods) // 2
            left_rows = self._download_adaptive(query_url=query_url, periods=periods[:middle], feedback=feedback)
            right_rows = self._download_adaptive(query_url=query_url, periods=periods[middle:], feedback=feedback)
            return left_rows + right_rows

    @classmethod
    def replace_periods(cls, query_url, periods):
        parsed = urlsplit(query_url)
        path = parsed.path
        period_value = ",".join(periods)

        pattern = re.compile(r"/p/[^/]+", re.IGNORECASE)
        if pattern.search(path):
            new_path = pattern.sub(f"/p/{period_value}", path, count=1)
        else:
            new_path = path.rstrip("/") + f"/p/{period_value}"

        return urlunsplit((parsed.scheme, parsed.netloc, new_path, parsed.query, parsed.fragment))

    @staticmethod
    def _validate_values_payload(payload):
        if not isinstance(payload, list):
            raise SidraInvalidResponseError("A API de valores retornou uma estrutura inesperada.")

        if not payload:
            return []

        first_item = payload[0]
        if isinstance(first_item, dict):
            possible_message = first_item.get("erro") or first_item.get("error") or first_item.get("message")
            if possible_message:
                raise SidraInvalidResponseError(str(possible_message))

        return payload

    @staticmethod
    def _is_query_too_large(exc):
        if isinstance(exc, SidraHttpError):
            if exc.status_code in {413, 414}:
                return True

        message = str(exc).lower()
        indicators = (
            "more than 20k",
            "more than 100000",
            "mais de 20 mil",
            "mais de 100000",
            "too many values",
            "too many records",
            "request entity too large",
            "query will result",
            "consulta resultará",
            "excede o limite",
        )

        return any(indicator in message for indicator in indicators)

    @staticmethod
    def _split_batches(values, batch_size):
        if batch_size <= 0:
            raise ValueError("O tamanho do lote deve ser maior que zero.")
        return [values[index:index + batch_size] for index in range(0, len(values), batch_size)]

    @staticmethod
    def _merge_rows(rows):
        if not rows:
            return []

        merged = []
        header = None

        for item in rows:
            if not isinstance(item, dict):
                merged.append(item)
                continue

            if ValuesPayload.is_header(item):
                if header is None:
                    header = item
                    merged.append(item)
                continue

            merged.append(item)

        return merged

    def _read_checkpoint(self, key):
        if self.checkpoint_store is None:
            return None
        return self.checkpoint_store.read(key)

    def _write_checkpoint(self, key, rows):
        if self.checkpoint_store is not None:
            self.checkpoint_store.write(key, rows)

    @staticmethod
    def _batch_key(query_url, periods):
        return f"{query_url}|{','.join(periods)}"


class ValuesPayload:
    @staticmethod
    def is_header(item):
        if not isinstance(item, dict):
            return False

        value = str(item.get("V", "")).strip().lower()
        return value in {"valor", "value"}
