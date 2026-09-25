# -*- coding: utf-8 -*-

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone

from ..constants import CHECKPOINT_SCHEMA_VERSION
from ..exceptions import SidraCheckpointError


class JsonCheckpointStore:
    def __init__(self, directory):
        self.directory = os.path.abspath(directory)
        os.makedirs(self.directory, exist_ok=True)

    def read(self, key):
        path = self._path_for_key(key)

        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as stream:
                document = json.load(stream)
        except (OSError, ValueError) as exc:
            raise SidraCheckpointError("Não foi possível ler o checkpoint.") from exc

        if document.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
            raise SidraCheckpointError("O checkpoint foi criado por uma versão incompatível.")

        payload = document.get("payload")
        expected_checksum = document.get("checksum")
        actual_checksum = self._checksum(payload)

        if expected_checksum != actual_checksum:
            raise SidraCheckpointError("O checkpoint está corrompido.")

        return payload

    def write(self, key, payload):
        path = self._path_for_key(key)

        document = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "key_hash": self._key_hash(key),
            "checksum": self._checksum(payload),
            "payload": payload,
        }

        file_descriptor, temporary_path = tempfile.mkstemp(
            prefix="sidra-",
            suffix=".tmp",
            dir=self.directory,
        )

        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as stream:
                json.dump(document, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())

            os.replace(temporary_path, path)
        except Exception:
            try:
                os.remove(temporary_path)
            except OSError:
                pass
            raise

    def clear(self):
        for name in os.listdir(self.directory):
            if not name.endswith(".json"):
                continue

            path = os.path.join(self.directory, name)

            try:
                os.remove(path)
            except OSError:
                pass

    def _path_for_key(self, key):
        return os.path.join(self.directory, f"{self._key_hash(key)}.json")

    @staticmethod
    def _key_hash(key):
        return hashlib.sha256(str(key).encode("utf-8")).hexdigest()

    @staticmethod
    def _checksum(payload):
        serialized = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(serialized).hexdigest()
