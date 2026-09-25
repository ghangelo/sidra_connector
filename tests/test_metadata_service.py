import pytest

from exceptions import SidraInvalidResponseError
from services.sidra_metadata_service import SidraMetadataService


class DummyHttpClient:
    def __init__(self):
        self.calls = []

    def get_json(self, url, feedback=None):
        self.calls.append(url)
        if url.endswith("/metadados"):
            return {
                "nome": "IPCA - Variação mensal",
                "variaveis": [{"id": 63, "nome": "IPCA - Variação mensal", "unidade": "%"}],
                "classificacoes": [
                    {
                        "id": 315,
                        "nome": "Geral, grupo, subgrupo...",
                        "categorias": [{"id": 7169, "nome": "Índice geral"}],
                    }
                ],
            }
        if url.endswith("/periodos"):
            return [{"id": 202401, "literal": "jan 2024"}, {"id": 202402, "literal": "fev 2024"}]
        if url.endswith("/localidades"):
            return []
        raise AssertionError(f"URL inesperada: {url}")


def test_metadata_service_returns_normalized_metadata():
    service = SidraMetadataService(DummyHttpClient())
    metadata = service.get_table_metadata("7060")

    assert metadata["table_id"] == "7060"
    assert metadata["source"] == "ibge-aggregates-v3"
    assert metadata["variables"][0]["id"] == "63"


def test_metadata_service_raises_clear_error_when_response_is_invalid():
    class FailingClient:
        def get_json(self, url, feedback=None):
            raise SidraInvalidResponseError("estrutura inválida")

    service = SidraMetadataService(FailingClient())

    with pytest.raises(SidraInvalidResponseError):
        service.get_table_metadata("7060")
