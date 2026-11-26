# -*- coding: utf-8 -*-
import unittest

import tests  # noqa: F401 - registra pacote sidra_connector
from sidra_connector.exceptions import SidraInvalidResponseError
from sidra_connector.services.sidra_metadata_service import SidraMetadataService


class DummyHttpClient:
    def __init__(self):
        self.calls = []

    def get_json(self, url, feedback=None):
        self.calls.append(url)
        if url.endswith("/metadados"):
            return {
                "nome": "IPCA - Variação mensal",
                "nivelTerritorial": {
                    "Administrativo": ["N1", "N6", "N7"],
                    "Especial": [],
                    "IBGE": [],
                },
                "variaveis": [{"id": 63, "nome": "IPCA - Variação mensal", "unidade": "%"}],
                "classificacoes": [
                    {
                        "id": 315,
                        "nome": "Geral, grupo, subgrupo...",
                        "categorias": [
                            {"id": 7169, "nome": "Índice geral", "nivel": 0},
                            {"id": 7170, "nome": "1.Alimentação e bebidas", "nivel": 1},
                        ],
                    }
                ],
            }
        if url.endswith("/periodos"):
            return [
                {"id": "202401", "literals": ["janeiro 2024", "janeiro de 2024"]},
                {"id": "202402", "literals": ["fevereiro 2024", "fevereiro de 2024"]},
            ]
        raise AssertionError(f"URL inesperada chamada: {url}")


class TestSidraMetadataService(unittest.TestCase):
    def test_metadata_service_returns_normalized_metadata_without_404_localidades_call(self):
        http = DummyHttpClient()
        service = SidraMetadataService(http)
        metadata = service.get_table_metadata("7060")

        self.assertEqual(metadata["table_id"], "7060")
        self.assertEqual(metadata["source"], "ibge-aggregates-v3")
        self.assertEqual(metadata["variables"][0]["id"], "63")
        # Issue #7: deve extrair literals[0] (lista plural retornada pela API v3)
        self.assertEqual(metadata["periods"][0]["name"], "janeiro 2024")
        # Issue #7: não deve chamar /localidades sem nível (que sempre retorna 404)
        self.assertEqual(len(http.calls), 2)
        self.assertFalse(any(url.endswith("/localidades") for url in http.calls))

    def test_metadata_service_raises_clear_error_when_response_is_invalid(self):
        class FailingClient:
            def get_json(self, url, feedback=None):
                raise SidraInvalidResponseError("estrutura inválida")

        service = SidraMetadataService(FailingClient())

        with self.assertRaises(SidraInvalidResponseError):
            service.get_table_metadata("7060")


if __name__ == "__main__":
    unittest.main()
