# -*- coding: utf-8 -*-
import configparser
import io
import os
import unittest
import xml.etree.ElementTree as ET
import zipfile
from unittest.mock import patch

import tests  # noqa: F401 - registra pacote sidra_connector
from sidra_connector.core import api_helpers
from sidra_connector.core.mesh_downloader import MeshDownloader
from sidra_connector.core.sidra_api_client import SidraApiClient, _parse_numeric
from sidra_connector.exceptions import (
    SidraCancelledError,
    SidraHttpError,
    SidraQueryTooLargeError,
)
from sidra_connector.services.sidra_values_service import SidraValuesService

try:
    from qgis.core import (
        QgsApplication,
        QgsFeature,
        QgsField,
        QgsGeometry,
        QgsPointXY,
        QgsVectorLayer,
    )
    try:
        from qgis.PyQt.QtCore import QMetaType
        STR_FIELD_TYPE = QMetaType.Type.QString
        DBL_FIELD_TYPE = QMetaType.Type.Double
    except (ImportError, AttributeError):
        from qgis.PyQt.QtCore import QVariant
        STR_FIELD_TYPE = QVariant.String
        DBL_FIELD_TYPE = QVariant.Double

    QGIS_AVAILABLE = True
    import shutil as _shutil
    import atexit as _atexit
    import tempfile as _tempfile
    _QGS_PROFILE_DIR = _tempfile.mkdtemp(prefix="sidra_qgis_test_")
    os.environ.setdefault("QGIS_CUSTOM_CONFIG_PATH", _QGS_PROFILE_DIR)
    _atexit.register(_shutil.rmtree, _QGS_PROFILE_DIR, True)
    _QGS_APP = QgsApplication.instance() or QgsApplication([], False, _QGS_PROFILE_DIR)
except ImportError:
    QGIS_AVAILABLE = False
    _QGS_APP = None


class TestIssue1And7MetadataAndTerritorialLevels(unittest.TestCase):
    """Issues #1 e #7: N6 = Município, filtro por nivelTerritorial, literals plural e indentação."""

    def test_normalize_aggregate_metadata_maps_n6_to_municipio_and_filters_levels(self):
        metadata_payload = {
            "id": 5938,
            "nome": "PIB dos Municípios",
            "nivelTerritorial": {
                "Administrativo": ["N1", "N2", "N3", "N6", "N8", "N9"],
                "Especial": [],
                "IBGE": [],
            },
            "variaveis": [{"id": 37, "nome": "PIB a preços correntes", "unidade": "Mil Reais"}],
            "classificacoes": [
                {
                    "id": 2,
                    "nome": "Sexo",
                    "categorias": [
                        {"id": 0, "nome": "Total", "nivel": 0},
                        {"id": 4, "nome": "Homens", "nivel": 1},
                    ],
                }
            ],
        }
        periods_payload = [
            {"id": "202101", "literals": ["janeiro 2021", "janeiro de 2021"]},
        ]

        normalized = api_helpers._normalize_aggregate_metadata("5938", metadata_payload, periods_payload)

        # Issue #7: literals plural
        self.assertEqual(normalized["Periodos"]["Periodos"][0]["Nome"], "janeiro 2021")

        # Issue #7: indentação das categorias preservada a partir de 'nivel'
        cats = normalized["Classificacoes"][0]["Categorias"]
        self.assertEqual(cats[0]["IdentacaoApresentacao"], 0)
        self.assertEqual(cats[1]["IdentacaoApresentacao"], 1)

        # Issue #1: N6 deve ser Município, N7 NÃO deve aparecer para a tabela 5938!
        territorios = normalized["Territorios"]
        dic_niveis = dict(
            zip(
                territorios["DicionarioNiveis"]["Ids"],
                territorios["DicionarioNiveis"]["Nomes"],
            )
        )
        niveis_ids = [n["Id"] for n in territorios["NiveisTabela"]]

        self.assertEqual(niveis_ids, [1, 2, 3, 6, 8, 9])
        self.assertNotIn(7, niveis_ids)
        self.assertIn("Município", dic_niveis[6])
        self.assertIn("Mesorregião", dic_niveis[8])
        self.assertIn("Microrregião", dic_niveis[9])

        # E montar_url_interativa com Município deve gerar /n6/all
        url = api_helpers.montar_url_interativa(
            "5938",
            (6, dic_niveis[6], "N6"),
            [(37, "PIB", "Mil Reais")],
            [("202101", "janeiro 2021", "202101")],
            {},
        )
        self.assertIn("/n6/all/", url)


class TestIssue2MultiDimensionalParsing(unittest.TestCase):
    """Issue #2: Colunas multidimensionais sem perda de nome nem desalinhamento."""

    def test_multi_variable_and_multi_period_preserves_both_names(self):
        client = SidraApiClient(5938)
        header = {
            "NC": "Nível Territorial (Código)",
            "NN": "Nível Territorial",
            "V": "Valor",
            "D1C": "Unidade da Federação (Código)",
            "D1N": "Unidade da Federação",
            "D2C": "Variável (Código)",
            "D2N": "Variável",
            "D3C": "Ano (Código)",
            "D3N": "Ano",
        }
        rows = [
            {"NC": "3", "NN": "Unidade da Federação", "V": "100", "D1C": "11", "D1N": "Rondônia", "D2N": "PIB", "D3N": "2020"},
            {"NC": "3", "NN": "Unidade da Federação", "V": "110", "D1C": "11", "D1N": "Rondônia", "D2N": "PIB", "D3N": "2021"},
            {"NC": "3", "NN": "Unidade da Federação", "V": "20", "D1C": "11", "D1N": "Rondônia", "D2N": "Agro", "D3N": "2020"},
            {"NC": "3", "NN": "Unidade da Federação", "V": "25", "D1C": "11", "D1N": "Rondônia", "D2N": "Agro", "D3N": "2021"},
        ]
        columns = list(header.keys())
        data_dict, _ = client._convert_rows_to_dict(rows, columns, header)

        cols_ro = data_dict["11"]
        self.assertEqual(
            cols_ro,
            {
                "PIB_2020": 100.0,
                "PIB_2021": 110.0,
                "Agro_2020": 20.0,
                "Agro_2021": 25.0,
            },
        )

    def test_ibge_special_missing_symbols_return_none(self):
        self.assertEqual(_parse_numeric("-"), 0.0)
        for symbol in ("...", "..", "X", "x"):
            self.assertIsNone(_parse_numeric(symbol))
        self.assertEqual(_parse_numeric("1.234,56"), 1234.56)

    def test_apply_uf_filter_to_url_scopes_sub_uf_levels_only(self):
        from sidra_connector.core.sidra_api_client import apply_uf_filter_to_url

        base_n6 = "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/2021"
        self.assertEqual(
            apply_uf_filter_to_url(base_n6, ["41"]),
            "https://apisidra.ibge.gov.br/values/t/5938/n6/in%20n3%2041/v/37/p/2021",
        )
        self.assertEqual(
            apply_uf_filter_to_url(base_n6, ["42", "41"]),
            "https://apisidra.ibge.gov.br/values/t/5938/n6/in%20n3%2041,42/v/37/p/2021",
        )
        all_27 = [
            "11", "12", "13", "14", "15", "16", "17",
            "21", "22", "23", "24", "25", "26", "27", "28", "29",
            "31", "32", "33", "35",
            "41", "42", "43",
            "50", "51", "52", "53",
        ]
        self.assertEqual(apply_uf_filter_to_url(base_n6, all_27), base_n6)

        base_n3 = "https://apisidra.ibge.gov.br/values/t/5938/n3/all/v/37/p/2021"
        self.assertEqual(apply_uf_filter_to_url(base_n3, ["41"]), base_n3)

        base_n8 = "https://apisidra.ibge.gov.br/values/t/5938/n8/all/v/37/p/2021"
        self.assertEqual(
            apply_uf_filter_to_url(base_n8, ["41"]),
            "https://apisidra.ibge.gov.br/values/t/5938/n8/in%20n3%2041/v/37/p/2021",
        )

    def test_sidra_api_client_falls_back_to_all_when_uf_filter_returns_empty(self):
        calls = []

        class FakeHttp:
            def get_json(self, url, feedback=None, headers=None):
                calls.append(url)
                if "/n6/in%20n3%2041/" in url:
                    return [{"V": "Valor", "D1C": "Município (Código)", "D2N": "Variável"}]
                return [
                    {"NC": "Nível", "NN": "Nível", "V": "Valor", "D1C": "Município (Código)", "D1N": "Município", "D2N": "Variável"},
                    {"NC": "6", "NN": "Município", "V": "15", "D1C": "1200401", "D1N": "Rio Branco - AC", "D2N": "IPCA"},
                ]

        client = SidraApiClient(
            "https://apisidra.ibge.gov.br/values/t/7060/n6/in%20n3%2041/v/63/p/202401/c315/7169",
            http_client=FakeHttp(),
        )
        data, _ = client.fetch_and_parse()
        self.assertEqual(data, {"1200401": {"IPCA": 15.0}})
        self.assertEqual(len(calls), 2)
        self.assertIn("/n6/all/", calls[1])


class TestIssue3ValuesServiceAndHttpClient(unittest.TestCase):
    """Issue #3: SidraValuesService detecta 'excedeu o limite' real do IBGE e SidraHttpClient trata HTTP 400."""

    def test_values_service_detects_portuguese_past_tense_excedeu_o_limite(self):
        calls = []

        class FakeHttp:
            def get_json(self, url, feedback=None):
                calls.append(url)
                if "/p/2020,2021" in url:
                    raise SidraHttpError(
                        "O serviço do IBGE respondeu com HTTP 400: Quantidade de valores solicitados: 120000 excedeu o limite: 100000",
                        status_code=400,
                        url=url,
                    )
                period = url.split("/p/")[1]
                return [
                    {"V": "Valor", "D1C": "Município (Código)", "D3N": "Ano"},
                    {"V": "10", "D1C": "3550308", "D3N": period},
                ]

        svc = SidraValuesService(FakeHttp())
        merged = svc.collect(
            "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/2020,2021",
            periods=["2020", "2021"],
            batch_size=12,
        )
        self.assertEqual(len(merged), 3)  # 1 header + 2 data rows
        self.assertEqual([r["D3N"] for r in merged[1:]], ["2020", "2021"])

    def test_sidra_api_client_splits_large_multi_period_queries_via_values_service(self):
        calls = []

        class FakeHttp:
            def get_json(self, url, feedback=None, headers=None):
                calls.append(url)
                if "/p/2020,2021" in url:
                    raise SidraHttpError(
                        "HTTP 400: Quantidade de valores solicitados: 150000 excedeu o limite: 100000",
                        status_code=400,
                        url=url,
                    )
                period = url.split("/p/")[1]
                return [
                    {"NC": "Nível", "NN": "Nível", "V": "Valor", "D1C": "Município (Código)", "D1N": "Município", "D2N": "Variável", "D3N": "Ano"},
                    {"NC": "6", "NN": "Município", "V": "42", "D1C": "3550308", "D1N": "São Paulo", "D2N": "PIB", "D3N": period},
                ]

        client = SidraApiClient(
            "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/37/p/2020,2021",
            http_client=FakeHttp(),
        )
        progress_calls = []
        data, _ = client.fetch_and_parse(
            progress_callback=lambda done, total, batch: progress_calls.append((done, total, batch))
        )
        self.assertEqual(data, {"3550308": {"2020": 42.0, "2021": 42.0}})
        self.assertEqual(len(calls), 3)  # 1 tentativa combinada + 2 lotes divididos
        self.assertTrue(len(progress_calls) >= 1)

    def test_sidra_api_client_single_period_too_large_raises_query_too_large_error(self):
        class TooLargeHttp:
            def get_json(self, url, feedback=None, headers=None):
                raise SidraHttpError(
                    "O serviço do IBGE respondeu com HTTP 400: Quantidade de valores solicitados: 256220 excedeu o limite",
                    status_code=400,
                    url=url,
                )

        client = SidraApiClient(
            "https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/all/p/2021",
            http_client=TooLargeHttp(),
        )
        with self.assertRaises(SidraQueryTooLargeError):
            client.fetch_and_parse()

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_get_metadata_from_api_cancel_does_not_log_critical(self):
        class CancellingHttp:
            def get_json(self, url, feedback=None, headers=None):
                raise SidraCancelledError("Operação cancelada pelo usuário.")

        with patch("sidra_connector.core.api_helpers.QgsMessageLog", create=True) as mock_log:
            res = api_helpers.get_metadata_from_api("5938", http_client=CancellingHttp())

        self.assertIsNone(res)
        # Se logar algo além da linha inicial Info, não pode usar nível Critical
        for call in mock_log.logMessage.call_args_list:
            args = call.args
            if len(args) >= 3:
                self.assertNotEqual(args[2], api_helpers.Qgis.MessageLevel.Critical)

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_http_client_does_not_retry_http_400_and_preserves_status_code(self):
        from sidra_connector.infrastructure.http_client import SidraHttpClient

        client = SidraHttpClient(max_retries=3, retry_base_delay=0.01)

        class FakeReply:
            def attribute(self, _):
                return 400

            def rawHeader(self, _):
                return b"text/plain; charset=utf-8"

            def content(self):
                return b"Quantidade de valores solicitados: 500000 excedeu o limite: 100000"

        calls = []

        class FakeBlockingRequest:
            NoError = 0

            def get(self, request, forceRefresh=True, feedback=None):
                calls.append(1)
                return 3  # ServerExceptionError != NoError

            def reply(self):
                return FakeReply()

            def errorMessage(self):
                return "Bad Request"

        with patch("sidra_connector.infrastructure.http_client.QgsBlockingNetworkRequest", FakeBlockingRequest):
            with self.assertRaises(SidraHttpError) as ctx:
                client.get_json("https://apisidra.ibge.gov.br/values/t/5938/n6/all/v/all/p/all")

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("excedeu o limite", str(ctx.exception))
        # Não deve fazer retry em HTTP 400!
        self.assertEqual(len(calls), 1)


class TestIssue4And10MeshUrlsAndCancellation(unittest.TestCase):
    """Issues #4 e #10: URLs corretas no GeoFTP por época e cancelamento imediato."""

    def test_build_mesh_url_handles_2024_2022_2019_and_rejects_invalid_uf_pais(self):
        from sidra_connector.core.mesh_downloader import build_mesh_url

        # 2024 Brasil -> Brasil/BR_Municipios_2024.zip
        self.assertTrue(
            build_mesh_url("2024", "BR", "Municipios").endswith(
                "municipio_2024/Brasil/BR_Municipios_2024.zip"
            )
        )
        # 2022 Brasil -> Brasil/BR/BR_Municipios_2022.zip (subpasta BR extra!)
        self.assertTrue(
            build_mesh_url("2022", "BR", "Municipios").endswith(
                "municipio_2022/Brasil/BR/BR_Municipios_2022.zip"
            )
        )
        # 2018 SP -> UFs/SP/sp_unidades_da_federacao.zip
        self.assertTrue(
            build_mesh_url("2018", "SP", "UF").endswith(
                "municipio_2018/UFs/SP/sp_unidades_da_federacao.zip"
            )
        )
        # Combinação inválida: UF + País -> ValueError
        with self.assertRaises(ValueError):
            build_mesh_url("2024", "SP", "Pais")

    def test_mesh_downloader_cancels_mid_stream_and_cleans_temp_dir(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("test.shp", b"x" * 1000)
        payload = buf.getvalue()

        class FakeResponse:
            headers = {"content-length": str(len(payload) * 4)}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size=10):
                for _ in range(4):
                    yield payload

        downloader = MeshDownloader("https://geoftp.ibge.gov.br/fake.zip")
        temp_dir = downloader.temp_dir_path
        chunks_seen = 0

        def cancel_after_first_chunk():
            return chunks_seen >= 1

        def on_progress(_pct):
            nonlocal chunks_seen
            chunks_seen += 1

        with patch("sidra_connector.core.mesh_downloader.requests.get", return_value=FakeResponse()):
            with self.assertRaises(SidraCancelledError):
                downloader.download_and_extract(
                    progress_callback=on_progress,
                    is_canceled=cancel_after_first_chunk,
                )

        self.assertEqual(chunks_seen, 1)
        self.assertFalse(os.path.exists(temp_dir))


@unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
class TestIssue9DataJoiner(unittest.TestCase):
    """Issue #9: DataJoiner não sobrescreve colunas existentes nem colide nomes longos."""

    def _make_layer(self, existing_extra_field=None):
        layer = QgsVectorLayer("Point?crs=EPSG:4674", "municipios", "memory")
        pr = layer.dataProvider()
        fields = [QgsField("CD_MUN", STR_FIELD_TYPE)]
        if existing_extra_field:
            fields.append(QgsField(existing_extra_field, DBL_FIELD_TYPE))
        pr.addAttributes(fields)
        layer.updateFields()

        feat = QgsFeature(layer.fields())
        feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(-46.6, -23.5)))
        if existing_extra_field:
            feat.setAttributes(["3550308", 999.0])
        else:
            feat.setAttributes(["3550308"])
        pr.addFeatures([feat])
        layer.updateExtents()
        return layer

    def test_repeated_join_does_not_overwrite_existing_layer_field(self):
        from sidra_connector.core.data_joiner import DataJoiner

        layer = self._make_layer(existing_extra_field="pib")
        sidra_data = {"3550308": {"PIB": 123.45}}
        joined, count, _, _ = DataJoiner(layer, "CD_MUN", sidra_data, {}).join_data()

        self.assertEqual(count, 1)
        field_names = [f.name() for f in joined.fields()]
        self.assertIn("pib", field_names)
        self.assertIn("pib_1", field_names)
        feat = next(joined.getFeatures())
        self.assertEqual(feat["pib"], 999.0)
        self.assertEqual(feat["pib_1"], 123.45)

    def test_long_multidimensional_names_keep_year_suffix_distinct(self):
        from sidra_connector.core.data_joiner import DataJoiner

        layer = self._make_layer()
        long_prefix = "Participação do produto interno bruto a preços correntes no total"
        sidra_data = {
            "3550308": {
                f"{long_prefix}_2020": 10.5,
                f"{long_prefix}_2021": 11.2,
            }
        }
        joined, count, _, _ = DataJoiner(layer, "CD_MUN", sidra_data, {}).join_data()
        self.assertEqual(count, 1)
        field_names = [f.name() for f in joined.fields()]
        added_names = [n for n in field_names if n != "CD_MUN"]
        self.assertEqual(len(added_names), 2)
        for name in added_names:
            self.assertTrue(all(ord(c) < 128 for c in name), f"Non-ASCII field name: {name}")
        self.assertTrue(any(name.endswith("2020") for name in added_names), added_names)
        self.assertTrue(any(name.endswith("2021") for name in added_names), added_names)

        idx_2020 = next(i for i, f in enumerate(joined.fields()) if f.name().endswith("2020"))
        self.assertEqual(
            joined.attributeAlias(idx_2020),
            "Participação do produto interno bruto a preços correntes no total — 2020",
        )

    def test_sanitize_preserves_full_last_dimension_segment(self):
        from sidra_connector.core.data_joiner import _sanitize_and_fit_field_name

        raw = "População residente - percentual do total geral_Homens_0 a 4 anos"
        sanitized = _sanitize_and_fit_field_name(raw)
        self.assertLessEqual(len(sanitized), 60)
        self.assertTrue(all(ord(c) < 128 for c in sanitized))
        self.assertTrue(sanitized.endswith("_0_a_4_anos"), sanitized)


class TestIssue5And12UiAndMetadataConfig(unittest.TestCase):
    """Issues #5, #8, #12 e refinamentos 2.2.3: UI colapsável, auto-seleção, siglas e empacotador."""

    def test_manual_url_hidden_by_default_in_ui_file(self):
        ui_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "ui", "main_dialog_base.ui")
        )
        root = ET.parse(ui_path).getroot()
        chk = root.findall(".//widget[@name='chk_manual_url']")
        self.assertEqual(len(chk), 1, "Checkbox chk_manual_url deve existir no .ui")
        le_widgets = root.findall(".//widget[@name='le_api_url']")
        self.assertEqual(len(le_widgets), 1)
        vis_props = [p.text.strip().lower() for p in le_widgets[0].findall("property[@name='visible']/bool")]
        self.assertEqual(vis_props, ["false"])

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_acronym_search_pib_finds_table_5938_in_updated_db(self):
        import sqlite3
        from sidra_connector.ui.query_builder_dialog import build_search_query, normalize_search_text

        db_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "agregados_ibge.db")
        )
        conn = sqlite3.connect(db_path)
        try:
            conn.create_function("norm", 1, normalize_search_text)
            total = conn.execute("SELECT COUNT(*) FROM agregados").fetchone()[0]
            self.assertGreaterEqual(total, 9350)

            query, params = build_search_query("pib municipios")
            rows = conn.execute(query, params).fetchall()
            ids = {row[0] for row in rows}
            self.assertIn(5938, ids)
        finally:
            conn.close()

    def test_empacotar_creates_clean_qgis_plugin_zip(self):
        import importlib.util
        import tempfile

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        empacotar_path = os.path.join(repo_root, "dev", "empacotar.py")
        spec = importlib.util.spec_from_file_location("empacotar", empacotar_path)
        empacotar = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(empacotar)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_zip = os.path.join(tmpdir, "sidra_connector.zip")
            empacotar.build_plugin_zip(repo_root, out_zip)
            with zipfile.ZipFile(out_zip, "r") as zf:
                names = zf.namelist()
            self.assertIn("sidra_connector/metadata.txt", names)
            self.assertIn("sidra_connector/__init__.py", names)
            self.assertIn("sidra_connector/agregados_ibge.db", names)
            for forbidden in ("/tests/", "/dev/", "/.git/", "/__pycache__/", ".pyc", ".psd", "symbology-style.db"):
                self.assertFalse(any(forbidden in n for n in names), f"Proibido no zip: {forbidden}")

    def test_metadata_txt_is_valid_ini(self):
        meta_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "metadata.txt")
        )
        cp = configparser.ConfigParser(delimiters=("=",))
        cp.read(meta_path, encoding="utf-8")
        self.assertIn("general", cp)
        self.assertIn(cp["general"]["category"], {"Vector", "Web", "Database", "Raster", "Plugins"})

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_manual_url_hidden_by_default_and_toggled_by_checkbox(self):
        from unittest.mock import MagicMock
        from sidra_connector.ui.main_dialog import SidraConnectorDialog

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        iface = MagicMock()

        layer = QgsVectorLayer("Point?crs=EPSG:4674", "municipios_pr", "memory")
        layer.dataProvider().addAttributes([
            QgsField("fid", STR_FIELD_TYPE),
            QgsField("CD_MUN", STR_FIELD_TYPE),
        ])
        layer.updateFields()

        with patch("sidra_connector.ui.main_dialog.task_manager.run_years_task"), \
             patch("sidra_connector.ui.main_dialog.layer_manager.get_project_vector_layers", return_value=[layer]):
            dlg = SidraConnectorDialog(iface, repo_root)

        self.assertTrue(dlg.le_api_url.isHidden())
        self.assertFalse(dlg.chk_manual_url.isChecked())
        dlg.chk_manual_url.setChecked(True)
        self.assertFalse(dlg.le_api_url.isHidden())

        idx = dlg.cb_target_layer.findData(layer)
        dlg.cb_target_layer.setCurrentIndex(idx)
        self.assertEqual(dlg.cb_target_field.currentText(), "CD_MUN")

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_main_dialog_does_not_block_startup_with_sync_fetch_and_filters_malha_by_uf(self):
        from unittest.mock import MagicMock
        from sidra_connector.ui.main_dialog import SidraConnectorDialog

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        iface = MagicMock()

        with patch("sidra_connector.ui.main_dialog.fetch_available_years", side_effect=AssertionError("Não deve bloquear a UI no __init__!")), \
             patch("sidra_connector.ui.main_dialog.task_manager.run_years_task"):
            dlg = SidraConnectorDialog(iface, repo_root)

        self.assertGreater(dlg.cb_ano_malha.count(), 0)
        # Issue #4 na UI: para UF específica (ex: São Paulo), 'País' e 'Grandes Regiões' não devem aparecer
        idx_sp = dlg.cb_localidade_malha.findText("São Paulo")
        dlg.cb_localidade_malha.setCurrentIndex(idx_sp)
        tipos_sp = [dlg.cb_tipo_malha.itemText(i) for i in range(dlg.cb_tipo_malha.count())]
        self.assertNotIn("País", tipos_sp)
        self.assertNotIn("Grandes Regiões", tipos_sp)

    @unittest.skipUnless(QGIS_AVAILABLE, "Requer QGIS")
    def test_query_builder_close_cancels_feedback_without_thread_terminate(self):
        from qgis.PyQt.QtGui import QCloseEvent
        from sidra_connector.ui.query_builder_dialog import QueryBuilderDialog

        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        dlg = QueryBuilderDialog(repo_root)
        dlg.selected_table_id = 5938

        captured = {}

        def fake_run_metadata_task(table_id, on_success, on_error):
            from qgis.core import QgsFeedback
            fb = QgsFeedback()
            captured["feedback"] = fb
            return fb

        with patch("sidra_connector.ui.query_builder_dialog.task_manager.run_metadata_task", side_effect=fake_run_metadata_task):
            dlg.build_query()
            dlg.closeEvent(QCloseEvent())

        self.assertIn("feedback", captured)
        self.assertTrue(captured["feedback"].isCanceled())


if __name__ == "__main__":
    unittest.main()
