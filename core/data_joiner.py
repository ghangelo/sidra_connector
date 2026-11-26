# -*- coding: utf-8 -*-
"""
Faz o join entre os dados do SIDRA e uma camada vetorial do QGIS.

Recebe o dicionario {geo_code: {variavel: valor}} que vem do
SidraApiClient e uma camada do projeto, e gera uma COPIA da
camada com as colunas do SIDRA adicionadas. A camada original
nao eh alterada.
"""

import re
import unicodedata

from qgis.core import (
    QgsVectorLayer,
    QgsField,
    QgsFeature,
    QgsFields,
    QgsWkbTypes,
)

# QGIS 3.38+ usa QMetaType, versoes anteriores usam QVariant
try:
    from qgis.PyQt.QtCore import QMetaType
    DOUBLE_TYPE = QMetaType.Type.Double
except (ImportError, AttributeError):
    from qgis.PyQt.QtCore import QVariant
    DOUBLE_TYPE = QVariant.Double

MAX_FIELD_LEN = 60


def _sanitize_segment(text):
    nfkd = unicodedata.normalize("NFKD", str(text))
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    safe = ascii_text.encode("ascii", "ignore").decode("ascii").lower()
    safe = safe.replace(" - ", "_").replace(" ", "_").replace("/", "_").replace("-", "_")
    safe = re.sub(r"[^a-z0-9_]+", "_", safe)
    return re.sub(r"_+", "_", safe).strip("_")


def _sanitize_and_fit_field_name(raw_name, max_len=MAX_FIELD_LEN):
    raw_parts = [p for p in str(raw_name).split("_") if p.strip()]
    clean_parts = [s for s in (_sanitize_segment(p) for p in raw_parts) if s]
    safe = "_".join(clean_parts) or "valor"

    if len(safe) <= max_len:
        return safe

    # Preserva o último segmento de dimensão completo (ex: 0_a_4_anos, 2020) dentro de 24 chars
    if len(clean_parts) >= 2:
        last_seg = clean_parts[-1]
        if last_seg and len(last_seg) <= 24:
            prefix = "_".join(clean_parts[:-1])
            budget = max_len - len(last_seg) - 1
            if budget >= 1:
                return f"{prefix[:budget].rstrip('_')}_{last_seg}"

    return safe[:max_len]


def _format_field_alias(raw_name):
    parts = [part.strip() for part in str(raw_name).split("_") if part.strip()]
    if not parts:
        return str(raw_name).strip()
    return " — ".join(parts)


def _normalize_join_key(raw_key):
    if raw_key is None:
        return None
    if isinstance(raw_key, int):
        return str(raw_key)
    if isinstance(raw_key, float):
        return str(int(raw_key)) if raw_key.is_integer() else str(raw_key)
    text = str(raw_key).strip()
    if not text:
        return None
    return re.sub(r"^(\d+)\.0+$", r"\1", text)


class DataJoiner:
    """Junta dados tabulares do SIDRA a uma camada vetorial."""

    def __init__(self, target_layer, join_field_name, sidra_data, header_info):
        if not isinstance(target_layer, QgsVectorLayer):
            raise TypeError("Isso nao eh uma camada vetorial valida.")

        if not target_layer.isValid():
            raise ValueError("A camada vetorial ta invalida.")

        if not join_field_name or join_field_name not in [
            field.name() for field in target_layer.fields()
        ]:
            raise ValueError(
                f"Campo '{join_field_name}' nao existe na camada."
            )

        if not isinstance(sidra_data, dict):
            raise TypeError("Os dados do SIDRA precisam ser um dicionario.")

        if not sidra_data:
            raise ValueError("Nenhum dado do SIDRA foi passado.")

        self.target_layer = target_layer
        self.join_field_name = join_field_name
        self.sidra_data = sidra_data
        self.header_info = header_info if header_info else {}

    def join_data(self):
        """Faz o join e retorna (camada_nova, qtd_unidas, nao_encontradas, chaves_exemplo)."""
        new_fields = QgsFields()
        used_field_names_lower = set()
        for field in self.target_layer.fields():
            new_fields.append(field)
            used_field_names_lower.add(field.name().lower())

        all_class_values = sorted(
            {k for item in self.sidra_data.values() for k in item.keys()}
        )

        field_map = {}
        field_aliases = {}

        for class_value in all_class_values:
            original_field_name = _sanitize_and_fit_field_name(class_value)
            field_name = original_field_name

            counter = 1
            while field_name.lower() in used_field_names_lower:
                suffix = f"_{counter}"
                base = original_field_name[: MAX_FIELD_LEN - len(suffix)]
                field_name = f"{base}{suffix}"
                counter += 1

            alias = _format_field_alias(class_value)
            used_field_names_lower.add(field_name.lower())
            qgs_field = QgsField(field_name, DOUBLE_TYPE)
            qgs_field.setAlias(alias)
            new_fields.append(qgs_field)
            field_map[class_value] = field_name
            field_aliases[field_name] = alias

        temp_layer = QgsVectorLayer(
            f"{QgsWkbTypes.displayString(self.target_layer.wkbType())}",
            f"{self.target_layer.name()}_sidra",
            "memory",
        )
        temp_layer.setCrs(self.target_layer.crs())
        provider = temp_layer.dataProvider()
        provider.addAttributes(new_fields)
        temp_layer.updateFields()

        for field_name, alias in field_aliases.items():
            idx = temp_layer.fields().indexFromName(field_name)
            if idx != -1:
                temp_layer.setFieldAlias(idx, alias)

        join_count = 0
        unmatched_keys_sample = []
        layer_keys_sample = []
        new_features = []

        for feature in self.target_layer.getFeatures():
            new_feat = QgsFeature(new_fields)
            new_feat.setGeometry(feature.geometry())
            for i in range(len(feature.fields())):
                new_feat.setAttribute(i, feature.attribute(i))

            normalized_layer_key = _normalize_join_key(feature[self.join_field_name])

            if len(layer_keys_sample) < 5 and normalized_layer_key:
                layer_keys_sample.append(normalized_layer_key)

            if normalized_layer_key and normalized_layer_key in self.sidra_data:
                join_count += 1
                for class_value, data_value in self.sidra_data[normalized_layer_key].items():
                    field_name = field_map.get(class_value)
                    if field_name and data_value is not None:
                        try:
                            new_feat[field_name] = float(data_value)
                        except (ValueError, TypeError):
                            new_feat[field_name] = None
            elif normalized_layer_key and len(unmatched_keys_sample) < 5:
                unmatched_keys_sample.append(normalized_layer_key)

            new_features.append(new_feat)

        provider.addFeatures(new_features)
        temp_layer.updateExtents()

        return temp_layer, join_count, unmatched_keys_sample, layer_keys_sample
