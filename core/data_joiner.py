# -*- coding: utf-8 -*-
"""
Faz o join entre os dados do SIDRA e uma camada vetorial do QGIS.

Recebe o dicionario {geo_code: {variavel: valor}} que vem do
SidraApiClient e uma camada do projeto, e gera uma COPIA da
camada com as colunas do SIDRA adicionadas. A camada original
nao eh alterada.
"""

from qgis.core import (
    QgsVectorLayer,
    QgsField,
    QgsFeature,
    QgsFields,
    QgsWkbTypes,
)

# QGIS 3.38+ usa QMetaType, versoes anteriores usam QVariant
try:
    from PyQt5.QtCore import QMetaType
    DOUBLE_TYPE = QMetaType.Double
except (ImportError, AttributeError):
    try:
        from qgis.PyQt.QtCore import QMetaType
        DOUBLE_TYPE = QMetaType.Type.Double
    except (ImportError, AttributeError):
        from qgis.PyQt.QtCore import QVariant
        DOUBLE_TYPE = QVariant.Double


class DataJoiner:
    """Junta dados tabulares do SIDRA a uma camada vetorial.

    Cria uma camada nova em memoria com todos os campos originais
    mais as colunas dos dados do SIDRA.
    """

    def __init__(self, target_layer, join_field_name, sidra_data, header_info):
        """Valida os parametros antes de fazer qualquer coisa."""
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
        """Faz o join e retorna a camada nova com estatisticas.

        Retorna (camada_nova, qtd_unidas, nao_encontradas, chaves_exemplo).
        """
        # Monta o schema: campos originais + campos novos do SIDRA
        new_fields = QgsFields()
        for field in self.target_layer.fields():
            new_fields.append(field)

        # Pega todos os nomes de variavel que aparecem nos dados
        all_class_values = sorted(
            list(set(k for item in self.sidra_data.values() for k in item.keys()))
        )


        # Cria nomes de campo seguros pro QGIS (sem espaco, sem acento esquisito)
        field_map = {}
        used_field_names = set()

        for class_value in all_class_values:
            safe_class = str(class_value).lower()
            safe_class = safe_class.replace(' - ', '_').replace(' ', '_').replace('/', '_')
            safe_class = safe_class.replace('(', '').replace(')', '').replace('-', '_')

            base_name = safe_class[:40]
            field_name = f"{base_name}"

            # QGIS aceita no maximo 63 chars
            if len(field_name) > 60:
                field_name = field_name[:60]

            # Garante que nao repete nome
            counter = 1
            original_field_name = field_name
            while field_name in used_field_names:
                field_name = f"{original_field_name}_{counter}"
                counter += 1
                if len(field_name) > 60:
                    trim = len(field_name) - 60
                    field_name = f"{original_field_name[:-trim]}_{counter - 1}"

            used_field_names.add(field_name)

            if new_fields.indexFromName(field_name) == -1:
                new_fields.append(QgsField(field_name, DOUBLE_TYPE))
            field_map[class_value] = field_name

        # Cria a camada de resultado em memoria
        temp_layer = QgsVectorLayer(
            f"{QgsWkbTypes.displayString(self.target_layer.wkbType())}",
            f"{self.target_layer.name()}_sidra",
            "memory",
        )
        temp_layer.setCrs(self.target_layer.crs())
        provider = temp_layer.dataProvider()
        provider.addAttributes(new_fields)
        temp_layer.updateFields()

        # Percorre cada feicao e tenta achar no dicionario do SIDRA
        join_count = 0
        unmatched_keys_sample = []
        layer_keys_sample = []
        new_features = []

        for feature in self.target_layer.getFeatures():
            new_feat = QgsFeature(new_fields)
            new_feat.setGeometry(feature.geometry())
            for i, field in enumerate(feature.fields()):
                new_feat.setAttribute(i, feature.attribute(i))

            raw_key = feature[self.join_field_name]

            # Normaliza: tira .0 de float, strip de string
            normalized_layer_key = None
            if raw_key is not None:
                try:
                    normalized_layer_key = str(int(float(raw_key)))
                except (ValueError, TypeError):
                    normalized_layer_key = str(raw_key).strip()

            if len(layer_keys_sample) < 5 and normalized_layer_key:
                layer_keys_sample.append(normalized_layer_key)

            # Procura esse codigo no dicionario do SIDRA
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

        # Insere tudo de uma vez (muito mais rapido que uma por uma)
        provider.addFeatures(new_features)
        temp_layer.updateExtents()

        return temp_layer, join_count, unmatched_keys_sample, layer_keys_sample
