# -*- coding: utf-8 -*-
"""
Módulo de união de dados tabulares SIDRA a camadas vetoriais.

O ``DataJoiner`` recebe o dicionário de lookup retornado por
``SidraApiClient.fetch_and_parse()`` e uma camada alvo do projeto,
e produz uma **cópia** da camada com colunas adicionais contendo os
valores do SIDRA — sem alterar a camada original.

Etapas:
1. Gera nomes de campo seguros a partir dos rótulos de variável.
2. Cria uma camada temporária (memory provider) com campos extras.
3. Percorre as feições da camada alvo, normaliza a chave de união
   (código geográfico) e associa os valores correspondentes.
"""

from qgis.core import (
    QgsVectorLayer,
    QgsField,
    QgsFeature,
    QgsFields,
    QgsWkbTypes,
    Qgis,
)
from qgis.PyQt.QtCore import QVariant


class DataJoiner:
    """Une dados tabulares do SIDRA a uma camada vetorial do QGIS.

    Cria uma nova camada em memória (cópia) contendo todos os campos
    originais **mais** as colunas de dados SIDRA.
    """

    def __init__(self, target_layer, join_field_name, sidra_data, header_info):
        """Inicializa e valida os parâmetros da união.

        :param target_layer: Camada vetorial do QGIS (fonte das feições).
        :param join_field_name: Nome do campo cujos valores serão usados
            como chave de lookup no ``sidra_data`` (ex: ``CD_MUN``).
        :param sidra_data: Dicionário ``{geo_code: {variavel: valor}}``.
        :param header_info: Metadados extras retornados pela API.
        :raises TypeError: Se os tipos forem inválidos.
        :raises ValueError: Se os dados estiverem vazios ou o campo não existir.
        """
        if not isinstance(target_layer, QgsVectorLayer):
            raise TypeError("O parâmetro 'target_layer' não é uma camada vetorial válida.")

        if not target_layer.isValid():
            raise ValueError("A camada vetorial fornecida não é válida.")

        if not join_field_name or join_field_name not in [
            field.name() for field in target_layer.fields()
        ]:
            raise ValueError(
                f"Campo de união '{join_field_name}' não encontrado na camada."
            )

        if not isinstance(sidra_data, dict):
            raise TypeError("Os dados do SIDRA devem ser fornecidos como um dicionário.")

        if not sidra_data:
            raise ValueError(
                "Nenhum dado do SIDRA foi fornecido. Verifique se a URL da API "
                "está correta e retorna dados válidos."
            )

        self.target_layer = target_layer
        self.join_field_name = join_field_name
        self.sidra_data = sidra_data
        self.header_info = header_info if header_info else {}

    def join_data(self):
        """Executa a união e retorna a nova camada com estatísticas.

        :returns: Tupla ``(nova_camada, join_count, unmatched_sample, layer_keys_sample)``.
            - ``nova_camada``: ``QgsVectorLayer`` em memória com os dados unidos.
            - ``join_count``: Quantidade de feições que tiveram correspondência.
            - ``unmatched_sample``: Amostra de chaves sem correspondência (debug).
            - ``layer_keys_sample``: Amostra de chaves da camada (debug).
        """
        # ------------------------------------------------------------------
        # 1. Montar o schema de campos (originais + novos do SIDRA)
        # ------------------------------------------------------------------
        new_fields = QgsFields()
        for field in self.target_layer.fields():
            new_fields.append(field)

        # Coletar todos os nomes de variável presentes nos dados
        all_class_values = sorted(
            list(set(k for item in self.sidra_data.values() for k in item.keys()))
        )
        period = self.header_info.get('D3N', 'periodo')

        # Mapeamento: nome da variável SIDRA → nome seguro do campo QGIS
        field_map = {}
        used_field_names = set()

        for class_value in all_class_values:
            # Gerar nome de campo seguro: minúsculas, sem acentos problemáticos
            safe_class = str(class_value).lower()
            safe_class = safe_class.replace(' - ', '_').replace(' ', '_').replace('/', '_')
            safe_class = safe_class.replace('(', '').replace(')', '').replace('-', '_')

            base_name = safe_class[:40]
            field_name = f"{base_name}"

            # Limite do QGIS para nomes de campo: 63 caracteres.
            # Trunca ANTES de verificar unicidade para garantir que o
            # nome final (incluindo sufixo numérico) jamais colida.
            if len(field_name) > 60:
                field_name = field_name[:60]

            # Garantir unicidade do nome
            counter = 1
            original_field_name = field_name
            while field_name in used_field_names:
                field_name = f"{original_field_name}_{counter}"
                counter += 1
                # Ré-truncar caso o sufixo numérico exceda o limite
                if len(field_name) > 60:
                    trim = len(field_name) - 60
                    field_name = f"{original_field_name[:-trim]}_{counter - 1}"

            used_field_names.add(field_name)

            if new_fields.indexFromName(field_name) == -1:
                # O usuário solicitou que as camadas geradas pelo join sempre devem vir como camada decimal real
                new_fields.append(QgsField(field_name, QVariant.Double))
            field_map[class_value] = field_name

        # ------------------------------------------------------------------
        # 2. Criar a camada temporária de resultado
        # ------------------------------------------------------------------
        # Monta a URI apenas com o tipo de geometria; o CRS é aplicado
        # via setCrs() para suportar projeções customizadas sem authid.
        temp_layer = QgsVectorLayer(
            f"{QgsWkbTypes.displayString(self.target_layer.wkbType())}",
            f"{self.target_layer.name()}_sidra",
            "memory",
        )
        temp_layer.setCrs(self.target_layer.crs())
        provider = temp_layer.dataProvider()
        provider.addAttributes(new_fields)
        temp_layer.updateFields()

        # ------------------------------------------------------------------
        # 3. Percorrer feições e fazer a união por código geográfico
        # ------------------------------------------------------------------
        join_count = 0
        unmatched_keys_sample = []
        layer_keys_sample = []
        new_features = []

        for feature in self.target_layer.getFeatures():
            new_feat = QgsFeature(new_fields)
            new_feat.setGeometry(feature.geometry())
            # Copiar atributos originais
            for i, field in enumerate(feature.fields()):
                new_feat.setAttribute(i, feature.attribute(i))

            raw_key = feature[self.join_field_name]

            # Normalizar chave: remover .0 de floats, strip de strings
            normalized_layer_key = None
            if raw_key is not None:
                try:
                    normalized_layer_key = str(int(float(raw_key)))
                except (ValueError, TypeError):
                    normalized_layer_key = str(raw_key).strip()

            # Coletar amostras para mensagens de diagnóstico
            if len(layer_keys_sample) < 5 and normalized_layer_key:
                layer_keys_sample.append(normalized_layer_key)

            # Lookup no dicionário SIDRA
            if normalized_layer_key and normalized_layer_key in self.sidra_data:
                join_count += 1
                for class_value, data_value in self.sidra_data[normalized_layer_key].items():
                    field_name = field_map.get(class_value)
                    if field_name and data_value is not None:
                        # Converte para float para manter como decimal real (Double)
                        try:
                            new_feat[field_name] = float(data_value)
                        except (ValueError, TypeError):
                            new_feat[field_name] = None
            elif normalized_layer_key and len(unmatched_keys_sample) < 5:
                unmatched_keys_sample.append(normalized_layer_key)

            new_features.append(new_feat)

        # Adiciona todas as feições de uma vez via provider — muito mais
        # rápido que edit() + addFeature() unitário.
        provider.addFeatures(new_features)
        temp_layer.updateExtents()

        return temp_layer, join_count, unmatched_keys_sample, layer_keys_sample
