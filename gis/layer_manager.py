# -*- coding: utf-8 -*-
"""
Utilitários para manipulação de camadas vetoriais no QGIS.

Funções usadas pelo diálogo principal e pelo gerenciador de tarefas:
- Listar camadas do projeto.
- Obter nomes de campos.
- Adicionar camadas ao projeto.
- Converter camadas de arquivo para camadas temporárias em memória,
  permitindo que os arquivos temporários sejam apagados com segurança.
"""

from qgis.core import QgsProject, QgsVectorLayer, QgsWkbTypes


def get_project_vector_layers():
    """Retorna todas as camadas vetoriais **com geometria** do projeto aberto.

    Exclui camadas sem geometria (ex.: tabelas CSV) para popular a
    combobox de camadas-alvo no diálogo principal.
    """
    layers = QgsProject.instance().mapLayers().values()
    return [
        lyr for lyr in layers
        if isinstance(lyr, QgsVectorLayer) and lyr.wkbType() != QgsWkbTypes.NoGeometry
    ]


def get_layer_fields(layer):
    """Retorna os nomes dos campos (atributos) de uma camada vetorial.

    :param layer: Camada vetorial do QGIS.
    :returns: Lista de strings com os nomes dos campos, ou lista vazia.
    """
    if not layer or not isinstance(layer, QgsVectorLayer):
        return []
    return [field.name() for field in layer.fields()]


def add_layer_to_project(layer):
    """Adiciona uma camada válida ao projeto QGIS atual.

    :param layer: ``QgsVectorLayer`` a ser adicionada.
    :returns: ``True`` se adicionada com sucesso, ``False`` caso contrário.
    """
    if layer and layer.isValid():
        QgsProject.instance().addMapLayer(layer)
        return True
    return False


def load_vector_layer(path, name):
    """Cria uma ``QgsVectorLayer`` a partir de um arquivo em disco (via OGR).

    Nota: a camada retornada ainda aponta para o arquivo em disco;
    use ``file_layer_to_memory()`` se precisar desvincular do arquivo.

    :param path: Caminho completo para o shapefile (ou outro formato OGR).
    :param name: Nome de exibição da camada.
    :returns: ``QgsVectorLayer`` (pode estar inválida — verifique ``.isValid()``).
    """
    layer = QgsVectorLayer(path, name, "ogr")
    return layer


def file_layer_to_memory(file_layer, name):
    """Converte uma camada de arquivo (OGR) para camada temporária em memória.

    Isso permite remover os arquivos temporários do disco logo após a
    conversão, sem invalidar a camada no projeto.

    O processo copia:
    1. Estrutura de campos (schema).
    2. Todas as feições (geometria + atributos).

    :param file_layer: ``QgsVectorLayer`` de origem (provider ``ogr``).
    :param name: Nome a atribuir à camada temporária.
    :returns: ``QgsVectorLayer`` em memória, ou ``None`` se a conversão falhar.
    """
    if not file_layer or not file_layer.isValid():
        return None

    # Monta a URI de memória apenas com o tipo de geometria;
    # o CRS é aplicado explicitamente via setCrs() para cobrir CRS
    # sem authid (ex.: projeções customizadas).
    geom_type = QgsWkbTypes.displayString(file_layer.wkbType())
    mem_layer = QgsVectorLayer(f"{geom_type}", name, "memory")
    mem_layer.setCrs(file_layer.crs())
    mem_provider = mem_layer.dataProvider()

    # Copia a estrutura de campos (schema)
    mem_provider.addAttributes(file_layer.fields().toList())
    mem_layer.updateFields()

    # Copia todas as feições (geometria + atributos)
    mem_provider.addFeatures(list(file_layer.getFeatures()))
    mem_layer.updateExtents()

    return mem_layer if mem_layer.isValid() else None
