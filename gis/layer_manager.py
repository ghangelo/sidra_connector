# -*- coding: utf-8 -*-
"""
Funcoes pra mexer com camadas vetoriais no QGIS.

Coisas tipo: listar camadas do projeto, pegar os campos,
adicionar camada, e converter camada de arquivo pra memoria.
"""

from qgis.core import QgsProject, QgsVectorLayer, QgsWkbTypes


def get_project_vector_layers():
    """Pega todas as camadas vetoriais do projeto que tem geometria.

    Ignora tabelas CSV e outras camadas sem geometria.
    """
    layers = QgsProject.instance().mapLayers().values()
    return [
        lyr for lyr in layers
        if isinstance(lyr, QgsVectorLayer) and lyr.wkbType() != QgsWkbTypes.NoGeometry
    ]


def get_layer_fields(layer):
    """Retorna a lista de nomes dos campos de uma camada."""
    if not layer or not isinstance(layer, QgsVectorLayer):
        return []
    return [field.name() for field in layer.fields()]


def add_layer_to_project(layer):
    """Adiciona uma camada ao projeto se ela for valida."""
    if layer and layer.isValid():
        QgsProject.instance().addMapLayer(layer)
        return True
    return False


def load_vector_layer(path, name):
    """Abre um shapefile (ou outro formato OGR) como camada vetorial.

    A camada fica vinculada ao arquivo em disco. Se quiser desvincular,
    usa file_layer_to_memory() depois.
    """
    layer = QgsVectorLayer(path, name, "ogr")
    return layer


def file_layer_to_memory(file_layer, name):
    """Copia uma camada de arquivo pra memoria.

    Isso permite apagar os arquivos temporarios do disco
    sem perder a camada no QGIS.
    """
    if not file_layer or not file_layer.isValid():
        return None

    geom_type = QgsWkbTypes.displayString(file_layer.wkbType())
    mem_layer = QgsVectorLayer(f"{geom_type}", name, "memory")
    mem_layer.setCrs(file_layer.crs())
    mem_provider = mem_layer.dataProvider()

    # Copia campos
    mem_provider.addAttributes(file_layer.fields().toList())
    mem_layer.updateFields()

    # Copia feicoes (geometria + atributos)
    mem_provider.addFeatures(file_layer.getFeatures())
    mem_layer.updateExtents()

    return mem_layer if mem_layer.isValid() else None
