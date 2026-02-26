#Matheus Aviz
# TESTE SIMPLES - listar camadas de Geopackage
from qgis.core import QgsVectorLayer
gpkg_path = r"..."
gpkg_layer_object = QgsVectorLayer(gpkg_path, "test", "ogr")
provider = gpkg_layer_object.dataProvider()
sublayers = provider.subLayers()
print("Teste bem-sucedido! Camadas encontradas:")
print(sublayers)