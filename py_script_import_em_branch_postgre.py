
#Matheus Aviz
#2025-06-25


#script importar camadas para o PostgreSQL em branch
# --- SCRIPT FINAL, CORRIGIDO APÓS O TESTE BEM-SUCEDIDO ---

import processing
from qgis.core import QgsMessageLog, Qgis, QgsVectorLayer

# --- CONFIGURAÇÕES ---

# Caminho do GeoPackage de origem
gpkg_path = r"C:/Users/mdaviz/OneDrive - CDHU/Documentos/@Geohab/Planejamento/20250613_Envio_Geo_CDHU/AtributosAmbientais.gpkg"

# Parâmetros da conexão PostGIS
database_name = "Planejamento"
schema_name = "geohab"
geometry_column = "geom"
# Prefixo para as novas tabelas
table_prefix = "dpdu_aa"


# --- FUNÇÕES AUXILIARES ---

def list_gpkg_layers(path):
    """
    Lista os nomes das camadas de um GeoPackage, com a extração de nome
    corrigida de acordo com o resultado do teste.
    """
    try:
        gpkg_layer_object = QgsVectorLayer(path, "temp_layer_for_listing", "ogr")
        if not gpkg_layer_object.isValid():
            msg = f"Arquivo GeoPackage inválido ou não encontrado: {path}"
            QgsMessageLog.logMessage(msg, 'Importação Lote', level=Qgis.Critical)
            return []

        provider = gpkg_layer_object.dataProvider()
        sublayer_list = provider.subLayers()
        
        # separador é '!!::!!' e o nome é o segundo elemento (índice 1).
        layer_names = [item.split('!!::!!')[1] for item in sublayer_list]
        
        return layer_names

    except Exception as e:
        msg = f"Ocorreu um erro inesperado ao listar as camadas: {e}"
        QgsMessageLog.logMessage(msg, 'Importação Lote', level=Qgis.Critical)
        return []


# --- SCRIPT PRINCIPAL ---

print("Iniciando processo de importação em lote...")

# 1. Listar as camadas do GeoPackage
layers_to_import = list_gpkg_layers(gpkg_path)

if not layers_to_import:
    print("FINALIZADO: Nenhuma camada encontrada ou ocorreu um erro na leitura. Verifique o Log de Mensagens do QGIS.")
else:
    print(f"Sucesso! {len(layers_to_import)} camadas encontradas.")
    print("-" * 50)

    # 2. Loop para importar cada camada
    for i, layer_name in enumerate(layers_to_import, start=1):
        # Limpeza do nome para uso em tabelas de banco de dados
        clean_layer_name = layer.name().replace(' ', '_').replace('-', '_').replace('´', '_').replace('Á', 'A').replace('(', '').replace(')', '').replace('.', '').replace('ç', 'c').replace('ã', 'a').replace("'", '').replace(",", '').replace("á", 'a').replace("ó", 'o').replace("õ", 'o').replace("í", 'i').replace(";", '').replace("ú", 'u').replace("é", 'e').replace("Ú", 'U').lower()
        base_tablename = f"{table_prefix}_{str(i).zfill(2)}_{clean_layer_name}"
        tablename = base_tablename[:63]

        print(f"--> Processando {i}/{len(layers_to_import)}: '{layer_name}'")
        print(f"    Tabela de destino: '{schema_name}.{tablename}'")

        params = {
            'INPUT': f"{gpkg_path}|layername={layer_name}",
            'DATABASE': database_name,
            'SCHEMA': schema_name,
            'TABLENAME': tablename,
            'GEOMETRY_COLUMN': geometry_column,
            'ENCODING': 'UTF-8',
            'OVERWRITE': True,
            'CREATEINDEX': True,
            'LOWERCASE_NAMES': True,
            'DROP_STRING_LENGTH': False,
            'FORCE_SINGLEPART': False,
            'INVALID_FEATURES_FILTER': 1 
        }

        # 3. Executar a importação com tratamento de erro
        try:
            processing.run("native:importintopostgis", params)
            success_message = f"Camada '{layer_name}' importada com sucesso para '{tablename}'"
            print(f"    ✔ SUCESSO: {success_message}\n")
            QgsMessageLog.logMessage(success_message, 'Importação Lote', level=Qgis.Success)

        except Exception as e:
            error_message = f"Falha ao importar a camada '{layer_name}'. Erro: {e}"
            print(f"    ❌ ERRO: {error_message}\n")
            QgsMessageLog.logMessage(error_message, 'Importação Lote', level=Qgis.Critical)

    print("=" * 50)
    print("Importação em lote concluída.")
    print("Verifique o painel 'Log de Mensagens' do QGIS para um resumo detalhado.")
