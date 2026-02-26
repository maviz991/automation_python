####01
#Matheuws Aviz
#Data: 2025-05-25


#Importar camadas em branch para o PostgreSQL
import processing
from qgis.core import QgsMessageLog, Qgis, QgsVectorLayer

# ======================================================================
# --- MODO DE EXECUÇÃO ---
MODO_DE_RETENTATIVA = True #ligar=True ou desligar=False
NUMEROS_DAS_CAMADAS_PARA_REIMPORTAR = [15, 16, 17]
# ======================================================================

# --- CONFIGURAÇÕES FIXAS ---
gpkg_path = r"C:\Users\mdaviz\OneDrive - CDHU\Documentos\@Geohab\Planejamento\20250613_Envio_Geo_CDHU\UsoDoSoloMapbiomas.gpkg"
database_name = "Planejamento"
schema_name = "geohab"
geometry_column = "geom"
table_prefix = "dpdu_usmb"

# --- FUNÇÕES AUXILIARES ---
def list_gpkg_layers(path):
    try:
        gpkg_layer_object = QgsVectorLayer(path, "temp_layer_for_listing", "ogr")
        provider = gpkg_layer_object.dataProvider()
        return [item.split('!!::!!')[1] for item in provider.subLayers()]
    except Exception as e:
        QgsMessageLog.logMessage(f"Erro ao listar camadas: {e}", 'Importação Lote', level=Qgis.Critical)
        return []

# --- SCRIPT PRINCIPAL ---

print("Iniciando processo com ESTRATÉGIA DE CORREÇÃO DE GEOMETRIAS.")
if MODO_DE_RETENTATIVA:
    print(f"MODO DE RETENTATIVA ATIVADO. Processando apenas as camadas: {NUMEROS_DAS_CAMADAS_PARA_REIMPORTAR}")
print("-" * 50)

all_layers = list_gpkg_layers(gpkg_path)
if not all_layers:
    print("FINALIZADO: Nenhuma camada encontrada.")
else:
    for i, layer_name in enumerate(all_layers, start=1):
        if MODO_DE_RETENTATIVA and i not in NUMEROS_DAS_CAMADAS_PARA_REIMPORTAR:
            continue

        print(f"--> Processando camada {i}: '{layer_name}'")

        try:
            # --- PASSO 1: CORRIGIR AS GEOMETRIAS ---
            print("    1. Corrigindo geometrias...")
            
            # Parâmetros para a ferramenta "Corrigir geometrias"
            fix_params = {
                'INPUT': f"{gpkg_path}|layername={layer_name}",
                'OUTPUT': 'memory:' # Salva a camada corrigida na memória
            }
            
            # Executa a correção e pega o resultado
            result = processing.run("native:fixgeometries", fix_params)
            fixed_layer = result['OUTPUT']
            
            print("    -> Geometrias corrigidas com sucesso.")

            # --- PASSO 2: IMPORTAR A CAMADA JÁ CORRIGIDA ---
            print("    2. Importando camada corrigida para o PostGIS...")
            
            #clean_layer_name = layer_name.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '').replace(',', '').lower()
            clean_layer_name = layer.name().replace(' ', '_').replace('-', '_').replace('´', '_').replace('Á', 'A').replace('(', '').replace(')', '').replace('.', '').replace('ç', 'c').replace('ã', 'a').replace("'", '').replace(",", '').replace("á", 'a').replace("ó", 'o').replace("õ", 'o').replace("í", 'i').replace(";", '').replace("ú", 'u').replace("é", 'e').replace("Ú", 'U').lower()
            base_tablename = f"{table_prefix}_{str(i).zfill(2)}_{clean_layer_name}"
            tablename = base_tablename[:63]
            print(f"       Tabela de destino: '{schema_name}.{tablename}'")

            import_params = {
                'INPUT': fixed_layer,  # << A ENTRADA AGORA É A CAMADA CORRIGIDA
                'DATABASE': database_name,
                'SCHEMA': schema_name,
                'TABLENAME': tablename,
                'GEOMETRY_COLUMN': geometry_column,
                'ENCODING': 'UTF-8',
                'OVERWRITE': True,
                'CREATEINDEX': True,
                'LOWERCASE_NAMES': True
            }
            
            processing.run("native:importintopostgis", import_params)
            
            success_message = f"Camada '{layer_name}' corrigida e importada com sucesso para '{tablename}'"
            print(f"    ✅ SUCESSO: {success_message}\n")
            QgsMessageLog.logMessage(success_message, 'Importação Lote', level=Qgis.Success)

        except Exception as e:
            error_message = f"Falha CRÍTICA no processo da camada '{layer_name}'. Erro: {e}"
            print(f"    ❌ ERRO: {error_message}\n")
            QgsMessageLog.logMessage(error_message, 'Importação Lote', level=Qgis.Critical)

    print("=" * 50)
    print("Processo concluído.")