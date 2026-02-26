# =============================================================================
# upload_gpkg_to_postgis.py
# Autor: Matheus Aviz
# Finalidade:
#   - Le camadas de um GPKG (do disco ou abertas no projeto QGIS)
#   - Sanitiza nome de tabela (camelCase + prefixo dpdu_)
#   - Sanitiza nomes de campos (camelCase, sem prefixo)
#   - Importa direto para o PostgreSQL/PostGIS
#   - Credenciais lidas das conexoes salvas no QGIS (sem senha no codigo)
#
# Como rodar:
#   QGIS: Complementos > Terminal Python > Mostrar Editor > Abrir > Executar
#   Credenciais: Configuracoes > Opcoes > Fontes de Dados > Conexoes PostGIS
# =============================================================================

import os
import re
import sys
import unicodedata
import traceback

try:
    from qgis.core import (
        QgsApplication, QgsProject, QgsVectorLayer,
        QgsMapLayer, QgsMessageLog, Qgis
    )
    import processing
    INSIDE_QGIS = QgsApplication.instance() is not None
except ImportError:
    print("ERRO: Este script requer QGIS/PyQGIS no PATH.")
    sys.exit(1)

# =============================================================================
# ---              CONFIGURACOES DO USUARIO [ALTERAR AQUI]                  ---
# =============================================================================

# --- Origem ---
USE_OPEN_LAYERS = True                        # True = camadas abertas no QGIS
INPUT_GPKG      = r"C:/Caminho/Arquivo.gpkg"  # usado se USE_OPEN_LAYERS = False

# --- Destino PostgreSQL ---
PG_CONNECTION_NAME = "Planejamento"   # nome exato da conexao salva no QGIS
PG_SCHEMA          = "geohab"
PG_GEOMETRY_COLUMN = "geom"

# --- Nomenclatura ---
TABLE_PREFIX   = "teste_"
TRUNCATE_LIMIT = 63

# --- Comportamento ---
OVERWRITE              = True
CREATE_INDEX           = True
LOWERCASE_NAMES        = True
DROP_STRING_LENGTH     = False
FORCE_SINGLEPART       = False
INVALID_FEATURES_FILTER = 1   # 0=ignorar | 1=pular invalidas | 2=parar no erro

# =============================================================================
# --- SANITIZACAO ---
# =============================================================================

def sanitize_name(text, add_prefix=False):
    if not text or not text.strip():
        return f"{TABLE_PREFIX}semNome" if add_prefix else "semNome"
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    words = re.sub(r'[^a-zA-Z0-9]+', ' ', text).split()
    if not words:
        return f"{TABLE_PREFIX}semNome" if add_prefix else "semNome"
    camel = words[0].lower() + "".join(w.capitalize() for w in words[1:])
    if add_prefix and not camel.startswith(TABLE_PREFIX):
        camel = TABLE_PREFIX + camel
    return camel[:TRUNCATE_LIMIT]


def build_field_rename(layer):
    """Retorna {nome_original: nome_camelCase} resolvendo duplicatas."""
    field_rename = {}
    seen = set()
    for field in layer.fields():
        f_orig  = field.name()
        f_clean = sanitize_name(f_orig, add_prefix=False)
        base    = f_clean
        count   = 1
        while f_clean in seen:
            f_clean = f"{base}_{count}"
            count  += 1
        seen.add(f_clean)
        field_rename[f_orig] = f_clean
    return field_rename


# =============================================================================
# --- LOG ---
# =============================================================================

def log(msg, level=None):
    if level is None:
        level = Qgis.MessageLevel.Info
    print(f"  {msg}")
    sys.stdout.flush()
    if INSIDE_QGIS:
        QgsMessageLog.logMessage(str(msg), 'Upload PostGIS', level=level)

def log_ok(msg):   log(f"OK  {msg}", Qgis.MessageLevel.Success)
def log_warn(msg): log(f"AV  {msg}", Qgis.MessageLevel.Warning)
def log_err(msg):  log(f"ERR {msg}", Qgis.MessageLevel.Critical)


# =============================================================================
# --- CREDENCIAIS via conexao salva no QGIS ---
# =============================================================================

def get_pg_database_name(connection_name):
    """
    Le o nome do banco da conexao PostGIS salva no QGIS.
    Nao exige senha no codigo - usa o que ja esta configurado no QGIS.
    """
    from qgis.PyQt.QtCore import QSettings
    settings = QSettings()
    key = f"PostgreSQL/connections/{connection_name}/database"
    db  = settings.value(key, None)
    if not db:
        log_err(
            f" X Conexao '{connection_name}' nao encontrada.\n"
            f"     Verifique: Configuracoes > Opcoes > Fontes de Dados > PostGIS"
        )
        return None
    return db


# =============================================================================
# --- COLETA DE CAMADAS ---
# =============================================================================

def parse_sublayer_name(sub):
    if '!!::!!' in sub:
        parts = sub.split('!!::!!')
        return parts[1] if len(parts) > 1 else None
    parts = sub.split(':')
    return parts[1].strip() if len(parts) > 1 else None


def get_layers_to_process():
    layers = []

    if USE_OPEN_LAYERS:
        if not INSIDE_QGIS:
            log_err("USE_OPEN_LAYERS=True requer QGIS aberto.")
            return []
        all_layers = list(QgsProject.instance().mapLayers().values())
        log(f"Analisando {len(all_layers)} camadas no projeto...")
        for layer in all_layers:
            if layer.type() != QgsMapLayer.VectorLayer:
                continue
            source = layer.source()
            if '.gpkg' not in source.lower():
                continue
            entry = {
                'obj':        layer,
                'name_orig':  layer.name(),
                'name_clean': sanitize_name(layer.name(), add_prefix=True),
                'source_path': source.split('|')[0],
            }
            log(f"   [OK] {layer.name()}  ->  {entry['name_clean']}")
            layers.append(entry)

    else:
        if not os.path.exists(INPUT_GPKG):
            log_err(f"Arquivo nao encontrado: {INPUT_GPKG}")
            return []
        gpkg_obj = QgsVectorLayer(INPUT_GPKG, "temp", "ogr")
        if not gpkg_obj.isValid():
            log_err(f"GPKG invalido: {INPUT_GPKG}")
            return []
        for sub in gpkg_obj.dataProvider().subLayers():
            name = parse_sublayer_name(sub)
            if not name:
                continue
            layer = QgsVectorLayer(f"{INPUT_GPKG}|layername={name}", name, "ogr")
            if not layer.isValid():
                log_warn(f"Camada invalida, ignorada: {name}")
                continue
            entry = {
                'obj':        layer,
                'name_orig':  name,
                'name_clean': sanitize_name(name, add_prefix=True),
                'source_path': INPUT_GPKG,
            }
            log(f"   [OK] {name}  ->  {entry['name_clean']}")
            layers.append(entry)

    return layers


# =============================================================================
# --- SCRIPT PRINCIPAL ---
# =============================================================================

def main():
    print("=" * 60)
    print("  UPLOAD GPKG -> PostGIS")
    print(f"  Conexao  : {PG_CONNECTION_NAME}")
    print(f"  Schema   : {PG_SCHEMA}")
    print(f"  Prefixo  : {TABLE_PREFIX}")
    print("=" * 60)

    # 1. Validar conexao salva no QGIS e obter nome do banco
    db_name = get_pg_database_name(PG_CONNECTION_NAME)
    if not db_name:
        return
    log(f"Banco: {db_name}")

    # 2. Coletar camadas
    try:
        layers = get_layers_to_process()
    except Exception:
        log_err("Erro ao coletar camadas:")
        traceback.print_exc()
        return

    if not layers:
        print("\nNenhuma camada GPKG encontrada.")
        return

    print(f"\nProcessando {len(layers)} camadas...\n")

    sucesso = 0
    falha   = 0

    for i, l_data in enumerate(layers, 1):
        layer      = l_data['obj']
        orig_name  = l_data['name_orig']
        clean_name = l_data['name_clean']

        # 3. Mapear campos para camelCase
        field_rename = build_field_rename(layer)

        print(f"[{i}/{len(layers)}]  {orig_name}  ->  {PG_SCHEMA}.{clean_name}")
        for fo, fc in field_rename.items():
            if fo != fc:
                print(f"         campo: {fo}  ->  {fc}")

        field_map = []
        for field in layer.fields():
            field_map.append({
                'name':       field_rename[field.name()],
                'type':       field.type(),
                'length':     field.length(),
                'precision':  field.precision(),
                'expression': f'"{field.name()}"',
            })

        try:
            # Passo A: refatorar campos em memoria (renomeia sem alterar dados)
            result = processing.run("native:refactorfields", {
                'INPUT':          layer,
                'FIELDS_MAPPING': field_map,
                'OUTPUT':         'memory:',
            })
            layer_clean = result['OUTPUT']

            # Passo B: importar camada com campos limpos para o PostGIS
            processing.run("native:importintopostgis", {
                'INPUT':                   layer_clean,
                'DATABASE':                db_name,
                'SCHEMA':                  PG_SCHEMA,
                'TABLENAME':               clean_name,
                'GEOMETRY_COLUMN':         PG_GEOMETRY_COLUMN,
                'ENCODING':                'UTF-8',
                'OVERWRITE':               OVERWRITE,
                'CREATEINDEX':             CREATE_INDEX,
                'LOWERCASE_NAMES':         LOWERCASE_NAMES,
                'DROP_STRING_LENGTH':      DROP_STRING_LENGTH,
                'FORCE_SINGLEPART':        FORCE_SINGLEPART,
                'INVALID_FEATURES_FILTER': INVALID_FEATURES_FILTER,
            })

            log_ok(f"Importada: {PG_SCHEMA}.{clean_name}")
            sucesso += 1

        except Exception as e:
            log_err(f"Erro em '{orig_name}': {e}")
            traceback.print_exc()
            falha += 1

        print()

    # Resumo
    print("=" * 60)
    print(f"  CONCLUIDO  |  OK {sucesso} importadas  |  ERR {falha} com erro")
    print(f"  Destino: {db_name} / {PG_SCHEMA}")
    print("=" * 60)


# =============================================================================
# --- ENTRADA ---
# =============================================================================

if INSIDE_QGIS:
    main()
else:
    qgs = QgsApplication([], False)
    qgs.initQgis()
    from qgis.analysis import QgsNativeAlgorithms
    from processing.core.Processing import Processing
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
    main()
    qgs.exitQgis()