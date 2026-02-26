# =============================================================================
# clean_export_and_upload.py
# Autor: Matheus Aviz
# Fluxo completo em um unico script:
#
#   1. Le camadas GPKG (abertas no QGIS ou do disco)
#   2. Sanitiza nome de tabela  -> camelCase + prefixo dpdu_
#   3. Sanitiza nomes de campos -> camelCase (sem prefixo)
#   4. Salva novo GPKG local com estilos (QML + SLD) em layer_styles
#   5. Importa para PostgreSQL/PostGIS
#   6. Grava QML + SLD na tabela public.layer_styles do banco
#
# Credenciais: lidas da conexao salva no QGIS (sem senha no codigo)
#
# Como rodar:
#   QGIS: Complementos > Terminal Python > Mostrar Editor > Abrir Script... > Executar
# =============================================================================

import os
import re
import sys
import sqlite3
import unicodedata
import traceback
import tempfile

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
USE_OPEN_LAYERS = True                          # True = camadas abertas no QGIS
INPUT_GPKG      = r"C:/Caminho/Arquivo.gpkg"    # usado se USE_OPEN_LAYERS = False

# --- GPKG local de saida ---
OUTPUT_GPKG        = ""                         # "" = mesma pasta do GPKG original
GPKG_OUTPUT_PREFIX = "Teste006_"                # prefixo do arquivo gerado

# --- Destino PostgreSQL ---
PG_CONNECTION_NAME = "Planejamento"             # nome exato da conexao salva no QGIS
PG_SCHEMA          = "geohab"                   #schema do banco  
PG_GEOMETRY_COLUMN = "geom"                     #coluna de geometria

# --- Nomenclatura ---
TABLE_PREFIX   = "teste006_"                    #prefixo do arquivo gerado 
TRUNCATE_LIMIT = 63                             #limite de caracteres do nome da tabela

# --- Seletor de camadas ---
# Liste os INDICES (base 0) das camadas a processar, ex: [0, 2, 5]
# Deixe [] para processar TODAS
LAYER_FILTER = [0, 1, 2, 3, 4] #Inica em 0, para processar camadas espeficias usar ex: [0, 2, 5, ...]

# --------------------------------------------------------------------------
# --- Controle para o que gerar ---
GENERATE_LOCAL_GPKG   = True   # Salvar GPKG limpo local
GENERATE_LOCAL_STYLES = True   # Gravar QML+SLD no layer_styles do GPKG local
UPLOAD_TO_POSTGIS     = True   # Importar camadas para o PostgreSQL
UPLOAD_STYLES_TO_PG   = True   # Gravar QML+SLD na public.layer_styles do banco
GENERATE_SLD_FILES    = True   # Salvar arquivos .sld em pasta separada

SLD_OUTPUT_FOLDER     = ""     #Pasta onde os arquivos .sld serao salvos | "" = pasta gerada no mesmo local do GPKG


# --- Comportamento ---
OVERWRITE               = True             # True = sobreescrever tabela existente
CREATE_INDEX            = True             # True = criar indice espacial
LOWERCASE_NAMES         = True             # True = nomes em minusculo
DROP_STRING_LENGTH      = False            # True = remover limites de string
FORCE_SINGLEPART        = False            #Força poligonos simples
INVALID_FEATURES_FILTER = 1                # 0=ignorar | 1=pular invalidas | 2=parar

# =============================================================================
# --- SANITIZACAO ---
# =============================================================================

def sanitize_name(text, add_prefix=False):
    """
    snake_case minusculo + prefixo opcional + truncate.
    Espacos e caracteres especiais viram underscore.
    Ex: "Uso do Solo (2024)" -> "dpdu_uso_do_solo_2024"
        "Area km2"           -> "area_km2"
    """
    if not text or not text.strip():
        return f"{TABLE_PREFIX}sem_nome" if add_prefix else "sem_nome"
    # 1. Remover diacriticos
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    # 2. Nao alfanumerico -> underscore, colapsar multiplos, strip
    text = re.sub(r'[^a-zA-Z0-9]+', '_', text).strip('_').lower()
    if not text:
        return f"{TABLE_PREFIX}sem_nome" if add_prefix else "sem_nome"
    # 3. Prefixo
    if add_prefix and not text.startswith(TABLE_PREFIX):
        text = TABLE_PREFIX + text
    # 4. Truncar
    return text[:TRUNCATE_LIMIT]


def build_field_rename(layer):
    """Retorna {nome_original: nome_snake_case}, resolve duplicatas com sufixo numerico."""
    field_rename = {}
    seen = set()
    for field in layer.fields():
        f_orig  = field.name()
        f_clean = sanitize_name(f_orig, add_prefix=False)
        base = f_clean
        count = 1
        while f_clean in seen:
            f_clean = f"{base}_{count}"
            count += 1
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
        QgsMessageLog.logMessage(str(msg), 'Clean+Upload', level=level)

def log_ok(msg):   log(f"OK  {msg}", Qgis.MessageLevel.Success)
def log_warn(msg): log(f"AV  {msg}", Qgis.MessageLevel.Warning)
def log_err(msg):  log(f"ERR {msg}", Qgis.MessageLevel.Critical)


# =============================================================================
# --- CONEXAO PostgreSQL via QSettings do QGIS ---
# =============================================================================

def get_pg_credentials(connection_name):
    """
    Le host, porta, banco, usuario e senha da conexao salva no QGIS.
    Retorna dict ou None se conexao nao encontrada.
    """
    from qgis.PyQt.QtCore import QSettings
    from qgis.core import QgsAuthMethodConfig, QgsApplication

    s    = QSettings()
    base = f"PostgreSQL/connections/{connection_name}"

    host = s.value(f"{base}/host",     "localhost")
    port = s.value(f"{base}/port",     "5432")
    db   = s.value(f"{base}/database", None)

    if not db:
        log_err(
            f"Conexao '{connection_name}' nao encontrada no QGIS.\n"
            f"     Verifique: Configuracoes > Opcoes > Fontes de Dados > PostGIS"
        )
        return None

    user     = s.value(f"{base}/username", "")
    password = s.value(f"{base}/password", "")

    # Se usuario/senha estiverem em branco, tentar via authcfg
    authcfg = s.value(f"{base}/authcfg", "")
    if authcfg and (not user or not password):
        cfg = QgsAuthMethodConfig()
        QgsApplication.authManager().loadAuthenticationConfig(authcfg, cfg, True)
        user     = cfg.config("username", user)
        password = cfg.config("password", password)

    return {
        "host":     host,
        "port":     port,
        "database": db,
        "user":     user,
        "password": password,
    }


def pg_connect(creds):
    """Abre conexao psycopg2 com as credenciais do QGIS."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=creds["host"],
            port=creds["port"],
            dbname=creds["database"],
            user=creds["user"],
            password=creds["password"],
        )
        return conn
    except ImportError:
        log_err("psycopg2 nao instalado. No OSGeo4W Shell: pip install psycopg2-binary")
        return None
    except Exception as e:
        log_err(f"Falha ao conectar no PostgreSQL: {e}")
        return None


# =============================================================================
# --- GPKG local: layer_styles via SQLite ---
# =============================================================================

DDL_LAYER_STYLES_SQLITE = """
CREATE TABLE IF NOT EXISTS layer_styles (
    id                INTEGER  PRIMARY KEY AUTOINCREMENT,
    f_table_catalog   TEXT     DEFAULT '',
    f_table_schema    TEXT     DEFAULT '',
    f_table_name      TEXT     NOT NULL,
    f_geometry_column TEXT     DEFAULT 'geom',
    styleName         TEXT     NOT NULL,
    styleQML          TEXT,
    styleSLD          TEXT,
    useAsDefault      INTEGER  DEFAULT 1,
    description       TEXT     DEFAULT '',
    owner             TEXT     DEFAULT '',
    ui                TEXT,
    update_time       DATETIME DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now'))
);
"""

def ensure_sqlite_layer_styles(gpkg_path):
    con = sqlite3.connect(gpkg_path)
    con.executescript(DDL_LAYER_STYLES_SQLITE)
    con.commit()
    con.close()


def save_style_sqlite(gpkg_path, table_name, geom_col, qml_xml, sld_xml):
    con = sqlite3.connect(gpkg_path)
    cur = con.cursor()
    cur.execute(
        "DELETE FROM layer_styles WHERE f_table_name=? AND styleName=?",
        (table_name, table_name)
    )
    cur.execute(
        """INSERT INTO layer_styles
           (f_table_catalog, f_table_schema, f_table_name,
            f_geometry_column, styleName, styleQML, styleSLD,
            useAsDefault, description, owner)
           VALUES ('','',?,?,?,?,?,1,'','')""",
        (table_name, geom_col, table_name, qml_xml, sld_xml)
    )
    con.commit()
    con.close()


# =============================================================================
# --- PostgreSQL: public.layer_styles via psycopg2 ---
# =============================================================================

def get_geom_type_name(layer):
    """
    Retorna o tipo de geometria como string legivel (Point, LineString, Polygon, etc.)
    para preencher o campo type da public.layer_styles.
    """
    from qgis.core import QgsWkbTypes
    wkb_type = layer.wkbType()
    type_map = {
        QgsWkbTypes.Point:              "Point",
        QgsWkbTypes.MultiPoint:         "MultiPoint",
        QgsWkbTypes.LineString:         "LineString",
        QgsWkbTypes.MultiLineString:    "MultiLineString",
        QgsWkbTypes.Polygon:            "Polygon",
        QgsWkbTypes.MultiPolygon:       "MultiPolygon",
        QgsWkbTypes.PointZ:             "PointZ",
        QgsWkbTypes.MultiPointZ:        "MultiPointZ",
        QgsWkbTypes.LineStringZ:        "LineStringZ",
        QgsWkbTypes.MultiLineStringZ:   "MultiLineStringZ",
        QgsWkbTypes.PolygonZ:           "PolygonZ",
        QgsWkbTypes.MultiPolygonZ:      "MultiPolygonZ",
    }
    return type_map.get(wkb_type, QgsWkbTypes.displayString(wkb_type))


def save_style_pg(conn, db_name, schema, table_name, geom_col, geom_type, qml_xml, sld_xml):
    """
    Insere QML e SLD na tabela public.layer_styles do PostgreSQL.

    Correcoes aplicadas:
      - f_table_catalog : nome real do banco (db_name), nao string vazia
      - description     : data/hora do carregamento no formato ISO
      - type            : tipo de geometria da camada (Point, Polygon, etc.)
    """
    from datetime import datetime
    load_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur = conn.cursor()
    cur.execute(
        """DELETE FROM public.layer_styles
           WHERE f_table_catalog=%s AND f_table_schema=%s
             AND f_table_name=%s AND stylename=%s""",
        (db_name, schema, table_name, table_name)
    )
    cur.execute(
        """INSERT INTO public.layer_styles
           (f_table_catalog, f_table_schema, f_table_name,
            f_geometry_column, stylename, styleqml, stylesld,
            useasdefault, description, type)
           VALUES (%s, %s, %s, %s, %s,
                   %s::xml, %s::xml,
                   true, %s, %s)""",
        (
            db_name,
            schema,
            table_name,
            geom_col,
            table_name,
            qml_xml,
            sld_xml,
            f"Carregado em {load_time}",   # description = data/hora do upload
            geom_type,                      # type = tipo de geometria real
        )
    )
    conn.commit()
    cur.close()


# =============================================================================
# --- EXPORTAR QML + SLD via arquivo temporario ---
# =============================================================================

def export_styles(layer, field_rename):
    """
    Grava QML e SLD em arquivos temp, le o conteudo,
    substitui nomes de campos antigos pelos novos no XML.
    Retorna (qml_str, sld_str).
    """
    tmp_dir  = tempfile.mkdtemp()
    qml_path = os.path.join(tmp_dir, "style.qml")
    sld_path = os.path.join(tmp_dir, "style.sld")

    # QML
    qml_xml = ""
    layer.saveNamedStyle(qml_path)
    if os.path.exists(qml_path):
        with open(qml_path, 'r', encoding='utf-8') as f:
            qml_xml = f.read()
        os.remove(qml_path)
        for f_orig, f_clean in field_rename.items():
            if f_orig != f_clean:
                qml_xml = qml_xml.replace(f'field="{f_orig}"',      f'field="{f_clean}"')
                qml_xml = qml_xml.replace(f'<field name="{f_orig}"', f'<field name="{f_clean}"')
                qml_xml = qml_xml.replace(f'>{f_orig}</',            f'>{f_clean}</')

    # SLD
    sld_xml = ""
    layer.saveSldStyle(sld_path)
    if os.path.exists(sld_path):
        with open(sld_path, 'r', encoding='utf-8') as f:
            sld_xml = f.read()
        os.remove(sld_path)
        for f_orig, f_clean in field_rename.items():
            if f_orig != f_clean:
                sld_xml = sld_xml.replace(
                    f'<ogc:PropertyName>{f_orig}</ogc:PropertyName>',
                    f'<ogc:PropertyName>{f_clean}</ogc:PropertyName>'
                )

    try:
        os.rmdir(tmp_dir)
    except Exception:
        pass

    return qml_xml, sld_xml


# =============================================================================
# --- COLETA DE CAMADAS ---
# =============================================================================

def parse_sublayer_name(sub):
    if '!!::!!' in sub:
        parts = sub.split('!!::!!')
        return parts[1] if len(parts) > 1 else None
    parts = sub.split(':')
    return parts[1].strip() if len(parts) > 1 else None


# Tabelas internas do GPKG/QGIS que devem ser ignoradas
GPKG_INTERNAL_TABLES = {
    "layer_styles", "qgis_projects", "gpkg_contents",
    "gpkg_geometry_columns", "gpkg_spatial_ref_sys",
    "gpkg_extensions", "gpkg_metadata", "gpkg_metadata_reference",
    "gpkg_data_columns", "gpkg_data_column_constraints",
    "gpkg_tile_matrix", "gpkg_tile_matrix_set",
}


def is_valid_geo_layer(layer):
    """
    Retorna True somente se a camada:
      - For vetorial valida
      - Tiver geometria real (nao NullGeometry)
      - Nao for uma tabela interna do GPKG ou do QGIS
    """
    from qgis.core import QgsWkbTypes
    if not layer.isValid():
        return False
    if layer.type() != QgsMapLayer.VectorLayer:
        return False
    if layer.wkbType() == QgsWkbTypes.NoGeometry:
        return False
    name_lower = layer.name().lower()
    if name_lower in GPKG_INTERNAL_TABLES:
        return False
    return True


def get_layers_to_process():
    layers = []
    if USE_OPEN_LAYERS:
        if not INSIDE_QGIS:
            log_err("USE_OPEN_LAYERS=True requer QGIS aberto.")
            return []
        all_layers = list(QgsProject.instance().mapLayers().values())
        log(f"Analisando {len(all_layers)} camadas no projeto...")
        for layer in all_layers:
            source = layer.source()
            if '.gpkg' not in source.lower():
                continue
            if not is_valid_geo_layer(layer):
                log(f"   [SKIP] sem geometria ou tabela interna: {layer.name()}")
                continue
            entry = {
                'obj':        layer,
                'name_orig':  layer.name(),
                'name_clean': sanitize_name(layer.name(), add_prefix=True),
                'source_path': source.split('|')[0],
            }
            log(f"   [{len(layers)}] {layer.name()}  ->  {entry['name_clean']}")
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
            if name.lower() in GPKG_INTERNAL_TABLES:
                log(f"   [SKIP] tabela interna: {name}")
                continue
            layer = QgsVectorLayer(f"{INPUT_GPKG}|layername={name}", name, "ogr")
            if not is_valid_geo_layer(layer):
                log(f"   [SKIP] sem geometria ou invalida: {name}")
                continue
            entry = {
                'obj':        layer,
                'name_orig':  name,
                'name_clean': sanitize_name(name, add_prefix=True),
                'source_path': INPUT_GPKG,
            }
            log(f"   [{len(layers)}] {name}  ->  {entry['name_clean']}")
            layers.append(entry)
    return layers


# =============================================================================
# --- SCRIPT PRINCIPAL ---
# =============================================================================

def main():
    print("=" * 60)
    print("  CLEAN + EXPORT GPKG + UPLOAD PostGIS")
    print(f"  Conexao PG : {PG_CONNECTION_NAME}  |  Schema: {PG_SCHEMA}")
    print(f"  Prefixo    : {TABLE_PREFIX}")
    print("=" * 60)

    # 1. Credenciais PostgreSQL (somente se necessario)
    creds = None
    if UPLOAD_TO_POSTGIS or UPLOAD_STYLES_TO_PG:
        creds = get_pg_credentials(PG_CONNECTION_NAME)
        if not creds:
            return
        log(f"Banco: {creds['database']}  host: {creds['host']}:{creds['port']}")

    # 2. Conexao psycopg2 (somente se necessario)
    pg_conn = None
    if UPLOAD_TO_POSTGIS or UPLOAD_STYLES_TO_PG:
        pg_conn = pg_connect(creds)
        if not pg_conn:
            return

    # 3. Coletar camadas
    try:
        layers = get_layers_to_process()
    except Exception:
        log_err("Erro ao coletar camadas:")
        traceback.print_exc()
        if pg_conn:
            pg_conn.close()
        return

    if not layers:
        print("\nNenhuma camada GPKG encontrada.")
        if pg_conn:
            pg_conn.close()
        return

    # Aplicar filtro de indices se definido
    if LAYER_FILTER:
        invalid = [i for i in LAYER_FILTER if i < 0 or i >= len(layers)]
        if invalid:
            log_warn(f"Indices fora do range ignorados: {invalid} (total: {len(layers)} camadas)")
        layers_selected = [layers[i] for i in LAYER_FILTER if 0 <= i < len(layers)]
        log(f"Filtro ativo: {len(layers_selected)}/{len(layers)} camadas selecionadas {LAYER_FILTER}")
        layers = layers_selected
    else:
        log(f"Sem filtro: processando todas as {len(layers)} camadas")

    # 4. Definir caminho do GPKG de saida
    final_output = OUTPUT_GPKG
    if final_output and os.path.isdir(final_output):
        file_name    = os.path.basename(layers[0]['source_path'])
        final_output = os.path.join(final_output, f"{GPKG_OUTPUT_PREFIX}{file_name}")
    if not final_output:
        base         = layers[0]['source_path']
        final_output = os.path.join(
            os.path.dirname(base),
            f"{GPKG_OUTPUT_PREFIX}{os.path.basename(base)}"
        )

    log(f"GPKG local de saida: {final_output}")

    out_dir = os.path.dirname(final_output)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Pasta para arquivos .sld separados
    if GENERATE_SLD_FILES:
        sld_folder = SLD_OUTPUT_FOLDER if SLD_OUTPUT_FOLDER else os.path.join(
            os.path.dirname(final_output), "SLD"
        )
        os.makedirs(sld_folder, exist_ok=True)
        log(f"Pasta SLD: {sld_folder}")
    else:
        sld_folder = None

    if os.path.exists(final_output):
        try:
            os.remove(final_output)
            log("GPKG anterior removido.")
        except Exception as e:
            log_warn(f"Nao foi possivel remover GPKG existente: {e}")

    print(f"\nProcessando {len(layers)} camadas...\n")

    sucesso_gpkg = 0
    sucesso_pg   = 0
    falha        = 0

    for i, l_data in enumerate(layers, 1):
        layer      = l_data['obj']
        orig_name  = l_data['name_orig']
        clean_name = l_data['name_clean']

        # Mapa de renomeacao de campos
        field_rename = build_field_rename(layer)

        print(f"[{i}/{len(layers)}]  {orig_name}  ->  {clean_name}")
        for fo, fc in field_rename.items():
            if fo != fc:
                print(f"         campo: {fo}  ->  {fc}")

        # field_map para refactorfields
        field_map = []
        for field in layer.fields():
            field_map.append({
                'name':       field_rename[field.name()],
                'type':       field.type(),
                'length':     field.length(),
                'precision':  field.precision(),
                'expression': f'"{field.name()}"',
            })

        # Detectar coluna de geometria
        geom_col = "geom"
        try:
            geom_col = layer.dataProvider().geometryColumnName() or "geom"
        except Exception:
            pass

        # --- A: Salvar no GPKG local ---
        if GENERATE_LOCAL_GPKG:
            try:
                output_uri = (
                    f"ogr:dbname='{final_output}' "
                    f"table=\"{clean_name}\" (geom) format=GPKG"
                )
                processing.run("native:refactorfields", {
                    'INPUT':          layer,
                    'FIELDS_MAPPING': field_map,
                    'OUTPUT':         output_uri,
                })
                log_ok(f"✅📂🖥️ GPKG local: {clean_name}")
                sucesso_gpkg += 1

            except Exception as e:
                log_err(f"❌📂🖥️Erro ao salvar GPKG '{orig_name}': {e}")
                traceback.print_exc()
                falha += 1
                print()
                continue
        else:
            log(f"[SKIP] GPKG local desligado: {clean_name}")

        # --- B: Exportar estilos e gravar no GPKG local (layer_styles SQLite) ---
        qml_xml, sld_xml = "", ""
        try:
            qml_xml, sld_xml = export_styles(layer, field_rename)
        except Exception as e:
            log_warn(f"Falha ao exportar estilos de '{orig_name}': {e}")
            traceback.print_exc()

        if GENERATE_LOCAL_GPKG and GENERATE_LOCAL_STYLES and (qml_xml or sld_xml):
            try:
                ensure_sqlite_layer_styles(final_output)
                save_style_sqlite(final_output, clean_name, geom_col, qml_xml, sld_xml)
                log_ok(f"✅📂🖼️ Estilos GPKG (QML+SLD): {clean_name}")
            except Exception as e:
                log_warn(f"❌📂🖼️ Estilos GPKG nao gravados para '{orig_name}': {e}")
                traceback.print_exc()
        elif not GENERATE_LOCAL_STYLES:
            log(f"[SKIP] Estilos GPKG local desligado: {clean_name}")

        # --- B2: Salvar arquivo .sld em pasta separada ---
        if GENERATE_SLD_FILES and sld_folder and sld_xml:
            try:
                sld_file = os.path.join(sld_folder, f"{clean_name}.sld")
                with open(sld_file, 'w', encoding='utf-8') as f:
                    f.write(sld_xml)
                log_ok(f"SLD salvo: {sld_file}")
            except Exception as e:
                log_warn(f"Erro ao salvar SLD de '{orig_name}': {e}")
        elif not GENERATE_SLD_FILES:
            log(f"[SKIP] SLD arquivo desligado: {clean_name}")

        # --- C: Importar para PostgreSQL ---
        if UPLOAD_TO_POSTGIS:
            try:
                result = processing.run("native:refactorfields", {
                    'INPUT':          layer,
                    'FIELDS_MAPPING': field_map,
                    'OUTPUT':         'memory:',
                })
                layer_clean = result['OUTPUT']

                # native:importintopostgis espera o NOME DA CONEXAO salva
                # no QGIS (campo DATABASE), nao o nome do banco
                processing.run("native:importintopostgis", {
                    'INPUT':                   layer_clean,
                    'DATABASE':                PG_CONNECTION_NAME,
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
                log_ok(f"✅🛢️⬆️ PostGIS: {PG_SCHEMA}.{clean_name}")
                sucesso_pg += 1

            except Exception as e:
                log_err(f"❌🛢️⬆️ Erro ao importar '{orig_name}' para PostGIS: {e}")
                traceback.print_exc()
                falha += 1
                print()
                continue
        else:
            log(f"[SKIP] Upload PostGIS desligado: {clean_name}")

        # --- D: Gravar estilos na public.layer_styles do PostgreSQL ---
        if UPLOAD_STYLES_TO_PG and (qml_xml or sld_xml):
            try:
                save_style_pg(pg_conn, creds['database'], PG_SCHEMA, clean_name, geom_col, get_geom_type_name(layer), qml_xml, sld_xml)
                log_ok(f"✅🛢️🖼️ Estilos PostGIS (public.layer_styles): {clean_name}")
            except Exception as e:
                log_warn(f"❌🛢️🖼️ Estilos PostGIS nao gravados para '{orig_name}': {e}")
                traceback.print_exc()
                try:
                    pg_conn.rollback()
                except Exception:
                    pass
        elif not UPLOAD_STYLES_TO_PG:
            log(f"[SKIP] Estilos PostGIS desligado: {clean_name}")

        print()

    if pg_conn:
        pg_conn.close()

    # Resumo
    print("=" * 60)
    print(f"  ✅ CONCLUIDO!\n")
    if GENERATE_LOCAL_GPKG:
        print(f"  📂 GPKG local : {sucesso_gpkg}\n Caminho: {final_output}")
    if GENERATE_SLD_FILES and sld_folder:
        print(f"  📂 SLD local  : arquivos .sld\n Caminho: {sld_folder}")
    if UPLOAD_TO_POSTGIS and creds:
        print(f" 🛢️ PostGIS    : {sucesso_pg} camadas   ->  {creds['database']}.{PG_SCHEMA}")
    print(f"  ❌ Erros      : {falha}")
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