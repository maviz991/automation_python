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
#   QGIS: Complementos > Terminal Python > Mostrar Editor > Abrir > Executar
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
USE_OPEN_LAYERS = True                        # True = camadas abertas no QGIS
INPUT_GPKG      = r"C:/Caminho/Arquivo.gpkg"  # usado se USE_OPEN_LAYERS = False

# --- GPKG local de saida ---
OUTPUT_GPKG        = ""         # "" = mesma pasta do GPKG original
GPKG_OUTPUT_PREFIX = "teste_"   # prefixo do arquivo gerado

# --- Destino PostgreSQL ---
PG_CONNECTION_NAME = "Planejamento"  # nome exato da conexao salva no QGIS
PG_SCHEMA          = "geohab"
PG_GEOMETRY_COLUMN = "geom"

# --- Nomenclatura ---
TABLE_PREFIX   = "teste_"
TRUNCATE_LIMIT = 63

# --- Comportamento ---
OVERWRITE               = True
CREATE_INDEX            = True
LOWERCASE_NAMES         = True
DROP_STRING_LENGTH      = False
FORCE_SINGLEPART        = False
INVALID_FEATURES_FILTER = 1    # 0=ignorar | 1=pular invalidas | 2=parar

# =============================================================================
# --- SANITIZACAO ---
# =============================================================================

def sanitize_name(text, add_prefix=False):
    """camelCase + prefixo opcional + truncate."""
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
    """Retorna {nome_original: nome_camelCase}, resolve duplicatas com sufixo."""
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

def save_style_pg(conn, schema, table_name, geom_col, qml_xml, sld_xml):
    """
    Insere QML e SLD na tabela public.layer_styles do PostgreSQL.
    Usa o DDL existente:
      styleqml xml, stylesld xml, useasdefault bool ...
    """
    cur = conn.cursor()
    cur.execute(
        """DELETE FROM public.layer_styles
           WHERE f_table_schema=%s AND f_table_name=%s AND stylename=%s""",
        (schema, table_name, table_name)
    )
    cur.execute(
        """INSERT INTO public.layer_styles
           (f_table_catalog, f_table_schema, f_table_name,
            f_geometry_column, stylename, styleqml, stylesld,
            useasdefault, description, type)
           VALUES (%s, %s, %s, %s, %s,
                   %s::xml, %s::xml,
                   true, '', 'qgis')""",
        ('', schema, table_name, geom_col, table_name, qml_xml, sld_xml)
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
    print("  CLEAN + EXPORT GPKG + UPLOAD PostGIS")
    print(f"  Conexao PG : {PG_CONNECTION_NAME}  |  Schema: {PG_SCHEMA}")
    print(f"  Prefixo    : {TABLE_PREFIX}")
    print("=" * 60)

    # 1. Credenciais PostgreSQL
    creds = get_pg_credentials(PG_CONNECTION_NAME)
    if not creds:
        return
    log(f"Banco: {creds['database']}  host: {creds['host']}:{creds['port']}")

    # 2. Conexao psycopg2
    pg_conn = pg_connect(creds)
    if not pg_conn:
        return

    # 3. Coletar camadas
    try:
        layers = get_layers_to_process()
    except Exception:
        log_err("Erro ao coletar camadas:")
        traceback.print_exc()
        pg_conn.close()
        return

    if not layers:
        print("\nNenhuma camada GPKG encontrada.")
        pg_conn.close()
        return

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
            log_ok(f"GPKG local: {clean_name}")
            sucesso_gpkg += 1

        except Exception as e:
            log_err(f"Erro ao salvar GPKG '{orig_name}': {e}")
            traceback.print_exc()
            falha += 1
            print()
            continue

        # --- B: Exportar estilos e gravar no GPKG local (layer_styles SQLite) ---
        qml_xml, sld_xml = "", ""
        try:
            qml_xml, sld_xml = export_styles(layer, field_rename)
            ensure_sqlite_layer_styles(final_output)
            save_style_sqlite(final_output, clean_name, geom_col, qml_xml, sld_xml)
            log_ok(f"Estilos GPKG (QML+SLD): {clean_name}")
        except Exception as e:
            log_warn(f"Estilos GPKG nao gravados para '{orig_name}': {e}")
            traceback.print_exc()

        # --- C: Importar para PostgreSQL ---
        try:
            result = processing.run("native:refactorfields", {
                'INPUT':          layer,
                'FIELDS_MAPPING': field_map,
                'OUTPUT':         'memory:',
            })
            layer_clean = result['OUTPUT']

            processing.run("native:importintopostgis", {
                'INPUT':                   layer_clean,
                'DATABASE':                creds['database'],
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
            log_ok(f"PostGIS: {PG_SCHEMA}.{clean_name}")
            sucesso_pg += 1

        except Exception as e:
            log_err(f"Erro ao importar '{orig_name}' para PostGIS: {e}")
            traceback.print_exc()
            falha += 1
            print()
            continue

        # --- D: Gravar estilos na public.layer_styles do PostgreSQL ---
        if qml_xml or sld_xml:
            try:
                save_style_pg(pg_conn, PG_SCHEMA, clean_name, geom_col, qml_xml, sld_xml)
                log_ok(f"Estilos PostGIS (public.layer_styles): {clean_name}")
            except Exception as e:
                log_warn(f"Estilos PostGIS nao gravados para '{orig_name}': {e}")
                traceback.print_exc()
                try:
                    pg_conn.rollback()
                except Exception:
                    pass

        print()

    pg_conn.close()

    # Resumo
    print("=" * 60)
    print(f"  CONCLUIDO")
    print(f"  GPKG local : {sucesso_gpkg} camadas  ->  {final_output}")
    print(f"  PostGIS    : {sucesso_pg} camadas   ->  {creds['database']}/{PG_SCHEMA}")
    print(f"  Erros      : {falha}")
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