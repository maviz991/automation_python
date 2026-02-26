# =============================================================================
# clean_and_export_gpkg.py
# Autor: Matheus Aviz
# Finalidade:
#   - Sanitizar SOMENTE o nome da camada/tabela (remove acentos, caracteres
#     especiais, espaços; aplica camelCase e prefixo dpdu_)
#   - Preservar campos EXATAMENTE como estão na fonte
#   - Copiar QML e SLD de cada camada para a tabela layer_styles do novo GPKG
#   - Exportar com codificação UTF-8
#
#
# Como rodar:
#   - QGIS: Complementos > Terminal Python > Mostrar Editor > Abrir Script... > Executar
#   - OSGeo4W Shell: python clean_and_export_gpkg.py (pip dos imports e qgis)
# =============================================================================

import os
import re
import sys
import sqlite3
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
    print("No OSGeo4W Shell, rode primeiro: py3_env")
    sys.exit(1)

# =============================================================================
# ---              CONFIGURAÇÕES DO USUÁRIO [ALTERAR AQUI]                  ---
# =============================================================================

# True  = usa camadas GPKG já abertas no projeto QGIS
# False = lê um GPKG do disco (preencha INPUT_GPKG)
USE_OPEN_LAYERS = True

INPUT_GPKG         = r"C:/Caminho/Para/Seu/Arquivo.gpkg"
OUTPUT_GPKG        = ""          # "" = mesma pasta do original
GPKG_OUTPUT_PREFIX = "teste_"

ADD_TO_PROJECT = False            # Adicionar camadas limpas ao projeto QGIS? (Importar novo GPKG, senão ele mistura tudo)

TABLE_PREFIX   = "teste_"         # GeoServer não aceita tabelas iniciando em número
TRUNCATE_LIMIT = 63              # Limite PostgreSQL / GeoServer

# =============================================================================
# --- SANITIZAÇÃO (apenas nome de tabela/camada) [NÃO ALTERAR A PARTIR DAQUI] ---
# =============================================================================

def sanitize_layer_name(text):
    """
    Sanitiza nome de camada:
      1. NFD - remove diacríticos
      2. Não alfanumérico - separador de palavra
      3. camelCase
      4. Adiciona TABLE_PREFIX
      5. Trunca em TRUNCATE_LIMIT

    CAMPOS NAO SAO ALTERADOS - preservados como estao na fonte.
    """
    if not text or not text.strip():
        return f"{TABLE_PREFIX}semNome"

    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')
    words = re.sub(r'[^a-zA-Z0-9]+', ' ', text).split()

    if not words:
        return f"{TABLE_PREFIX}semNome"

    camel = words[0].lower() + "".join(w.capitalize() for w in words[1:])

    if not camel.startswith(TABLE_PREFIX):
        camel = TABLE_PREFIX + camel

    return camel[:TRUNCATE_LIMIT]


# =============================================================================
# --- LOG ---
# =============================================================================

def log(msg, level=None):
    if level is None:
        level = Qgis.MessageLevel.Info
    print(f"  {msg}")
    sys.stdout.flush()
    if INSIDE_QGIS:
        QgsMessageLog.logMessage(str(msg), 'Limpeza GPKG', level=level)

def log_ok(msg):   log(f"OK  {msg}", Qgis.MessageLevel.Success)
def log_warn(msg): log(f"AV  {msg}", Qgis.MessageLevel.Warning)
def log_err(msg):  log(f"ERR {msg}", Qgis.MessageLevel.Critical)


# =============================================================================
# --- COLETA DE CAMADAS ---
# =============================================================================

def parse_sublayer_name(sub):
    """Extrai nome da subcamada (separador varia por versao do QGIS)."""
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
        log(f"🔍 Analisando {len(all_layers)} camadas abertas no projeto...")

        for layer in all_layers:
            if layer.type() != QgsMapLayer.VectorLayer:
                continue
            source = layer.source()
            if '.gpkg' not in source.lower():
                log(f"   [SKIP] nao e GPKG: {layer.name()}")
                continue

            entry = {
                'obj':         layer,
                'name_orig':   layer.name(),
                'name_clean':  sanitize_layer_name(layer.name()),
                'source_path': source.split('|')[0],
            }
            log(f"   [OK] {layer.name()}  ->  {entry['name_clean']}")
            layers.append(entry)

    else:
        if not os.path.exists(INPUT_GPKG):
            log_err(f"Arquivo nao encontrado: {INPUT_GPKG}")
            return []

        log(f"Lendo GPKG: {INPUT_GPKG}")
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
                'obj':         layer,
                'name_orig':   name,
                'name_clean':  sanitize_layer_name(name),
                'source_path': INPUT_GPKG,
            }
            log(f"   [OK] {name}  ->  {entry['name_clean']}")
            layers.append(entry)

    return layers


# =============================================================================
# --- GRAVAR layer_styles NO GPKG VIA SQLite ---
# =============================================================================

DDL_LAYER_STYLES = """
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


def ensure_layer_styles_table(gpkg_path):
    """Cria a tabela layer_styles se nao existir."""
    con = sqlite3.connect(gpkg_path)
    con.executescript(DDL_LAYER_STYLES)
    con.commit()
    con.close()


def save_style_to_gpkg(gpkg_path, table_name, geom_col, qml_xml, sld_xml):
    """
    Insere QML e SLD na tabela layer_styles do GPKG.
    Remove entrada anterior da mesma tabela antes de inserir.
    """
    con = sqlite3.connect(gpkg_path)
    cur = con.cursor()

    cur.execute(
        "DELETE FROM layer_styles WHERE f_table_name = ? AND styleName = ?",
        (table_name, table_name)
    )

    cur.execute(
        """INSERT INTO layer_styles
           (f_table_catalog, f_table_schema, f_table_name,
            f_geometry_column, styleName, styleQML, styleSLD,
            useAsDefault, description, owner)
           VALUES ('', '', ?, ?, ?, ?, ?, 1, '', '')""",
        (table_name, geom_col, table_name, qml_xml, sld_xml)
    )

    con.commit()
    con.close()


# =============================================================================
# --- SCRIPT PRINCIPAL ---
# =============================================================================

def main():
    print("=" * 60)
    print("  🧼 LIMPEZA E EXPORTACAO DE GPKG")
    print("  -> Nome da tabela : sanitizado (camelCase + prefixo dpdu_)")
    print("  -> Campos         : preservados exatamente como na fonte")
    print("  -> Estilos        : QML e SLD gravados em layer_styles")
    print("  -> Codificacao    : UTF-8")
    print("=" * 60)

    try:
        layers = get_layers_to_process()
    except Exception:
        log_err("Erro ao coletar camadas:")
        traceback.print_exc()
        return

    if not layers:
        print("\nNenhuma camada GPKG encontrada. Verifique as configuracoes.")
        return

    final_output = OUTPUT_GPKG

    if final_output and os.path.isdir(final_output):
        file_name = os.path.basename(layers[0]['source_path'])
        final_output = os.path.join(final_output, f"{GPKG_OUTPUT_PREFIX}{file_name}")

    if not final_output:
        base = layers[0]['source_path']
        final_output = os.path.join(
            os.path.dirname(base),
            f"{GPKG_OUTPUT_PREFIX}{os.path.basename(base)}"
        )

    log(f"Arquivo de saida: {final_output}")

    out_dir = os.path.dirname(final_output)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    if os.path.exists(final_output):
        try:
            os.remove(final_output)
            log("Arquivo anterior removido.")
        except Exception as e:
            log_warn(f"Nao foi possivel remover arquivo existente: {e}")

    print(f"\nProcessando {len(layers)} camadas...\n")

    sucesso = 0
    falha   = 0

    import tempfile

    for i, l_data in enumerate(layers, 1):
        layer      = l_data['obj']
        orig_name  = l_data['name_orig']
        clean_name = l_data['name_clean']

        field_rename = {}   # {nome_original: nome_limpo}
        seen = {}
        for field in layer.fields():
            f_orig  = field.name()
            f_clean = sanitize_layer_name(f_orig).replace(TABLE_PREFIX, "", 1)  # sem prefixo em campos
            base = f_clean
            count = 1
            while f_clean in seen.values():
                f_clean = f"{base}_{count}"
                count += 1
            field_rename[f_orig] = f_clean
            seen[f_orig] = f_clean

        print(f"[{i}/{len(layers)}]  {orig_name}  ->  {clean_name}")
        for fo, fc in field_rename.items():
            if fo != fc:
                print(f"         campo: {fo}  ->  {fc}")

        try:
            output_uri = (
                f"ogr:dbname='{final_output}' "
                f"table=\"{clean_name}\" (geom) format=GPKG"
            )

            field_map = []
            for field in layer.fields():
                field_map.append({
                    'name':       field_rename[field.name()],
                    'type':       field.type(),
                    'length':     field.length(),
                    'precision':  field.precision(),
                    'expression': f'"{field.name()}"',
                })

            processing.run("native:refactorfields", {
                'INPUT':          layer,
                'FIELDS_MAPPING': field_map,
                'OUTPUT':         output_uri,
            })

            log_ok(f"Exportada: {clean_name}")
            sucesso += 1

        except Exception as e:
            log_err(f"Erro ao exportar '{orig_name}': {e}")
            traceback.print_exc()
            falha += 1
            print()
            continue

        try:
            ensure_layer_styles_table(final_output)

            tmp_dir  = tempfile.mkdtemp()
            qml_path = os.path.join(tmp_dir, "style.qml")
            sld_path = os.path.join(tmp_dir, "style.sld")

            # --- QML ---
            qml_xml = ""
            layer.saveNamedStyle(qml_path)
            if os.path.exists(qml_path):
                with open(qml_path, 'r', encoding='utf-8') as f:
                    qml_xml = f.read()
                os.remove(qml_path)
                for f_orig, f_clean in field_rename.items():
                    if f_orig != f_clean:
                        qml_xml = qml_xml.replace(
                            f'field="{f_orig}"', f'field="{f_clean}"')
                        qml_xml = qml_xml.replace(
                            f'<field name="{f_orig}"', f'<field name="{f_clean}"')
                        qml_xml = qml_xml.replace(
                            f'>{f_orig}</', f'>{f_clean}</')
                log_ok(f"✅🖼️ QML exportado e atualizado: {clean_name}")
            else:
                log_warn(f"❌🖼️ QML nao gerado para '{orig_name}'")

            # --- SLD ---
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
                            f'<ogc:PropertyName>{f_clean}</ogc:PropertyName>')
                log_ok(f"✅🖼️ SLD exportado e atualizado: {clean_name}")
            else:
                log_warn(f"❌🖼️ SLD nao gerado para '{orig_name}'")

            try:
                os.rmdir(tmp_dir)
            except Exception:
                pass

            # Detectar coluna de geometria
            geom_col = "geom"
            try:
                geom_col = layer.dataProvider().geometryColumnName() or "geom"
            except Exception:
                pass

            save_style_to_gpkg(final_output, clean_name, geom_col, qml_xml, sld_xml)
            log_ok(f"✅ layer_styles atualizado: {clean_name}")

        except Exception as e:
            log_warn(f"❌ Nao foi possivel gravar estilos de '{orig_name}': {e}")
            traceback.print_exc()

        print()


    if ADD_TO_PROJECT and INSIDE_QGIS and os.path.exists(final_output):
        print("Adicionando camadas limpas ao projeto QGIS...")
        gpkg_final = QgsVectorLayer(final_output, "temp", "ogr")
        if not gpkg_final.isValid():
            log_warn("Nao foi possivel abrir o GPKG de saida.")
        else:
            for sub in gpkg_final.dataProvider().subLayers():
                name = parse_sublayer_name(sub)
                if not name:
                    continue
                vlayer = QgsVectorLayer(f"{final_output}|layername={name}", name, "ogr")
                if vlayer.isValid():
                    QgsProject.instance().addMapLayer(vlayer)
                    log_ok(f"Adicionada ao projeto: {name}")
                else:
                    log_warn(f"Camada invalida ao adicionar: {name}")

    print("\n" + "=" * 60)
    print(f" ✅ TAREFA CONCLUIDA\n")
    print(f" ✅ OK {sucesso} exportadas  |  ❌ ERR {falha} com erro\n")
    print(f" 📂 Saida: {final_output}")
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