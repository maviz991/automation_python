# =============================================================================
# clean_and_export_gpkg.py
# Autor: Matheus Aviz
# Finalidade: Sanitizar nomes de camadas e campos de um GPKG (remove acentos,
#             caracteres especiais, espaços; aplica camelCase e prefixo dpdu_)
#             e exporta um novo GPKG limpo com codificação UTF-8.
#
# Como rodar:
#   - QGIS: Plugins > Console Python > Show Editor > Open > Run
#   - OSGeo4W Shell: python clean_and_export_gpkg.py
# =============================================================================

import os
import re
import sys
import unicodedata
import traceback

# --- Guard ---
try:
    from qgis.core import (
        QgsApplication, QgsProject, QgsVectorLayer,
        QgsMapLayer, QgsMessageLog, Qgis
    )
    import processing
    INSIDE_QGIS = QgsApplication.instance() is not None
except ImportError:
    print("❌ ERRO: Este script requer QGIS/PyQGIS no PATH.")
    print("   No OSGeo4W Shell, rode primeiro: py3_env  (ou o bat de configuração do QGIS)")
    sys.exit(1)

# =============================================================================
# ---              CONFIGURAÇÕES DO USUÁRIO [ALTERAR AQUI]                  ---
# =============================================================================

# Modo de origem:
#   True  = usa as camadas GPKG já abertas no projeto QGIS
#   False = lê um arquivo GPKG do disco (preencha INPUT_GPKG abaixo)
USE_OPEN_LAYERS = True

# Caminho do GPKG de entrada (usado só se USE_OPEN_LAYERS = False)
INPUT_GPKG = r"C:/Caminho/Para/Seu/Arquivo.gpkg"

# Pasta/caminho de saída.
# Deixe "" para salvar automaticamente na mesma pasta do GPKG original,
# com o prefixo GPKG_OUTPUT_PREFIX no nome do arquivo.
OUTPUT_GPKG = ""
GPKG_OUTPUT_PREFIX = "LIMPO_"

# Adicionar as camadas exportadas ao projeto QGIS ao final?
ADD_TO_PROJECT = True

# Prefixo obrigatório nas tabelas (GeoServer não aceita nomes iniciando em número)
TABLE_PREFIX = "dpdu_"

# Limite de caracteres para nomes (compatível com PostgreSQL e GeoServer)
TRUNCATE_LIMIT = 63

# =============================================================================
# --- FUNÇÕES DE SANITIZAÇÃO ---
# =============================================================================

def sanitize(text, add_prefix=False):
    """
    Sanitiza uma string para uso seguro em banco de dados / GeoServer:
      1. Normaliza para NFD e remove diacríticos (acentos).
      2. Substitui qualquer caractere não alfanumérico por espaço.
      3. Aplica camelCase (primeira palavra minúscula, demais capitalizadas).
      4. Adiciona prefixo TABLE_PREFIX se add_prefix=True.
      5. Trunca para TRUNCATE_LIMIT caracteres.

    Exemplos:
      "Uso do Solo (2024)"  -> "dpdu_usoDoSolo2024"          (camada)
      "Área_km²"            -> "areaKm"                      (campo)
      "123 Estradas"            -> "dpdu_123estradas"        (camada — prefixo evita início numérico)
    """
    if not text or not text.strip():
        return f"{TABLE_PREFIX}semNome" if add_prefix else "semNome"

    # 1. Remover diacríticos via NFD
    text = unicodedata.normalize('NFD', text)
    text = "".join(c for c in text if unicodedata.category(c) != 'Mn')

    # 2. Separar em palavras (qualquer não alfanumérico vira separador)
    words = re.sub(r'[^a-zA-Z0-9]+', ' ', text).split()

    if not words:
        return f"{TABLE_PREFIX}semNome" if add_prefix else "semNome"

    # 3. CamelCase
    camel = words[0].lower() + "".join(w.capitalize() for w in words[1:])

    # 4. Prefixo (para nomes de camada/tabela)
    if add_prefix:
        if not camel.startswith(TABLE_PREFIX):
            camel = TABLE_PREFIX + camel

    # 5. Truncar
    return camel[:TRUNCATE_LIMIT]


# =============================================================================
# --- FUNÇÕES DE LOG ---
# =============================================================================

def log(msg, level=None):
    if level is None:
        level = Qgis.MessageLevel.Info
    print(f"  {msg}")
    sys.stdout.flush()
    if INSIDE_QGIS:
        QgsMessageLog.logMessage(str(msg), 'Limpeza GPKG', level=level)


def log_ok(msg):
    log(f"✔ {msg}", Qgis.MessageLevel.Success)


def log_warn(msg):
    log(f"⚠ {msg}", Qgis.MessageLevel.Warning)


def log_err(msg):
    log(f"❌ {msg}", Qgis.MessageLevel.Critical)


# =============================================================================
# --- COLETA DE CAMADAS ---
# =============================================================================

def parse_sublayer_name(sub):
    """Extrai nome da subcamada de forma robusta (separador varia por versão)."""
    if '!!::!!' in sub:
        parts = sub.split('!!::!!')
        return parts[1] if len(parts) > 1 else None
    parts = sub.split(':')
    return parts[1].strip() if len(parts) > 1 else None


def get_layers_to_process():
    """
    Retorna lista de dicts:
      { obj: QgsVectorLayer, name_orig: str, name_clean: str, source_path: str }
    """
    layers = []

    if USE_OPEN_LAYERS:
        if not INSIDE_QGIS:
            log_err("USE_OPEN_LAYERS=True requer QGIS aberto. Use USE_OPEN_LAYERS=False no Shell.")
            return []

        project = QgsProject.instance()
        all_layers = list(project.mapLayers().values())
        log(f"Analisando {len(all_layers)} camadas abertas no projeto...")

        for layer in all_layers:
            if layer.type() != QgsMapLayer.VectorLayer:
                continue
            source = layer.source()
            if '.gpkg' not in source.lower():
                log(f"   [SKIP] não é GPKG: {layer.name()}")
                continue

            file_path = source.split('|')[0]
            entry = {
                'obj':        layer,
                'name_orig':  layer.name(),
                'name_clean': sanitize(layer.name(), add_prefix=True),
                'source_path': file_path,
            }
            log(f"   [OK] {layer.name()}  ->  {entry['name_clean']}")
            layers.append(entry)

    else:
        if not os.path.exists(INPUT_GPKG):
            log_err(f"Arquivo não encontrado: {INPUT_GPKG}")
            return []

        log(f"Lendo GPKG do disco: {INPUT_GPKG}")
        gpkg_obj = QgsVectorLayer(INPUT_GPKG, "temp", "ogr")
        if not gpkg_obj.isValid():
            log_err(f"GPKG inválido: {INPUT_GPKG}")
            return []

        for sub in gpkg_obj.dataProvider().subLayers():
            name = parse_sublayer_name(sub)
            if not name:
                log_warn(f"Não foi possível parsear subcamada: {sub}")
                continue
            layer = QgsVectorLayer(f"{INPUT_GPKG}|layername={name}", name, "ogr")
            if not layer.isValid():
                log_warn(f"Camada inválida, ignorada: {name}")
                continue
            entry = {
                'obj':        layer,
                'name_orig':  name,
                'name_clean': sanitize(name, add_prefix=True),
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
    print("  LIMPEZA E EXPORTAÇÃO DE GPKG")
    print("  Remove acentos, caracteres especiais, espaços.")
    print("  Aplica camelCase + prefixo dpdu_ + UTF-8.")
    print("=" * 60)

    # 1. Coletar camadas
    try:
        layers = get_layers_to_process()
    except Exception:
        log_err("Erro ao coletar camadas:")
        traceback.print_exc()
        return

    if not layers:
        print("\nNenhuma camada GPKG encontrada. Verifique as configurações.")
        return

    # 2. Definir caminho de saída
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

    log(f"Arquivo de saída: {final_output}")

    # Criar diretório se necessário
    out_dir = os.path.dirname(final_output)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Remover GPKG anterior para evitar append indesejado
    if os.path.exists(final_output):
        try:
            os.remove(final_output)
            log(f"Arquivo anterior removido: {final_output}")
        except Exception as e:
            log_warn(f"Não foi possível remover arquivo existente: {e}")

    print(f"\nProcessando {len(layers)} camadas...\n")

    sucesso = 0
    falha   = 0

    for i, l_data in enumerate(layers, 1):
        layer      = l_data['obj']
        orig_name  = l_data['name_orig']
        clean_name = l_data['name_clean']

        print(f"[{i}/{len(layers)}]  {orig_name}")
        print(f"         -> tabela: {clean_name}")

        # 3. Montar mapeamento de campos sanitizados
        fields    = layer.fields()
        field_map = []
        seen_names = {}

        for field in fields:
            f_orig  = field.name()
            f_clean = sanitize(f_orig, add_prefix=False)

            # Resolver duplicatas pós-sanitização
            if f_clean in seen_names:
                seen_names[f_clean] += 1
                f_clean = f"{f_clean}_{seen_names[f_clean]}"
            else:
                seen_names[f_clean] = 0

            if f_orig != f_clean:
                print(f"         campo: {f_orig}  ->  {f_clean}")

            field_map.append({
                'name':       f_clean,
                'type':       field.type(),
                'length':     field.length(),
                'precision':  field.precision(),
                'expression': f'"{f_orig}"',
            })

        # 4. Refatorar campos e salvar no GPKG de saída
        try:
            # URI no formato OGR para GPKG (aspas simples no dbname = compatível Windows)
            output_uri = f"ogr:dbname='{final_output}' table=\"{clean_name}\" (geom) format=GPKG"

            processing.run("native:refactorfields", {
                'INPUT':          layer,
                'FIELDS_MAPPING': field_map,
                'OUTPUT':         output_uri,
            })

            log_ok(f"Exportada: {clean_name}")
            sucesso += 1

        except Exception as e:
            log_err(f"Erro em '{orig_name}': {e}")
            traceback.print_exc()
            falha += 1

        print()

    # 5. Adicionar ao projeto QGIS
    if ADD_TO_PROJECT and INSIDE_QGIS and os.path.exists(final_output):
        print("Adicionando camadas limpas ao projeto QGIS...")
        gpkg_final = QgsVectorLayer(final_output, "temp", "ogr")
        if gpkg_final.isValid():
            for sub in gpkg_final.dataProvider().subLayers():
                name = parse_sublayer_name(sub)
                if not name:
                    continue
                vlayer = QgsVectorLayer(f"{final_output}|layername={name}", name, "ogr")
                if vlayer.isValid():
                    QgsProject.instance().addMapLayer(vlayer)
                    log_ok(f"Adicionada ao projeto: {name}")
        else:
            log_warn("Não foi possível abrir o GPKG de saída para adicionar ao projeto.")

    # 6. Resumo final
    print("\n" + "=" * 60)
    print(f"  CONCLUÍDO  |  ✔ {sucesso} exportadas  |  ❌ {falha} com erro")
    print(f"  Saída: {final_output}")
    print("=" * 60)


# =============================================================================
# --- ENTRADA ---
# =============================================================================
# Funciona tanto via exec() do QGIS quanto direto no OSGeo4W Shell.

if INSIDE_QGIS:
    # Rodando dentro do QGIS (Console ou Editor de Scripts)
    main()
else:
    # Modo standalone: OSGeo4W Shell
    qgs = QgsApplication([], False)
    qgs.initQgis()

    from qgis.analysis import QgsNativeAlgorithms
    from processing.core.Processing import Processing
    Processing.initialize()
    QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())

    main()
    qgs.exitQgis()""