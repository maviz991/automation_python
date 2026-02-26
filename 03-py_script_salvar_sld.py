####03
# ======================================================================
# SCRIPT PARA EXPORTAR ESTILOS SLD DAS CAMADAS DO PROJETO
# Autor: Matheus Aviz
# Versão Aprimorada: 26/06/2024
#
# Funcionalidades:
# - Exporta o estilo de cada camada vetorial para um arquivo .sld.
# - Garante que a ordem de numeração siga a ordem do Painel de Camadas.
# - Permite escolher entre dois estilos de nomenclatura de arquivos.
# - **NOVO: Corrige a capitalização (maiúsculas/minúsculas) dos nomes
#   dos campos dentro do arquivo SLD gerado.**
# ======================================================================

import os
from qgis.core import QgsProject, QgsMapLayer

# ======================================================================
# --- CONFIGURAÇÕES (Ajuste aqui conforme a necessidade) ---
# ======================================================================

# 1. Defina a pasta onde os arquivos .sld serão salvos.
output_folder = r"C:\Users\mdaviz\OneDrive - CDHU\Documentos\@Geohab\Planejamento\20250613_Envio_Geo_CDHU\SLD\USMB" 

# 2. Escolha o estilo de nomenclatura do arquivo de saída:
#    'PREFIXED' -> Usa um prefixo e um número sequencial (ex: DPDU_AA_01_minha_camada.sld)
#    'SIMPLE'   -> Usa apenas o nome da camada (ex: minha_camada.sld)
naming_style = 'PREFIXED'

# 3. (Opcional) Defina o prefixo se usar o modo 'PREFIXED'.
prefix = "DPDU_USMB"

# ======================================================================
# --- FUNÇÃO AUXILIAR PARA CORREÇÃO DOS CAMPOS ---
# (Esta é a nova função que resolve o seu problema)
# ======================================================================

def corrigir_nomes_campos_no_sld(sld_filepath, layer):
    """
    Lê um arquivo SLD e substitui os nomes de campo em maiúsculas
    pelos nomes de campo com a capitalização correta da camada.
    """
    try:
        # 1. Obter os nomes de campo com a capitalização correta da camada
        # Usamos um dicionário para mapear MAIÚSCULO -> correto
        field_map = {field.name().upper(): field.name() for field in layer.fields()}
        
        # 2. Ler o conteúdo do arquivo SLD gerado
        with open(sld_filepath, 'r', encoding='utf-8') as f:
            sld_content = f.read()

        # Flag para saber se fizemos alguma alteração
        modificado = False
        
        # 3. Substituir cada nome de campo maiúsculo pelo nome correto
        for uppercase_name, correct_name in field_map.items():
            # Apenas substitui se o nome maiúsculo for diferente do correto
            if uppercase_name != correct_name and uppercase_name in sld_content:
                # Usamos <ogc:PropertyName> para ser mais específico e evitar substituições erradas
                tag_upper = f"<ogc:PropertyName>{uppercase_name}</ogc:PropertyName>"
                tag_correct = f"<ogc:PropertyName>{correct_name}</ogc:PropertyName>"
                
                # Substituímos no conteúdo
                if tag_upper in sld_content:
                    sld_content = sld_content.replace(tag_upper, tag_correct)
                    modificado = True

        # 4. Se o conteúdo foi modificado, salva o arquivo de volta
        if modificado:
            with open(sld_filepath, 'w', encoding='utf-8') as f:
                f.write(sld_content)
            print(f"    -> Campos corrigidos para minúsculas.")
            
    except Exception as e:
        print(f"    -> ❌ Erro ao tentar corrigir os campos no arquivo {os.path.basename(sld_filepath)}: {e}")


# ======================================================================
# --- LÓGICA PRINCIPAL DO SCRIPT ---
# ======================================================================

# Garante que a pasta de saída exista; se não, cria-a.
os.makedirs(output_folder, exist_ok=True)
print(f"Estilos serão salvos em: {output_folder}")
print(f"Modo de nomenclatura ativado: {naming_style}\n")

# A FORMA CORRETA de obter as camadas na ordem de desenho do Painel de Camadas
root = QgsProject.instance().layerTreeRoot()
layers_in_order = [node.layer() for node in root.findLayers()]

# Inicia o contador para camadas vetoriais válidas
vector_layer_count = 0

if not layers_in_order:
    print("Nenhuma camada encontrada no projeto.")
else:
    # Percorre as camadas na ordem correta
    for layer in layers_in_order:
        
        # Filtra para processar apenas camadas vetoriais válidas
        if layer.isValid() and layer.type() == QgsMapLayer.VectorLayer:
            
            # Incrementa o contador APENAS para as camadas que serão processadas
            vector_layer_count += 1
            
            # Limpa o nome da camada para ser usado no nome do arquivo
            # Adicionei mais .replace() para cobrir outros caracteres comuns
            clean_layer_name = layer.name().replace(' ', '_').replace('-', '_').replace('´', '_').replace('Á', 'A').replace('(', '').replace(')', '').replace('.', '').replace('ç', 'c').replace('ã', 'a').replace("'", '').replace(",", '').replace("á", 'a').replace("ó", 'o').replace("õ", 'o').replace("í", 'i').replace(";", '').replace("ú", 'u').replace("é", 'e').replace("Ú", 'U')

            
            # Monta o nome do arquivo com base no estilo escolhido
            if naming_style == 'PREFIXED':
                sequencial = str(vector_layer_count).zfill(2)
                filename = f"{prefix}_{sequencial}_{clean_layer_name}.sld"
            else: # 'SIMPLE'
                filename = f"{clean_layer_name}.sld"
            
            # Caminho completo do arquivo de saída
            filepath = os.path.join(output_folder, filename)

            # Tenta salvar o estilo SLD
            success = layer.saveSldStyle(filepath)

            if success:
                print(f"✅ Estilo exportado: {filename}")
                # --- CHAMADA DA NOVA FUNÇÃO ---
                # Após salvar, chamamos a função para corrigir os nomes dos campos
                corrigir_nomes_campos_no_sld(filepath, layer)
            else:
                # O QGIS pode retornar 'False' sem um erro claro. Isso ajuda a depurar.
                error = layer.styleManager().lastError()
                print(f"❌ Falha ao exportar estilo para a camada '{layer.name()}'. Erro: {error}")

print("\n" + "="*50)
print("Processo concluído  🎉")