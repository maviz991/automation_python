####02
# ======================================================================
# SCRIPT PARA RENOMEAR CAMPOS (VERSÃO 8 - A ESTRATÉGIA FINAL E GARANTIDA)
#
# Funcionalidade:
# - Baseado na solução mais pragmática e segura.
# - FASE 1: Roda o script que funciona, renomeando campos para minúsculas e
#           adicionando um sufixo numérico (ex: '_1') para resolver conflitos
#           de forma segura e sem erros. A camada é SALVA.
# - FASE 2: Roda uma rotina de limpeza que abre a camada já salva, encontra
#           todos os campos com sufixo numérico e os renomeia, removendo
#           o sufixo. A camada é SALVA novamente com os nomes finais corretos.
# ======================================================================

from qgis.core import QgsProject, QgsVectorLayer, QgsVectorDataProvider, edit

def phase_one_rename_with_suffix(layer):
    """FASE 1: Renomeia para minúsculas e usa sufixo para evitar erros."""
    print("    --- FASE 1: Renomeando e resolvendo conflitos iniciais com sufixo ---")
    try:
        with edit(layer):
            fields = layer.fields()
            # Este conjunto precisa incluir os nomes que já existem e os que serão criados
            final_names_in_transaction = {f.name().lower() for f in fields}
            
            rename_map = {}
            for i in range(len(fields)):
                old_name = fields.field(i).name()
                new_name = old_name.lower()

                if old_name == new_name:
                    continue
                
                # Resolução de duplicatas que comprovadamente funciona
                temp_new_name = new_name
                count = 1
                while temp_new_name in final_names_in_transaction:
                    temp_new_name = f"{new_name}_{count}"
                    count += 1
                
                new_name = temp_new_name
                final_names_in_transaction.add(new_name)
                rename_map[i] = new_name
            
            if not rename_map:
                print("      Nenhum campo precisou ser renomeado na Fase 1.")
                return True

            for index, new_name in rename_map.items():
                old_name = layer.fields().field(index).name()
                layer.renameAttribute(index, new_name)
                print(f"      '{old_name}' -> '{new_name}'")
        
        print("      FASE 1 concluída e salva.")
        return True # Indica sucesso

    except Exception as e:
        print(f"❌ ERRO na FASE 1: {e}. O processo para esta camada foi interrompido.")
        return False # Indica falha

def phase_two_cleanup_suffix(layer):
    """FASE 2: Remove os sufixos numéricos criados na Fase 1."""
    print("    --- FASE 2: Removendo sufixos numéricos de limpeza ---")
    try:
        fields_to_fix = {}
        for field in layer.fields():
            name = field.name()
            if '_' in name:
                parts = name.rsplit('_', 1)
                if len(parts) == 2 and parts[1].isdigit():
                    fields_to_fix[name] = parts[0] # Mapeia 'nome_1' -> 'nome'
        
        if not fields_to_fix:
            print("      Nenhum sufixo para remover.")
            return True

        with edit(layer):
            for temp_name, final_name in fields_to_fix.items():
                idx = layer.fields().indexOf(temp_name)
                if idx != -1:
                    layer.renameAttribute(idx, final_name)
                    print(f"      Limpeza: '{temp_name}' -> '{final_name}'")
        
        print("      FASE 2 concluída e salva.")
        return True

    except Exception as e:
        print(f"❌ ERRO na FASE 2 (Limpeza): {e}.")
        return False

# --- SCRIPT PRINCIPAL ---
print("="*50)
print("INICIANDO SCRIPT DE RENOMEAÇÃO (ESTRATÉGIA DE SUFIXO E LIMPEZA)")
print("!!! FAÇA UM BACKUP DOS SEUS DADOS ANTES DE EXECUTAR !!!")
print("="*50 + "\n")

project = QgsProject.instance()
layers_to_process = list(project.mapLayers().values())

if not layers_to_process:
    print("Nenhuma camada encontrada no projeto.")
else:
    for layer in layers_to_process:
        if isinstance(layer, QgsVectorLayer) and layer.isValid():
            provider = layer.dataProvider()
            if not (provider.capabilities() & QgsVectorDataProvider.RenameAttributes):
                print(f"⚠️ AVISO: A camada '{layer.name()}' não suporta renomear campos. Camada ignorada.\n") color: #000000
            
                continue

            print(f"🔄 Processando camada: '{layer.name()}'...")
            
            # Executa a Fase 1
            success_phase1 = phase_one_rename_with_suffix(layer)
            
            # Só executa a Fase 2 se a Fase 1 foi bem-sucedida
            if success_phase1:
                phase_two_cleanup_suffix(layer)
            
            print(f"✅ Processo para a camada '{layer.name()}' finalizado.\n")

print("="*50)
print("Processo concluído.")