import os
from pathlib import Path
import pandas as pd
# from IPython.display import display # Não é necessário se você não está exibindo DataFrames

# =========================================================================
# 0. DEFINIÇÃO DE CAMINHOS
# =========================================================================

# Caminho para a pasta onde estão os arquivos CSV originais (INPUT)
BASE_PATH = Path('data/ibge/censo2022/original')

# Caminho para a pasta onde os arquivos processados serão salvos (OUTPUT)
OUTPUT_PATH = Path('data/ibge/censo2022/clean')

# Garante que a pasta de saída exista antes de tentar salvar
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Define dtypes para garantir que os IDs de Setor Censitário sejam lidos como string
SECTOR_COL_DTYPE = {'CD_SETOR': str, 'CD_setor': str}

# --- DICA DE DEBUG ---
print(f"DEBUG: Caminho de Entrada Absoluto: {Path.cwd().joinpath(BASE_PATH).resolve()}") 
print(f"DEBUG: Caminho de Saída Absoluto: {Path.cwd().joinpath(OUTPUT_PATH).resolve()}") 
print("-" * 50)
# ---------------------

# =========================================================================
# 1. EXECUÇÃO: Loop para processar todos os arquivos na pasta 'original'
# =========================================================================

# O método .glob() encontra todos os arquivos que correspondem ao padrão (todos os .csv)
csv_files = BASE_PATH.glob('*.csv')
processed_count = 0

for full_input_path in csv_files:
    # 1. CONSTRUIR CAMINHO DE SAÍDA
    file_name = full_input_path.name
    
    # O arquivo de saída terá o mesmo nome, mas será salvo na pasta 'clean'
    # Você pode remover o '_filter' já que estamos salvando em uma pasta limpa.
    full_output_path = OUTPUT_PATH / file_name 
    
    print(f"\n--- Processando: {file_name} ---")

    try:
        print(f"Iniciando leitura de {file_name}...") 
        
        # 2. LER O ARQUIVO
        # O parâmetro na_values=['X'] garante que o caractere 'X' seja lido como NaN (nulo)
        df = pd.read_csv(
            full_input_path, 
            sep=';',
            quotechar='"',
            na_values=['X'], # <--- AQUI ESTÁ A LÓGICA PARA TRATAR O 'X' COMO NULO
            dtype=SECTOR_COL_DTYPE
        )
        
        print(f"Leitura concluída. Total de linhas lidas: {len(df)}") # Mudei o print de lugar
        
        # 3. Salvar o novo DataFrame
        df.to_csv(full_output_path, index=False)

        print(f"SUCESSO! Arquivo salvo em: **{full_output_path}**")
        processed_count += 1
        
    except FileNotFoundError:
        print(f"ERRO: O arquivo '{full_input_path}' não foi encontrado. Ignorando.")
    except Exception as e:
        print(f"ERRO ao processar {file_name}: {e}")

print("\n\n#####################################################")
print(f"Processamento de {processed_count} arquivos concluído!")
print("#####################################################")