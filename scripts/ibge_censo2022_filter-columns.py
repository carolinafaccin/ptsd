import pandas as pd
import numpy as np
import os
from pathlib import Path

# =========================================================================
# 0. DEFINIÇÃO DO CAMINHO BASE
# =========================================================================

# 1. Encontrar o caminho do notebook/script atual
# __file__ não funciona em notebooks, então usamos Path.cwd() ou assumimos o ponto de partida
# Vamos assumir que o diretório de trabalho atual (Path.cwd()) é a raiz do seu repositório ou o nível acima dos dados.

# O caminho para a pasta onde estão os arquivos CSV do IBGE (a partir do local de execução do notebook)

# Se o seu diretório de trabalho atual for o nível ACIMA da pasta 'ibge':
# BASE_PATH = Path('../ibge/censo2022') 

# Se o seu diretório de trabalho atual for a RAIZ do repositório:
BASE_PATH = Path('data/ibge/censo2022') 


# --- DICA DE DEBUG ---
# O print a seguir mostrará o caminho ABSOLUTO que o Python está tentando usar.
# Execute esta linha e confira se a pasta existe no seu Mac:
print(f"DEBUG: Caminho de Dados Absoluto: {Path.cwd().joinpath(BASE_PATH).resolve()}") 
print("-" * 50)
# ---------------------

# =========================================================================
# 1. FUNÇÃO DE PROCESSAMENTO
# =========================================================================

def process_and_filter_csv(file_name, columns_to_keep, base_path, sep=';', quotechar='"'):
    """
    Lê um arquivo CSV do IBGE, filtra as colunas e salva o resultado.
    
    Parâmetros:
    - file_name: Nome do arquivo (ex: 'Agregados_por_setores_obitos_BR.csv')
    - columns_to_keep: Lista de colunas a manter
    - base_path: Caminho da pasta onde o arquivo está e onde será salvo (ex: 'data_processing/ibge/censo2022')
    """
    
    # 1. CONSTRUIR CAMINHOS COMPLETOS
    # os.path.join é a forma ideal de unir caminhos, pois funciona no Windows, Mac e Linux
    full_input_path = Path(base_path) / file_name # Caminho completo usando o operador /
    
    # O arquivo de saída terá o mesmo caminho, mas com o sufixo '_filter'
    base, ext = os.path.splitext(file_name)
    new_file_name_base = f"{base}_filter{ext}"
    full_output_path = os.path.join(base_path, new_file_name_base)
    
    print(f"\n--- Processando: {full_input_path} ---")

    # 2. Determina o nome da coluna do setor censitário para o dtype
    sector_col_dtype = {}
    if 'CD_SETOR' in columns_to_keep:
        sector_col_dtype['CD_SETOR'] = str
    elif 'CD_setor' in columns_to_keep:
        sector_col_dtype['CD_setor'] = str

    try:
        # 3. LER O ARQUIVO USANDO O CAMINHO COMPLETO
        # O Pandas é inteligente e aceita o objeto Path diretamente
        df = pd.read_csv(
            full_input_path, 
            usecols=columns_to_keep,
            sep=sep,
            quotechar=quotechar,
            na_values='X',
            dtype=sector_col_dtype
        )

        print(f"Total de linhas lidas: {len(df)}")
        
        # 4. Salvar o novo DataFrame USANDO O CAMINHO COMPLETO DE SAÍDA
        df.to_csv(full_output_path, index=False)

        print(f"SUCESSO! Arquivo filtrado salvo como: **{full_output_path}**")
        
        return df

    except FileNotFoundError:
        print(f"ERRO: O arquivo '{full_input_path}' não foi encontrado.")
        print("Verifique se o nome do arquivo está correto e se o caminho BASE_PATH está apontando para o local correto.")
    except Exception as e:
        print(f"Ocorreu um erro ao processar o arquivo {full_input_path}: {e}")
        return None


# =========================================================================
# 5. EXECUÇÃO: Loop para processar todas as tabelas
# =========================================================================

# NOTA: O 'CD_setor' é o campo principal em todas as listas de colunas.
list_of_tables = [
    {
        'name': 'DOMICILIO 1',
        'file_name': 'Agregados_por_setores_caracteristicas_domicilio1_BR.csv',
        'columns': ['CD_setor', 'V00047', 'V00048', 'V00049', 'V00050']
    },
    {
        'name': 'DOMICILIO 2',
        'file_name': 'Agregados_por_setores_caracteristicas_domicilio2_BR.csv',
        'columns': ['CD_setor', 'V00111', 'V00201', 'V00238', 'V00397', 'V00398', 'V00495']
    },
    {
        'name': 'ALFABETIZAÇÃO',
        'file_name': 'Agregados_por_setores_alfabetizacao_BR.csv',
        'columns': ['CD_setor', 'V00853', 'V00855', 'V00857']
    },
    {
        'name': 'COR OU RAÇA',
        'file_name': 'Agregados_por_setores_cor_ou_raca_BR.csv',
        'columns': ['CD_SETOR', 'V01317', 'V01318', 'V01319', 'V01320', 'V01321']
    },
    {
        'name': 'ÓBITOS',
        'file_name': 'Agregados_por_setores_obitos_BR.csv',
        'columns': ['CD_SETOR', 'V01224']
    },
    {
        'name': 'DEMOGRAFIA',
        'file_name': 'Agregados_por_setores_demografia_BR.csv',
        'columns': ['CD_setor', 'V01006', 'V01007', 'V01008']
    }
]

# =========================================================================
# 5. EXECUÇÃO: Loop para processar todas as tabelas
# =========================================================================

all_dataframes = {}

for table in list_of_tables:
    # PASSANDO O BASE_PATH COMO PARÂMETRO NA CHAMADA DA FUNÇÃO
    df_result = process_and_filter_csv(
        file_name=table['file_name'],
        columns_to_keep=table['columns'],
        base_path=BASE_PATH # <--- AQUI ESTÁ A SEGUNDA MUDANÇA PRINCIPAL
    )
    
    if df_result is not None:
        all_dataframes[table['name']] = df_result

print("\n\n#####################################################")
print("Processamento de todos os arquivos concluído!")
print("#####################################################")