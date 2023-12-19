import pandas as pd
import time
import os
import glob
import re
import unidecode
from colorama import Fore
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from database import ConnectPostgresQL, OrdersTable
from sqlalchemy.exc import IntegrityError
import logging


# configuração do logger
logging.basicConfig(filename='logs.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


class FinalReport:

    def __init__(self, host):
        # Crie uma instância de ConnectPostgresQL usando o host do seu banco de dados PostgreSQL
        self.db_connection = ConnectPostgresQL(host)
        self.session = self.db_connection.Session()

    """ Feche a sessão ao destruir a instância da classe """
    def __del__(self):
        
        self.session.close()
        self.db_connection.connect().close()

    
    """ função para renomear as colunas do arquivo final"""
    def rename_columns(self, directory):
        new_names = {
            'codigo_cliente': 'CÓDIGO CLIENTE',
            'loja_cliente': 'LOJA CLIENTE',
            'nome_do_cliente': 'NOME DO CLIENTE',
            'cnpj_do_cliente': 'CNPJ DO CLIENTE',
            'cnpj_de_faturamento': 'CNPJ DE FATURAMENTO',
            'cnpj_de_remessa': 'CNPJ DE REMESSA',
            'equipamento': 'EQUIPAMENTO',
            'nota_de_remessa': 'NOTA DE REMESSA',
            'data_de_remessa': 'DATA DE REMESSA',
            'serie_da_nf_remessa': 'SERIE DA NF REMESSA',
            'produto': 'PRODUTO',
            'descricao_do_produto': 'DESCRICAO DO PRODUTO',
            'quantidade': 'QUANTIDADE',
            'pedido_de_remessa': 'PEDIDO DE REMESSA',
            'projeto': 'PROJETO',
            'obra': 'OBRA',
            'prazo_do_contrato': 'PRAZO DO CONTRATO',
            'data_de_ativacao': 'DATA DE ATIVACAO',
            'periodo_inicial': 'PERIODO INICIAL',
            'periodo_final': 'PERIODO FINAL',
            'data_do_termo': 'DATA DO TERMO',
            'aniversario': 'ANIVERSARIO',
            'desc_ajuste': 'DESC AJUSTE',
            'indice_aplicado': 'INDICE APLICADO',
            'dias_de_locacao': 'DIAS DE LOCACAO',
            'valor_de_origem': 'VALOR DE ORIGEM',
            'valor_unitario': 'VALOR UNITARIO',
            'valor_bruto': 'VALOR BRUTO',
            'tipo_do_mes': 'TIPO DO MES',
            'nr_chamado': 'NR CHAMADO',
            'contrato_legado': 'CONTRATO LEGADO',
            'acrescimo': 'ACRESCIMO',
            'franquia': 'FRANQUIA',
            'id_equipamento': 'ID EQUIPAMENTO',
            'id_equip_substituido': 'ID EQUIP SUBSTITUIDO',
            'data_da_substituicao': 'DATA DA SUBSTITUICAO',
            'data_proximo_faturamento': 'DATA PROXIMO FATURAMENTO',
            'data_inicio': 'DATA INICIO',
            'data_fim_locacao': 'DATA FIM LOCACAO',
            'tipo_de_servico': 'TIPO DE SERVICO',
            'email': 'E-MAIL',
            'descricao_do_ajuste': 'DESCRICAO DO AJUSTE',
            'nome_da_obra': 'NOME DA OBRA',
            'numero_da_as': 'NUMERO DA AS',
            'pedido_faturamento': 'PEDIDO FATURAMENTO',
            'nf_de_faturamento': 'NF DE FATURAMENTO',
            'serie_de_faturamento': 'SERIE DE FATURAMENTO',
            'data_de_faturamento': 'DATA DE FATURAMENTO',
            'qtde_faturamento': 'QTDE FATURAMENTO',
            'vlr_unitario_faturamento': 'VLR UNITARIO FATURAMENTO',
            'vlr_total_faturamento': 'VLR TOTAL FATURAMENTO',
            'periodo_de_faturamento': 'PERIODO DE FATURAMENTO'
            }

        for filename in os.listdir(directory):
            if filename.endswith('.xlsx'):
                file_path = os.path.join(directory, filename)

                # Lê o arquivo
                df = pd.read_exce(file_path, sheet_name='CONSOLIDADO', engine='openpyxl')
                df.rename(columns=new_names)
                
                                




    
    """ Função para checar novos pedidos e atualizar o banco de dados"""
    def check_and_update_orders(self, extractor_file_path, col):
        start = time.time()
        """Método para verificar e atualizar pedidos ausentes no banco de dados"""

        print('Verificando novos pedidos e atualizando o banco de dados...\n')

        # Carrega o arquivo e verifica extrator TOTVS com os pedidos
        extract_df = pd.read_excel(extractor_file_path, sheet_name='2-Resultado', engine='openpyxl')

        # Padroniza o nome da coluna para minúsculas e substitui espaços por underscore
        extract_df.columns = extract_df.columns.str.lower().str.replace(' ', '_').str.replace('.', '').str.replace('-', '')

        # Verifica se a coluna existe no arquivo
        col_lower = col.lower().replace(' ', '_')
        if col_lower not in extract_df.columns:
            print(f'Coluna {col} não encontrada no arquivo')
            return

        # Converte a coluna PEDIDO para numérico
        extract_df[col_lower] = pd.to_numeric(extract_df[col_lower], errors='coerce')
        print(f'Total de pedidos no extrator: {len(extract_df)}\n')

        # Filtra o DataFrame para incluir apenas os pedidos mais recentes
        extract_df = extract_df[extract_df[col_lower] >= 0]

        # Carrega os pedidos já existentes no banco de dados, convertendo a coluna para inteiro
        existing_orders = set(int(order) for order in pd.read_sql_query(f'SELECT DISTINCT {col} FROM {OrdersTable.__tablename__}', self.db_connection.engine)[col])
        
        # Identifica os pedidos ausentes
        new_orders = set(extract_df[col_lower]) - existing_orders
        print(f'Total de novos pedidos no extrator: {len(new_orders)}\n')       
            
        # Cria um DataFrame apenas com os pedidos ausentes
        new_orders_df = extract_df[extract_df[col_lower].isin(new_orders)].copy()
        
             
        # Salva os pedidos ausentes em um arquivo Excel        
        path = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS'
        for index, row in new_orders_df.iterrows():
            new = row[col_lower]
            client_name = row['nome_do_cliente']  # Obtém o nome do cliente da primeira linha
            client_name_valid = client_name.translate(str.maketrans('', '', r'\/:*?"<>|'))  # Remove caracteres inválidos

            file_name = f'{new}_{client_name_valid}.xlsx'
            file_path = os.path.join(path, file_name)

            new_orders_df.loc[[index]].to_excel(file_path, sheet_name='CONSOLIDADO',
                                    index=False, engine='openpyxl')
            print(f'Novo arquivo {new}_{client_name_valid}.xlsx criado.')


        # Atualiza o banco de dados com os pedidos ausentes
        try:
            if not new_orders_df.empty:
                new_orders_df.to_sql(OrdersTable.__tablename__, self.db_connection.engine, if_exists='append', index=False, method='multi')

        except IntegrityError as e:
            print('Banco de dados atualizado com novos pedidos')

       
        # pula o processamento dos clientes abaixo (grandes clientes)
        special_clients = ['ASF - MATRIZ', 'SOUZA CRUZ', 'METALFRIO', 'M. DIAS', 'EBD MATRIZ', 'QUALICICLO AGRICOLA S/A',
                                'LOCALIZA BELO HORIZONTE MG - MATRIZ', 'SPDM - HOSPITAL MUNICIPAL VEREADOR JOSE STOROPOLLI',
                                'SONDA', 'BRINKS SEGURANCA - MATRIZ', 'FUNDAÇÃO EDUCACIONAL SEVERINO SOMBRA',
                                'SPDM - HOSPITAL MUNICIPAL DR. IGNACIO PROENCA DE GOUVEA',
                                'SPDM - HOSPITAL MUNICIPAL DR. JOSE DE CARVALHO FLORENCE',
                                'SPDM - HOSPITAL MUNICIPAL DR. ARTHUR RIBEIRO DE SABOYA',
                                'SPDM - HOSPITAL MUNICIPAL DR. ALIPPIO CORREA NETTO',
                                'SPDM - HOSPITAL MUNICIPAL DR. BENEDICTO MONTENEGRO',
                                'SPDM - HOSPITAL MUNICIPAL DR. IGNACIO PROENCA DE GOUVEA',
                                'SPDM - HOSPITAL MUNICIPAL DR. JOSE DE CARVALHO FLORENCE',
                                'SPDM - HOSPITAL MUNICIPAL DR. ARTHUR RIBEIRO DE SABOYA',
                                'SPDM - HOSPITAL MUNICIPAL DR. ALIPPIO CORREA NETTO',
                                'SPDM - HOSPITAL MUNICIPAL DR. BENEDICTO MONTENEGRO',
                                'SPDM - HOSPITAL MUNICIPAL DR. JOSE SOARES HUNGRIA',
                                'SPDM - HOSPITAL MUNICIPAL DR. ALIPPIO CORREA NETTO', 'PROTEGE - MATRIZ', 'TIM - MATRIZ',
                                'BIOMEDICAL DISTRIBUTION MERCOSUR LTDA',
                                'PINHEIRO GUIMARAES E MEISSNER SOCIEDADE DE ADVOGADOS-MATRIZ']

        def save_order_excel(order):
            order_df = extract_df[extract_df[col_lower] == order]
            if not order_df.empty:
                client_name = order_df['CLIENTE'].iloc[0]
                if client_name in special_clients:
                    print(f'Relatório {client_name} será gerado manualmente')
                    return

                client_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', unidecode.unidecode(client_name))
                sheet_names = ['LAVORO', 'CONSOLIDADO']

                for sheet in sheet_names:
                    order_df.to_excel(os.path.join(path, f'{order}_{client_name_safe}.xlsx'), sheet_name=sheet,
                                    index=False, engine='openpyxl')
                    print(f'Arquivo {order}_{client_name_safe}.xlsx sendo criado.')

        with ThreadPoolExecutor() as executor:
            executor.map(save_order_excel, new_orders)

        print(f'Pedidos salvos no diretório NOVOS_PEDIDOS')
        print('Verificação e atualização concluídas.\n')
        end = time.time()
        print(f'Tempo de execução do código: {end - start}')
    
          
    """ Função que cria um arquivo único do cliente com todos os pedidos"""
    def merge_same_client(self, news_orders, output_path):
        # Inicia o contador de tempo de execução do método
        start_time = time.time()
        # lista os arquivos do diretório

        xlsx_files = glob.glob(os.path.join(news_orders, '*.xlsx'))
        # cria um dataframe vazio
        combined_df = pd.DataFrame()

        # itera sobre os arquivos do diretório
        for file in xlsx_files:
            # carrega o arquivo
            df = pd.read_excel(file, sheet_name='CONSOLIDADO')
            # concatena o dataframe do arquivo com o dataframe combinado
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Salva o dataframe combinado em um arquivo excel
        combined_df.to_excel(output_path, sheet_name='CONSOLIDADO', engine='openpyxl', index=False)
        end_time = time.time()
        print(f'Tempo de execução: {end_time - start_time}')

    
    """ Função que formata o arquivo final com cores da Arklok"""
    def color_dataframe(self):
        pass




# Classe para processar e listar arquivos do diretório
class FileProcessor:
    # Definindo atributos da classe
    def __init__(self, extractor_file_path, invoiced_orders, news_orders, output_merge_path):
        self.extractor_file_path = extractor_file_path
        self.invoiced_orders = invoiced_orders
        self.news_orders = news_orders
        self.output_merge_path = output_merge_path

    # Método para obter os arquivos
    def get_files(self, file_type='.xlsx'):
        return [(root, file) for root, dirs, files in os.walk(self.news_orders) \
                for file in files if file.endswith(file_type    )]

    # Método para processar os arquivos
    def process_file_list(self, filo_info):
        root, file = filo_info
        full_path = os.path.join(root, file)
        
        xlsx_files = []
        
        if file.lower().endswith('.xlsx') \
            and not file.startswith('~$'):
        
            print(f'{Fore.LIGHTCYAN_EX}Arquivo encontrado em: {full_path}{Fore.RESET}')
            sys.stdout.flush() # Limpa o buffer de saída
            xlsx_files.append(full_path)

            # Obtem informações do arquivo
            file_status = os.stat(full_path)
            file_size = file_status.st_size
            filename = os.path.basename(full_path)
            file_path = os.path.dirname(full_path)
            file_date_create = datetime.fromtimestamp(file_status.st_ctime).strftime('%d/%m/%Y %H:%M:%S')
            file_date_modified = datetime.fromtimestamp(file_status.st_mtime).strftime('%d/%m/%Y %H:%M:%S')
            full_path_file = os.path.join(file_path, filename)
            
            
            return {
                
                'FILENAME': filename,
                'FULL_PATH_FILE': full_path_file,
                'FILE_SIZE': file_size,
                'CREATE_DATE': file_date_create,
                'MODIFIED_DATE': file_date_modified,

            }
        
        else:
            return None
        
   # Método para processar os arquivos em paralelo (multithreading) 
    def process_files_in_parallel(self, file_infos):
        with ThreadPoolExecutor() as executor:
            results = executor.map(self.process_file_list, file_infos)
        return [result for result in results if result is not None]
    
    # Método para listar os arquivos
    def list_all_files(self, output_folder):
        start_time = time.time()
        # verficar se o diretório existe
        if not os.path.exists(output_folder):
            print(f'A pasta {output_folder} não existe')
            os.makedirs(output_folder)
            print(f'Criando a pasta {output_folder}')
            return 
        
        try:

            file_infos = self.get_files()
            file_list = self.process_files_in_parallel(file_infos)
            for file_info in file_infos:
                try:
                    processed_file = self.process_file_list(file_info)
                    if processed_file is not None:
                        file_list.append(processed_file)
                except FileNotFoundError as e:
                    print(f'Arquivo não encontrado: {e}')
                    continue
            
            # Cria um DataFrame com os dados dos arquivos
            df = pd.DataFrame(file_list)
            # Salva o DataFrame em um arquivo excel
            df.to_excel(os.path.join(output_folder, f'NOVOS_PEDIDOS_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'), 
                        sheet_name='NOVOS_PEDIDOS', index=False, engine='openpyxl')

            # finaliza a contagem do tempo de execução
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f'Rotina Listagem novos pedidos finalizda! Tempo de execução: {elapsed_time}')

        except PermissionError as e:
            print(f'O arquivo {self.news_orders} está aberto. Feche o arquivo e tente novamente')
            return
        
        except Exception as e:
            print(f'Ocorreu um erro no arquivo {self.news_orders}: {e}')
            return False

    # Criar pastas no diretório H:\\
    def make_folders_clients(self, batch_totvs_path, extractor_path, sheet_name, col):
        df = pd.read_excel(extractor_path, sheet_name, engine='openpyxl')
        basedir = batch_totvs_path
        
        for client in df[col]:
            client_path = os.path.join(basedir, client)
            
            if not os.path.exists(client_path):
                os.makedirs(client_path)
                print(f'Pasta {client} criada com sucesso!')
            else:
                print(f'Pasta {client} já existe!')

    # função para excluir todos os arquivos da pasta copied_files
    def delete_new_files(self, files_path):
        logging.info(f"INICIANDO ROTINA 2 - EXCLUINDO ARQUIVOS DA PASTA COPIED_FILES...")
        # verifica se a pasta existe
        if not os.path.exists(files_path):
            logging.info(f"A pasta {files_path} não existe.")
            return

        try:
            # lista todos os arquivos no diretório
            file_list = [f for f in os.listdir(files_path) if f.endswith('.xlsx') and not f.startswith('~$')]
            # itera sobre cada arquivo no diretório
            for file_name in file_list:
                # caminho completo do arquivo
                input_file_path = os.path.join(files_path, file_name)
                # verifica se o arquivo existe e tem permissões de leitura
                if os.path.isfile(input_file_path) and os.access(input_file_path, os.R_OK):
                    # exclui o arquivo
                    os.remove(input_file_path)
                    print(f"Arquivo {file_name} excluído com sucesso!")
                else:
                    print(f"Arquivo {file_name} não encontrado ou permissão negada.")
            print(Fore.WHITE + "EXCLUSÃO DE ARQUIVOS FINALIZADA..." + Fore.RESET)
            logging.info(f"ROTINA 2 - EXCLUSÃO DE ARQUIVOS FINALIZADA...")
            return True
        except PermissionError as e:
            print(f"O arquivo {self.folder} está aberto: {e}")
            logging.error(f"Erro de permissão para abrir o arquivo {self.folder}. Arquivo aberto : {e}")
            return False
        except Exception as e:
            print(f"Ocorreu um erro ao excluir os arquivos: {e}")
            logging.error(f"Ocorreu um erro ao excluir os arquivos: {e}")
            return False 
            

        







    #


        
    

    