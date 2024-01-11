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
import shutil 
import re
import locale
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
#import pyspark.pandas as pd
from fuzzywuzzy import process




# configuração do logger
logging.basicConfig(filename=r'C:\Users\marcos.silvaext\Documents\final_report_client\logs.log', level=logging.INFO,
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

    # função para padronizar nomes das colunas        
    def padronizar_nomes_colunas(df):
        df.columns = (
            df.columns
            .str.lower()  # Converte para minúsculas
            .str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)  # Substitui caracteres especiais por underscores
        )

    # função para formatar cnpj
    def formatar_cnpj(self, cnpj):
        # Verifica se o CNPJ é uma string válida
        if isinstance(cnpj, str) and len(cnpj) == 14 and cnpj.isdigit():
            # Aplica a formatação
            cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
            return cnpj_formatado
        else:
            return cnpj
    
    # função para formatar as células do arquivo final
    def format_cells(self, path):
        """ função esta sendo usada na função rename_format_columns"""

        # define o diretório
        directory = path
        # percorre o diretório e localiza os arquivos excel
        for filename in os.listdir(directory):
            if filename.endswith('.xlsx'):
                # caminho completo do arquivo
                file_path = os.path.join(directory, filename)
                # Lê o arquivo
                df = pd.read_excel(file_path, sheet_name='CONSOLIDADO', engine='openpyxl')
                
                # seleciona as colunas que serão formatadas
                cnpj_cols = []
                try:
                    # itera sobre as colunas e aplica a função formatar_cnpj
                    for col in cnpj_cols:
                        df[col] = df[col].apply(self.formatar_cnpj)

                    # itera sobre as colunas e aplica a função strip e strftime    
                    for col in df.columns:
                        # Verifica se a coluna é do tipo string antes de aplicar .str
                        if pd.api.types.is_string_dtype(df[col]):
                            df[col] = df[col].str.strip()
                        elif pd.api.types.is_numeric_dtype(df[col]):
                            # Converte colunas numéricas para string antes de usar .str
                            df[col] = df[col].astype(str).str.strip()

                        # Verifica se a coluna é do tipo datetime antes de formatar
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].dt.strftime('%d/%m/%Y')                
                        # Retorna o DataFrame modificado
                        print(f'Renomeando coluna dataframes ...')
                        return df
                    
                # caso ocorra algum erro, exibe o erro    
                except Exception as e:
                    print(f"Erro ao formatar colunas: {e}")
                    return None
                    
                
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
            'descricao_do_produto': 'DESCRIÇÃO DO PRODUTO',
            'quantidade': 'QUANTIDADE',
            'pedido_de_remessa': 'PEDIDO DE REMESSA',
            'projeto': 'PROJETO',
            'obra': 'OBRA',
            'prazo_do_contrato': 'PRAZO DO CONTRATO',
            'data_ativacao_legado': 'DATA ATIVAÇÃO LEGADO',
            'data_de_ativacao': 'DATA DE ATIVACAO',
            'ultimo_faturamento': 'ÚLTIMO FATURAMENTO',
            #'periodo_final': 'PERIODO FINAL',
            'data_do_termo': 'DATA DO TERMO',
            'aniversario': 'DATA BASE REAJUSTE',
            'desc_ajuste': 'DESC AJUSTE',
            'indice_aplicado': 'ÍNDICE APLICADO',
            'dias_de_locacao': 'DIAS DE LOCAÇÃO',
            'valor_de_origem': 'VALOR DE ORIGEM',
            'valor_unitario': 'VALOR UNITÁRIO',
            'valor_bruto': 'VALOR BRUTO',
            'tipo_do_mes': 'TIPO DO MES',
            #'nr_chamado': 'NR CHAMADO',
            'contrato_legado': 'CONTRATO LEGADO',
            'acrescimo': 'ACRÉSCIMO',
            'franquia': 'FRANQUIA',
            'id_equipamento': 'ID EQUIPAMENTO',
            'id_equip_substituido': 'ID EQUIP SUBSTITUIDO',
            'data_da_substituicao': 'DATA DA SUBSTITUICAO',
            'data_proximo_faturamento': 'DATA PRÓXIMO FATURAMENTO',
            #'data_inicio': 'DATA INICIO',
            'data_fim_locacao': 'DATA FIM LOCACAO',
            'tipo_de_servico': 'TIPO DE SERVICO',
            'email': 'E-MAIL',
            'calculo_reajuste': 'CÁLCULO REAJUSTE',
            'nome_da_obra': 'NOME DA OBRA',
            'numero_da_as': 'NUMERO DA AS',
            'pedido_faturamento': 'PEDIDO FATURAMENTO',
            'nf_de_faturamento': 'NF DE FATURAMENTO',
            'serie_de_faturamento': 'SERIE DE FATURAMENTO',
            'data_de_faturamento': 'DATA DE FATURAMENTO',
            'qtde_faturamento': 'QTDE FATURAMENTO',
            'vlr_unitario_faturamento': 'VLR UNITÁRIO FATURAMENTO',
            'vlr_total_faturamento': 'VLR TOTAL FATURAMENTO',
            'periodo_de_faturamento': 'PERÍODO DE FATURAMENTO',
            'status_de_cobranca': 'STATUS DE COBRANÇA',
            }

        for filename in os.listdir(directory):
            if filename.endswith('.xlsx'):
                # caminho completo do arquivo
                file_path = os.path.join(directory, filename)
                # Lê o arquivo
                df = pd.read_excel(file_path, sheet_name='CONSOLIDADO', engine='openpyxl')
                # renomeia as colunas
                df = df.rename(columns=new_names)
                # Salva o DataFrame em um arquivo excel
                df.to_excel(file_path, sheet_name='CONSOLIDADO', index=False, engine='openpyxl')


    # verifica se o pedido existe no banco de dados
    def does_order_exist(self, order_number):
        query = f'SELECT 1 FROM {OrdersTable.__tablename__} WHERE {OrdersTable.__tablename__}.pedido_faturamento = {order_number} LIMIT 1'
        result = self.db_connection.engine.execute(query)
        return result.scalar() is not None


    """ Função para checar novos pedidos e atualizar o banco de dados"""
    def check_and_update_orders(self, extractor_file_path, col):
        start = time.time()
        """Método para verificar e atualizar pedidos ausentes no banco de dados"""

        print('Verificando novos pedidos e atualizando o banco de dados...\n')

        try:
            for filename in os.listdir(extractor_file_path):
                if filename.endswith('.xlsx') and not filename.startswith('~$'):
                    # caminho completo do arquivo
                    file_path = os.path.join(extractor_file_path, filename)
                    # Carrega o arquivo e verifica extrator TOTVS com os pedidos
                    extract_df = pd.read_excel(file_path, sheet_name='2-Resultado', engine='openpyxl')

                    # Padroniza o nome da coluna para minúsculas e substitui espaços por underscore
                    extract_df.columns = extract_df.columns.str.lower().str.replace(' ', '_').str.replace('.', '') \
                        .str.replace('-', '') \
                        .str.replace('ç', 'c') \
                        .str.replace('cálculo_reajuste', 'calculo_reajuste')  # Remove o acento "´"

                    # Verifica se a coluna existe no arquivo
                    col_lower = col.lower().replace(' ', '_')
                    if col_lower not in extract_df.columns:
                        print(f'Coluna {col} não encontrada no arquivo')
                        return

                    # Converte a coluna PEDIDO para numérico
                    extract_df[col_lower] = pd.to_numeric(extract_df[col_lower], errors='coerce')
                    print(f'Total de registros no extrator: {len(extract_df)} | Arquivo {filename}\n')

                    # Filtra o DataFrame para incluir apenas os pedidos mais recentes
                    extract_df = extract_df[extract_df[col_lower] >= 0]

                    # Carrega os pedidos já existentes no banco de dados, convertendo a coluna para inteiro
                    existing_orders = set(int(order) for order in pd.read_sql_query(
                        f'SELECT DISTINCT {col} FROM {OrdersTable.__tablename__}', self.db_connection.engine)[col])

                    # Identifica os pedidos ausentes
                    new_orders = set(extract_df[col_lower]) - existing_orders
                    print(f'Total de novos pedidos no extrator: {len(new_orders)} | Arquivo {filename}\n')

                    if len(new_orders) > 0:
                        # Reinicializa a variável new_orders_df
                        new_orders_df = pd.DataFrame()

                        # Cria um DataFrame apenas com os pedidos ausentes
                        new_orders_df = extract_df[extract_df[col_lower].isin(new_orders)].copy()

                        # Verifica se há novos pedidos antes de continuar
                        if not new_orders_df.empty:
                            # caminho do diretório NOVOS_PEDIDOS
                            path = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS'
                            # cria o diretório NOVOS_PEDIDOS se não existir
                            os.makedirs(path, exist_ok=True)
                            # percorre o DataFrame agrupando os pedidos por cliente
                            for order_number, order_group in new_orders_df.groupby(col_lower):
                                # remove caracteres inválidos do nome do cliente e cria o nome do arquivo
                                client_name_valid = order_group['nome_do_cliente'].iloc[0].translate(
                                    str.maketrans('', '', r'\/:*?"<>|'))
                                # define o nome e cria o arquivo
                                file_name = f'{order_number}_{client_name_valid}.xlsx'
                                # caminho completo do arquivo para salvar
                                file_path = os.path.join(path, file_name)
                                # salva o arquivo em excel
                                order_group.to_excel(file_path, sheet_name='CONSOLIDADO', index=False, engine='openpyxl')
                                print(f'Novo arquivo {file_name} criado.')

                            # Atualiza o banco de dados com os pedidos ausentes
                            print('Atualizando banco de dados....!')
                            try:
                                new_orders_df = new_orders_df.rename(columns={'cálculo_reajuste': 'calculo_reajuste'})

                                new_orders_df.to_sql(OrdersTable.__tablename__, self.db_connection.engine, if_exists='append', index=False, method='multi')
                                print(Fore.GREEN + 'Banco de dados atualizado com novos pedidos' + Fore.RESET)
                            except IntegrityError as e:
                                print('Erro ao atualizar o banco de dados:', e)

                            # pula o processamento dos clientes abaixo (grandes clientes)
                            special_clients = ['teste']

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
                            #print(f'Tempo de execução do código: {end - start}')

                        else:
                            print('Nenhum pedido novo encontrado.\n')

        except PermissionError as e:
            print(f"Erro de permissão ao acessar {extractor_file_path}: {e}")
            print('Corrija as permissões e tente novamente.')
            return False
        except Exception as e:
            print(f"Erro ao processar arquivo: {e}")
            return False

        print(f'Colunas não usadas no relatório final removidas com sucesso')       
            
        
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

    
    """ Função que renomeia e formata o arquivo final com cores da Arklok"""
    def rename_format_columns(self, directory):
        
        # dicionário com os nomes das colunas (chave) e os nomes formatados (valor
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
            'descricao_do_produto': 'DESCRIÇÃO DO PRODUTO',
            'quantidade': 'QUANTIDADE',
            'pedido_de_remessa': 'PEDIDO DE REMESSA',
            'projeto': 'PROJETO',
            'obra': 'OBRA',
            'prazo_do_contrato': 'PRAZO DO CONTRATO',
            'data_de_ativacao_legado': 'DATA DE ATIVAÇÃO LEGADO',
            'data_de_ativacao': 'DATA DE ATIVAÇÃO',
            'ultimo_faturamento': 'ÚLTIMO FATURAMENTO',
            #'periodo_final': 'PERIODO FINAL',
            'data_do_termo': 'DATA DO TERMO',
            'aniversario': 'DATA BASE REAJUSTE',
            'desc_ajuste': 'ÍNDICE',
            'indice_aplicado': 'ÍNDICE APLICADO',
            'dias_de_locacao': 'DIAS DE LOCAÇÃO',
            'valor_de_origem': 'VALOR DE ORIGEM',
            'valor_unitario': 'VALOR UNITÁRIO',
            'valor_bruto': 'VALOR BRUTO',
            'tipo_do_mes': 'TIPO DO MES',
            #'nr_chamado': 'NR CHAMADO',
            'contrato_legado': 'CONTRATO LEGADO',
            'acrescimo': 'ACRÉSCIMO',
            'franquia': 'FRANQUIA',
            'id_equipamento': 'ID EQUIPAMENTO',
            'id_equip_substituido': 'ID EQUIP SUBSTITUIDO',
            'data_da_substituicao': 'DATA DA SUBSTITUICAO',
            'data_proximo_faturamento': 'DATA PRÓXIMO FATURAMENTO',
            #'data_inicio': 'DATA INICIO',
            'data_fim_locacao': 'DATA FIM LOCACAO',
            'tipo_de_servico': 'TIPO DE SERVICO',
            'email': 'E-MAIL',
            'calculo_reajuste': 'CÁLCULO REAJUSTE',
            'nome_da_obra': 'NOME DA OBRA',
            'numero_da_as': 'NUMERO DA AS',
            'pedido_faturamento': 'PEDIDO FATURAMENTO',
            'nf_de_faturamento': 'NF DE FATURAMENTO',
            'serie_de_faturamento': 'SERIE DE FATURAMENTO',
            'data_de_faturamento': 'DATA DE FATURAMENTO',
            'qtde_faturamento': 'QTDE FATURAMENTO',
            'vlr_unitario_faturamento': 'VLR UNITÁRIO FATURAMENTO',
            'vlr_total_faturamento': 'VLR TOTAL FATURAMENTO',
            'periodo_de_faturamento': 'PERÍODO DE FATURAMENTO',
            'status_de_cobranca': 'STATUS DE COBRANÇA',
            }
        
        try:
            # percorre o diretório e localiza os arquivos excel 
            for filename in os.listdir(directory):
                if filename.endswith('.xlsx') and not filename.startswith('~$'):
                    # caminho completo do arquivo
                    file_path = os.path.join(directory, filename)
                    # Lê o arquivo
                    df = pd.read_excel(file_path, sheet_name='CONSOLIDADO', engine='openpyxl')                   
                    
                    # renomeia as colunas
                    df = df.rename(columns=new_names)

                    # dropas as colunas que não serão usadas
                    columns_to_drop = ['CNPJ DE REMESSA', 'NOTA DE REMESSA', 'DATA DE REMESSA', 'SÉRIE DA NF REMESSA', 'PRODUTO',
                                    'PEDIDO DE REMESSA', 'PRAZO DO CONTRATO', 'ÚLTIMO FATURAMENTO', 'DATA DO TERMO', 'TIPO DO MÊS',
                                    'FRANQUIA', 'ID EQUIP SUBSTITUIDO', 'DATA DA SUBSTITUIÇÃO', 'DATA PRÓXIMO FATURAMENTO',
                                    'DATA FIM LOCAÇÃO', 'TIPO DE SERVIÇO', 'E-MAIL', 'NOME DA OBRA', 'NUMERO DA AS', 'PEDIDO FATURAMENTO',
                                    'NF DE FATURAMENTO', 'SÉRIE DE FATURAMENTO', 'DATA DE FATURAMENTO', 'QTDE FATURAMENTO', 'STATUS DE COBRANÇA']

                    df = df.drop(columns_to_drop, axis=1, errors='ignore')

                    # reordena as colunas
                    df = df[['CÓDIGO CLIENTE', 'NOME DO CLIENTE', 'LOJA CLIENTE', 'CNPJ DO CLIENTE', 'CNPJ DE FATURAMENTO',
                            'PROJETO', 'OBRA', 'ID EQUIPAMENTO', 'EQUIPAMENTO', 'DESCRIÇÃO DO PRODUTO', 'DATA DE ATIVAÇÃO LEGADO', 'DATA DE ATIVAÇÃO',
                            'PERÍODO DE FATURAMENTO', 'DIAS DE LOCAÇÃO', 'VALOR UNITÁRIO', 'VALOR BRUTO',
                            'VLR UNITÁRIO FATURAMENTO', 'QUANTIDADE', 'VLR TOTAL FATURAMENTO', 'DATA BASE REAJUSTE', 'ÍNDICE', 'VALOR DE ORIGEM',
                            'CÁLCULO REAJUSTE', 'ÍNDICE APLICADO', 'ACRÉSCIMO', 'CONTRATO LEGADO']]
                     
                    
                    # formata células com datas para o formato dd/mm/aaaa
                    cols_date = ['DATA DE ATIVAÇÃO', 'DATA DE ATIVAÇÃO LEGADO', 'DATA BASE REAJUSTE']
                    
                    # itera sobre as colunas 
                    for col in cols_date:
                        # Verifica se a coluna é do tipo datetime antes de formatar
                        if pd.api.types.is_datetime64_any_dtype(df[col]):
                            df[col] = df[col].dt.strftime('%d/%m/%Y')
                        else:
                            try:
                                # converte a coluna para datetime
                                df[col] = pd.to_datetime(df[col].loc[df[col].notna()], format='%d/%m/%Y', errors='coerce')
                            except Exception as e:
                                print(f'Erro ao converter data: {e}')
                    
                    # formata células com cnpj para o formato xx.xxx.xxx/xxxx-xx
                    cols_cnpj = ['CNPJ DO CLIENTE', 'CNPJ DE FATURAMENTO']
                    # itera sobre as colunas
                    for col in cols_cnpj:
                        # Verifica se a coluna é do tipo string antes de aplicar .str
                        if pd.api.types.is_string_dtype(df[col]):
                            # defini o formato do cnpj xx.xxx.xxx/xxxx-xx
                            df[col] = df[col].str.replace(r'(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', r'\1.\2.\3/\4-\5')
                        else:
                            try:
                                # converte a coluna para string
                                df[col] = df[col].astype(str).str.replace(r'(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})', r'\1.\2.\3/\4-\5')
                            except Exception as e:
                                print(f'Erro ao converter cnpj: {e}')          
                     
                    # Converte os valores da coluna 'VALOR BRUTO' para float, substituindo vírgulas por pontos se necessário
                    #df['VLR TOTAL FATURAMENTO'] = df['VLR TOTAL FATURAMENTO'].apply(lambda x: float(str(x).replace(',', '.')) if pd.notna(x) else x)
                    df['VLR TOTAL FATURAMENTO'] = df['VLR TOTAL FATURAMENTO'].apply(lambda x: float(str(x).replace('.', '').replace(',', '.')) if pd.notna(x) else x)

                    

                    # salva o arquivo em excel
                    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                        df.to_excel(writer, sheet_name='CONSOLIDADO', index=False, engine='openpyxl')

                        
                        sintese_df = df.groupby(['PROJETO', 'OBRA', 'CONTRATO LEGADO'], as_index=False)['VLR TOTAL FATURAMENTO'].sum()
                        sintese_df = sintese_df.rename(columns={'VLR TOTAL FATURAMENTO': 'VALOR A COBRAR'})

                        # formatação da planilha "CONSOLIDADO"
                        worksheet = writer.sheets['CONSOLIDADO']
                        for column in range(1, worksheet.max_column + 1):
                            worksheet.column_dimensions[worksheet.cell(row=1, column=column).column_letter].width = 25
                            worksheet.cell(row=1, column=column).font = Font(color='FFFFFF', bold=True, name='Lato Regular', size=10)
                            worksheet.cell(row=1, column=column).fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
                            # alinhar o texto no meio
                            worksheet.cell(row=1, column=column).alignment = Alignment(horizontal='center', vertical='center', )

                            worksheet.row_dimensions[1].height = 24
                        
                                                
                        # Configuração para o formato brasileiro
                        locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

                        # formatação da coluna VALOR A COBRAR
                        sintese_df['VALOR A COBRAR'] = sintese_df['VALOR A COBRAR'].apply(lambda x: locale.currency(float(x) if pd.notna(x) else 0, grouping=True, symbol='R$'))

                        sintese_df.to_excel(writer, sheet_name='SÍNTESE', index=False)


                        # Adiciona "TOTAL" abaixo da célula "C"
                        worksheet = writer.sheets['SÍNTESE']
                        worksheet.cell(row=worksheet.max_row + 2, column=3, value='TOTAL')

                        # negrito na célula "TOTAL"
                        worksheet.cell(row=worksheet.max_row, column=3).font = Font(bold=True)
                        

                        # Soma os valores da coluna "D" (VALOR A COBRAR)
                        total_valor_a_cobrar = sintese_df['VALOR A COBRAR'].apply(lambda x: locale.atof(x.split()[1])).sum()

                        # negrito na célula "VALOR A COBRAR"
                        worksheet.cell(row=worksheet.max_row, column=4).font = Font(bold=True)
                        
                        # formatação da soma dos valores da coluna "D" (VALOR A COBRAR)
                        total_valor_a_cobrar = locale.currency(total_valor_a_cobrar, grouping=True)
                        
                        worksheet.cell(row=worksheet.max_row, column=4, value=total_valor_a_cobrar)

                        # Adiciona logotipo ao cabeçalho da sheet 'SÍNTESE'
                        #img = Image(logo_path)
                        #worksheet.add_image(img, 'A1')

                        
                        # Aplicar cor vermelha ao cabeçalho das colunas A, B, C e D e negrito e tipografia "Alwyn New Light"
                        for column in 'ABCD':
                            header_cell = worksheet[f"{column}1"]
                            header_cell.font = Font(color='FFFFFF', bold=True, name='Lato Regular', size=10)
                            header_cell.fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
                            header_cell.border = Border(left=Side(border_style='thin'), right=Side(border_style='thin'), 
                                                    top=Side(border_style='thin'), bottom=Side(border_style='thin'))
                            header_cell.alignment = Alignment(horizontal='center', vertical='center', )

                            # Ajusta a altura da linha do cabeçalho
                            worksheet.row_dimensions[1].height = 24
                        
                        # adicona bordas externas à planilha "SÍNTESE"
                        for row in worksheet.iter_rows(min_row=1, max_row=worksheet.max_row, min_col=1, max_col=worksheet.max_column):
                            for cell in row:
                                cell.border = Border(left=Side(border_style='thin'), right=Side(border_style='thin'), 
                                                    top=Side(border_style='thin'), bottom=Side(border_style='thin'))
                                

                        # formatar largura das colunas
                        writer.sheets['SÍNTESE'].column_dimensions['A'].width = 20
                        writer.sheets['SÍNTESE'].column_dimensions['B'].width = 15
                        writer.sheets['SÍNTESE'].column_dimensions['C'].width = 31
                        writer.sheets['SÍNTESE'].column_dimensions['D'].width = 23           

                    #print(f'Arquivos formatados com sucesso em {file_path}')

        except PermissionError as e:
            print(f"O arquivo {filename} está aberto: {e}")
            print('Feche o arquivo manualmente e tente novamente.')
            return False
        #print(f'Colunas não usadas no relatório final removidas com sucesso')        

    
    """ Função para formatar células do arquivo final com datas no formato dd/mm/aaaa"""
    def format_date_cells(self, directory):
        # define o diretório
        directory = directory
        # percorre o diretório e localiza os arquivos excel
        for filename in os.listdir(directory):
            if filename.endswith('.xlsx'):
                # caminho completo do arquivo
                file_path = os.path.join(directory, filename)
                # Lê o arquivo
                df = pd.read_excel(file_path, sheet_name='CONSOLIDADO', engine='openpyxl')
                # lista com as colunas que serão formatadas
                date_cols = ['DATA DE ATIVAÇÃO', 'PERÍODO INICIAL',
                            'DATA PRÓXIMO FATURAMENTO']
                try:
                    # itera sobre as colunas e aplica a função formatar_cnpj
                    for col in date_cols:
                        df[col] = df[col].dt.strftime('%d/%m/%Y')
                except Exception as e:
                    print(f"Erro ao formatar colunas: {e}")
                    return None
                



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
        # percorre o diretório e retorna uma lista de arquivos com o caminho completo usando list comprehension
        return [(root, file) for root, dirs, files in os.walk(self.news_orders) \
                for file in files if file.endswith(file_type    )]

    # Método para processar os arquivos
    def process_file_list(self, filo_info):
        
        # obtém o caminho completo do arquivo
        root, file = filo_info
        full_path = os.path.join(root, file)
        
        # cria uma lista vazia
        xlsx_files = []
        
        # verifica se o arquivo é um arquivo excel e não é um arquivo temporário
        if file.lower().endswith('.xlsx') \
            and not file.startswith('~$'):
            print(f'{Fore.LIGHTCYAN_EX}Arquivo encontrado em: {full_path}{Fore.RESET}')

            # limpa o buffer de saída    
            sys.stdout.flush() 
            # adiciona o arquivo na lista de arquivos
            xlsx_files.append(full_path)

            # Obtem informações do arquivo
            file_status = os.stat(full_path)
            file_size = file_status.st_size
            filename = os.path.basename(full_path)
            file_path = os.path.dirname(full_path)
            file_date_create = datetime.fromtimestamp(file_status.st_ctime).strftime('%d/%m/%Y %H:%M:%S')
            file_date_modified = datetime.fromtimestamp(file_status.st_mtime).strftime('%d/%m/%Y %H:%M:%S')
            full_path_file = os.path.join(file_path, filename)
            
            # retorna um dicionário com as informações do arquivo
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
        # Cria um pool de threads
        with ThreadPoolExecutor() as executor:            
            # Processa os arquivos em paralelo com o método map (função, iterável)
            results = executor.map(self.process_file_list, file_infos)
        # retorna os resultados usando list comprehension
        return [result for result in results if result is not None]
    
    # Método para listar os arquivos
    def list_all_files(self, output_folder):
        start_time = time.time()
        # verficar se o diretório existe
        if not os.path.exists(output_folder):
            print(f'A pasta {output_folder} não existe')
            # se não existir, cria o diretório
            os.makedirs(output_folder)
            print(f'Criando a pasta {output_folder}')
            return 
        
        # 
        try:
            # lista todos os arquivos no diretório
            file_infos = self.get_files()
            
            # processa os arquivos em paralelo
            file_list = self.process_files_in_parallel(file_infos)
            # itera sobre cada arquivo
            for file_info in file_infos:
                try:
                    processed_file = self.process_file_list(file_info)
                    # verifica se o arquivo foi processado
                    if processed_file is not None:
                        # adiciona o arquivo na lista de arquivos
                        file_list.append(processed_file)
                # caso ocorra algum erro, exibe o erro        
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

        # caso ocorra algum erro, exibe o erro
        except PermissionError as e:
            print(f'O arquivo {self.news_orders} está aberto. Feche o arquivo e tente novamente')
            return
        
        # caso ocorra algum erro, exibe o erro
        except Exception as e:
            print(f'Ocorreu um erro no arquivo {self.news_orders}: {e}')
            return False

    # Criar pastas no diretório H:\\
    def make_folders_clients(self, batch_totvs_path, extractor_path, sheet_name, col):
        # carrega o arquivo com os clientes
        df = pd.read_excel(extractor_path, sheet_name, engine='openpyxl')
        # cria a coluna com o caminho completo do diretório
        basedir = batch_totvs_path
        
        # itera sobre cada cliente
        for client in df[col]:
            # cria o caminho completo do diretório
            client_path = os.path.join(basedir, client)
            
            # verifica se o diretório já existe
            if not os.path.exists(client_path):
                # se não existir, cria o diretório
                os.makedirs(client_path)
                print(f'Pasta {client} criada com sucesso!')
            else:
                print(f'Pasta {client} já existe!')

    # função para excluir todos os arquivos da pasta copied_files
    def delete_xlsx(self, files_path):
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
            
            #print(f"Todos os arquivos excluídos com sucesso!")
            return True
        
        except PermissionError as e:
            print(f"O arquivo {files_path} está aberto: {e}")
            print('Delete o arquivo manualmente.')
            return False
        
        except Exception as e:
            print(f"Ocorreu um erro ao excluir os arquivos: {e}")
            return False 
        
    

    # função para excluir todos os arquivos da pasta copied_files
    def delete_xml(self, files_path):
        logging.info(f"EXCLUINDO ARQUIVOS XML...")
        # verifica se a pasta existe
        if not os.path.exists(files_path):
            logging.info(f"A pasta {files_path} não existe.")
            return

        try:
            # lista todos os arquivos no diretório
            file_list = [f for f in os.listdir(files_path) if f.endswith('.xml') and not f.startswith('~$')]
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
            
            #print(f"Todos os arquivos excluídos com sucesso!")
            return True
        
        except PermissionError as e:
            print(f"O arquivo {files_path} está aberto: {e}")
            print('Delete o arquivo manualmente.')
            return False
        
        except Exception as e:
            print(f"Ocorreu um erro ao excluir os arquivos: {e}")
            return False 
            
    
    # função para previsão de pastas no diretório H:\\ usando inteligência artificial
    def find_closest_match(self, client_name, target_base_directory):
        
        #Lista de diretórios de destino disponíveis
        target_diretories = [d for d in os.listdir(target_base_directory)]
                             
        
        # Encontra a correspondência mais próxima no diretório de destino usando fuzzywuzzy
        best_match, _ = process.extractOne(client_name, target_diretories)

        # define um limite de confiança de 80%
        threshold = 80

        if best_match and best_match[1] >= threshold:
            return os.path.join(target_base_directory, best_match[0])
        else:
            return None



    # função para distribuir os arquivos por cliente
    def move_file_to_client_folder(self, source_directory, target_directory):
       
        # diretório de origem dos arquivos (NOVOS_PEDIDOS)
        path = source_directory
        
        try:
            for filename in os.listdir(path):
                if filename.endswith('.xlsx') and not filename.startswith('~$'):
                    # caminho completo do arquivo de origem que será movido
                    full_source = os.path.join(path, filename)

                    # extrai o nome do cliente do arquivo
                    client_name = filename.split('_')[1].split('.')[0]

                    # Encontra a correspondência mais próxima no diretório de destino
                    #target_directory = self.find_closest_match(client_name)

                    # diretório de destino do arquivo
                    target_path =  os.path.join(target_directory, client_name)
                    os.makedirs(target_path, exist_ok=True)

                    # caminho completo do arquivo de destino
                    target_path_file = os.path.join(target_path, filename)

                    # Se o arquivo já existe, remova-o antes de mover o novo
                    if os.path.exists(target_path_file):
                        os.remove(target_path_file)
                        print(f'Removendo arquivo existente: {target_path_file}')

                    # Move o arquivo para o diretório de destino
                    shutil.move(full_source, target_path)
                    print(f'Movendo arquivo {filename} para {target_path}...')
                
        except PermissionError as e:
            print(f"O arquivo {source_directory} está aberto: {e}")
            print(f'Mova o arquivo manualmente para o diretório {target_path}')
            return False


    # função soma dos valores da coluna "VALOR BRUTO" de todos os arquivos do diretório
    def accurent_billing_value(self, directory):
        # lista os arquivos do diretório
        xlsx_files = glob.glob(os.path.join(directory, '*.xlsx'))
        # cria um dataframe vazio
        combined_df = pd.DataFrame()

        # itera sobre os arquivos do diretório
        for file in xlsx_files:
            # carrega o arquivo
            df = pd.read_excel(file, sheet_name='CONSOLIDADO', engine='openpyxl')
            # concatena o dataframe do arquivo com o dataframe combinado
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Soma os valores da coluna "VALOR BRUTO"
        total_billing_value = combined_df['VALOR BRUTO'].sum()
        print(f'Total billing value: {total_billing_value}')

        # formatação da soma dos valores da coluna "VALOR BRUTO"
        #total_billing_value = locale.currency(total_billing_value, grouping=True)

        return total_billing_value

        







    #


        
    

    