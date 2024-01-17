from controllers import *
from database import *
from check_orders import *
from colorama import Fore
from termcolor import cprint
import art
from time import sleep

ascii_banner = art.text2art("Relatorio Final")
colored_banner = cprint(ascii_banner, 'green')

#ENTRDA DOS ARQUIVOS
extractor_file_path = r"H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\99-EXTRATOR_PEDIDOS_DE_CLIENTES" # EXTRATOR
# SAÍDA DOS ARQUIVOS
batch_totvs_path = r'H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' # CRIARÁ AS PASTA AQUI
#verificar se o pedido já foi faturado no banco de dados PostgresQL
invoiced_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\PEDIDOS_FATURADOS' # PEDIDOS FATURADOS NO BANCO DE DADOS
news_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS' # NOVOS PEDIDOS IDENTIFICADOS NO EXTRATOR
output_merge_path = r'C:\DataWare\data\consolidated_files\consolidated_validated\MERGE_RELATÓRIO_FINAL' # RELATÓRIO FINAL 
source_directory = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS' # DIRETÓRIO DE ORIGEM DOS PEDIDOS
target_directory = r'H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' # DIRETÓRIO DE DESTINO DOS PEDIDOS

# Diretório de destino para os arquivos que serão enviados para o cliente
# usando fuzzywuzzy para encontrar o nome do cliente
target_base_directory = r'H:\01 - FATURAMENTO\CLIENTES - CONTROLE\CLIENTES 2023 LEGADO DN4'




file_processor = FileProcessor(extractor_file_path, invoiced_orders, news_orders, output_merge_path)
host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
sql = ConnectPostgresQL(host_postgres)
final_report = FinalReport(host_postgres)
#sql.create_database()



if __name__ == "__main__":
    
    
    
    while True:
        print('------------------------------------------------')
        print(Fore.LIGHTYELLOW_EX + '                   MENU PRINCIPAL          ' + Fore.RESET)
        print('------------------------------------------------\n')

        print(' 1 - Checar novos pedidos\n',
              '2 - Extrato de novos pedidos\n',
              '3 - Inserir novos pedidos no banco de dados\n',
              '4 - Criar pastas para cada cliente diretório H:\n',
              '5 - Criar novo banco de dados\n',
              '6 - Deletar pedidos do diretório Novos Pedidos\n',
              '7 - Mover pedidos para diretório H:\n',   
              '8 - Renomear e formatar colunas\n',
              '9 - Valor Faturado Atual\n'           
             
              )

        try:
            option = int(input('Digite a opção desejada: '))

            if option == 1:
                # CHAMA FUNÇÃO 1

                sql.create_database()
                sleep(0.5)
                print(Fore.GREEN + 'CHECANDO NOVOS PEDIDOS ...' + Fore.RESET)
                final_report.check_and_update_orders(extractor_file_path, 'pedido_faturamento')
                sleep(0.5)
                print(Fore.GREEN + 'PEDIDOS CHECADOS COM SUCESSO!' + Fore.RESET)
                print(Fore.GREEN + 'RENOMEANDO E FORMATANDO COLUNAS' + Fore.RESET)
                #final_report.rename_format_columns(news_orders)
                sleep(0.5)
                #file_processor.move_file_to_client_folder(source_directory=source_directory, target_directory=target_base_directory)
                print(Fore.GREEN + f'MOVENDO PEDIDOS PARA DIRETÓRIO H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR4' + Fore.RESET)
                #print(Fore.GREEN + 'PEDIDOS MOVIDOS COM SUCESSO PARA H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' + Fore.RESET)
                sleep(0.5)
                #file_processor.delete_new_files(files_path=news_orders)
                print(Fore.GREEN + 'AUTOMAÇÃO CONCLUÍDA!' + Fore.RESET)
            
            elif option == 2:
                file_processor.list_all_files(news_orders)
                final_report.rename_columns(news_orders)
                print('Colunas renomeadas com sucesso!')

            elif option == 3:
                sql.create_database()

                for filename in os.listdir(extractor_file_path):
                    if filename.endswith('.xlsx'):
                        file_path = os.path.join(extractor_file_path, filename)
                        df_news_orders = pd.read_excel(file_path, sheet_name='2-Resultado', engine='openpyxl')
                        
                        df_news_orders = df_news_orders.astype(str)

                        for i in range(len(df_news_orders.astype(str))):
                            try:
                                sql.insert_data('pedidosfaturados',
                                            codigo_cliente = df_news_orders['Codigo Cliente'][i],
                                            loja_cliente = df_news_orders['Loja Cliente'][i],
                                            nome_do_cliente = df_news_orders['Nome do Cliente'][i],
                                            cnpj_do_cliente = df_news_orders['CNPJ do Cliente'][i],
                                            cnpj_de_faturamento = df_news_orders['CNPJ de Faturamento'][i],
                                            cnpj_de_remessa = df_news_orders['CNPJ de Remessa'][i],
                                            equipamento = df_news_orders['Equipamento'][i],
                                            nota_de_remessa = df_news_orders['Nota de Remessa'][i],
                                            data_de_remessa = df_news_orders['Data de Remessa'][i],
                                            serie_da_nf_remessa = df_news_orders['Serie da NF Remessa'][i],
                                            produto = df_news_orders['Produto'][i],
                                            descricao_do_produto = df_news_orders['Descricao do Produto'][i],
                                            quantidade = df_news_orders['Quantidade'][i],
                                            pedido_de_remessa = df_news_orders['Pedido de Remessa'][i],
                                            projeto = df_news_orders['Projeto'][i],
                                            obra = df_news_orders['Obra'][i],
                                            prazo_do_contrato = df_news_orders['Prazo do Contrato'][i],
                                            data_de_ativacao_legado = df_news_orders['Data de Ativacao Legado'][i],
                                            data_de_ativacao = df_news_orders['Data de Ativacao'][i],
                                            ultimo_faturamento = df_news_orders['Ultimo Faturamento'][i],
                                            data_do_termo = df_news_orders['Data do Termo'][i],
                                            aniversario = df_news_orders['Aniversario'][i],
                                            desc_ajuste = df_news_orders['Desc. Ajuste'][i],
                                            indice_aplicado = df_news_orders['Indice Aplicado'][i],
                                            dias_de_locacao = df_news_orders['Dias de Locacao'][i],
                                            valor_de_origem = df_news_orders['Valor de Origem'][i],
                                            valor_unitario = df_news_orders['Valor Unitario'][i],
                                            valor_bruto = df_news_orders['Valor Bruto'][i],
                                            tipo_do_mes = df_news_orders['Tipo do Mes'][i],
                                            contrato_legado = df_news_orders['Contrato Legado'][i],
                                            acrescimo = df_news_orders['Acrescimo'][i],
                                            franquia = df_news_orders['Franquia'][i],
                                            id_equipamento = df_news_orders['ID Equipamento'][i],
                                            id_equip_substituido = df_news_orders['ID Equip. Substituido'][i],
                                            data_da_substituicao = df_news_orders['Data da Substituicao'][i],
                                            data_proximo_faturamento = df_news_orders['Data Proximo Faturamento'][i],
                                            data_fim_locacao = df_news_orders['Data Fim Locacao'][i],
                                            tipo_de_servico = df_news_orders['Tipo de Servico'][i],
                                            email = df_news_orders['E-Mail'][i],
                                            calculo_reajuste = df_news_orders['Cálculo Reajuste'][i],
                                            nome_da_obra = df_news_orders['Nome da Obra'][i],
                                            numero_da_as = df_news_orders['Numero da AS'][i],
                                            pedido_faturamento = df_news_orders['Pedido Faturamento'][i],
                                            nf_de_faturamento = df_news_orders['NF de Faturamento'][i],
                                            serie_de_faturamento = df_news_orders['Serie de Faturamento'][i],
                                            data_de_faturamento = df_news_orders['Data de Faturamento'][i],
                                            qtde_faturamento = df_news_orders['Qtde. Faturamento'][i],
                                            vlr_unitario_faturamento = df_news_orders['Vlr. Unitario Faturamento'][i],
                                            vlr_total_faturamento = df_news_orders['Vlr. Total Faturamento'][i],
                                            periodo_de_faturamento = df_news_orders['Periodo de Faturamento'][i],
                                            status_de_cobranca = df_news_orders['Status de Cobrança'][i]

                                            )
                                
                            except Exception as e:                    
                                print(f'Erro ao inserir dados no banco de dados: {e}')
                
            elif option == 4:
                files = file_processor.make_folders_clients(batch_totvs_path=batch_totvs_path,
                                                            extractor_path=extractor_file_path,
                                                            sheet_name="2-Resultado", col="Nome do Cliente")
            
            elif option == 5:
                sql.create_database()

            elif option == 6:
                file_processor.delete_new_files(files_path=news_orders)

            elif option == 7:
                file_processor.move_file_to_client_folder(source_directory=source_directory,
                                                          target_directory=target_directory)
            
            elif option == 8:
                final_report.rename_format_columns(news_orders)
                print('Colunas renomeadas com sucesso!')

            elif option == 9:
                file_processor.accurent_billing_value(
                    r"\\10.10.4.7\Dados\Financeiro\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR\ADVANCED SERVICOS DE APOIO ADMINISTRATIV"
                )
                print('Valor de faturamento atualizado com sucesso!')

            elif option == 10:
                file_processor.move_file_to_client_folder(source_directory=r'C:\Users\marcos.silvaext\Documents',
                                                          target_directory=r'C:\Users\marcos.silvaext\Documents\pasta_arklok_destino')                
            
            elif option == 11:
                file_processor.make_new_folders(dataframe=r'H:\01 - FATURAMENTO\FATURAMENTO 2024\EXTRATOR\01 - JANEIRO 2024\1601_EXTRATOR COM PEDIDO.xlsx',
                                                sheet_name='2-Resultado', engine='openpyxl',
                                                directory=r'\\10.10.4.7\Dados\Financeiro\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\PASTAS_AJUSTADAS')
            
            elif option == 12:
                file_processor.move_files_to_month_subfolder(directory_origin=r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS',
                                                             target_directory=r'H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\PASTAS_AJUSTADAS_2'

                )
            
            else:
                print('Opção inválida')
        except Exception as e:
            print(e)











