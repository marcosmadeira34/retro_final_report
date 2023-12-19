from controllers import *
from database import *
from check_orders import *
from colorama import Fore
from termcolor import cprint
import art

ascii_banner = art.text2art("Relatorio de Pedidos")
colored_banner = cprint(ascii_banner, 'green')

batch_totvs_path = r'H:\01 - FATURAMENTO\RELATORIO-TOTVS_2024'
extractor_file_path = r"C:\DataWare\data\consolidated_files\consolidated_validated\EXTRATOR_OFICIAL.xlsx" # EXTRATOR
#verificar se o pedido já foi faturado no banco de dados PostgresQL
invoiced_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\PEDIDOS_FATURADOS' # PEDIDOS FATURADOS NO BANCO DE DADOS
news_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS' # NOVOS PEDIDOS IDENTIFICADOS NO EXTRATOR
output_merge_path = r'C:\DataWare\data\consolidated_files\consolidated_validated\MERGE_RELATÓRIO_FINAL' # RELATÓRIO FINAL 


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
              
             
              )

        try:
            option = int(input('Digite a opção desejada: '))

            if option == 1:
                # CHAMA FUNÇÃO 1
                final_report.check_and_update_orders(extractor_file_path, 'pedido_faturamento')
            
            #elif option == 2:
            #   print('Gerando relatório final...')
                # CHAMA FUNÇÃO 2
            #  final_report.merge_same_client(news_orders, output_merge_path)

            elif option == 2:
                file_processor.list_all_files(news_orders)
                final_report.rename_columns(r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS')
                print('Colunas renomeadas com sucesso!')

            elif option == 3:
                sql.create_database()
                df_news_orders = pd.read_excel(r'C:\DataWare\data\consolidated_files\consolidated_validated\EXTRATOR_OFICIAL.xlsx', 
                                            sheet_name='2-Resultado', engine='openpyxl')
                
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
                                    data_de_ativacao = df_news_orders['Data de Ativacao'][i],
                                    periodo_inicial = df_news_orders['Periodo Inicial'][i],
                                    periodo_final = df_news_orders['Periodo Final'][i],
                                    data_do_termo = df_news_orders['Data do Termo'][i],
                                    aniversario = df_news_orders['Aniversario'][i],
                                    desc_ajuste = df_news_orders['Desc. Ajuste'][i],
                                    dias_de_locacao = df_news_orders['Dias de Locacao'][i],
                                    valor_unitario = df_news_orders['Valor Unitario'][i],
                                    valor_bruto = df_news_orders['Valor Bruto'][i],
                                    tipo_do_mes = df_news_orders['Tipo do Mes'][i],
                                    nr_chamado = df_news_orders['Nr. Chamado'][i],
                                    contrato_legado = df_news_orders['Contrato Legado'][i],
                                    acrescimo = df_news_orders['Acrescimo'][i],
                                    franquia = df_news_orders['Franquia'][i],
                                    id_equipamento = df_news_orders['ID Equipamento'][i],
                                    id_equip_substituido = df_news_orders['ID Equip. Substituido'][i],
                                    data_da_substituicao = df_news_orders['Data da Substituicao'][i],
                                    data_proximo_faturamento = df_news_orders['Data Proximo Faturamento'][i],
                                    data_inicio = df_news_orders['Data Inicio'][i],
                                    data_fim_locacao = df_news_orders['Data Fim Locacao'][i],
                                    tipo_de_servico = df_news_orders['Tipo de Servico'][i],
                                    email = df_news_orders['E-Mail'][i],
                                    valor_de_origem = df_news_orders['Valor de Origem'][i],
                                    #descricao_do_ajuste = df_news_orders['Descricao do Ajuste'][i],
                                    nome_da_obra = df_news_orders['Nome da Obra'][i],
                                    numero_da_as = df_news_orders['Numero da AS'][i],
                                    pedido_faturamento = df_news_orders['Pedido Faturamento'][i],
                                    nf_de_faturamento = df_news_orders['NF de Faturamento'][i],
                                    serie_de_faturamento = df_news_orders['Serie de Faturamento'][i],
                                    data_de_faturamento = df_news_orders['Data de Faturamento'][i],
                                    qtde_faturamento = df_news_orders['Qtde. Faturamento'][i],
                                    vlr_unitario_faturamento = df_news_orders['Vlr. Unitario Faturamento'][i],
                                    vlr_total_faturamento = df_news_orders['Vlr. Total Faturamento'][i],
                                    periodo_de_faturamento = df_news_orders['Periodo de Faturamento'][i]

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
                
            else:
                print('Opção inválida')
        except ValueError:
            print('Opção inválida')











