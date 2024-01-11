from controllers import *
from database import *
from check_orders import *
from colorama import Fore
from termcolor import cprint
import art
from time import sleep
import schedule
import functools

ascii_banner = art.text2art("Relatorio Final")
colored_banner = cprint(ascii_banner, 'green')

#ENTRDA DOS ARQUIVOS
extractor_file_path = r"H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\01-EXTRATOR_PEDIDOS_DE_CLIENTES" # EXTRATOR
# SAÍDA DOS ARQUIVOS
batch_totvs_path = r'H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' # CRIARÁ AS PASTA AQUI
#verificar se o pedido já foi faturado no banco de dados PostgresQL
invoiced_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\PEDIDOS_FATURADOS' # PEDIDOS FATURADOS NO BANCO DE DADOS
news_orders = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS' # NOVOS PEDIDOS IDENTIFICADOS NO EXTRATOR
output_merge_path = r'C:\DataWare\data\consolidated_files\consolidated_validated\MERGE_RELATÓRIO_FINAL' # RELATÓRIO FINAL 
source_directory = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS' # DIRETÓRIO DE ORIGEM DOS PEDIDOS
target_directory = r'H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' # DIRETÓRIO DE DESTINO DOS PEDIDOS
# move os arquivos para a pasta de arquivos processados a cada determinado tempo
processed_extrator_path = r"\\10.10.4.7\Dados\Financeiro\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\04 - EXTRATORES PROCESSADOS"


file_processor = FileProcessor(extractor_file_path, invoiced_orders, news_orders, output_merge_path)
host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
sql = ConnectPostgresQL(host_postgres)
final_report = FinalReport(host_postgres)
#sql.create_database()



if __name__ == "__main__":
    while True:
        #sql.create_database()
        file_processor.delete_xml(files_path=extractor_file_path)
        sleep(0.5)
        print(Fore.GREEN + 'CHECANDO NOVOS PEDIDOS ...' + Fore.RESET)
        final_report.check_and_update_orders(extractor_file_path, 'pedido_faturamento')
        sleep(0.5)
        print(Fore.GREEN + 'PEDIDOS CHECADOS COM SUCESSO!' + Fore.RESET)
        print(Fore.GREEN + 'RENOMEANDO E FORMATANDO COLUNAS' + Fore.RESET)
        final_report.rename_format_columns(news_orders)
        sleep(0.5)
        file_processor.move_file_to_client_folder(source_directory=source_directory, target_directory=target_directory)
        print(Fore.GREEN + 'MOVENDO PEDIDOS PARA DIRETÓRIO H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' + Fore.RESET)
        #print(Fore.GREEN + 'PEDIDOS MOVIDOS COM SUCESSO PARA H:\01 - FATURAMENTO\01 - CLIENTES - CONTROLE - 2024 TOTVS\02-SAÍDA_EXTRATOR' + Fore.RESET)
        sleep(0.5)
        #file_processor.delete_new_files(files_path=news_orders)
        #file_processor.delete_xlsx(files_path=extractor_file_path)
        print(Fore.GREEN + 'AUTOMAÇÃO CONCLUÍDA!' + Fore.RESET)
        # printa o horario da ultima execução
        print(Fore.GREEN + 'ÚLTIMA EXECUÇÃO: ' + Fore.RESET + str(datetime.now()))

        # Agendar a execução da função delete_xlsx para cada 10 minutos
        # Agendar a execução da função delete_xlsx para cada 10 minutos
        schedule_function = functools.partial(file_processor.move_file_to_client_folder, 
                                      source_directory=extractor_file_path,
                                      target_directory=processed_extrator_path)

        schedule.every(1).hours.do(schedule_function)
                                              
        schedule.run_pending()
        sleep(0.5)

        #print(Fore.LIGHTYELLOW_EX + 'DELETANDO ARQUIVOS XLSX' + Fore.RESET)

        
