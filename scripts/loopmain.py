from controllers import *
from consolidador import MergeExcelReports
from database import *
from colorama import Fore
from termcolor import cprint
import art
from time import sleep

ascii_banner = art.text2art("Relatorio Final")
colored_banner = cprint(ascii_banner, 'green')



# DIRETÓRIO DE ENTRADA DOS ARQUIVOS EXTRATORES (EXTRACTION)
extractor_file_path = r"/home/administrator/WindowsShare/01 - FATURAMENTO/01 - CLIENTES - CONTROLE - 2024 TOTVS/01-EXTRATOR_PEDIDOS_DE_CLIENTES" # EXTRATOR

# DIRETÓRIOS DE SAÍDA DOS ARQUIVOS CRIADOS (LOADING)
batch_totvs_path = r'/home/administrator/WindowsShare/01 - FATURAMENTO/01 - CLIENTES - CONTROLE - 2024 TOTVS' # CRIARÁ AS PASTA AQUI

# DIRETÓRIO DE TRATAMENTO DOS ARQUIVOS (TRANSFORMATION)
news_orders = r'/home/administrator/WindowsShare/01 - FATURAMENTO/03 - DATA_RAW' # NOVOS PEDIDOS IDENTIFICADOS NO EXTRATOR
source_directory = r'/home/administrator/WindowsShare/01 - FATURAMENTO/03 - DATA_RAW' # DIRETÓRIO DE ORIGEM DOS PEDIDOS
target_directory = r'/home/administrator/WindowsShare/01 - FATURAMENTO/01 - CLIENTES - CONTROLE - 2024 TOTVS' # DIRETÓRIO DE DESTINO DOS PEDIDOS

# DIRETÓRIO DE ARQUIVOS PROCESSADOS (DRAFT)
process_files = r'/home/administrator/WindowsShare/01 - FATURAMENTO/04 - EXTRATORES PROCESSADOS'

# DIRETÓRIOS AUXILIARES (SANDBOX)
output_merge_path = r'C:/DataWare/data/consolidated_files/consolidated_validated/MERGE_RELATÓRIO_FINAL' # RELATÓRIO FINAL 
invoiced_orders = r'C:/DataWare/data/consolidated_files/consolidated_validated/PEDIDOS_FATURADOS' # PEDIDOS FATURADOS NO BANCO DE DADOS


file_processor = FileProcessor(extractor_file_path, invoiced_orders, news_orders, output_merge_path)
host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
sql = ConnectPostgresQL(host_postgres)
final_report = FinalReport(host_postgres)
merge_reports = MergeExcelReports()



if __name__ == "__main__":
    
    # LOOP PRINCIPAL
    while True:        
        sleep(0.5)
        print(Fore.YELLOW + 'CHECANDO NOVOS PEDIDOS ...' + Fore.RESET)
        final_report.check_and_update_orders(extractor_file_path, 'pedido_faturamento')
        sleep(0.5)
        print(Fore.YELLOW + 'FORMANTO ARQUIVOS....' + Fore.RESET)
        final_report.rename_format_columns(news_orders)
        sleep(0.5)
        print(Fore.YELLOW + 'MOVENDO ARQUIVOS PARA DIRETÓRIO....' + Fore.RESET)
        file_processor.move_files_to_month_subfolder(
            directory_origin=news_orders, target_directory=target_directory)
        sleep(0.5)
        
        # ETAPA DE CONSOLIDAÇÃO DOS ARQUIVOS
                
        # # variável para armazenar a data atual
        current_date = datetime.now()
        # formata a data atual para o formato mm-aaaa
        month_year = current_date.strftime('%m-%Y')
        # Obtém a lista de subpastas criadas no diretório de destino
        subfolders = [folder for folder in os.listdir(target_directory) if os.path.isdir(os.path.join(target_directory, folder))]

        # Itera sobre cada subpasta
        for subfolder in subfolders:
            # Caminho para a pasta do cliente
            client_folder = os.path.join(target_directory, subfolder, month_year)

            if not os.path.exists(client_folder):
                os.makedirs(client_folder)
                print(f'Criando pasta para o cliente {subfolder} em {client_folder} ...')

            if client_folder.endswith('01-EXTRATOR_PEDIDOS_DE_CLIENTES'):
                print(Fore.RED + 'Pasta de origem encontrada!' + Fore.RESET)
                continue    

            # Chama a função para mesclar os relatórios Excel na pasta do cliente
            print(Fore.YELLOW + f'CONSOLIDANDO ARQUIVOS EM {client_folder} ...' + Fore.RESET)
            # verifica se algum arquivo no diretório inicia com "CONSOLIDADO"
            if any(file.startswith('CONSOLIDADO') for file in os.listdir(client_folder)):
                print(Fore.RED + 'Arquivo consolidado já existe!' + Fore.RESET)
                continue


            merge_reports.merge_excel_reports(client_folder, client_folder)       

            print(Fore.YELLOW + f'ENVIANDO ARQUIVO PARA PASTA DE PROCESSADOS EM {extractor_file_path} ...' + Fore.RESET)
            file_processor.move_files_to_processed_folder(
                            directory_origin=extractor_file_path,
                            target_directory=process_files)
            
        # #file_processor.delete_xlsx(extractor_file_path)       
        print(Fore.LIGHTBLUE_EX + 'AUTOMAÇÃO CONCLUÍDA : ' + Fore.RESET + str(datetime.now().strftime('%d-%m-%Y_%H-%M-%S\n')))


              

        

        





