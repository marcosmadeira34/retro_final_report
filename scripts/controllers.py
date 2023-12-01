import pandas as pd
import time

class FinalReport:

   
    def check_orders(self, extractor_file_path, invoiced_orders, col):
        
        """ Método para verificar se os pedidos do extrator TOTVS 
           já foram emitidos relatorio final """
        
        print('Verificando pedidos não emitidos...')
        # Inicia o contador de tempo de execução do método
        start = time.time()
        # Carrega o arquivo e verifica extrator TOTVS com os pedidos
        extract_df = pd.read_excel(extractor_file_path, sheet_name='CONSOLIDADO')

        # Carrega o arquivo com os pedidos faturados
        invoiced_orders_df = pd.read_excel(invoiced_orders, sheet_name='CONSOLIDADO')

        # Use o método merge para combinar os DataFrames com base na coluna 'PEDIDO'
        merged_df = pd.merge(extract_df[[col]], invoiced_orders_df[[col]], on=col, how='outer', indicator=True)

        # Filtra os pedidos não encontrados
        not_found_df = merged_df[merged_df['_merge'] == 'left_only']

        # Dropa a coluna de indicação (_merge) e transforma a Series em uma lista
        not_found = not_found_df.drop(columns=['_merge'])[col].tolist()

        # cria um dataframe com os pedidos não encontrados com base nos dados do extrator TOTVS
         # e salva em um arquivo excel
        not_found_df = extract_df[extract_df[col].isin(not_found)]
        not_found_df.to_excel('Pedidos_para_emitir_relatório.xlsx', index=False)       

        # Imprime o tempo de execução do método
        end = time.time()
        print(f'Tempo de execução: {end - start}')
        print('Pedidos com relatórios não emitidos salvos em Pedidos_para_emitir_relatório.xlsx')
        return not_found_df
    

    """ Método para realizar a emissão do relatório final individualmente """
    
    
    
    

    


        
    

    