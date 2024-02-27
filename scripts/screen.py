import streamlit as st 
import pandas as pd
from controllers import TesteStreamlit, FinalReport
from consolidador import MergeExcelReports
import time

st.set_page_config(
    page_title='Automatic Report Generator',
    initial_sidebar_state='expanded',
    layout='wide'
)
exit_path = r'C:\DataWare\data\consolidated_files\consolidated_validated\NOVOS_PEDIDOS'
host_postgres = 'postgresql://postgres:123456789@localhost:5432/postgres'
instance = TesteStreamlit(host_postgres)
final_report = FinalReport(host_postgres)
consolidador = MergeExcelReports()

st.title('Validador de arquivo extrator')

def load_data(file, sheet_name):
    try:
        # verifica se a sheet 2-Resultado existe no arquivo
        if sheet_name not in pd.ExcelFile(file).sheet_names:
            st.error('Planilha "2-Resultado" não existe no arquivo! O relatório não será gerado!')
        else:
            df = pd.read_excel(file, sheet_name=sheet_name, header=1)
            return df
    except Exception as e:
        return None


# verifica se a coluna pedido_faturamento não está vazia
def check_pedido_faturamento(file):
    try:
        if file['Pedido Faturamento'].isnull().sum() > 0:
            st.error('Coluna Pedido Faturamento esta vazia! O relatório não será gerado!')
        else:
            st.success('Coluna Pedido Faturamento está preenchida!')
            st.success('Planilha 2-Resultado existe no arquivo!')
            st.success('Arquivo Ok. Relatório final pode ser gerado!')    
        
    except Exception as e:
       return None
  

def check_header(file):
    try:
        if file.columns[0] != 'Pedido Faturamento':
            st.error('Coluna Pedido Faturamento não existe na planilha! O relatório não será gerado!')
        else:
            st.success('Coluna Pedido Faturamento existe na planilha!')
            st.success('Arquivo Ok. Relatório final pode ser gerado!')
    except Exception as e:
        st.error('Erro ao verificar a coluna Pedido Faturamento!')
        return 


col1, col2 = st.columns(2)


upload_file = st.sidebar.file_uploader('Faça o upload aqui do arquivo', type='xlsx')

# quero ao clicar no botão consolidar na sidebar, aparecça na coluna 2 duas caixa file_uploader, uma para o diretorio de entrada e outro para o diretorio de saída
# e ao clicar no botão consolidar, o arquivo seja consolidado e movido para o diretorio de saída



if upload_file is None:
    st.info('Aguardando upload do arquivo...', icon="ℹ️")
    st.stop()    
else:
    col1.write('Dados do arquivo carregado:')
    file = load_data(upload_file, sheet_name='2-Resultado')
    st.dataframe(file, use_container_width=True, hide_index=True, width=1280)

# exibe mensagem na sidebar que a coluna pedido_faturamento esta ou não vazia usando a função check_pedido_faturamento
with st.sidebar:
    check_pedido_faturamento(file)
    load_data(file, sheet_name='2-Resultado')
    #button = st.sidebar.button('Gerar Relatório')

    # if button:
    #     progress_text = "Relatório sendo gerado...Aguarde um momento!"
    #     my_bar = st.progress(0, text=progress_text)

    #     def update_progress(progress):
    #         # Garante que o valor de progresso esteja dentro do intervalo válido de 0 a 1
    #         progress = min(1.0, max(0.0, progress))
    #         my_bar.progress(progress)

    #     instance.check_and_update_orders_streamlit(uploaded_file=upload_file, col='pedido_faturamento', progress_callback=update_progress)
        
    #     st.success('Relatório gerado com sucesso!')
    #     st.balloons()
    #     final_report.rename_format_columns(exit_path)
    #     st.stop()
        # final_report.move_files_to_month_subfolder_streamlit(uploaded_file=upload_file)
        # final_report.move_file_to_client_folder_streamlit(uploaded_file=upload_file)
        # final_report.delete_xml_streamlit(uploaded_file=upload_file)

    

