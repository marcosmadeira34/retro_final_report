from controllers import *




if __name__ == "__main__":
    extractor_file_path = r"C:\DataWare\data\consolidated_files\consolidated_validated\CONSOLIDADO_OUTUBRO_LAYOUT_SOLICITADO.xlsx"
    invoiced_orders = r"C:\DataWare\data\consolidated_files\consolidated_validated\PEDIDOS FATURADOS_EXAMPLE.xlsx"
    final_report = FinalReport()
    print(final_report.check_orders(extractor_file_path, invoiced_orders, 'PEDIDO'))
