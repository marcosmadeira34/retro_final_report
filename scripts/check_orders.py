import pandas as pd



class CheckOrders:

    # loading TOTVS extract file
    def load_extract(self, path):
        extract_df = pd.read_excel(path, sheet_name="2-Resultado", engine='openpyxl')
        return extract_df
    
    # loading PostgresQL table
    def load_table(self, table_name, sql):
        table = pd.read_sql_table(table_name, sql.engine)
        return table
    
    # checking if there are new orders
    def check_new_orders(self, extract_df, table_df):
        new_orders = extract_df[~extract_df['Pedido Faturamento'].isin(table_df['Pedido Faturamento'])]
        return new_orders
    
    