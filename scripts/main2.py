from check_orders import *
from database import *


sql = ConnectPostgresQL(f'postgresql://postgres:123456789@localhost:5432/postgres')
sql.create_database()

app = CheckOrders()
app.load_extract(r'H:\99 - MELHORIAS PYTHON\AUTOMAÇÃO RELATÓRIO FATURAMENTO\extrator.xlsx')
app.load_table('pedidosfaturado', sql)

