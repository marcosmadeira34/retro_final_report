from sqlalchemy import *

from sqlalchemy import create_engine, text, insert

# Connect to the database
DATABASE_URL = 'postgresql://postgres:123456789@localhost:5432/postgres'
engine = create_engine(DATABASE_URL, echo=True)

# Create a metadata object
metadata = MetaData()

# Define a table for the DataWarehouse
table = Table('pedidosfaturados', metadata, autoload_with=engine)

# Define a table for the DataMart
datamart = Table('protege_datamart', metadata, autoload_with=engine)

# Create a schema for the DataMart in the database
metadata.create_all(engine)

# Run a query ETL to populate the DataMart
with engine.connect() as conn:
    try:
        # Extrair dados relevantes do DataWarehouse usando SQL puro
        sql_query = text("SELECT codigo_cliente FROM pedidosfaturados WHERE codigo_cliente = '633'")
        
        # Executar a consulta
        result = conn.execute(sql_query)

        # Inserir em massa na tabela protege_datamart usando SQLAlchemy
        values = [{"codigo_cliente": row.codigo_cliente} for row in result]
        conn.execute(datamart.insert().values(values))
        conn.commit()
    except Exception as e:
        print(f'Erro durante a execução da query: {e}')
        conn.rollback()
