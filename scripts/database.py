from sqlalchemy import create_engine, Column, String, Integer, Date, inspect, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from colorama import Fore
from sqlalchemy import Index
from sqlalchemy.exc import IntegrityError


Base = declarative_base()

# classe para definir a tabela no banco de dados
class OrdersTable(Base):
    # nome da tabela no banco de dados
    __tablename__ = 'pedidosfaturados'
    
    # evita que dados duplicados sejam inseridos no banco de dados
    __table_args__ = (UniqueConstraint('pedido_faturamento', 'id_equipamento'),)

    # colunas da tabela
    id = Column(Integer, primary_key=True, autoincrement='auto')
    codigo_cliente = Column(String)
    loja_cliente = Column(String)
    nome_do_cliente = Column(String)
    cnpj_do_cliente = Column(String)
    cnpj_de_faturamento = Column(String)
    cnpj_de_remessa = Column(String)
    equipamento = Column(String, nullable=True)
    nota_de_remessa = Column(String)
    data_de_remessa = Column(Date)
    serie_da_nf_remessa = Column(String)
    produto = Column(String)
    descricao_do_produto = Column(String)
    quantidade = Column(Integer)
    pedido_de_remessa = Column(String)
    projeto = Column(String)
    obra = Column(String)
    prazo_do_contrato = Column(String)
    data_de_ativacao_legado = Column(Date)
    data_de_ativacao = Column(Date)
    ultimo_faturamento = Column(Date)
    #periodo_final = Column(Date)
    data_do_termo = Column(Date)
    aniversario = Column(Date)
    desc_ajuste = Column(String)
    indice_aplicado = Column(String)
    dias_de_locacao = Column(Integer)
    valor_de_origem = Column(String)
    valor_unitario = Column(String)
    valor_bruto = Column(String)
    tipo_do_mes = Column(String)
    #nr_chamado = Column(String)
    contrato_legado = Column(String)
    acrescimo = Column(String)
    franquia = Column(String)
    id_equipamento = Column(String)
    id_equip_substituido = Column(String)
    data_da_substituicao = Column(Date)
    data_proximo_faturamento = Column(Date)
    #data_inicio = Column(Date)
    data_fim_locacao = Column(Date)
    tipo_de_servico = Column(String)
    email = Column(String)
    calculo_reajuste = Column(String)
    nome_da_obra = Column(String)
    numero_da_as = Column(String)
    pedido_faturamento = Column(String)
    nf_de_faturamento = Column(String)
    serie_de_faturamento = Column(String)
    data_de_faturamento = Column(Date)
    qtde_faturamento = Column(Integer)
    vlr_unitario_faturamento = Column(String)
    vlr_total_faturamento = Column(String)
    periodo_de_faturamento = Column(String)
    status_de_cobranca = Column(String)

# classe para definir o nome da tabela no banco de dados    
class OrdersClass:
    def __init__(self, table_name):
        self.__table_name__ = table_name
        self.Table = OrdersTable


# classe para conexão com o banco de dados
class ConnectPostgresQL:
    # definindo construtor de classe com o parâmetros de conexão
    def __init__(self, host):
        self.engine = create_engine(host)
        
        self.Session = sessionmaker(bind=self.engine)
        

    # função para conexão com o banco de dados(PostgreSQL) self.engine.connect()
    def connect(self):
        return self.engine.connect()

    # função para criação do banco de dados e tabela
    def create_database(self):
        try:
            # inspeciona se a tabela existe no banco de dados
            inspector = inspect(self.engine)

            if not inspector.has_table(OrdersTable.__tablename__):
                Base.metadata.create_all(self.engine)
                print(f'Banco de Dados e Tabela {OrdersTable.__tablename__} criada com sucesso!')
            else:
                print(f'Dados já inseridos anteriormente na tabela {OrdersTable.__tablename__} !')
        except Exception as e:
            print(f'Erro ao criar banco de dados e tabel {OrdersTable.__tablename__}: {e}')


    # função para inserção de dados na tabela
    def insert_data(self, table_name, **kwargs):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                for key, value in kwargs.items():
                    if pd.isna(value) or isinstance(value, pd.Timestamp):
                        kwargs[key] = None
                    elif isinstance(value, str) and value == '-':
                        kwargs[key] = None
                    elif isinstance(value, str) and value == 'nan':
                        kwargs[key] = None
                    elif isinstance(value, str) and value == 'NaT':
                        kwargs[key] = None

                record = table(**kwargs)
                session.add(record)
                session.commit()
                print(Fore.GREEN + f'Dados {key} inseridos com sucesso!' + Fore.RESET)
            
        # caso o registro já exista no banco de dados, não insere novamente
        except IntegrityError as e:
            pass

        # caso ocorra algum erro, exibe o erro    
        except Exception as e:
            raise e

        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()

    
    # função para exclusão de dados da tabela
    def delete_data(self, table_name, id):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                session.query(table).filter(table.id == id).delete()
                session.commit()
                print(Fore.GREEN + f'Dados {id} excluídos com sucesso!' + Fore.RESET)

        # caso ocorra algum erro, exibe o erro
        except Exception as e:
            raise e

        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()
           

    # função para atualização de dados da tabela
    def update_data(self, table_name, id):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                session.query(table).filter(table.id == id).update()
                session.commit()
                print(Fore.GREEN + f'Dados {id} atualizados com sucesso!' + Fore.RESET)

        # caso ocorra algum erro, exibe o erro                
        except Exception as e:
            raise e
        
        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()


    # função para consulta de dados da tabela
    def query_data(self, table_name, query):
        try:
            table = OrdersClass(table_name).Table

            with self.Session() as session:
                result = session.query(table).filter(query).all()
                return result

        # caso ocorra algum erro, exibe o erro                
        except Exception as e:
            raise e
        
        # fecha a conexão com o banco de dados
        finally:
            if session:
                session.close()

    
    # função para criar novo usuário
    def create_user(self, username, password):
        try:
            with self.engine.connect() as conn:
                conn.execute(f"CREATE USER {username} WITH PASSWORD '{password}'")
                print(Fore.GREEN + f'Usuário {username} criado com sucesso!' + Fore.RESET)
        except Exception as e:
            raise e 
        
    # função para conceder privilégios ao usuário
    def grant_privileges(self, username, database):
        try:
            with self.engine.connect() as conn:
                conn.execute(f"GRANT ALL PRIVILEGES ON DATABASE {database} TO {username}")
                print(Fore.GREEN + f'Privilégios concedidos ao usuário {username} com sucesso!' + Fore.RESET)
        except Exception as e:
            raise e








