from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

# Global Variable
SQLITE = 'sqlite'
# Table Names
USERS = 'users'
ADDRESSES = 'addresses'
class DbConnection:

    # Url for creating engine
    DB_ENGINE = {
        SQLITE: 'sqlite:///{DB}'
    }

    db_engine = None
    def __init__(self, dbtype, dbname=""):
        dbtype = dbtype.lower()
        if dbtype in self.DB_ENGINE.keys():
            engine_url = self.DB_ENGINE[dbtype].format(DB=dbname)
            self.db_engine = create_engine(engine_url)
            print("The Name of that Engine is ", self.db_engine)

        else:
            print("DBtype not found in DBEngine")

    def create_table(self):
        metadata = MetaData()
        users = Table(USERS, metadata,
                      Column('id', Integer, primary_key=True),
                      Column('first_name', String),
                      Column('last_name', String))
        addresses = Table(ADDRESSES, metadata,
                          Column('id', Integer, primary_key=True),
                          Column('user_id', Integer, ForeignKey('users.id')),
                          Column('email', String),
                          Column('address', String))
        try:
            metadata.create_all(self.db_engine)
            print("Tables Created")
        except Exception as e:
            print("Error Occurred during Table Creation")
            print(e)

    def execute_query(self, query):
        if query != "":
            with self.db_engine.connect() as connection:
                try:
                    connection.execute(query)
                except Exception as e:
                    print("Error: ", e)
        else:
            print("The Query is ", query)

    def insert_data(self):
        query = f"insert into {USERS} (id,first_name,last_name) values(104,'subash','kumar')"
        self.execute_query(query)

    def delete_query(self):
        query = f"Delete From {USERS} where id=101"
        self.execute_query(query)

    def update_query(self):
        query = f"update {USERS} set last_name='Ramalingam' where id=104"
        self.execute_query(query)

db_object = DbConnection(SQLITE, dbname="vickysqlalchemy.sqlite")
# db_object.create_table()
# db_object.insert_data()
# db_object.delete_query()
#db_object.update_query()