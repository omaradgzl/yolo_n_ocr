from sqlalchemy import create_engine 
import pandas as pd

       

class DbConnection:

    def __init__(self, username, password, host, port, name):
        self.conn = create_engine('oracle+cx_oracle://{user}:{passw}@{hostN}:{portN}/?service_name={nameN}'.format(user = username, passw = password, hostN = host ,portN = port ,nameN = name))
 
    def getDataFrame(self,query):
        return pd.read_sql(query,self.conn)

    def dataToDB(self, df,table, if_there, index ):
        df.to_sql(table, self.conn, if_exists = if_there , index = index)











