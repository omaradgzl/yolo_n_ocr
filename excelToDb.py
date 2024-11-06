DB_USERNAME = "***"
DB_PASSWORD = "***"
DB_HOST = "***"
DB_PORT = "***"
DB_NAME = "***"

from dbConn import DbConnection
import pandas as pd



def main(file_kafk,file_serial, file_dup):
    
    conn = DbConnection(username= DB_USERNAME, password= DB_PASSWORD, host = DB_HOST, port= DB_PORT, name = DB_NAME)
    kafk = pd.read_excel(file_kafk)
    serial = pd.read_excel(file_serial)
    dup = pd.read_excel(file_dup)
    
    kafk = kafk.rename(columns={'****' : '****', 
                     '****' : '****',
                     '****' : '****',
                     '****' : '****', 
                     '****': '****', 
                     '****' : '****',
                      '****' : '****'})   
    
    serial = serial.rename(columns={'****' : '****', 
                     '****' : '****',
                     '****' : '****',
                     '****' : '****', 
                     '****': '****', 
                     '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****', 
                      '****' : '****', 
                      '****' : '****'}) 
    
    dup = dup.rename(columns={'****' : '****', 
                     '****': '****', 
                     '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****',
                      '****' : '****', 
                      '****' : '****'}) 
    

    try:
        conn.dataToDB(kafk, 'KAFK_P1', if_there = 'append', index = False)
        conn.dataToDB(serial, 'KAFK_P2', if_there = 'append', index = False)
        conn.dataToDB(dup, 'KAFK_DUP', if_there = 'append', index = False)
    except Exception as e:
        return str(e)
    return 'DB uptaded,for all three.'
