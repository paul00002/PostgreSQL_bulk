from PostgresSQL_Connect_Class import psycopg_connect
from Azure_datalake_Conect_Class import datalake_Download
import time 
import json 

if __name__=="__main__":
        
        with open('parameters.json') as f :
             parameters = json.load(f)
        
        #--------------------DATALAKE CONCEPT----------------
        print("Start: {}".format(time.ctime(time.time())))
        
        dw=datalake_Download()
        #-------------------Set Credential
        dw.adlsTenantId=parameters["adlsTenantId"]
        dw.adlsClientId=parameters["adlsClientId"]
        dw.adlsClientSecret=parameters["adlsClientSecret"]
        adlsURL_Connectin=parameters["adlsURL_Connectin"]
        dw.adlsFileSystemName=parameters["adlsFileSystemName"]
        dw.adlsPath=parameters["adlsPath"]   
        #------------------name file
        file_name="fileprova.csv"
        dw.connection()
#
        #--------------------DATALAKE Download----------------
        dw.Download("new_file.csv")

        #--------------------DATABASE CONCEPT----------------
        print("DATABASE CONCEPT")
        psg=psycopg_connect()

        #-------------------Set Credential
        psg.host=parameters["Host"]
        psg.database=parameters["Database"]
        psg.user=parameters["Username"]
        psg.password=parameters["Password"]
        
        psg.connect()
        ##--------------------test connection
        SQL="select count(*) from  table_test;"
        psg.cur.execute(SQL)
        print("Count Start Row {}".format(psg.cur.fetchone()))
#
        #--------------------COPY TO POSTGRES----------------
        with open("new_file.csv") as f:
             psg.cur.copy_from(f, "table_test", sep=";")
             f.close()
#
        #--------------------COMMIT TO POSTGRES----------------
        psg.connect.commit()
#
        #------------- last test 
        psg.cur.execute(SQL)
        print("Count End Row {}".format(psg.cur.fetchone()))
        print("End: {}".format(time.ctime(time.time())))
        psg.Close()
        print("Complit")
