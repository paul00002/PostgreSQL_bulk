import psycopg2

class psycopg_connect:
    def __init__(self,host,database,user,password) -> None:
        self.host="host"
        self.database="database"
        self.user="user"
        self.password="password"
    
    def connect(self):
        self.connect= psycopg2.connect(host=self.host,
                                      database=self.database,
                                      user=self.user,
                                      password=self.password)
        self.cur = self.connect.cursor()
     
if __name__=="__main__":
    psg=psycopg_connect()
    psg.database="paul_test"
    psg.connect()
    SQL="select count(*) from  table_test;"
    psg.cur.execute(SQL)
    print(psg.cur.fetchone())