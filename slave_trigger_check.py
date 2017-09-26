from datetime import datetime
import MySQLdb
import MySQLdb.cursors

#MySQL Credentials
mysql_host = 'localhost'
mysql_user = 'root'
mysql_passwd = 'password'
mysql_socket = '/var/lib/mysql/mysql.sock'

db_con = MySQLdb.connect(host=mysql_host,user= mysql_user,passwd=mysql_passwd,unix_socket=mysql_socket,cursorclass=MySQLdb.cursors.DictCursor)
cur = db_con.cursor()
cur.execute("show all slaves status")
#cur.execute("show slave status")
slave_status = cur.fetchall()
for slave_state in slave_status:
    if slave_state['Slave_SQL_Running'] == 'No' and slave_state['Last_SQL_Errno'] == 1442:

        trg_query = "select TRIGGER_SCHEMA,TRIGGER_NAME from information_schema.Triggers where ACTION_TIMING='AFTER';"
        cur.execute(trg_query)
        trg_result = cur.fetchall()
        if trg_result:
            for res in trg_result:
                print str(datetime.now())
                drop_trg = "drop trigger %s.%s"%(res['TRIGGER_SCHEMA'],res['TRIGGER_NAME'])
                print drop_trg  
                cur.execute(drop_trg)
            cur.execute("start all slaves;")
