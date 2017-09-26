#!/usr/bin/python
#coding: utf-8
import MySQLdb
import socket
import os,sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

#Anon Config file path
config_file = '/tmp/anon_config'

#MySQL Credentials
mysql_host = 'localhost'
mysql_user = 'root'
mysql_passwd = 'password'
mysql_socket = '/var/lib/mysql/mysql.sock'


host=socket.gethostname()
table_dict = {}
phone = {'hash_phone': []}
mail = {'hash_email': []}

db_con = MySQLdb.connect(host=mysql_host,user= mysql_user,passwd=mysql_passwd,unix_socket=mysql_socket)
cur = db_con.cursor()


addr_to=['your@domain.com']




def mail_sending(subject,mail_html):

    addr_from='alerts@domain.com'
    smtp_server="localhost"
    msg=MIMEMultipart('alternative')
    msg['To']=", ".join(addr_to)
    msg['From']=addr_from
    msg['Subject'] = subject
    mssg = MIMEText(mail_html, 'html')
    msg.attach(mssg)
    s=smtplib.SMTP(smtp_server)
    s.sendmail(addr_from, addr_to, msg.as_string())
    s.quit()


def query_execute(query,commit):
    print query
    con=MySQLdb.connect(host=mysql_host,read_default_file=mysql_config_file,unix_socket=mysql_socket)
    curs = con.cursor()
    if commit ==1:
        
        curs.execute(query)
        con.commit()
        curs.close()	
        con.close()
    
    else :
        curs.execute(query)
	curs.close()
        con.close()
            
            

#This function will create the dictonary based on the config file
def create_table_list(filename):
    global table_dict,phone,mail
    with open(filename, 'r') as infile:
        data = infile.read()
    for line in data.splitlines():
        if len(line) == 0 or '#' in line:
            continue
        else:
            tbl_col_list = line.split()[1].split('.')[-3:]
            if tbl_col_list[0] + '.' + tbl_col_list[1] in table_dict:
                if tbl_col_list[2] not in table_dict[tbl_col_list[0] + '.' + tbl_col_list[1]]:
                    table_dict[tbl_col_list[0] + '.' + tbl_col_list[1]].append(tbl_col_list[2])

            else:

                table_dict[tbl_col_list[0] + '.' + tbl_col_list[1]] = [tbl_col_list[2]]

        if line.split()[0] in phone:
            phone['hash_phone'].append(line.split()[1].split(".")[2])
        if line.split()[0] in mail:
            mail['hash_email'].append(line.split()[1].split(".")[2])

#    print table_dict, phone, mail
    
    
    
#This function will create the update statement and creates insert and update triggers

def create_trigger(db_name,tbl_name,column_list):

    t_name = tbl_name
    l_name = column_list

    ins_trig = "CREATE TRIGGER %s.%s_before_ins BEFORE INSERT ON %s FOR EACH ROW BEGIN SET " % (db_name,t_name,t_name)
    upt_trig = "CREATE TRIGGER %s.%s_before_upt BEFORE UPDATE ON %s FOR EACH ROW BEGIN SET  "% (db_name,t_name,t_name)
    upt_query = "Update %s.%s set "%(db_name,t_name)
    trig = ''	
    html1 = "<p>Creating trigger for %s.%s</p>"%(db_name,t_name)
    for index, item in enumerate(l_name):

        if index == 0 and len(l_name) != 1:
            if item in phone.values()[0]:
                trig += "NEW.%s= concat(65, FLOOR(RAND() * 78585850))" % ( item)
            elif item in mail.values()[0]:
                trig += "NEW.%s= concat(md5(NEW.%s),'@ma.sk') " % (item, item)
            else:
                trig += 'NEW.%s= md5(NEW.%s)' % (item, item)
        elif index != len(l_name) - 1:
            if item in phone.values()[0]:

                trig += ",NEW.%s= concat(65, FLOOR(RAND() * 78585850))" % (item)
            elif item in mail.values()[0]:
                trig += ",NEW.%s= concat(md5(NEW.%s),'@ma.sk') " % (item, item)
            else:
                trig += ',NEW.%s= md5(NEW.%s)' % (item, item)
        elif len(l_name) == 1:
            if item in phone.values()[0]:
                trig += "NEW.%s= concat(65, FLOOR(RAND() * 78585850)); \
                            END;$" % (item, item)
            elif item in mail.values()[0]:
                trig += "NEW.%s= concat(md5(NEW.%s),'@ma.sk') ; \
                END;$" % (item, item)
            else:
                trig += 'NEW.%s= md5(NEW.%s); \
                            END;$' % (item, item)
        else:
            if item in phone.values()[0]:
                trig += ",NEW.%s = concat(65, FLOOR(RAND() * 78585850)); \
                         END;$" % (item, item)
            elif item in mail.values()[0]:
                trig += ",NEW.%s= concat(md5(NEW.%s),'@ma.sk') ; \
                END;$" % (item, item)
            else:
                trig += ',NEW.%s= md5(NEW.%s); \
                END;$' % (item, item)

    
    insert_trg = ins_trig+trig.replace('END;$','END;')
#    print insert_trg
    update_trg =  upt_trig+trig.replace('END;$','END;')
#    print update_trg 
    upt_check="select column_name from information_schema.COLUMNS where TABLE_SCHEMA='%s' and Table_name ='%s' and COLUMN_DEFAULT='CURRENT_TIMESTAMP' and EXTRA like 'on update CURRENT_TIMESTAMP';"%(db_name,t_name)
    cur.execute(upt_check)
    upt_res=cur.fetchall()
    if upt_res and len(upt_res)==1:
        update_query = upt_query+trig.replace('NEW.','').split('END')[0].replace(';',',')+upt_res[0][0]+'='+upt_res[0][0]+';'   
    elif  upt_res and len(upt_res)>1:
        update_query = upt_query+trig.replace('NEW.','').split('END')[0].replace(';','')
        for i,column in enumerate(upt_res):
            if i == len(upt_res)-1:
                update_query += ',' +upt_res[0][0]+'='+upt_res[0][0]+';'
            else:
                update_query += ',' +upt_res[0][0]+'='+upt_res[0][0] 
            
    else:
        update_query = upt_query+trig.replace('NEW.','').split('END')[0]
    html1 += "<p> Slave Stopped </p>"
    query_execute('stop all slaves',0)
    html1 += "<p> %s </p>"%(str(datetime.now()))
    html1 += "<p> %s </p>"%(update_query)
    query_execute(update_query,1)
    html1 += "<p> %s </p>"%(str(datetime.now()))
    html1 += "<p> %s </p>"%(insert_trg)
    insert_trigger = "set session sql_log_bin=0;" + insert_trg
    query_execute(insert_trigger,0)
    html1 += "<p> %s </p>"%(str(datetime.now()))
    html1 += "<p> %s </p>"%(update_trg)
    update_trigger = "set session sql_log_bin=0;" + update_trg
    query_execute(update_trigger,0)
    html1 += "<p> %s </p>"%(str(datetime.now()))
    query_execute('start all slaves',0)
    html1 += "<p> Slave Started </p>"
    print html1	
    mail_sending("Trigger Re-created on "+ host , html1)

if __name__ == "__main__":

# This section will check whether the triggers exists or not based on the config file. if triggers doesn't exists it will create it.

    create_table_list(config_file)
    for key in table_dict.keys():
        db_name = key.split('.')[0]
        tbl_name = key.split('.')[1]
        tbl_check = "select TABLE_NAME from information_schema.Tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s';"%(db_name,tbl_name)
        cur.execute(tbl_check)
        tbl_res = cur.fetchall()
        if tbl_res:
            trg_check = "select TRIGGER_NAME from information_schema.Triggers where TRIGGER_SCHEMA ='%s' and EVENT_OBJECT_TABLE ='%s';"%(db_name,tbl_name)
            cur.execute(trg_check)
            trg_res = cur.fetchall()
        else:
            html = "Table doesn't exit for creating triggers %s.%s"%(db_name,tbl_name)
            print html
            mail_sending("Trigger creation failed on "+ host , html)
            continue
        if len(trg_res) == 2:
            continue
        elif len(trg_res) == 1 or len(trg_res) >2:
            html = "Table doesn't have correct triggers. Pls check %s.%s"%(db_name,tbl_name)
            print html
            mail_sending("Trigger creation failed on "+ host , html)
            continue
            
        else:
            try:
                create_trigger(db_name,tbl_name,table_dict[key])
            except  Exception, err:
                print "Error in creating trigger for %s.%s"%(db_name,tbl_name)
                html = "<p>Error in creating trigger for %s.%s</p>"%(db_name,tbl_name)
                html += "%s %s" %(Exception,err)
                mail_sending("Trigger creation failed on "+ host , html)
                print Exception,err

