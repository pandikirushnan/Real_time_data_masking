#!/usr/bin/python
#coding: utf-8
import MySQLdb
import socket
import os,sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

#config file location
config_file = '/tmp/anon_config'

#Path where update query and trigger files need to be created
file_path = '/tmp/anonymisation/'

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
update_file = open(file_path+'update_query.sql','w')
update_trigger = open(file_path+'update_trigger.sql','w')
insert_trigger = open(file_path+'insert_trigger.sql','w')
update_file.write('set foreign_key_checks=0; \n')
update_file.write('set sql_log_bin=0;')
insert_trigger.write('set sql_log_bin=0; \n')
insert_trigger.write('delimiter $')
update_trigger.write('set sql_log_bin=0; \n')
update_trigger.write('delimiter $')
addr_to=['your_email@domain.com']


#This function will create the dict based on the config file
def create_table_list(filename):
    global table_dict,phone,mail,update_file
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

#    print table_dict
#    print phone,mail
    
    
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
                trig += "NEW.%s= concat(65, FLOOR(RAND() * 78585850))" % (item)
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
                            END;$" % (item)
            elif item in mail.values()[0]:
                trig += "NEW.%s= concat(md5(NEW.%s),'@ma.sk') ; \
                END;$" % (item, item)
            else:
                trig += 'NEW.%s= md5(NEW.%s); \
                            END;$' % (item, item)
        else:
            if item in phone.values()[0]:
                trig += ",NEW.%s=concat(65, FLOOR(RAND() * 78585850)); \
                         END;$" % (item)
            elif item in mail.values()[0]:
                trig += ",NEW.%s= concat(md5(NEW.%s),'@ma.sk') ; \
                END;$" % (item, item)
            else:
                trig += ',NEW.%s= md5(NEW.%s); \
                END;$' % (item, item)

    insert_trg = ins_trig+trig
#    print insert_trg   
    update_trg =  upt_trig+trig	
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
                update_query += ',' +column[0]+'='+column[0]+';'
            else:
                update_query += ',' +column[0]+'='+column[0] 
            
    else:
        update_query = upt_query+trig.replace('NEW.','').split('END')[0]
    print update_query
    update_file.write('\n')
    update_file.write(update_query)
    update_trigger.write('\n')
    update_trigger.write(update_trg)
    insert_trigger.write('\n')
    insert_trigger.write(insert_trg)
    
    
if __name__ == "__main__":
    create_table_list(config_file)
    for key in table_dict.keys():
        db_name = key.split('.')[0]
        tbl_name = key.split('.')[1]
        create_trigger(db_name,tbl_name,table_dict[key])
    update_file.close()
    update_trigger.close()
    insert_trigger.close()
