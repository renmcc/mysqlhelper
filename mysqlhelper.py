#!/usr/bin/env python

import pymysql
import threading
from prettytable import PrettyTable
import readline
import ConfigParser

class MySQLHelper:
    def __init__(self, host, user, password, charset="utf8"):
        self.host = host
        self.user = user
        self.password = password
        self.charset = charset
        try:
            self.conn = pymysql.connect(host=self.host, user=self.user, passwd=self.password, )
            self.conn.set_charset(self.charset)
            self.cursor = self.conn.cursor()
        except Exception as e:
            print('MySql Error : %d %s' % (e.args[0], e.args[1]))
    def grant(self, ip, db, sql):
        try:
            title = '\033[32m主机:%s\n数据库:%s\n\033[0m' %(ip, db,)
            self.cursor.execute(sql)           
            self.cursor.execute("flush privileges") 
            self.conn.commit() 
            print(title, '授权成功')             
        except Exception as e:
            self.conn.rollback()
            print(title, e)
        finally:
            self.cursor.close()
            self.conn.close()
    def select(self, ip, db, sql):
        try:
            title = '\033[32m主机:%s\n数据库:%s\n\033[0m' %(ip, db,)
            self.conn.select_db(db)
            self.cursor.execute(sql)           
            result = self.cursor.fetchall()   
            field = [x[0] for x in self.cursor.description]
            table = self.tables(field, result)
            print(title, table)
        except IndexError:
            print(title, '空表')
        except Exception as e:
            print(title, e)
        finally:
            self.cursor.close()
            self.conn.close()               
    def exesql(self, ip, db, sql):
        try:
            title = '\033[32m主机:%s\n数据库:%s\n\033[0m' %(ip, db,)
            self.conn.select_db(db)
            self.cursor.execute(sql)           
            self.conn.commit()               
            print(title, '操作成功')
        except Exception as e:
            self.conn.rollback()
            print(title, e)
        finally:
            self.cursor.close()
            self.conn.close()
    def tables(self, field, data,  ):
        table = PrettyTable(field)
        table.padding_width = 3
        for i in data:
            table.add_row(i)
        return table

class My_Thread(threading.Thread):
    def __init__(self, host, user, passwd, db, action, args):
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__db = db
        self.__action = action
        self.__args = args
        super(My_Thread,self).__init__()
    def run(self):
        semaphore.acquire()
        db = MySQLHelper(self.__host,self.__user, self.__passwd)
        func = getattr(db, self.__action)
        func(self.__host, self.__db, self.__args)
        semaphore.release()   

def Ip_list():
    with open('list', 'r') as f:
        iplist = [x.strip('\n') for x in f.readlines() if x.strip()]
    return iplist

def db_conf():
    cf = ConfigParser.ConfigParser()
    cf.read('./db.conf')
    conf = cf.items("db")
    return dict(conf)

def run_threads(ip_func, db, sql, user, passwd, act):
    threads = []
    try:
        for ip in ip_func():
            thread  = My_Thread(ip, user, passwd, db, act, sql)
            threads.append(thread)   
        for i in threads:
            i.start()                           
        for i in threads:
            i.join()
    except Exception as e:
        return e

if __name__ == "__main__" :
    semaphore = threading.Semaphore(10)
    dbconf = db_conf()
    db = dbconf['defaultdb']
    user = dbconf['db_user']
    password = dbconf['db_pass']
    port = dbconf['db_port']
    while True:
        print("1、选择数据库.2、授权 3、查询 4、增删改(按q为退出)")
        number=input()
        if(number=='q'):
            break
        elif(number=='1'):
            db=input("请输入操作的数据库: ")
        elif(number=='2'):
            sql=input("请输入SQL语句(b退到上层)：")
            if(sql=='b'):
                continue
            else:
                run_threads(Ip_list, db, sql, user, password, 'grant')
        elif(number=='3'):
            sql=input("请输入SQL语句(b退到上层)：")
            if(sql=='b'):
                continue
            else:
                run_threads(Ip_list, db, sql, user, password, 'select')   
        elif(number=='4'):
            sql=input("请输入SQL语句(b退到上层)：")
            if(sql=='b'):
                continue
            else:
                run_threads(Ip_list, db, sql, user, password, 'exesql')
        else:
            print("非法输入，将结束操作!")
            break
