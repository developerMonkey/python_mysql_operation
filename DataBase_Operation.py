import pymysql.cursors
import requests
import time
#
#CREATE TABLE radar_message(m_id int NOT NULL AUTO_INCREMENT,body_save varchar(255) ,sleep_state varchar(255) ,body_distance varchar(255),breathe_state varchar(255) ,breathe_value int,heartrate_num int,body_state varchar(255) ,save_time datetime,PRIMARY KEY (m_id))AUTO_INCREMENT=1;
# #alter table radar_message add primary key(m_id);#
#CREATE TABLE Temperature_message(t_id int NOT NULL AUTO_INCREMENT,Terget_temperature varchar(255) ,evrenament_temperature varchar(255) ,Body_temperature varchar(255),distance varchar(255) ,save_time datetime,PRIMARY KEY (t_id))AUTO_INCREMENT=1;
#CREATE TABLE Heartrate_message(h_id int NOT NULL AUTO_INCREMENT,Heartrate varchar(255) ,Oximetry varchar(255) ,Microcirculation varchar(255),Sbp varchar(255),Dbp varchar(255),RR varchar(255),SDANN varchar(255),save_time datetime,PRIMARY KEY (h_id))AUTO_INCREMENT=1;
class DataBase_Operation:
    def Connect_MySQL(self,db_password):
        connection = pymysql.connect(host='localhost',
                                     port=3306,
                                     user='root',
                                     password=db_password,
                                     database='Health_data',
                                     cursorclass=pymysql.cursors.DictCursor)
        return connection
    def add_data(self,db,table_name,data_dic):
        t=time.gmtime()
        save_time=time.strftime('%Y-%m-%d %H:%M:%S',t)
        data_dic['save_time']=save_time
        dic_keys=list(data_dic.keys())
        dic_values=list(data_dic.values())
        data_str=""
        for i in range(0,len(dic_keys)):
            if i==0:
                data_str=data_str+"\'%s\'"%dic_values[i]
            else:
                data_str=data_str+",\'%s\'"%dic_values[i]


        sql="insert into %s (%s)values(%s);"%(table_name,','.join(dic_keys),data_str)#
        print(sql)
        cursor=db.cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
            db.commit()
            #print('未创建数据库前：',cursor.fetchall()) #获取创建数据库前全部数据库
        except Exception as e:
            print(e)
            db.rollback()  #回滚事务

        finally:
            cursor.close()
            db.close()  #关闭数据库连接

    def delete_data(self,db,table_name,constraints_condition):
        cursor=db.cursor()
        sql="DELETE FROM %s WHERE %s"%(table_name,constraints_condition)
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
            db.commit()
            print('未创建数据库前：',cursor.fetchall()) #获取创建数据库前全部数据库
        except Exception as e:
            print(e)
            db.rollback()  #回滚事务

        finally:
            cursor.close()
            db.close()  #关闭数据库连接

    def change_data(self,db,table_name,data_dic,constraints_condition):
        dic_keys=list(data_dic.keys())
        dic_values=list(data_dic.values())
        print(type(dic_keys))
        print(dic_keys)
        data_str=""
        for i in range(0,len(dic_keys)):
            if i==0:
                data_str=data_str+dic_keys[i]+"=\'%s\'"%dic_values[i]
            else:
                data_str=data_str+","+dic_keys[i]+"=\'%s\'"%dic_values[i]

        print(data_str)
        sql="UPDATE %s SET %s WHERE %s;"%(table_name,data_str,constraints_condition)
        print(sql)
        cursor=db.cursor()
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
            db.commit()
            print('未创建数据库前：',cursor.fetchall()) #获取创建数据库前全部数据库
        except Exception as e:
            print(e)
            db.rollback()  #回滚事务

        finally:
            cursor.close()
            db.close()  #关闭数据库连接


    def Get_database_Massage(self,db,table_name,constraints_condition):
        cursor = db.cursor() #创建游标对象

        try:

            sql_string="SELECT * FROM %s %s;"%(table_name,constraints_condition)
            print(sql_string)
            cursor.execute(sql_string)
            one_data=cursor.fetchone()
            print("get one data",one_data)
            many_data=cursor.fetchmany(3)
            print("many data",many_data)
            all_data=cursor.fetchall()
            print("get all data",all_data)
        except Exception as e:
            print(e)
            db.rollback()  #回滚事务

        finally:
            cursor.close()
            db.close()  #关闭数据库连接


    # 关闭游标
    #cursor.close()
    # 关闭数据库连接，目的为了释放内存
    #connection.close()

if __name__=="__main__":
    db_obj=DataBase_Operation()
    con_obj=db_obj.Connect_MySQL('device_Password,123.')
    # radar_message
    insert_dic={'body_save':'无', 'body_distance':'0cm ','save_time':'2023-08-10 01:36:07'}
    data_keys=insert_dic.keys()
    data_values=insert_dic.values()
    print(data_keys)
    print(data_values)
    key_str=','.join(data_keys)
    values_str=','.join(data_values)
    print(key_str)
    print(values_str)
    db_obj.add_data(con_obj,'radar_message',insert_dic)
    con_obj=db_obj.Connect_MySQL('device_Password,123.')
    db_obj.change_data(con_obj,'radar_message',insert_dic,"save_time=\'2023-08-12 08:35:16\'")
    con_obj=db_obj.Connect_MySQL('device_Password,123.')
    db_obj.Get_database_Massage(con_obj,'radar_message',"WHERE save_time=\'2023-08-12 09:01:43\'")
    con_obj=db_obj.Connect_MySQL('device_Password,123.')
    db_obj.delete_data(con_obj,'radar_message',"save_time=\'2023-08-12 08:35:16\'")

