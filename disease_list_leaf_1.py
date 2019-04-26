# -*- coding:utf-8 -*-

import pymysql
import re

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_id = self.db.cursor()
        self.cursor = self.db.cursor()
        self.source_table = 'CTD_diseases_v2'
    

    def get_id(self):
        sql = 'select DiseaseName, TreeNumbers, ParentTreeNumbers from {table};'.format(table=self.source_table)
        try:
            self.cursor_id.execute(sql)
            row = self.cursor_id.fetchone()
            count = 13854
            while row:
                # print(row)
                # self.get_name(row)
                
                if re.match(r'C\d\d(?!\.|\/)', row[1], re.I):
                    # self.save_leaf(count, row[0])
                    self.save_list( row[0])
                    count += 1
                    # print(row[0], row[1])
                row = self.cursor_id.fetchone()
                
        except Exception as e:
            print(e)


    def save_leaf(self, count, leaf_name):
        """
        保存树名字到_cs_disease_dict
        """
        
        cursor = self.db.cursor()
        data = {
            'id': count,
            'name': leaf_name
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s']*len(data))
        sql = 'insert into _cs_disease_dict({keys}) values({values}) ;'.format(keys=keys, values=values)
        try:
            if cursor.execute(sql, tuple(data.values())):
                self.db.commit()
            else:
                self.db.rollback()
        except Exception as e:
            print(e.args)


    def save_list(self, name):
        print(name)
        sql = 'select id from _cs_disease_dict where name="{name}";'.format(name=name)
        judge = self.cursor.execute(sql)
        # print(judge)
        if judge:
            # print(ParentIDs_row)
            dis_id = self.cursor.fetchone()[0]
            print(dis_id)
            cursor = self.db.cursor()
            data = {
                'dis_id': dis_id,
                'leaf': 1
            }
            keys = ', '.join(data.keys())
            values = ', '.join(['%s']*len(data))
            sql = 'insert into _cs_disease_list({keys}) values({values}) ;'.format(keys=keys, values=values)
            try:
                if cursor.execute(sql, tuple(data.values())):
                    self.db.commit()
                else:
                    self.db.rollback()
            except Exception as e:
                print(e.args)

        # if len(id_list)>0:
        #     parent_id = ','.join(id_list)
        #     self.save_list(dis_id, parent_id)
        # 获取疾病名称



    def __del__(self):
        self.cursor.close()
        self.cursor_id.close()
        self.db.close()

if __name__ == "__main__":
    my = Mysql()
    my.get_id()