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
                    # print(row[0])
                    # print(row[0], row[1])
                row = self.cursor_id.fetchone()
                
        except Exception as e:
            print(e)


    def save_list(self, name):
        print(name)
        sql = 'select id from _cs_disease_dict where name="{name}";'.format(name=name)
        judge = self.cursor.execute(sql)
        # print(judge)
        if judge:
            # print(ParentIDs_row)
            dis_id = self.cursor.fetchone()[0]
            print(dis_id)

            sql = 'update _cs_disease_list set leaf = 1 where dis_id={dis_id} ;'.format(dis_id=dis_id)
            print(sql)
            try:
                if self.cursor.execute(sql):
                    self.db.commit()
                else:
                    self.db.rollback()
            except Exception as e:
                print(e.args)


    def __del__(self):
        self.cursor.close()
        self.cursor_id.close()
        self.db.close()

if __name__ == "__main__":
    my = Mysql()
    my.get_id()