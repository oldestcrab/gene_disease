# -*- coding:utf-8 -*-

import pymysql
import re

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_root = self.db.cursor()
        self.cursor_child = self.db.cursor()
        self.cursor = self.db.cursor()
    

    def get_root(self):
        # 循环遍历所有疾病
        for dis_id in range(1, 17000):
        
            # 判断是否有对应基因
            judge_gene = self.judge_gene(dis_id)
            # print('judge_gene', judge_gene)
            # 如果没有对应基因，看是否有对应子类
            if not judge_gene:
                judge_child = self.judge_child(dis_id)
                # print('judge_child', judge_child)
                if not judge_child:
                    # print(dis_id)
                    sql = 'delete from _cs_disease_list where  dis_id={dis_id} ;'.format(dis_id=dis_id)
                    # sql = 'select count(*) from _cs_disease_list where  dis_id={dis_id} ;'.format(dis_id=dis_id)
                    # print(sql)
                    try:
                        self.cursor_root.execute(sql)
                        # row = self.cursor_root.fetchone()
                        # print(row)
                    except Exception as e:
                        print('get_root error', e)

    def get_child(self, row):
        sql_child = 'select dis_id from _cs_disease_list where parent_id={parent_id} ;'.format(parent_id=row[0])
        print(sql_child)
        try:
            self.cursor_child.execute(sql_child)
            row = self.cursor_child.fetchone()
            while row:
                print(row)

                row = self.cursor_child.fetchone()
                
        except Exception as e:
            print('get_child error', e)

    def judge_gene(self, dis_id):
        sql = 'select count(*) from _cs_disease_map where  dis_id={dis_id} ;'.format(dis_id=dis_id)
        # print(sql)
        try:
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
            # print(row)
            if row[0]:
                return True
            else:
                # print(sql)
                return False
        except Exception as e:
            print('judge_root error', e)

    def judge_child(self, parent_id):
        sql = 'select count(*) from _cs_disease_list where parent_id={parent_id} ;'.format(parent_id=parent_id)
        # print(sql)
        try:
            self.cursor_child.execute(sql)
            row = self.cursor_child.fetchone()
            if row[0]:
                return True
            else:
                # print(sql)
                return False
        except Exception as e:
            print('judge_child error', e)

    def __del__(self):
        self.cursor.close()
        self.cursor_root.close()
        self.db.close()

if __name__ == "__main__":
    my = Mysql()
    my.get_root()