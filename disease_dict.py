# -*- coding:utf-8 -*-

import pymysql
import re

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_id = self.db.cursor()
        self.cursor = self.db.cursor()
    

    def disease_all(self):
        sql = 'select name, acronym from disease_all_v2;'
        try:
            self.cursor_id.execute(sql)
            row = self.cursor_id.fetchone()
            count = 1
            while row:
                # print(row)
                # print(count)
                self.save_dict(row[0], row[1], count)
                
                # if re.match(r'C\d\d(?!\.|\/)', row[1], re.I):
                    # self.save_leaf(count, row[0])
                    # self.save_list( row[0])
                count += 1
                
                    # print(row[0], row[1])
                row = self.cursor_id.fetchone()
                
        except Exception as e:
            print(e)


    def save_dict(self, name, alias, id):
        """
        保存树名字到_cs_disease_dict
        """
        # print(name, alias, id)
        cursor = self.db.cursor()
        data = {
            'id': id,
            'name': name,
            'alias': alias
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s']*len(data))
        sql = 'insert into _cs_disease_dict_v2({keys}) values({values}) ;'.format(keys=keys, values=values)
        try:
            if cursor.execute(sql, tuple(data.values())):
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
    my.disease_all()