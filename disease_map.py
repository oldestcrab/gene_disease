# -*- coding:utf-8 -*-

import pymysql

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_id = self.db.cursor()
        self.cursor = self.db.cursor()
        self.source_table = 'kegg'
    

    def get_id(self):
        sql = 'select disease_id, gene_id from gd_{table};'.format(table=self.source_table)
        try:
            self.cursor_id.execute(sql)
            row = self.cursor_id.fetchone()
            while row:
                # print(row)
                self.get_name(row)
                row = self.cursor_id.fetchone()
                
                
        except Exception as e:
            print(e)

    def get_name(self, row):
        # 获取疾病名称
        disease_name_sql = 'select name from disease_{table} where id="{id}";'.format(table=self.source_table, id=row[0])
        self.cursor.execute(disease_name_sql)
        disease_name_one = self.cursor.fetchone()
        if disease_name_one:
            disease_name = disease_name_one[0]
        else:
            disease_name = ''
        

        # 获取基因名称
        gene_name_sql = 'select name from gene_primary_{table} where id="{id}";'.format(table=self.source_table, id=row[1])
        self.cursor.execute(gene_name_sql)
        gene_name_one = self.cursor.fetchone()
        if gene_name_one:
            gene_name = gene_name_one[0]
        else:
            gene_name = ''

        # 获取疾病ID
        disease_id_sql = 'select id from _cs_disease_dict where name="{name}";'.format(name=disease_name)
        self.cursor.execute(disease_id_sql)
        disease_id_one = self.cursor.fetchone()
        # print(disease_id_one)
        if disease_id_one:
            disease_id = disease_id_one[0]
        else:
            disease_id = ''
        
        # 获取基因ID
        gene_id_sql = 'select id from gene_all where name="{name}";'.format(name=gene_name)
        self.cursor.execute(gene_id_sql)
        gene_id_one = self.cursor.fetchone()
        # print(gene_id_one)
        if gene_id_one:
            gene_id = gene_id_one[0]
        else:
            gene_id = ''
        if gene_name and gene_id and disease_name and disease_id:
            # pass
            self.save_map(disease_id, gene_name, gene_id, 'kegg')
        else:
            pass
            # print(disease_id, gene_name, gene_id, 'disgenet')
        # print('----------')

    def save_map(self, dis_id, gene_symbol, gene_id, source):
        cursor = self.db.cursor()
        data = {
            'dis_id':dis_id,
            'gene_symbol':gene_symbol,
            'gene_id':gene_id,
            'source':source
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s']*len(data))
        sql = 'insert into _cs_disease_map({keys}) values({values}) ON DUPLICATE KEY UPDATE'.format(keys=keys, values=values)
        update = ' source = concat(source, ",{source}");'.format(source=source)
        sql += update
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
    my.get_id()