# -*- coding:utf-8 -*-

import time
import xlrd
import pymysql
import re

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_gene = self.db.cursor()
        self.cursor_Synonyms = self.db.cursor()
        self.cursor = self.db.cursor()
        self.cursor_update = self.db.cursor()
        self.source_table = 'gene_symbol_correct'
    

    def get_gene(self):
        """
        获取gene
        """
        sql = 'select gene from {table} where is_true is null;'.format(table=self.source_table)
        try:
            self.cursor_gene.execute(sql)
            row = self.cursor_gene.fetchone()
            while row:
                # print(row)
                # 判断gene是否与info中的一致
                self.judge_gene(row[0])
                row = self.cursor_gene.fetchone()
                
        except Exception as e:
            print(e)

    def judge_gene(self, gene):
        """
        判断gene是否与info中的一致
        """
        # 通过Symbol查询_cs_disease_gene_info
        sql = 'select GeneID, Symbol, Synonyms from _cs_disease_gene_info where Symbol="{Symbol}";'.format(Symbol=gene)
        judge = self.cursor.execute(sql)

        # 更新gene数据，is_true为1，通过Symbol匹配到数据
        if judge:
            info = self.cursor.fetchone()
            
            self.update(info, gene, 'symbol')


        # 查询gene能否通过Synonyms匹配到
        else:
            # print(gene)
            sql = 'select GeneID, Symbol, Synonyms from _cs_disease_gene_info;'
            
            # print(pattern)
            try:
                gene_list = []
                self.cursor_Synonyms.execute(sql)
                row = self.cursor_Synonyms.fetchone()
                while row:
                    synonyms = row[2].split('|')
                    if gene in synonyms:
                        # print(gene, row[2])
                        # print(row[1])
                        # print('--------')
                        gene_list.append(row)
                    
                    # 更新gene数据，is_true为0，通过Synonyms匹配到数据
                    # self.update(info, 0)

                    row = self.cursor_Synonyms.fetchone()
                
                if len(gene_list) == 1:
                    # 更新gene数据，is_true为0，通过Synonyms匹配到数据
                    self.update(gene_list[0], gene, 'synonyms') 

                elif len(gene_list) > 1:
                    print(len(gene_list), gene_list)

            except Exception as e:
                print(e)


    def update(self, info, gene, judge):
        """
        更新信息
        :params info: info列表
        :params gene: gene
        :params judge: 判断从哪里匹配
        """
        data = {
                'info_geneid': info[0],
                'info_symbol': info[1],
                'info_synonyms': info[2],
                'source': judge
            }
        # 表名
        table = 'gene_symbol_correct'
        # sql语句
        sql = 'update {table} set '.format(table=table)
        update = ','.join([" {key} = %s".format(key=key) for key in data])
        where = ' where gene = "{gene}";'.format(gene=gene)
        sql = sql + update + where
        # print(sql)
        try:
            # 执行sql语句
            if self.cursor_update.execute(sql, tuple(data.values())):
                self.db.commit()
            else:
                self.db.rollback()
        except Exception as e:
            print('update is_true 1 error', e.args)

    def __del__(self):
        self.db.close()


if __name__ == "__main__":
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()

    my = Mysql()
    my.get_gene()

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)