# -*- coding:utf-8 -*-

import time
import xlrd
import pymysql

def read_excel(infile):
    """
    读取excel文件
    :params infile: excel文件路径
    """
    # 获取文件
    excel_file = xlrd.open_workbook(infile)

    # 获取sheet内容
    sheet = excel_file.sheet_by_index(0)

    # 获取每一行
    for row in range(sheet.nrows):
    # print(len(gene_symbol))
        result = sheet.row_values(row)
        # print(result)
        # 获取ADE行
        list = [result[0], result[3], result[4]]
        
        if result[4]:
            save_mysql(list)

def save_mysql(list):
    """
    保存到mysql中
    :params list: 数据列表
    """
    # 链接数据库
    db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
    # 获取游标
    cursor = db.cursor()
    cursor_insert = db.cursor()
    
    # 判断基因是否为空
    if list[2]:
        # 查询对应疾病
        select_sql = 'select * from _cs_disease_dict where id in( SELECT dis_id FROM `_cs_disease_map` where gene_symbol="{}");'.format(list[2])
        cursor.execute(select_sql)
        row = cursor.fetchone()
        # if not row:
            # print(list[2])
        while row:
            if list[0]:
                list[0] = int(list[0])
            if list[1]:
                list[1] = int(list[1])
            data = {
                'a':list[0],
                'd':list[1],
                'gene_symbol':list[2],
                'disease_name':row[1],
                'disease_id':row[0],
                'disease_zh_cn':row[2],
            }

            keys = ', '.join(data.keys())
            values = ', '.join(['%s']*len(data))
            # 表名
            table = 'array_list_new'
            # sql语句
            sql = 'insert into {table}({keys}) values({values});'.format(table=table, keys=keys, values=values)
            try:
                # 执行sql语句
                if cursor_insert.execute(sql, tuple(data.values())):
                    db.commit()
                else:
                    db.rollback()
            except Exception as e:
                print(e.args)


            row = cursor.fetchone()


if __name__ == "__main__":
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()

    # 读取数据
    infile = r'C:/Users/CRAB/Desktop/gene_disease/数据校对 5.9 array_list_new.xlsx'
    # infile = r'C:/Users/CRAB/Desktop/gene_disease/gene_symbol.xlsx'
    read_excel(infile)

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)