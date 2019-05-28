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
    # 获取sheet名
    # print(excel_file.sheet_names())
    # ['array_list_new', 'Sheet1']

    # 获取sheet内容
    sheet = excel_file.sheet_by_index(0)
    # sheet = excel_file.sheet_by_name('array_list_new')
    # print(sheet)
    # <xlrd.sheet.Sheet object at 0x0000029F479EAE10>

    # 获取sheet名称、行数、列数
    # print(sheet.name, sheet.nrows, sheet.ncols)
    # array_list_new 514 13

    # 获取基因名字
    # rows = sheet.row_values(2)
    gene_symbol = sheet.col_values(0)
    # print(len(gene_symbol))
    for gene in gene_symbol:
        save_mysql(gene)
        # pass

def save_mysql(gene):
    """
    保存到mysql中
    :params gene: 基因名字
    """
    # 链接数据库
    db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
    # 获取游标
    cursor = db.cursor()
    # 数据
    data = {
        'gene':gene
    }
    keys = ', '.join(data.keys())
    values = ', '.join(['%s']*len(data))
    # 表名
    table = 'gene_symbol_correct'
    # sql语句
    sql = 'insert into {table}({keys}) values({values});'.format(table=table, keys=keys, values=values)
    try:
        # 执行sql语句
        if cursor.execute(sql, tuple(data.values())):
            db.commit()
        else:
            db.rollback()
    except Exception as e:
        print(e.args)

    finally:
        cursor.close()
        db.close()


if __name__ == "__main__":
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()

    # infile = r'C:/Users/CRAB/Desktop/gene_disease/数据校对 5.9 array_list_new.xlsx'
    infile = r'C:/Users/CRAB/Desktop/gene_disease/gene_symbol.xlsx'
    read_excel(infile)

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)