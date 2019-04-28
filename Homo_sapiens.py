# -*- coding:utf-8 -*-

import pymysql
import csv
import time

def parse(infile):
    """
    以CSV格式读取文件
    :params infile: 文件路径
    """
    with open(infile, encoding='utf-8', newline='') as f:
        reader = csv.reader(f, delimiter = '\t')
        for row in reader:
            save_mysql(row)
        
def save_mysql(row):
    """
    保存到mysql中
    :params row: 要储存得内容,列表形式
    """
    # 链接数据库
    db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
    # 获取游标
    cursor = db.cursor()
    # 数据
    data = {
        'tax_id':row[0],
        'GeneID':row[1],
        'Symbol':row[2],
        'LocusTag':row[3],
        'Synonyms':row[4],
        'dbXrefs':row[5],
        'chromosome':row[6],
        'map_location':row[7],
        'description':row[8],
        'type_of_gene':row[9],
        'Symbol_from_nomenclature_authority':row[10],
        'Full_name_from_nomenclature_authority':row[11],
        'Nomenclature_status':row[12],
        'Other_designations':row[13],
        'Modification_date':row[14],
        'Feature_type':row[15]
    }
    keys = ', '.join(data.keys())
    values = ', '.join(['%s']*len(data))
    # 表名
    table = '_cs_disease_gene_info'
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
    # 文件路径
    infile = r'C:/Users/CRAB/Desktop/gene_disease/Homo_sapiens.gene_info'
    # 解析文件
    parse(infile)
    
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)