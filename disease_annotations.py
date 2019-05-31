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
            print(len(row), row)
            # save_mysql(row)
        
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
        'Gene_ID':row[0],
        'Gene_Symbol':row[1],
        'Species':row[2],
        'Genetic_Entity_ID':row[3],
        'Genetic_Entity_Symbol':row[4],
        'Genetic_Entity_Type':row[5],
        'Association_Type':row[6],
        'Disease_ID':row[7],
        'Disease_Name':row[8],
        'Evidence_Code':row[9],
        'Source':row[10],
        'Refer':row[11]
    }
    keys = ', '.join(data.keys())
    values = ', '.join(['%s']*len(data))
    # 表名
    table = 'disease_annotations'
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
    infile = r'C:/Users/CRAB/Desktop/工作记录/2019/05-31_disease_annotations/disease-annotations-DOID-4.tsv'
    # 解析文件
    parse(infile)
    
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)