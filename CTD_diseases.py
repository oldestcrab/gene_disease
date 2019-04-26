# -*- coding:utf-8 -*-

import time
import csv
import re
import pymysql

def parse_csv(infile):
    """
    解析csv数据
    :params infile: csv文件路径
    """
    # 读取csv文件
    with open(infile, encoding='utf-8', newline='') as f:
        # 获取句柄
        reader = csv.reader(f, delimiter='\t')
        # 逐行读取
        for row in reader:
            # print(row)
            # 过滤注释信息
            if not row[0].startswith('#'):
                # pass
                # print(row)
                # 内容为 , 分隔，再读取一遍
                # reader_row = csv.reader(row, delimiter=',')
                # # print(reader_row)
                # # 获取数据
                # for line in reader_row:
                # print('DiseaseName', row[0])
                # print('DiseaseID', row[5])
                # print('ParentIDs', row[6])
                # print('-------------')
                disease_list = [row[0], row[1], row[4], row[5], row[6]]
                #     # # 储存疾病信息
                save_mysql(disease_list, 'ctd_diseases_v2')


def save_mysql(source, table_name):
    """
    保存到mysql
    :param source: 数据
    :param table_name: 表名
    """
    # 连接数据库
    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    # 获得句柄
    cursor = db.cursor()
    # 时间
    update_time = time.strftime('%Y-%m-%d',time.localtime())
    try:
        # print(source)
        data = {
            'DiseaseName': source[0],
            'DiseaseID': source[1],
            'ParentIDs': source[2],
            'TreeNumbers': source[3],
            'ParentTreeNumbers': source[4]
        }
        table = table_name
        keys = ','.join(data.keys())
        values = ','.join(['%s']*len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values}) on duplicate key update '.format(table=table, keys=keys, values=values)
        update = ', '.join(['{key} = %s'.format(key=key) for key in data]) + ';'
        sql += update
        # print(sql)
        try:
            # 执行语句
            if cursor.execute(sql,tuple(data.values())*2):
                # 提交
                db.commit()
                # print(1)
        except Exception as e:
            # print(source[3])
            print("save mysql disease failed", e.args)
            # 错误则回滚
            db.rollback()

    except Exception as e:
        
        print('save_mysql error', e.args)
    finally:
        # 关闭句柄
        cursor.close()      
        # 关闭连接
        db.close()

if __name__ == '__main__':
    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()

    # 文件路径
    infile = r'C:/Users/CRAB/Desktop/gene_disease/ctd/CTD_diseases.tsv'
    # 解析数据
    parse_csv(infile)
     
    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)
