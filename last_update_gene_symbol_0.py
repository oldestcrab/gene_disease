import pymysql
import time
import xlrd

def read_excel():
    """
    读取手工对比过后的gene_symbol excel 文件
    """
    # 之后标记为0的
    excel_file_2 = xlrd.open_workbook(r'C:/Users/CRAB/Desktop/2019-05-23_最后一次修改gene_symbol/5.20 gene_symbol_correct.xlsx')

    # 获取sheet内容
    sheet_2 = excel_file_2.sheet_by_index(0)

    # 储存两次核对的基因
    list_2 = []

    for row in range(1, sheet_2.nrows):
        result = sheet_2.row_values(row)

        if result[4]==0:
            result_2 = sheet_2.row_values(row+1)
            result_3 = sheet_2.row_values(row+2)
            result_4 = sheet_2.row_values(row+3)
            if result_2[4] != 0:
                if result_2[4] == 2 or result_2[4] == 3 or  result_2[4] == 4:
                    value = str(int(result_2[1])) + ',' + result_2[2]
            if result_2[4] != 0 and result_3[4] != 0:
                if result_3[4] == 2 or result_3[4] == 3 or  result_3[4] == 4:
                    value = value + '|' + str(int(result_3[1])) + ',' + result_3[2]
            if result_2[4] != 0 and result_3[4] != 0 and result_4[4] != 0:
                if result_4[4] == 2 or result_4[4] == 3 or  result_4[4] == 4:
                    value = value + '|' + str(int(result_4[1])) + ',' + result_4[2]
            dict = {result[0]:value}
            # print(dict)
            # {'PAX8/PPARgamma': '7849,PAX8|5468,PPARG'}
            insert_gene_symbol(dict)
            
    # print(len(list_2), list_2)


def insert_gene_symbol(dict):
    """
    删除需要重新导入的gene
    :params dict: 字典
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
    cursor_up = db.cursor()
    gene_list = [ x for x in dict.keys()]
    sql = 'select * from _cs_disease_map_1 where gene_symbol ="{}";'.format(gene_list[0])
    # print(sql)
    # print(dict.keys())
    # print(dict)
    try:
        # print(cursor.execute(sql))
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            print(row)
            reslut = [x for x in dict.values()]
            list_result = reslut[0].split('|')
            for i in list_result:
                gene_id = i.split(',')[0]
                gene_symbol = i.split(',')[1]
                # print(gene_id, gene_symbol)
                data = {
                'dis_id':row[0],
                'gene_symbol':gene_symbol, 
                'gene_id':gene_symbol,
                'source':row[3]
                }
                print(data)
                table = '_cs_disease_map'   
                keys = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql_up = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=table, keys=keys, values=values)
                update = ','.join([" {key} = %s".format(key=key) for key in data])
                sql_up = sql_up + update +';'
                # print(sql_up)
                try:
                    if cursor_up.execute(sql_up, tuple(data.values())*2):
                        db.commit()
                except Exception as e:
                    print('up eor',e.args)


            row = cursor.fetchone()
            

    except Exception as e:
        # print(old)
        print(e.args)

    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":

    print('start', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    start = time.time()

    # 最后一次更新gene_symbol，插入手工核对的数据
   
    read_excel()

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)