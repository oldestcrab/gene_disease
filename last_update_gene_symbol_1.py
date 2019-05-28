import pymysql
import time
import xlrd

def read_excel():
    """
    读取手工对比过后的gene_symbol excel文件
    """
    # 之前标记为1的
    excel_file_1 = xlrd.open_workbook(r'C:/Users/CRAB/Desktop/2019-05-23_最后一次修改gene_symbol/5.17 gene_symbol_correct.xlsx')
    # 之后标记为0的
    excel_file_2 = xlrd.open_workbook(r'C:/Users/CRAB/Desktop/2019-05-23_最后一次修改gene_symbol/5.20 gene_symbol_correct.xlsx')

    # 获取sheet内容
    sheet_1 = excel_file_1.sheet_by_index(0)
    sheet_2 = excel_file_2.sheet_by_index(0)

    # 储存两次核对的基因
    list_1 = []
    list_2 = []

    for row in range(1, sheet_1.nrows):
        result = sheet_1.row_values(row)

        if result[4]==1:
            # print(result)
            list_1.append(result[0])
    # print(len(list_1), list_1)

    for row in range(1, sheet_2.nrows):
        result = sheet_2.row_values(row)

        if result[4]==0:
            # print(result)
            list_2.append(result[0])
    # print(len(list_2), list_2)

    # 对比，看哪个基因在第二次核对之后标记变为了0
    list_change = []
    for i in list_1:
        # print(i)
        for j in list_2:
            # print(i,j)
            # print(type(i), type(j))
            if i==j:
                # print(j)
                list_change.append(j)
    # print(len(list_change), list_change)

    # 储存改变了的gene，需要从表中重新删除再导入
    list_delete = []
    for row in range(1, sheet_1.nrows):
        result = sheet_1.row_values(row)

        if result[0] in list_change:
            # print(result)
            # list_delete.append(result[0])
            delete_gene_symbol(result[0])
    # print(len(list_delete), list_delete)

def delete_gene_symbol(result):
    """
    删除需要重新导入的gene
    :params result: gene_symbol
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
   
    sql = 'delete FROM `_cs_disease_map` where gene_symbol="{}";'.format(result)
    try:
        # print(cursor.execute(sql))
        if cursor.execute(sql):
            db.commit()
        else:
            db.rollback()
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