import pymysql
import time
import xlrd

def read_excel(infile):
    """
    读取对比过后的gene_symbol excel文件
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
    # gene_symbol = sheet.col_values()
    rows = sheet.nrows
    # print(rows)
    for row in range(1, sheet.nrows):
        result = sheet.row_values(row)
        
        # 替换为整数
        if result[1]:
            result[1] = int((result[1]))
        # print(result)
        if result[0] and result[2] and result[4]==1:
            # print(result)
            update_gene_symbol(result)
    # print(gene_symbol)
    # print(len(gene_symbol))


def update_gene_symbol(result):
    """
    更新gene
    :params result: 每一行gene记录
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
    table = '_cs_disease_map_1'
   
    sql = 'update _cs_disease_map_1 set gene_symbol = "{new}", gene_id = {id}, type=1 where gene_symbol = "{old}";'.format(new=result[2], id=result[1], old=result[0])
    try:
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

    # infile = r'C:/Users/CRAB/Desktop/gene_disease/数据校对 5.9 array_list_new.xlsx'
    infile = r'C:/Users/CRAB/Desktop/5.17 gene_symbol_correct.xlsx'
    read_excel(infile)

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)