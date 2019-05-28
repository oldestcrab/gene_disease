import pymysql
import time
import xlrd


def update_gene_symbol():
    """
    更新gene
    :params result: 每一行gene记录
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
    cursor_up = db.cursor()
   
    sql = 'select distinct dis_id, gene_symbol, gene_id from _cs_disease_map_1 where type =1;'
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            data = {
                'dis_id':row[0],
                'gene_symbol':row[1],
                'gene_id':row[2]
            }
            # print(data)
            table = '_cs_disease_map_2'   
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
                # print(old)
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

    update_gene_symbol()

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)