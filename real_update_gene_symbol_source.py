import pymysql
import time


def update_gene_symbol():
    """
    更新gene
    :params result: 每一行gene记录
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
    cursor_up = db.cursor()
    cursor_select = db.cursor()
   
    sql = 'select dis_id, gene_symbol, gene_id from _cs_disease_map_2 where source is null;'
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        while row:
            sql_source = 'select source from _cs_disease_map_1 where dis_id="{dis_id}" and gene_symbol="{gene_symbol}";'.format(dis_id = row[0], gene_symbol=row[1],)
            cursor_select.execute(sql_source)
            source = cursor_select.fetchone()
            # print(source)

            sql_up = 'update _cs_disease_map_2 set source = "{source}" where dis_id="{dis_id}" and gene_symbol="{gene_symbol}";'.format(source=source[0], dis_id = row[0], gene_symbol=row[1],)

            # print(sql_up)
            try:
                if cursor_up.execute(sql_up):
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