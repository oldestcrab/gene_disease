import pymysql
import time

def disease_annotations():
    """
    查询疾病
    """

    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
    cursor = db.cursor()
    cursor_select = db.cursor()
    cursor_id = db.cursor()
    cursor_insert = db.cursor()
   
    sql = 'select DISTINCT disease_name  from disease_annotations ;'
    try:
        cursor.execute(sql)
        row = cursor.fetchone()
        # 疾病ID
        id = 16965
        while row:
            # print(row)

            # 判断疾病名字，即疾病是否已存在表中
            sql_select = 'select name  from _cs_disease_dict where name = "{}" ;'.format(row[0])
            if not cursor_select.execute(sql_select):
                # 插入数据到表_cs_disease_dict
                # print(row[0])

                
                data = {
                    'id': id,
                    'name': row[0],
                }
                keys = ', '.join(data.keys())
                values = ', '.join(['%s']*len(data))
                sql = 'insert into _cs_disease_dict({keys}) values({values}) ;'.format(keys=keys, values=values)
                try:
                    if cursor_insert.execute(sql, tuple(data.values())):
                        db.commit()

                    else:
                        db.rollback()
                except Exception as e:
                    print(e.args)

                # id+1
                id += 1

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

    # 查询
    disease_annotations()

    print('stop', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
    print('all', time.time()-start)