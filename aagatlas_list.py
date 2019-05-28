import pymysql

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_id = self.db.cursor()
        self.cursor = self.db.cursor()
        self.cursor_judge = self.db.cursor()
        self.source_table = 'aagatlas_disease'

    def get_id(self):
        sql = 'select id, disease from {table};'.format(table=self.source_table)
        try:
            # 从表aagatlas_disease获取疾病ID、名字
            self.cursor_id.execute(sql)
            row = self.cursor_id.fetchone()
            while row:
                # print(row)
                self.get_name(row)
                row = self.cursor_id.fetchone()
                
        except Exception as e:
            print(e)

    def get_name(self, row):
        # print(row)
        # id_list = []
        # 疾病名字
        disease_name = row[1]
        # 疾病ID
        disease_id = row[0]
        
        try:
            # 查询表_cs_disease_dict是否有对应疾病
            sql = 'select id from _cs_disease_dict where name="{disease_name}"'.format(disease_name=disease_name)
            self.cursor.execute(sql)
            diseases = self.cursor.fetchone()

            if diseases:
                # print(diseases)
                # 查询表_cs_disease_list中对应的疾病的父辈
                parent_sql = 'select name from _cs_disease_dict where id in (select parent_id from _cs_disease_list where  dis_id={dis_id});'.format(dis_id=diseases[0])
                self.cursor_judge.execute(parent_sql)
                # 获取一个父辈
                parent_name = self.cursor_judge.fetchone()
                while parent_name:
                    # print(parent_name)

                    # 查询表aagatlas_disease中对应的疾病的父辈ID
                    sql_parent_id = 'select disease from aagatlas_disease where disease="{parent_name}";'.format(parent_name=parent_name[0])
                    pid_cursor = self.db.cursor()
                    pid_cursor.execute(sql_parent_id)
                    parent_id = pid_cursor.fetchone()

                    if parent_id:
                        self.save_list(disease_name, parent_id[0])
                    
                    parent_name = self.cursor_judge.fetchone()

        except Exception as e:
            print(e)

    def save_list(self, dis_id, parent_id):
        cursor = self.db.cursor()
        data = {
            'dis_id':dis_id,
            'parent_id':parent_id
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s']*len(data))
        sql = 'insert ignore into aagatlas_list_tmp({keys}) values({values}) ;'.format(keys=keys, values=values)
        try:
            if cursor.execute(sql, tuple(data.values())):
                self.db.commit()
            else:
                self.db.rollback()
        except Exception as e:
            print(e.args)

    def __del__(self):
        self.cursor.close()
        self.cursor_id.close()
        self.db.close()

if __name__ == "__main__":
    my = Mysql()
    my.get_id()