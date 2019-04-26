import pymysql

class Mysql():
    def __init__(self):
        self.db = pymysql.connect(host='localhost', port=3306, user='bmnars', password='vi93nwYV', db='gene_disease')
        self.cursor_id = self.db.cursor()
        self.cursor = self.db.cursor()
        self.source_table = 'ctd_diseases'
    

    def get_id(self):
        sql = 'select DiseaseName, ParentIDs from {table};'.format(table=self.source_table)
        try:
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
        id_list = []
        DiseaseName = row[0]
        ParentIDs = row[1]
        # print(DiseaseName, ParentIDs)
        ParentIDs_row = ParentIDs.split('|')
        
        sql = 'select id from _cs_disease_dict where name="{name}";'.format(name=DiseaseName)
        judge = self.cursor.execute(sql)
        # print(judge)
        if judge:
            # print(ParentIDs_row)
            dis_id = self.cursor.fetchone()[0]
            for each in ParentIDs_row:
                # each = each.replace('MESH:','')
                sql = 'select DiseaseName from ctd_diseases where DiseaseID="{id}";'.format(id=each)
                name_judge = self.cursor.execute(sql)
                if name_judge:
                    name = self.cursor.fetchone()[0]
                
                    # print(name)
                    sql_id = 'select id from _cs_disease_dict where name="{name}";'.format(name=name)
                    id_judge = self.cursor.execute(sql_id)
                    if id_judge:
                        parent_id = self.cursor.fetchone()[0]
                        # id_list.append(id)
                        # print(dis_id, parent_id)
                        self.save_list(dis_id, parent_id)

    def save_list(self, dis_id, parent_id):
        cursor = self.db.cursor()
        data = {
            'dis_id':dis_id,
            'parent_id':parent_id
        }
        keys = ', '.join(data.keys())
        values = ', '.join(['%s']*len(data))
        sql = 'insert into _cs_disease_list({keys}) values({values}) ;'.format(keys=keys, values=values)
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