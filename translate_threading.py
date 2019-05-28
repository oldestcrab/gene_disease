# -*- encoding:utf-8 -*-

import re
import time
import requests
from lxml import etree
from requests import ConnectionError
import sys
import os
from random import shuffle
import pymysql
import json
import threading
from queue import Queue 
import hashlib
from urllib.parse import quote
import random
import json

class mysql():
    def __init__(self):
        # 初始化
        self.db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='gene_disease')
        # 获取查询句柄
        self.select_cursor = self.db.cursor()
        self.update_cursor = self.db.cursor()

    def get_kw(self, queue):
        """
        查询队列放入关键字
        :params queue: 查询队列
        """
        # 查询没有中文的关键字
        sql = 'select name from _cs_disease_dict where zh_cn is null;'
        self.select_cursor.execute(sql)
        row = self.select_cursor.fetchone()
        while row:
            # print(row)
            # 把关键字放入队列
            queue.put(row[0])
            row = self.select_cursor.fetchone()

    def update(self, name, zh_cn):
        """
        更新疾病中文名称
        :params name: 疾病名
        :params zh_cn: 疾病中文名
        """
        sql = 'update _cs_disease_dict set zh_cn="{zh_cn}" where name="{name}";'.format(zh_cn=zh_cn, name=name)
        try:
            if self.update_cursor.execute(sql):
                self.db.commit()
        except:
            self.db.rollback()

                


    def __del__(self):
        # 关闭链接
        self.db.close()



def save_mysql(product_no, gene_no, detail, price, supply_time, kw):
    """
    把获取到的表格数据保存到mysql中
    """
    db = pymysql.connect(host='localhost', user='bmnars', password='vi93nwYV', port=3306, db='bmnars')
    cursor = db.cursor()
    update_time = time.strftime('%Y-%m-%d',time.localtime())
    data = {
        'product_no':product_no,
        'gene_no':gene_no,
        'detail':detail,
	    'update_time':update_time,
        'keyword':kw,
        'supply_time':supply_time,
        'price':price
    }
    table = '_cs_bmnars_vigenebio_result'
    keys = ','.join(data.keys())
    values = ','.join(['%s']*len(data))
    sql = 'INSERT INTO {table}({keys}) VALUES ({values}) on duplicate key update '.format(table=table, keys=keys, values=values)
    update = ', '.join(['{key} = %s'.format(key=key) for key in data]) + ';'
    sql += update
    # print(sql)
    try:
        if cursor.execute(sql,tuple(data.values())*2):
            db.commit()
            print('saving success!')
    except:
        print("save_mysql_failed:")
        db.rollback()
    
    finally:
        cursor.close()      
        db.close()

class ThreadCrawl(threading.Thread):
    def __init__(self, name_thread, queue_page, queue_data):
        super(ThreadCrawl, self).__init__()
        self.name_thread = name_thread
        self.queue_page = queue_page
        self.queue_data = queue_data

    def run(self):
        print('启动' + self.name_thread)
        while not CRAWL_EXIT:
            # 翻译
            try:
                q = self.queue_page.get(False)
                #你的appid
                appid = '20190513000296983' 
                #你的密钥
                secretKey = 'xN2HMTWwE8Jzl9Wec4FC' 

                myurl = 'http://api.fanyi.baidu.com/api/trans/vip/translate'
                # q = 'Hamartoma, Precalcaneal Congenital Fibrolipomatous'
                fromLang = 'en'
                toLang = 'zh'
                salt = random.randint(32768, 65536)

                sign = appid+q+str(salt)+secretKey
                sign = sign.encode(encoding='utf-8')
                m1 = hashlib.md5()
                m1.update(sign)
                sign = m1.hexdigest()
                # print(sign)
                myurl = myurl+'?appid='+appid+'&q='+quote(q)+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
                # print(myurl)

                try:
                    # sess = requests.Session()
                    response = requests.get(myurl)

                    #response是HTTPResponse对象
                    # print(response.text)
                    # print(result['trans_result'][0].get('dst',''))
                    result = json.loads(response.text).get('trans_result', '')
                    if result:
                        zh_cn = result[0].get('dst','')
                        # 英文名、中文名
                        dis_list = [q, zh_cn]
                        print(dis_list)
                        self.queue_data.put(dis_list)
                        # print(zh_cn)

                except Exception as e:
                    print(e.args)

            except:
                pass
        print('结束' + self.name_thread)

class ThreadParse(threading.Thread):
    def __init__(self, name_thread, queue_data, lock):
        super(ThreadParse, self).__init__()
        self.name_thread = name_thread
        self.lock = lock
        self.queue_data = queue_data

    def run(self):
        print('启动' + self.name_thread)
        while not PARSE_EXIT:
            try:
                dis_list = self.queue_data.get(False)
                # 更新疾病中文名称
                mysql().update(dis_list[0], dis_list[1])

            except:
                pass
        print('结束' + self.name_thread)
        


# 判断爬取线程是否跑完
CRAWL_EXIT = False
# 判断解析线程是否跑完
PARSE_EXIT = False

def main():
    """
    遍历每一页索引页
    """
    print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
    start_time = time.time()

    # 查询队列
    queue_page = Queue()

    # 查询队列放入关键字
    mysql().get_kw(queue_page)
    # print(kw)
    # print(queue_page.qsize())
    
    # 采集结果队列
    queue_data = Queue()

    # 锁
    lock = threading.Lock()

    # 四个采集线程的名字
    list_crawl = ["采集线程1号", "采集线程2号", "采集线程3号", "采集线程4号"]
    # print('list_crawl', list_crawl)
    # 存储四个采集线程的列表集合     
    thread_crawl = []
    for name_thread in list_crawl:
        thread = ThreadCrawl(name_thread, queue_page, queue_data)
        thread.start()
        thread_crawl.append(thread)
    # print('thread_crawl\t', thread_crawl)

    # 四个解析线程的名字
    list_parse = ["解析线程1号", "解析线程2号", "解析线程3号", "解析线程4号"]
    # print('list_parse', list_parse)
    
    # 存储四个解析线程的列表集合     
    thread_parse = []
    for name_thread in list_parse:
        thread = ThreadParse(name_thread, queue_data, lock)
        thread.start()
        thread_parse.append(thread)
    
    # 等待queue_page队列为空
    while not queue_page.empty():
        pass 
    # 如果queue_page队列为空,采集线程退出循环
    global CRAWL_EXIT
    CRAWL_EXIT = True
    print('queue_page为空！')

    for thread in thread_crawl:
        thread.join()

    # 等待queue_data队列为空
    while not queue_data.empty():
        pass 
    # 如果queue_data队列为空,采集线程退出循环
    global PARSE_EXIT
    PARSE_EXIT = True
    print('queue_data为空！')

    for thread in thread_parse:
        thread.join()


    print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))
    print('用时：\t' , time.time()-start_time)


if __name__ == '__main__':
    main()