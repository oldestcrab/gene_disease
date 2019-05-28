#/usr/bin/env python
#coding=utf8
 
import requests
import hashlib
from urllib.parse import quote
import random
import json

#你的appid
appid = '20190513000296983' 
#你的密钥
secretKey = 'xN2HMTWwE8Jzl9Wec4FC' 

myurl = 'http://api.fanyi.baidu.com/api/trans/vip/translate'
q = 'Hamartoma, Precalcaneal Congenital Fibrolipomatous'
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
    sess = requests.Session()
    response = sess.get(myurl)
 
    #response是HTTPResponse对象
    print(response.text)
    # print(result['trans_result'][0].get('dst',''))
    result = json.loads(response.text).get('trans_result', '')
    if result:
        zh_cn = result[0].get('dst','')
    
        print(zh_cn)
    
except Exception as e:
    print(e.args)

