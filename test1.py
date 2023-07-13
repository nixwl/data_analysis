import pymysql

con = pymysql.connect(host='localhost', password='123456', port=3306, user='root', charset='utf8')
cur = con.cursor()
cur.execute('show databases')

