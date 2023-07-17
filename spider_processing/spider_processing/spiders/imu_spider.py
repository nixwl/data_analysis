# -*- coding: utf-8 -*-
import time
import urllib
from idlelib import query
from multiprocessing import Pool
from unittest import result
import requests
from lxml import etree
import requests
import csv
import re
import os
import pandas
import pandas as pd
import chardet
import threading
# def get_deturl(url):
#         headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
#                              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
#         cookies = {
#                 'lianjia_uuid': 'lianjia_uuid=43ab065a-ad56-45db-830a-1de3fc9cc915; _smt_uid=64af9770.5cc65552; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22%24device_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _ga=GA1.2.1641582908.1689229182; _gid=GA1.2.2081089700.1689229182; _jzqckmp=1; _jzqx=1.1689233755.1689384710.1.jzqsr=cd%2Elianjia%2Ecom|jzqct=/ershoufang/rs/.-; _ga_XLL3Z3LPTW=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_NKBFZ7NGRV=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_0P06DN4FCM=GS1.2.1689391844.1.0.1689391844.0.0.0; select_city=510100; lianjia_ssid=19bb1112-861a-45f3-933a-e206ccca81e7; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1689296798,1689316289,1689378973,1689400163; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1689400163; _jzqa=1.2220308491814290000.1689229168.1689391843.1689400163.12; _jzqc=1; _jzqb=1.1.10.1689400163.1',
#                 'lianjia_ssid': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
#                  # 其他cookie信息...
#                   }
#         response2 = requests.get(url, headers=headers, cookies=cookies)
#         tree = etree.HTML(response2.text)  # 数据解析
#         df = open('lianjiadetail.csv', mode='a+', newline='', encoding='utf-8')
#         csv_writer1 = csv.writer(df)
#         response2.encoding = 'utf-8'
#         list2 = tree.xpath('//body')
#         for i in list2:
#             huose_name2 = i.xpath('./div[3]/div/div/div[1]/h1/text()')[0]
#             #print(huose_name2)
#             huose_detail2 = i.xpath('.div[5]/div[2]/div[5]/div[2]/span[2]/text()[2]')[0]
#             print(huose_detail2)
#         csv_writer1.writerow([huose_name2])
def get_url(url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
        cookies = {
            'lianjia_uuid': 'lianjia_uuid=43ab065a-ad56-45db-830a-1de3fc9cc915; _smt_uid=64af9770.5cc65552; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22%24device_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _ga=GA1.2.1641582908.1689229182; _gid=GA1.2.2081089700.1689229182; _jzqckmp=1; _jzqx=1.1689233755.1689384710.1.jzqsr=cd%2Elianjia%2Ecom|jzqct=/ershoufang/rs/.-; _ga_XLL3Z3LPTW=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_NKBFZ7NGRV=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_0P06DN4FCM=GS1.2.1689391844.1.0.1689391844.0.0.0; select_city=510100; lianjia_ssid=19bb1112-861a-45f3-933a-e206ccca81e7; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1689296798,1689316289,1689378973,1689400163; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1689400163; _jzqa=1.2220308491814290000.1689229168.1689391843.1689400163.12; _jzqc=1; _jzqb=1.1.10.1689400163.1',
            'lianjia_ssid': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            # 其他cookie信息...
        }
        response = requests.get(url, headers=headers, cookies=cookies)
        # 数据解析
        f = open('lianjia.csv', mode='a+', newline='', encoding='utf-8')
        csv_writer = csv.writer(f)
        response.encoding = 'utf-8'
        tree = etree.HTML(response.text)  # 数据解析
        #print(tree)
        list = tree.xpath('//ul[@class="sellListContent"]/li')
        #print(list)
        for i in list:
            try:
                name = i.xpath('.//div[1]/div[1]/a/text()')[0]
                location = i.xpath('.//div[1]/div[2]/div/a[2]/text()')[0]
                house_detail = i.xpath('.//div[1]/div[3]/div/text()')[0]
                a_price = i.xpath('.//div[1]/div[6]/div[1]/span/text()')[0]
                b_price = i.xpath('.//div[1]/div[6]/div[2]/span/text()')[0]
                # struct = i.xpath('.//div[1]/div[3]/div[1]/text()')[0]
                # house_info = i.xpath('./div[1]/div[3]/div/text()')  # 房间的详细信息
                time_info = i.xpath('./div[1]/div[4]/text()')  # 房屋的关注度
                next_url = i.xpath('./div[1]/div[1]/a/@href')[0] #详情页
                house_detail1, house_detail2, house_detail3, house_detail4, house_detail5, house_detail6, house_detail7 = house_detail.split(' | ', 7)
                for i2 in time_info:
                    time_info1, time_info2 = i2.split(' / ', 2)
                #print(next_url)
                csv_writer.writerow([name, location, house_detail1, house_detail2, house_detail3, house_detail4, house_detail5,house_detail6, house_detail7, a_price + '万', b_price, time_info1, time_info2])
                #get_deturl(next_url)
            except:
                 pass
        # print()
        f.close()



# 图片
# li_list2 = tree.xpath('//div[@class="slist"]/ul/li')
image = 'lianjia_picture'

def get_picture(url):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
        cookies = {
            'lianjia_uuid': 'lianjia_uuid=43ab065a-ad56-45db-830a-1de3fc9cc915; _smt_uid=64af9770.5cc65552; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22%24device_id%22%3A%221894de78f16700-0c6945dacb0058-26031d51-2073600-1894de78f1715e0%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; _ga=GA1.2.1641582908.1689229182; _gid=GA1.2.2081089700.1689229182; _jzqckmp=1; _jzqx=1.1689233755.1689384710.1.jzqsr=cd%2Elianjia%2Ecom|jzqct=/ershoufang/rs/.-; _ga_XLL3Z3LPTW=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_NKBFZ7NGRV=GS1.2.1689384722.9.0.1689384722.0.0.0; _ga_0P06DN4FCM=GS1.2.1689391844.1.0.1689391844.0.0.0; select_city=510100; lianjia_ssid=19bb1112-861a-45f3-933a-e206ccca81e7; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1689296798,1689316289,1689378973,1689400163; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1689400163; _jzqa=1.2220308491814290000.1689229168.1689391843.1689400163.12; _jzqc=1; _jzqb=1.1.10.1689400163.1',
            'lianjia_ssid': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
            # 其他cookie信息...
        }
        response = requests.get(url, headers=headers, cookies=cookies)
        page_text = response.text
        # print(page_text)
        # 数据解析：src属性值 alt属性值
        tree = etree.HTML(page_text)
        li_list = tree.xpath('//ul[@class="sellListContent"]/li')
        # #创建一个文件夹
        if not os.path.exists('./pictures'):
            os.mkdir('./pictures')
        for li in li_list:
            img_src = li.xpath('./a/img/@data-original')[0]
            img_name = li.xpath('.//div[1]/div[1]/a/text()')[0] + '.jpg'
        # 通用处理中文乱码解决方案
        # img_name = img_name.encode('iso-8859-1').decode('gbk')
        # print(img_name,img_src)
        # 请求图片，进行持久化存储
        img_data = requests.get(url=img_src, headers=headers).content
        img_path = 'pictures/' + img_name
        with open(img_path, 'wb') as fp:
            fp.write(img_data)
            #print(img_name, '下载成功！！')


# 数据中主要存在的问题包括：
#
# 1.列名中存在空格
# 2.存在重复数据
# 3.存在缺失数据
def data_cleaning():
        # 数据处理
        # 读取CSV文件并进行数据处理
        df = pd.read_csv('lianjia.csv', encoding='utf-8')
        # 去除列名中的空格
        new_df = df.dropna()
        # 去除重复数据
        new_df2 = new_df.drop_duplicates()
        #print(new_df2)
        # 填充缺失数据
        #df.fillna(value='', inplace=True)
        # 将处理后的数据重新写入CSV文件
        new_df2 = new_df2.to_csv('lianjia_processed.csv', index=False, encoding='utf-8-sig')

def detail(url):
        get_url(url)
        #get_picture(url)
        # codetype = result.get('encoding')
        # print(codetype)
        data_cleaning()

if __name__ == '__main__':
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
        f2 = open('lianjia.csv', mode='a+', newline='', encoding='utf-8')
        csv_writer = csv.writer(f2)
        csv_writer.writerow(['名称', '地址', '房型', '面积', '朝向', '装修', '楼层', '建成时间', '楼层结构', '总价格', '平方价格','关注度','发布时间'])
        f2.close()
        url_list = []
        for i in range(1, 100):
            url = "https://cd.lianjia.com/ershoufang/pg%s/" % i
            url_list.append (url)
        #print(url_list)
        # get_url(url)
        # get_picture(url)
        # # codetype = result.get('encoding')
        # # print(codetype)
        # data_cleaning()
        # 定义三个线程池
        pool = Pool(5)
        pool.map(detail, url_list)
        print('所有网页已经爬取完毕！')



