import os
import json
import time
import logging #主要用于输出运行日志，可以设置输出日志的等级、日志保存路径、日志文件回滚
import datetime
import requests


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s') #logger级别设置为INFO，格式为：日志的时间，日志信息
logger = logging.getLogger(__name__)



collections = {
    'DXYOverall': 'overall',
    'DXYArea': 'area',
    'DXYNews': 'news',
    'DXYRumors': 'rumors'
}

folder_path = "/home/data/3dxy_nconv_data"
class Listener:


    def run(self):
        while True:
            self.listener()
            time.sleep(3600)

    def listener(self):
        changed_files = list()
        print("开始监听啦")
        for collection in collections.keys():

            if collection == 'DXYOverall':
                print("开始听取DXYOverall数据")

                #获取现存数据
                json_file1 = open(
                    os.path.join(
                        folder_path, 'json', collection + '1.json'),
                    'r', encoding='utf-8'
                )

                static_data1 = json.load(json_file1)
                json_file1.close()

                json_file0 = open(
                    os.path.join(
                        folder_path, 'json', collection + '0.json'),
                    'r', encoding='utf-8'
                )

                static_data0 = json.load(json_file0)
                json_file0.close()

                while True:
                    #获取最新数据
                    #当前数据
                    request1 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?latest=1")
                    #时间序列数据
                    request0 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?latest=0")               

                    if request1.status_code == 200 | request0.status_code==200:
                        current_data1 = request1.json()
                        current_data0 = request0.json()
                        break
                    else:
                        continue

                #比较现存数据和最新数据，并保存
                if static_data1 != current_data1:
                    json_file1 = open(
                        os.path.join(
                            folder_path, 'json', collection + '1.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data1,json_file = json_file1)
                    changed_files.append('json/' + collection + '1.json')
                    logger.info('{collection} updated!'.format(collection=collection))

                if static_data0 != current_data0:
                    json_file0 = open(
                        os.path.join(
                            folder_path, 'json', collection + '0.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data0,json_file = json_file0)
                    changed_files.append('json/' + collection + '0.json')
                    logger.info('{collection} updated!'.format(collection=collection))

            if collection == 'DXYRumors':
                print("开始听取DXYRumor数据")

                #获取现存数据

                json_file2 = open(
                    os.path.join(
                        folder_path, 'json', collection + '2.json'),
                    'r', encoding='utf-8'
                )

                static_data2 = json.load(json_file2)
                json_file2.close()

                json_file1 = open(
                    os.path.join(
                        folder_path, 'json', collection + '1.json'),
                    'r', encoding='utf-8'
                )

                static_data1 = json.load(json_file1)
                json_file1.close()

                json_file0 = open(
                    os.path.join(
                        folder_path, 'json', collection + '0.json'),
                    'r', encoding='utf-8'
                )

                static_data0 = json.load(json_file0)
                json_file0.close()

                while True:
                    #获取最新数据

                    #未证实信息
                    request2 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?rumorType=2&num=all") 
                    #可信信息
                    request1 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?rumorType=1&num=all")
                    #谣言
                    request0 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?rumorType=0&num=all")               

                    if request1.status_code == 200 | request0.status_code==200 | request2.status_code == 200:
                        current_data1 = request1.json()
                        current_data0 = request0.json()
                        current_data2 = request2.json()
                        break
                    else:
                        continue

                #比较现存数据和最新数据，并保存
                if static_data2 != current_data2:
                    json_file2 = open(
                        os.path.join(
                            folder_path, 'json', collection + '2.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data2,json_file = json_file2)
                    changed_files.append('json/' + collection + '2.json')
                    logger.info('{collection} updated!'.format(collection=collection))

                if static_data1 != current_data1:
                    json_file1 = open(
                        os.path.join(
                            folder_path, 'json', collection + '1.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data1,json_file = json_file1)
                    changed_files.append('json/' + collection + '1.json')
                    logger.info('{collection} updated!'.format(collection=collection))

                if static_data0 != current_data0:
                    json_file0 = open(
                        os.path.join(
                            folder_path, 'json', collection + '0.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data0,json_file = json_file0)
                    changed_files.append('json/' + collection + '0.json')
                    logger.info('{collection} updated!'.format(collection=collection))
            
            if collection == 'DXYNews':
                print("开始听取DXYNews数据")
                #获取现存数据
                json_file0 = open(
                    os.path.join(
                        folder_path, 'json', collection + '0.json'),
                    'r', encoding='utf-8'
                )

                static_data0 = json.load(json_file0)
                json_file0.close()

                while True:
                    #获取最新数据
                
                    #所有新闻数据
                    request0 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?num=all")               

                    if request0.status_code==200:
                        current_data0 = request0.json()
                        break
                    else:
                        continue

                #比较现存数据和最新数据，并保存

                if static_data0 != current_data0:
                    json_file0 = open(
                        os.path.join(
                            folder_path, 'json', collection + '0.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data0,json_file = json_file0)
                    changed_files.append('json/' + collection + '0.json')
                    logger.info('{collection} updated!'.format(collection=collection))
            
            if collection == 'DXYArea':
                print("开始听取DXYArea数据")
                #获取现存数据
                json_file1 = open(
                    os.path.join(
                        folder_path, 'json', collection + '1.json'),
                    'r', encoding='utf-8'
                )

                static_data1 = json.load(json_file1)
                json_file1.close()

                json_file0 = open(
                    os.path.join(
                        folder_path, 'json', collection + '0.json'),
                    'r', encoding='utf-8'
                )

                static_data0 = json.load(json_file0)
                json_file0.close()

                while True:
                    #获取最新数据
                    #当前数据
                    request1 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?latest=1")
                    #时间序列数据
                    request0 = requests.get(url='https://lab.isaaclin.cn/nCoV/api/' + collections.get(collection)+"?latest=0")               

                    if request1.status_code == 200 | request0.status_code==200:
                        current_data1 = request1.json()
                        current_data0 = request0.json()
                        break
                    else:
                        continue

                #比较现存数据和最新数据，并保存
                if static_data1 != current_data1:
                    json_file1 = open(
                        os.path.join(
                            folder_path, 'json', collection + '1.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data1,json_file = json_file1)
                    changed_files.append('json/' + collection + '1.json')
                    logger.info('{collection} updated!'.format(collection=collection))

                if static_data0 != current_data0:
                    json_file0 = open(
                        os.path.join(
                            folder_path, 'json', collection + '0.json'),
                        'w', encoding='utf-8'
                    )
                    self.json_dumper(collection=collection, content=current_data0,json_file = json_file0)
                    changed_files.append('json/' + collection + '0.json')
                    logger.info('{collection} updated!'.format(collection=collection))

    #将最新数据写入json文件
    def json_dumper(self, collection, content,json_file):        
        json.dump(content, json_file, ensure_ascii=False, indent=4)
        json_file.close()

    #将最新数据写入csv文件
    #def csv_dumper(self, collection):
 

if __name__ == '__main__':
    listener = Listener()
    listener.run()
