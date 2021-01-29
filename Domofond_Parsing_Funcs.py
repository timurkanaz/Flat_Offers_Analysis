import requests as r
from bs4 import BeautifulSoup as bs
import re
import numpy as np
import random
import pandas as pd
from json import dumps,loads
from random import choice
from requests.packages.urllib3.exceptions import InsecureRequestWarning
r.packages.urllib3.disable_warnings(InsecureRequestWarning)
from multiprocessing.pool import ThreadPool
import time
import os
from datetime import datetime as dt

Link="https://www.domofond.ru/prodazha-nedvizhimosti/search?DistrictIds=676%2C677%2C679%2C678%2C680%2C681%2C675&PropertyTypeDescription=kvartiry&Rooms=Studio%2COne%2CTwo%2CThree%2CFour%2CFive%2CSix%2CSeven%2CEight%2CNine%2CMoreThan9&Page={}"
Git_path="C://Users//timna//OneDrive//Документы//Flat_Offers_Analysis//Data__{}.xlsx"

desktop_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
                 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0','Yandex/1.01.001']

WEB={'authority': 'www.domofond.ru', 'cache-control': 'max-age=0',
               'upgrade-insecure-requests': '1',
               'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWeb'
                             'Kit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.11'
                             '6 Safari/537.36',
               'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,'
                         'image/webp,image/apng,*/*;q=0.8,application/signed-'
                         'exchange;v=b3;q=0.9',
               'sec-fetch-site': 'none', 'sec-fetch-mode': 'navigate',
               'sec-fetch-user': '?1', 'sec-fetch-dest': 'document',
               'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7'}




def get_last_page_and_divide():
    print("Получаем количество страниц с недвижимостью.")
    dat=r.get(Link.format(1),headers={"User-Agent":random.choice(desktop_agents)},verify=False)
    soup=bs(dat.text,'lxml')
    lis=[ i.text for i in soup.find_all("li",{"class":"pagination__page___2dfw0"})]
    pages=[i for i in range(1,int(lis[-1]))]
    print("Всего в данный момент на сайте {} страниц".format(lis[-1]))
    content=[]
    pages_divided=list(np.array_split(pages,4))
    nums=[1,2,3,4]
    pages_divided=list(zip(nums,pages_divided))
    #os.system("clear")
    return pages_divided




def do_post(j):
    u=0
    while u==0:
        try:
            post_content=r.post("https://api.domofond.ru/rpc",data=j,headers=WEB)
            u=1
        except:
            time.sleep(30)
    return post_content
        


def get_info_from_json(href):
    i=0
    try:
        dicts={"id":"1","jsonrpc":"2.0","method":"Item.GetItemV3","params":{"meta":{"platform":"web","language":"ru"},"filters":{},"itemUrl":href}}
        js={}
        js=dumps(dicts,separators=(',', ':')).encode("utf8").decode('unicode-escape')
        raw_data=do_post(js)
        data=loads(raw_data.content)["result"]["item"]["data"]
        id1=loads(raw_data.content)["result"]["item"]["id"]
        lat=float(data["location"]["latitude"])
        lon=float(data["location"]["longitude"])
        price1=int(data["price"].replace(" ","").replace("₽",""))
        area1=float(data["floorAreaCalculated"])
        ppm1=float(data["pricePerMeterSquaredCalculated"])
        floor1=int(data["floorInt"])
        total_floor1=int(data["floorString"].split("/")[1])
        rooms1=(data["rooms"])
        material1=data["detailGroups"][0]["details"][9]["value"]
        addr1=(data["address"])
        try:
            distr1=data["district"]["name"]
        except:
            distr1=np.nan
        return (i,(id1,lat,lon,price1,area1,ppm1,floor1,total_floor1,rooms1,material1,addr1,distr1))
    except:
        return (i,"Error")



def do_get(g,h):
    m=0
    while m==0:
        try:
            rd=g.get(h,headers={"User-Agent":random.choice(desktop_agents)},verify=False)
            m=1
        except:
            time.sleep(30)
    return rd



def get_hrefs_and_info(pages_part):
    content=[]
    num,pages_part=pages_part
    with r.Session() as f:
        for index,part in enumerate(pages_part):
            i=0
            h=Link.format(part)
            raw_raw_raw_data=do_get(f,h)
            soup=bs(raw_raw_raw_data.text,'lxml')
            raw_raw_data=soup.find_all('a',{'class':'long-item-card__item___ubItG'},href=True)
            for href in raw_raw_data:
                ref=re.search(r'href="(.*?)"',str(href))
                z=np.random.randint(3,10)
                time.sleep(z)
                res=get_info_from_json(ref.group(1))
                if res[1]!="Error":
                    content.append(res[1])
                else:
                    i+=1
            z=np.random.randint(3,10)
            i+=1
            print('{} поток обработал {}/{} '.format(num,index+1,len(pages_part)),flush=True,end="\r")
            time.sleep(z)
    return content



def parsing(pages_divided):
    pool = ThreadPool(4)
    print("Начинаю просматривать страницы..")
    l=pool.map(get_hrefs_and_info,pages_divided)
    #os.system('clear')
    print("Парсинг окончен.Работаю с данными...")
    tuples=[]
    for part in l:
        for part_part in part:
            tuples.append(part_part)
    return tuples


def work_with_tuples(tuples):
    LAT=[]
    LON=[]
    ids=[]
    price=[]
    area=[]
    ppm=[]
    floor=[]
    total_floor=[]
    rooms=[]
    material=[]
    distr=[]
    addr=[]
    for tup in tuples:
        ids.append(tup[0])
        addr.append(tup[10])
        distr.append(tup[11])
        LAT.append(tup[1])
        LON.append(tup[2])
        material.append(tup[9])
        price.append(tup[3])
        ppm.append(tup[5])
        rooms.append(tup[8])
        floor.append(tup[6])
        total_floor.append(tup[7])
        area.append(tup[4])
    df=pd.DataFrame([ids,addr,distr,LAT,LON,material,price,ppm,rooms,floor,total_floor]).T
    df.columns=["id","Address","District","LAT","LON","Material","Price","Price_per_msq","Rooms","Floor","Total_Floors"]
    df.drop_duplicates("id",inplace=True)
    df.id=df.id.astype("int64")
    df.LAT=df.LAT.astype("float")
    df.LON=df.LON.astype("float")
    df.Price=df.Price.astype("float")
    df.Price_per_msq=df.Price_per_msq.astype("float")
    df.Floor=df.Floor.astype("int")
    df.Total_Floors=df.Total_Floors.astype("int")
    #os.system("clear")
    return df




def save(df):
    ct=dt.now()
    ct=ct.strftime("%d-%m-%Y__%H-%M")
    print("Сохраняем данные...")
    #os.system("clear")
    print("Данные на момент {} сохранены!".format(ct))
    df.to_excel(Git_path.format(ct),index=False)
    return ct





def Domofond_Parser():
    st=dt.now().strftime("%d.%m.%Y__%H:%M")
    page_divided=get_last_page_and_divide()
    ct=save(work_with_tuples(parsing(page_divided)))
    print("Парсинг закончен.Начался:{},а закончился {}".format(st,ct))






