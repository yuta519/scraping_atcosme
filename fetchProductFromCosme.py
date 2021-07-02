# -*- coding: utf-8 -*-
import asyncio
import csv
from datetime import datetime
import re
import sys
import typing

from bs4 import BeautifulSoup 
from requests import get


BASE_URL = 'https://www.cosme.net/category/'
FETCH_FIELDS = {'pri_cat': '大カテゴリ', 'sec_cat': '中カテゴリ', 
                'ter_cat': '小カテゴリ', 'cate_url': 'カテゴリページURL', 
                'product': '商品名', 'brand': 'ブランド名','price': '本体価格', 
                'release': '発売日', 'comment_counts': '口コミ数', 
                'evaluation': '評価（星の数）', 'pt_counts': 'pt数', 
                'img_url': '商品画像URL', 'pdct_url': '商品ページURL'}

def scraping_at_cosme(target_category='all') -> None:
    now = datetime.now().strftime('%Y%m%d%H%M%S')
    cate_dict = make_category_dict()
    if target_category == 'all':
        for pri_cat in cate_dict.keys():
            write_cosme_to_csv(now, cate_dict, pri_cat)
    else:
        pri_cat = target_category
        if cate_dict[pri_cat]:
            write_cosme_to_csv(now, cate_dict, pri_cat)
        else:
            raise KeyError('Error: Your input "{target_category}" is invalid')
    with open(f'cosmeinfo{now}.csv', 'r') as csvfile:
        csvtext = csvfile.read()
        for k,v in FETCH_FIELDS.items():
            csvtext = csvtext.replace(k, v)
    with open(f'cosmeinfo{now}.csv', 'w') as csvfile:
        csvfile.write(csvtext)

def fetch_soup(url) -> object:
    print('start:', url)
    try:
        soup = BeautifulSoup(get(url).text, 'html.parser')
    except:
        soup = None
    print('end:', url)
    return soup

def make_category_dict() -> dict:
    category_dict = {}
    soup = fetch_soup(BASE_URL)
    base = soup.find(id='theme-items')
    pri_cate = {cate.find('a').contents[0]: cate.find('a').get('href') 
                   for cate in base.find_all('h4')}
    sec_cate = []
    for highsection in base.find_all(class_='high-section'):
        p_tag_list = highsection.find_all('p')
        ul_tag_list = highsection.find_all('ul')
        temp_list = []
        if len(p_tag_list) == len(ul_tag_list):
            if len(p_tag_list) > 0 and len(ul_tag_list) > 0:
                for p_tag, ul_tag in zip(p_tag_list, ul_tag_list):
                    tertiary_cat = {a_tag.contents[0]: a_tag.get('href')
                                    for a_tag in ul_tag.find_all('a')}
                    sec_ter_dict = {p_tag.find('a').contents[0]: tertiary_cat}
                    temp_list.append(sec_ter_dict)
            elif len(p_tag_list) == 0:
                temp_list.append({'null': {'null': 'null'}})
            else:
                raise('Error: making category list was failed')
        elif len(p_tag_list) < 1 and len(ul_tag_list) > 0:
            for ul_tag in ul_tag_list:
                tertiary_cat = {a_tag.contents[0]: a_tag.get('href')
                                for a_tag in ul_tag.find_all('a')}
                sec_ter_dict = {'null': tertiary_cat}
                temp_list.append(sec_ter_dict)
        sec_cate.append(temp_list)
    if len(pri_cate) == len(sec_cate):
        for index, key in enumerate(pri_cate):
            if sec_cate[index] == [{'null': {'null': 'null'}}]:
                sec_cate[index] = [{'null': {'null': pri_cate[key]}}]
            category_dict[key] = sec_cate[index]
    return category_dict

def fetch_item(url) -> list:
    item_list = []
    maxpages = fetch_item_pages(url)
    loop = asyncio.get_event_loop()
    item_soup_list = [loop.run_until_complete(run(loop, url, maxpages))]
    for item_soup in item_soup_list[0]:
        item_tag_list = item_soup.find_all(class_='keyword-product-section')
        for item in item_tag_list:
            item_detail = fetch_item_details(item)
            item_list.append(item_detail)
    return item_list

def fetch_item_pages(category_url) -> int:
    soup = fetch_soup(category_url)
    maxpages = soup.find(class_='cmn-paging').find('p').contents[0]
    maxpages = re.match('^\d*', maxpages)
    return int(maxpages.group())

async def run(loop, url, maxpages):
    sem = asyncio.Semaphore(5)
    async def run_req(url):
        async with sem:
            return await loop.run_in_executor(None, fetch_soup, url)
    tasks = [run_req(url+'/page/'+str(p)) for p in range(0, -(-maxpages // 10))]
    return await asyncio.gather(*tasks)

def fetch_item_details(soup) -> dict:
    item_detail = {} 
    item_detail['product'] = soup.find(class_='item').find('a').contents[0]
    item_detail['brand'] = soup.find(class_='brand').find('a').contents[0]
    item_detail['price'] = soup.find(class_='price').contents[0]
    item_detail['release'] = soup.find(class_='sell').contents[0]
    item_detail['comment_counts'] = soup.find(class_='count').contents[0]
    item_detail['evaluation'] = soup.find(class_='value').contents[0]
    item_detail['pt_counts'] = soup.find(class_='point').contents[0]
    item_detail['img_url'] = soup.find(class_='pic').find('img').get('src').replace('?target=70x70', '')
    item_detail['pdct_url'] = soup.find(class_='item').find('a').get('href')
    return item_detail 

def write_cosme_to_csv(now, cate_dict, pri_cat) -> None:
    for sec_cat_dict in cate_dict[pri_cat]:
        sec_cat = list(sec_cat_dict.keys())[0]
        for ter_cat_dict in list(sec_cat_dict.values()):
            ter_cat = list(ter_cat_dict.keys())[0]
            cate_url = list(ter_cat_dict.values())[0]
            cates = [pri_cat, sec_cat, ter_cat, cate_url]
            print('start: ', datetime.now().strftime('%Y%m%d%H%M%S')) 
            items = fetch_item(cate_url.replace('top', 'products'))
            print('end: ', datetime.now().strftime('%Y%m%d%H%M%S')) 
            with open(f'cosmeinfo{now}.csv', 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=FETCH_FIELDS.keys())
                writer.writeheader()
                for item in items:
                    if item == None:
                        pass
                    else:
                        item['pri_cat'] = cates[0]
                        item['sec_cat'] = cates[1]
                        item['ter_cat'] = cates[2]
                        item['cate_url'] = cates[3]
                        writer.writerow(item)
            del items
    
if __name__ == '__main__':
    try:
        target = sys.argv[1]
    except:
        target = 'all'
    scraping_at_cosme(target)