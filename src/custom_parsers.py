from sys import stdout
from py import process
from selenium.common.exceptions import ElementNotInteractableException as error_interact
from selenium import webdriver
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import re
import codecs
from urllib.parse import quote
# import transliterate
from urllib.request import urlopen
from selenium.webdriver.support.ui import WebDriverWait
import requests
from datetime import datetime
from urllib.parse import urlparse
import cloudscraper
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
# правда блочит обычный requests


# Pipeline:
# 1. Получить из feedly по месяцу новости
# 2. Получить новости с 5 сайтов парсеров
# 3. Закачать новости из данных источников в базу (csv)
# 4. Добавить данные из csv в базу
# 5. Обработать алгоритмом новые новости и добавить их в econom_news (csv), потом в базу

def rt():  # со временем просто перестает появляться кнопка :(
    path = 'https://russian.rt.com/business/news'
    driver = webdriver.Chrome()
    driver.set_window_size(1920, 1080)
    driver.get(path)
    button_back = driver.find_element_by_class_name("button")
    action = ActionChains(driver)
    action.move_to_element(button_back).perform()
    button_cookies = driver.find_element_by_class_name("cookies__banner-button")
    button_cookies.click()
    button_back.click()
    wait = WebDriverWait(driver, 10)
    for i in range(100):
        button_back = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'button')))
        # button_back = driver.find_element_by_class_name("button")
        while True:
            try:
                action.move_to_element(button_back).perform()
            except error_interact:
                pass
            finally:
                element = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'button')))
                element.click()

    # footer = driver.find_element_by_class_name("layout__footer")
    # action = ActionChains(driver)
    # action.move_to_element(footer).perform()


def vedomosti(start, end):  # нужно искать в коде, иногда некоторое пропускает, т.к. код такой поехавший, но это редко.
    dates = [d.strftime('%Y/%m/%d') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time = [], [], []
    for date in dates[:-1]:
        stdout.write(f'\r{date}')
        stdout.flush()
        url = f'https://www.vedomosti.ru/archive/{date}'
        req = requests.get(url, headers=headers, timeout=10)
        if req.status_code == 404:
            print('проблемы с архивом ведомостей')
            return None  # None в concat просто не добавляется, поэтому все будет ок.
        encoding = req.encoding
        res = req.content.decode(encoding, errors='ignore')
        soup = BeautifulSoup(res, 'lxml')
        a = soup.find_all('div', {'class': 'archive-page__content'})
        s = str(soup)
        # фильтруем ссылки от лишнего, получаем список на новости
        list_info = re.findall(r',url:"(.*?)",title:"(.*?)"}', s)

        for url, title in list_info:
            url = codecs.decode(url, 'unicode-escape')
            if not ('/image/' in url or 'http' in url):
                title = title.replace('\\', '')
                title_proc.append(title)
                url_proc.append(f'https://www.vedomosti.ru{url}')
                time.append(datetime.strptime(date, '%Y/%m/%d'))
    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time
    df['feed_name'] = 'Ведомости'
    df['paper'] = 'Ведомости'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv(f'data/saved_feedlies/fed/customs/vedomosti_{start}_{end}.csv', encoding='utf-8-sig', sep=';')

    return df


def interfax(start, end):  # нужно искать в коде, иногда некоторое пропускает, т.к. код такой поехавший, но это редко.
    dates = [d.strftime('%Y/%m/%d') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time = [], [], []
    for date in dates[:-1]:
        stdout.write(f'\r{date}')
        stdout.flush()
        url = f'https://www.interfax.ru/business/news/{date}'
        req = requests.get(url, headers=headers, timeout=10)
        encoding = req.encoding
        res = req.content.decode(encoding, errors='ignore')
        soup = BeautifulSoup(res, 'lxml')

        found = soup.find_all('div')
        for div in found:
            if div.has_attr('data-id'):
                url_proc.append(f"https://www.interfax.ru{div.find('a')['href']}")
                title_proc.append(div.find('a').find('h3').text)
                time.append(datetime.strptime(date, '%Y/%m/%d'))

    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time
    df['feed_name'] = 'Интерфакс'
    df['paper'] = 'Интерфакс'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv(f'data/saved_feedlies/fed/customs/interfax_{start}_{end}.csv', encoding='utf-8-sig', sep=';')

    return df


def regnum(start, end):  # нужно искать в коде, иногда некоторое пропускает, т.к. код такой поехавший, но это редко.
    dates = [d.strftime('%d-%m-%Y') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time_ = [], [], []
    for date in dates[:-1]:
        stdout.write(f'\r{date}')
        stdout.flush()
        page = 1
        fetch_news = True
        while fetch_news:  # запрашиваем через api напрямую информацию
            url = f'https://regnum.ru/api/get/search/news?date={date}&theme=economy&q=&page={page}&filter=%7B' \
                  f'%22authorId%22%3A%22%22%2C%22regionsId%22%3A%22%22%2C%22theme%22%3A%22economy%22%2C%22date' \
                  f'%22%3A%5B%22{date}%22%2C%22{date}%22%5D%7D'
            res = False
            while not res:
                try:
                    req = requests.get(url, headers=headers, timeout=10).json()
                    res = True
                except requests.exceptions.ReadTimeout:
                    time.sleep(1)
                    continue
            if len(req['articles']) == 0:
                fetch_news = False
            else:
                articles = req['articles']
                for article in articles:
                    title_proc.append(article['news_header'])
                    url_proc.append(article['news_link'])
                    time_.append(datetime.strptime(date, '%d-%m-%Y'))
                    page += 1

        # url = f'https://regnum.ru/search/news?date={date}&theme=economy'
        # req = requests.get(url, headers=headers, timeout=10)
        # encoding = req.encoding
        # res = req.content.decode(encoding, errors='ignore')
        # soup = BeautifulSoup(res, 'lxml')
        #
        # found = soup.find_all('h3')
        # for f in found:
        #     title_proc.append(f.text.replace('\n', ''))
        #     url_proc.append(f.find('a')['href'])
        #     time.append(datetime.strptime(date, '%d-%m-%Y'))

    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time_
    df['feed_name'] = 'Regnum'
    df['paper'] = 'Regnum'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv(f'data/saved_feedlies/fed/customs/regnum_{start}_{end}.csv', encoding='utf-8-sig', sep=';')

    return df


def prime(start, end):
    dates = [d.strftime('%Y%m%d') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time = [], [], []
    for date in dates:
        stdout.write(f'\r{date}')
        stdout.flush()
        url = f'https://1prime.ru/state_regulation/?date={date}'
        req = requests.get(url, headers=headers, timeout=10)
        encoding = req.encoding
        res = req.content.decode(encoding, errors='ignore')
        soup = BeautifulSoup(res, 'lxml')

        found = soup.find('div', attrs={'class': 'rubric-list__articles'})
        for f in found.contents[1:]:
            if f.name == 'aside':
                break
            title_proc.append(f.find('h2').text.replace('\n', ''))
            url_proc.append(f"https://1prime.ru{f.find('h2').find('a')['href']}")
            time.append(datetime.strptime(date, '%Y%m%d'))

    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time
    df['feed_name'] = 'Prime'
    df['paper'] = 'Prime'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv('prime.csv', encoding='utf-8-sig', sep=';')

    return df


def pravda(start, end):
    dates = [d.strftime('%Y-%m-%d') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36'}
    title_proc, url_proc, time = [], [], []
    for date in dates[:-1]:
        stdout.write(f'\r{date}')
        stdout.flush()
        url = f'https://www.pravda.ru/archive/{date}'
        sess = cloudscraper.create_scraper()
        req = sess.get(url, allow_redirects=True)
        # req = requests.get(url, headers=headers, timeout=10) не работает из-за cloudflare
        # encoding = req.encoding
        res = req.content.decode('utf-8', errors='ignore')
        soup = BeautifulSoup(res, 'lxml')

        # found = soup.find('div', attrs={'class': 'news block'})
        found = soup.find('div', attrs={'id': 'archive-day-section'})
        if found is not None:
            articles = found.find_all('div', attrs={'class': 'article'})
            for article in articles:
                url_proc.append(article.find('div', attrs={'class': 'title'}).find('a')['href'])
                # есть новости список, еще есть их статьи отдельно идущие
                if len(article.find('div', attrs={'class': 'title'}).find('a').text.strip()) > 0:
                    title_proc.append(article.find('div', attrs={'class': 'title'}).find('a').text.strip())
                else:
                    title_proc.append(article.find('div', attrs={'class': 'title'}).find('a').find('img')['alt'])
                time.append(datetime.strptime(date, '%Y-%m-%d'))

    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time
    df['feed_name'] = 'правда.ру'
    df['paper'] = 'правда.ру'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv(f'data/saved_feedlies/fed/customs/pravda_{start}_{end}.csv',
              encoding='utf-8-sig', sep=';')

    return df


def finanz(start, end):
    dates = [d.strftime('%Y/%m/%d') for d in pd.date_range(start=start, end=end, freq='D')]
    headers = {
        'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time = [], [], []
    for date in dates[:-1]:
        stdout.write(f'\r{date}')
        stdout.flush()
        page = 1
        fetch_info = True
        while fetch_info:
            url = f'https://www.finanz.ru/novosti/arkhiv/{date}?p={page}'
            req = requests.get(url, headers=headers, timeout=30)
            encoding = req.encoding
            res = req.content.decode(encoding, errors='ignore')
            soup = BeautifulSoup(res, 'lxml')

            found = soup.find('tbody')
            articles = found.find_all('a')
            if len(articles) > 0:
                for article in articles:
                    url_proc.append(f"https://www.finanz.ru{article['href']}")
                    title_proc.append(article.text.strip())
                    time.append(datetime.strptime(date, '%Y/%m/%d'))
                page += 1
            else:
                fetch_info = False

    df = pd.DataFrame(title_proc)
    df.columns = ['title_proc']
    df['url_proc'] = url_proc
    df['time'] = time
    df['feed_name'] = 'finanz'
    df['paper'] = 'finanz'
    df['region'] = 'fed'
    df['key_word'] = ''
    df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
    df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
    df.to_csv(f'data/saved_feedlies/fed/customs/finanz_{start}_{end}.csv', encoding='utf-8-sig', sep=';')

    return df


def znak():
    dates = [d.strftime('%Y-%m-%d') for d in pd.date_range(start='2014-01-01', end='2021-06-01', freq='D')]
    headers = {'User-Agent': config['USER_AGENT']}
    title_proc, url_proc, time = [], [], []
    for date in dates:
        stdout.write(f'\r{date}')
        stdout.flush()
        page = 1
        fetch_info = True
        while fetch_info:
            url = f'https://www.finanz.ru/novosti/arkhiv/{date}?p={page}'
            req = requests.get(url, headers=headers, timeout=10)
            encoding = req.encoding
            res = req.content.decode(encoding, errors='ignore')
            soup = BeautifulSoup(res, 'lxml')

            found = soup.find('tbody')
            articles = found.find_all('a')
            if len(articles) > 0:
                for article in articles:
                    url_proc.append(f"https://www.finanz.ru{article['href']}")
                    title_proc.append(article.text.strip())
                    time.append(datetime.strptime(date, '%Y-%m-%d'))
                page += 1
            else:
                fetch_info = False

        df = pd.DataFrame(title_proc)
        df.columns = ['title_proc']
        df['url_proc'] = url_proc
        df['time'] = time
        df['feed_name'] = 'finanz'
        df['paper'] = 'finanz'
        df['region'] = 'fed'
        df['key_word'] = ''
        df = df[['title_proc', 'paper', 'time', 'url_proc', 'key_word', 'region', 'feed_name']]
        df.columns = ['title', 'paper', 'time', 'hyperlink', 'key_word', 'region', 'feed_name']
        df.to_csv('finanz.csv', encoding='utf-8-sig', sep=';')
