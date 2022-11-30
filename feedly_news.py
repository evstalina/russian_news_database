import http.client  # подключение к сайту для загрузки
import os
import pickle
import socket  # организация тунеля для подключения
import ssl  # организация тунеля для подключения
import requests
from requests.adapters import HTTPAdapter  # для HTTP-запросов, почитать: https://habr.com/ru/company/ruvds/blog/472858/
from bs4 import BeautifulSoup  # для анализа документов HTML и XML, почитать: https://habr.com/ru/post/544828/
import lxml  # парсинг HTML / XML документов, почитать: https://webdevblog.ru/vvedenie-v-biblioteku-python-lxml/
from lxml import html, etree
import urllib3  # Для HTTP-запросов
from urllib import error
from urllib.request import urlopen, Request
from multiprocessing.dummy import Pool as ThreadPool  # многопоточность в 1 строку,
# почитать: http://toly.github.io/blog/2014/02/13/parallelism-in-one-line/
import sqlalchemy
from feedly.api_client.session import FeedlySession
import pandas as pd
from datetime import datetime, timedelta
import re
import random
import string
from tqdm import tqdm  # progress-bar
from sys import stdout
import warnings
import time
from src.lemmatization import get_lem_text  # Наш класс для лемматизации
from src.sql_interection import add_data_to_sql, inflation_min_max_date  # Наши ф-ции для БД
import cloudscraper  # anti-bot page, полезно: https://github.com/Anorov/cloudflare-scrape
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

warnings.filterwarnings('ignore')
urllib3.disable_warnings()


class feedly_proc:
    def __init__(self, gu, method, data_begin=None,
                 feedly_df='today'):  # если data_begin не None, то переходим в режим
        """
        Вводные данные, задаем конфигурацию, фичи и пр.

        :param gu: VVGU or SZGU (для fed отдельный класс в FED_news)
        :param method: urlopen or request
        :param data_begin: Установка даты начала скачки в базе, если дата раньше, чем есть, то качает
        новости из прошлого. Но это не стоит юзать, т.к. Feedly все равно скачает все новости от текущей даты, ограничить
        это нельзя. Для обычного обновления базы это не нужно трогать, просто оставить None.
        :param feedly_df: None - новости будут получены с feedly api
        today - возьмет сегодня скачанный список feedly из saved_feedlies/vvgu(szgu). Если нет сегодняшн., то будет
        качать с feedly api
        либо ссылка на xlsx файл
        """
        self.df_errors = []  # список записей об ошибках. Потом сохраняется в виде excel через опред. время
        self.gu = gu
        self.acc_control = config['FEEDLY']['ACC_CONTROL']  # True/False, проверять ли качество на уровне в 90%
        # блокировок и проч. он False по дефолту, его лучше не включать, т.к. сейчас качество не стабильно из-за всего
        if data_begin is not None:
            self.data_begin = datetime.strptime(data_begin, "%Y-%m-%d")
        else:
            self.data_begin = data_begin
        self.feedly_open = pd.read_excel(config['FEED_PARAMS'][self.gu],
                                         index_col='feedname').to_dict(orient='index')  # собственно файл со СМИ и
        # фичи для каждого СМИ нужные
        self.headers = {'User-Agent': config['USER_AGENT']}  # для requests
        self.batch_size = config['FEEDLY']['BATCH_SIZE']  # размер батча обработки
        self.log_data = datetime.now().strftime("%Y-%m_%H-%M")  # Дата для логгирования
        self.url_method = method
        self.proxies_web = [{
            "http": '178.63.17.151:3128',
            "https": '178.63.17.151:3128',
            "ftp": '178.63.17.151:3128'},
            {
                "http": '202.180.54.97:8080',
                "https": '202.180.54.97:8080',
                "ftp": '202.180.54.97:8080'},
            {
                "http": '23.251.138.105:8080',
                "https": '23.251.138.105:8080',
                "ftp": '23.251.138.105:8080'}
        ]  # особо не работает
        self.writer = pd.ExcelWriter(f'logs/errors_{self.gu}_{self.log_data}.xlsx', engine='openpyxl')
        # writer для сохранения в Excel
        self.error_i = 1
        self.feedly_df = self.check_feedly(feedly_df)  # ищем файлик
        self.feedly_limit = 250  # лимит каждого токена в Feedly, он просто такой
        # three tokens ----
        self.tokens = [config['FEEDLY']['TOKENS'][i] for i in range(1, 4)]  # список токенов из config
        self.current_token = 0  # 0, 1, 2, какой по счету токен юзается
        self.cscraper = None  # это для обхода облачной защиты. Юзается только для пару фед изданий.

    def check_feedly(self, feedly):
        """
        Проверка есть ли файл фидли

        :param feedly: today или None
        :return: подходящий файл фидли
        """
        if feedly == 'today':  # то ищем файлик сегодняший
            feedly_file = None
            today = datetime.now().strftime("%Y-%m-%d")
            os.chdir(f'data/saved_feedlies/{self.gu}')
            files = os.listdir()
            for file in files:
                time_file = os.path.getmtime(file)  # получаем время послед. изменения файла (создания, тут одно и тоже)
                time_file = time.strftime("%Y-%m-%d", time.gmtime(time_file))
                if time_file == today:  # сравниваем день с сегодняшним
                    feedly_file = file
            os.chdir(f'../../../')
            if feedly_file is not None:  # если нашли подходящий, то его пандасом открываем
                return pd.read_excel(f'data/saved_feedlies/{self.gu}/{feedly_file}')
            else:
                return None

        elif feedly is not None and '.xlsx' in feedly:  # если ссылка на файл, то читаем его пандасом
            return pd.read_excel(f'data/saved_feedlies/{self.gu}/{feedly}')
        else:
            return feedly

    def _refresh_df_errors(self, time_news='', smi='', problem='', link='', error_group='', serious=3):
        """
        Вспомог. функция для добавления в список ошибок строки (тут как словарь, в df будет строка)

        :param time_news: время новости
        :param smi: название СМИ
        :param problem: в чем проблема
        :param link: ссылка на новость (если есть)
        :param error_group: группа ошибки
        :param serious: уровень серьезности (1,2,3)
        :return: Обновленный список ошибок с новыми строками
        """
        self.df_errors.append({'error_group': error_group, 'time_news': time_news, 'smi': smi,
                               'problem': problem, 'link': link, 'serious': serious})

    def _feedly_query(self, feed_id, continuation):
        """
        Запрос к api feedly по feed_id

        :param feed_id: feed_id СМИ
        :param continuation: С какого места запрашивать (т.е. это будет самая ранняя новость из прошлого батча).
        Либо номер новости, либо 100L
        :return: json с результатами
        """
        s = requests.Session()  # начинаем сессию
        # далее все для запроса ----
        s.mount('https://feedly.com', HTTPAdapter(max_retries=1))
        header = {'Authorization': self.tokens[self.current_token]}
        full_url = f'https://feedly.com/v3/streams/contents?' \
                   f'streamId={feed_id}' \
                   f'&count=1000&' \
                   f'continuation={continuation}' \
                   f'&client=feedly.python.client'
        # время ожидания:
        timeout = 120  # Было 30, можно менять по усмотрению
        t = True
        while t:  # пока не получим ответ долбим. Ограничения на бесконечный цикл нет, т.к. пока проблем не возникало.
            try:
                result = s.request('get', full_url, headers=header, timeout=timeout, json=None)
                t = False  # если получили ответ, то цикл стопаем так
                result.headers['X-RateLimit-Limit'] != '250' and result.headers['Connection'] != 'keep-alive'
                # Кривая проверка на адекватность инфы
            except KeyError:  # Кривая проверка на то, находит ли в result X-RateLimit-Limit или Connection
                print(f'Что-то не получилось с {feed_id}, но мы пробуем еще')
                time.sleep(3)
                t = True  # Если инфа неадекватная - попробуем еще раз
            except requests.exceptions.ConnectionError:  # при ошибке с коннектом опять долбим
                stdout.write(f'\rConnection failed...try again')
                stdout.flush()
                time.sleep(3)
                continue
        try:  # тут проверка на лимиты по токену. Всего 250 на 3 токенов (750). Если токены закончились, ошибку выдаст,
            # соответственно надо будет токены обновить.
            self.feedly_limit = int(result.headers['X-RateLimit-Limit']) - int(result.headers['X-RateLimit-Count'])
        except KeyError:
            raise Exception('Старые токены истекли, необходимо их обновить (получить новые).')
        result = result.json()
        # дальше контролируется чисто на лимите, но первый запрос так и так нужно проверить, есть вероятность,
        # что остался один запрос, после него станет 0 и код бы второй раз запустил тоже самое, чтобы избежать,
        # тут отдельная проверка для первой загрузки
        stdout.write(f'\rТокен {self.current_token+1} из 3. Осталось {self.feedly_limit} запросов')
        stdout.flush()
        try:
            result = result['items']  # берем только сами записи
        except KeyError:
            self.current_token += 1  # ошибка эта вылезает как правило из-за токенов, которые просрочены.
            if self.current_token == len(self.tokens):
                raise Exception('All limits are over for today')
            return 0
        # для остальных случаев:
        if self.feedly_limit == 0:
            self.current_token += 1
            if self.current_token == len(self.tokens):  # если уже все три токена потратили
                raise Exception('All limits are over for today')

        return result

    def _get_news_one_source(self, feed_id, feed_name_source, last_date, begin_db=None):
        """
        Вспомогательная функция для get_news. С ее помощью качает с feedly список новостей по одному источнику

        :param feed_id: id
        :param feed_name_source: название СМИ
        :param last_date: последняя дата (наша посл. дата в БД)
        :param begin_db: если есть, для того, чтобы качать в другую сторону. Для обычной скачки это не нужно.
        :return: датафрейм с ссылками на новости по одному СМИ из списка
        """
        first_batch = True  # можно попробовать переписать через pydantic
        news_part, paper, date_time_news, title, hyperlink, key_word, feed_name = [], [], [], [], [], [], []
        control = True  # True пока мы получаем новости датой больше, чем last_date, last_date получаем с базы
        continuation = '100L'  # для того, чтобы в api дать информацию, от какой новости по id выдавать результат,
        # это начальный id
        while control:  # пока мы не дошли до конечной даты
            result = 0
            while result == 0:
                # запрос к фидли:
                result = self._feedly_query(feed_id=feed_id, continuation=continuation)
            if result:  # Если result не None
                for news in result:  # итерация по записям
                    try:
                        date_news = datetime.fromtimestamp(int(str(news['published'])[:-3]))  # вычленяем время
                    except ValueError:  # это если случилась ошибка, далее аналогично.
                        self._refresh_df_errors(time_news='', smi=feed_name_source,
                                                problem='не получено время новости (не попало в новости)',
                                                link='', serious=3, error_group='feedly_features')
                    else:  # если try выполнен удачно
                        if date_news < last_date:  # идем с последней даты в
                            # БД, которую потом удалим в БД, так как последний день в базе скорее всего не полный
                            control = False  # заканчиваем закачку (цикл while)
                            break
                        else:  # если еще не закончили, то далее обрабатываем и добавляем значения
                            date_time_news.append(date_news)  # дата
                            try:
                                paper.append(news['origin']['title'])  # название СМИ
                            except KeyError:
                                paper.append('')
                                self._refresh_df_errors(time_news='', smi=feed_name_source,
                                                        problem='не получено название СМИ', link='', serious=3,
                                                        error_group='feedly_features')
                            try:
                                title_edited = re.sub('<.*?>|\n|&#13;|\t|\r|{([\s\S]+?)}', " ",
                                                      news['title'])  # название заголовка (удаляем лишнее, чтоб без
                                # ошибок)
                                title.append(title_edited)
                            except KeyError:
                                title.append('')
                                self._refresh_df_errors(time_news='', smi=feed_name_source,
                                                        problem='не получено название новости', link='', serious=3,
                                                        error_group='feedly_features')
                            try:
                                hyperlink.append(news['alternate'][0]['href'])  # ссылка на статью
                            except KeyError:
                                hyperlink.append('')
                                self._refresh_df_errors(time_news='', smi=feed_name_source,
                                                        problem='не получена ссылка на новость', link='', serious=1,
                                                        error_group='feedly_features')
                            try:
                                key_word.append(' ,'.join(news['keywords']))  # ключевые слова
                            except KeyError:
                                key_word.append('')
                                self._refresh_df_errors(time_news='', smi=feed_name_source,
                                                        problem='не получены ключевые слова новости', link='',
                                                        serious=3,
                                                        error_group='feedly_features')
                # важно отметить, что если что-то не получено, как напр. ключевые слова, то это не блокирует скачку.
                continuation = news['id']  # фиксим id, с которого передадим запрос в feedly
            else:  # Если ничего не получили, то заканчиваем обработку
                control = False
        if len(paper) == 0:  # если пусто
            self._refresh_df_errors(time_news='', smi=feed_name_source,
                                    problem='нет новостей за период (inactive)', link='', serious=1,
                                    error_group='feedly_inactive')

        # формируем df ----
        df = pd.DataFrame(title, columns=['title'])
        df['paper'] = paper
        df['time'] = date_time_news
        # df['news_part'] = news_part
        df['hyperlink'] = hyperlink
        df['key_word'] = key_word
        if begin_db is not None:
            df = df[df['time'] < begin_db]

        del result, date_time_news, title, paper, hyperlink, key_word  # не так и нужно

        return df

    def get_news(self, feedly_save=True, dump_sources=True):
        """
        Функция получения списка новостей со ссылками с feedly

        :param feedly_save: нужно ли сохранить скаченное?
        :param dump_sources: нужно ли это сохранить в формате pickle?
        :return: полный датафрейм с ссылками на новости по всем указанным СМИ
        """
        print('Получение списка актуальных новостей')
        begin_db = None
        # если self.data_begin есть, то при пустой базе начинаем с него, если не пустая, то качаем прошлые новости,
        # ранее начала базы с self.data_begin
        last_date, begin_db = inflation_min_max_date(self.gu, self.data_begin)  # определяем посл. дату в базе, если
        # нужно, то и начальную. Тут многое не надо, по умолчанию он просто скачает данные последние до нынешней даты.
        if self.feedly_df is None:  # если мы скаченные ссылки уже не передали при инициализации
            df_full = None
            for source in tqdm(self.feedly_open.keys()):  # идем по СМИ
                df_source = self._get_news_one_source(self.feedly_open[source]['feedId'], source, last_date,
                                                      begin_db=begin_db)  # качаем по одному СМИ
                df_source['region'] = self.feedly_open[source]['region']
                df_source['feed_name'] = source
                # убираю все новости, которые по итогу видео. Чаще всего в url это явно задано. Их все равно не спарсим,
                # так как видео:
                df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('video')]  # чистим новости,
                # где только видео
                if dump_sources:
                    pickle.dump(df_source, open(f'data/saved_feedlies/feedly_pickles/'
                                                f'{self.feedly_open[source]["id"]}_{self.log_data}_{self.gu}.pkl',
                                                'wb'))
                # убираю и спорт, так как часто верстка у них особая, бесполезные новости для нас:
                if self.gu == 'FED':
                    df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('sport')]
                if df_full is None:
                    df_full = df_source
                else:
                    df_full = df_full.append(df_source, ignore_index=True)  # добавляем скаченные df в общий df
            del df_source  # удаляем, чтоб память не сильно грузить
            smi_ = df_full[df_full['hyperlink'] != '']  # для логов записываем список СМИ, по которым закачали
            feedly_set_of_smi = set(smi_['feed_name'].unique())
            feedly_df_of_smi = pd.DataFrame(list(feedly_set_of_smi))
            feedly_df_of_smi.to_excel(f'logs/feedly_result_{self.gu}_{self.log_data}.xlsx')
            del feedly_df_of_smi, smi_
        else:
            self.feedly_df['time'] = pd.DatetimeIndex(self.feedly_df['time'])  # время правим
            df_full = self.feedly_df[self.feedly_df['time'] >= last_date]  # чистим от старых новостей (если вдруг)
            print(f'Загрузка с {last_date}')
            if begin_db is not None:  # Если качали в прошлое (для обычной закачки это и не надо)
                df_full = df_full[df_full['time'] < begin_db]
            self.feedly_df = None

        if feedly_save:  # сохраняем список feedly новостей с ссылками, если указывали это ранее
            # special writer to avoid url record restriction (65 630)
            writer_feedly = pd.ExcelWriter(f'data/saved_feedlies/{self.gu}/'
                                           f'feedly_{self.gu}_{self.log_data}.xlsx', engine='xlsxwriter',
                                           options={'strings_to_urls': False})
            df_full.to_excel(writer_feedly, sheet_name='feedly_result')
            writer_feedly.save()
            # df_full.to_excel(f'feedly_{self.gu}_{self.log_data}.xlsx')
        self._errors_to_excel()
        return df_full  # возвращаем df с ссылками далее

    def _get_data(self, list_of_tuples: list):
        """
        Функция для скачки по ссылке новостей

        :param list_of_tuples: список кортежей, в каждом ссылка, время новости и название СМИ
        :return: данные по новости в правильной кодировке
        """
        url, time_news, name = list_of_tuples
        if "https://anews.comhttps://anews.com" in str(url):
            url = url[17:]
        # коммерсант и слон дают ссылки через feedsportal.com, оттуда ничего не получим, поэтому ред. ссылки в норм вид:
        if 'feedsportal.com' in str(url):
            for smi_name in ['kommersant', 'slon0B']:
                if smi_name in url:
                    url = url[url.find(smi_name):]
            url = url.replace('0B', '.').replace('0C', '/').replace('0E', '_').replace('/story01.htm', '').replace('A',
                                                                                                                   '')
            url = 'http://www.' + url
        try:  # качаем html код
            if self.url_method == 'urlopen':
                url = url.replace('&template=main', '')  # убираем для парсинга ННТВ
                request = Request(url=url, headers=self.headers)
                soc = urlopen(request, timeout=60)
                return soc.read()
            elif self.url_method == 'request':
                try:
                    if 'www.pravda.ru' in url:  # для правды обход антипарсера
                        if self.cscraper is None:
                            self.cscraper = cloudscraper.create_scraper()
                        req = self.cscraper.get(url, allow_redirects=True)
                    else:
                        req = requests.get(url, headers=self.headers, timeout=60)
                except TimeoutError:  # пробуем через прокси если не отвечает (но прокси уже старые, не будет работать)
                    pass
                    for proxy in self.proxies_web:
                        req = requests.get(url, headers=self.headers, timeout=60, proxies=proxy)
                        if req.status_code == requests.codes['ok']:
                            break
                encoding = req.encoding  # кодировку получаем скаченного
                if 'www.pravda.ru' in url:  # Для правды ее неправильно передает
                    encoding = 'utf-8'
                res = req.content.decode(encoding, errors='ignore')  # декодируем
                if res.find('а') == -1:  # проверка на правильность кодировки такая, по букве)
                    res = req.content.decode('utf-8', errors='ignore')
                    if res.find('а') == -1:
                        res = req.content.decode('windows-1251', errors='ignore')
                return res
            else:
                raise Exception('Unknown method')
        # далее всякие проверки:
        except (error.HTTPError, error.URLError) as e:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem=e.reason, link=url, serious=1, error_group='http error')
            return ''
        except (socket.timeout, requests.exceptions.Timeout):
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem='timeout', link=url, serious=1, error_group='http error')
            return ''
        except http.client.HTTPException:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem='HTTPException', link=url, serious=1, error_group='http error')
            return ''
        except AttributeError:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem="'float' object has no attribute 'replace'", link=url, serious=1,
                                    error_group='http error')
            return ''
        except UnicodeEncodeError:
            try:
                self._refresh_df_errors(time_news=time_news, smi=name,
                                        problem="ошибка декодирования,", link=url, serious=1,
                                        error_group='http error')
            except:
                self._refresh_df_errors(time_news=time_news, smi=name,
                                        problem="Не может декодировать символ", link='Запись URL невозможна', serious=1,
                                        error_group='http error')
            return ''

        except ConnectionResetError:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem="Удаленный хост принудительно разорвал существующее подключение", link=url,
                                    serious=1, error_group='http error')
            return ''
        except ssl.SSLWantReadError:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem="ssl.SSLWantReadError: The operation did not complete (read)", link=url,
                                    serious=1, error_group='http error')
            return ''
        except requests.exceptions.ConnectionError:
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem='[WinError 10060] Попытка установить соединение была безуспешной',
                                    link=url, serious=1, error_group='http error')
            return ''
        except:  # Уже просто все остальное
            self._refresh_df_errors(time_news=time_news, smi=name,
                                    problem="проблема с получением html", link=url,
                                    serious=1, error_group='http error')
            return ''

    def _download_full_text(self, all_news_info: pd.DataFrame, failure, multiprocess_num=20):
        """
        Вспомогательная функция для get_news_for_db. Обрабатывает и качает новости по батчу из списка feedly.

        :param all_news_info: Данные с feedly и ссылка
        :param failure: сколько не загрузилось. Убрали параметр
        :param multiprocess_num: сколько одновременно обрабаывается
        :return: обновленный датафрейм, где добавлены тексты новостей: полные новости, для чтения и с цифрами
        """
        urls = list(all_news_info['hyperlink'])
        time_list = list(all_news_info['time'])
        feedname_list = list(all_news_info['feed_name'])
        zip_get_data = zip(urls, time_list, feedname_list)
        del urls, time_list, feedname_list
        pool = ThreadPool(multiprocess_num)  # это для мультипроцессинга
        results = list(pool.imap(self._get_data, zip_get_data))  # выполняем закачку одновременно по 20 делает
        pool.close()
        pool.join()
        del zip_get_data
        # списки для новостей с 3 видами обработки:
        res_for_read, res_with_num, res,  result_url = [], [], [], []
        for i in range(len(results)):
            name_i = all_news_info.iloc[i]['feed_name']
            if len(results[i]) == 0:  # если ничего не вернул
                result_url.append('no page code')
            else:
                result_url.append('yes page code')
            try:
                if self.url_method == 'urlopen':
                    results_decode = results[i].decode(self.feedly_open[name_i]['encoding'])
                else:
                    results_decode = results[i]
                # Далее ищем в коде текст новости:
                if 'method_extract' in self.feedly_open[name_i].keys():
                    method_extract = self.feedly_open[name_i]['method_extract']
                else:
                    method_extract = 'xpath'
                # этот метод чище берет текст, избегая большие куски мусора, но при этом код сложнее.
                # и не всегда работает, т.к. текст может быть не только в тэге. Но даже так, часто bs не захватывает
                # мусор, как при xpath, который может брать и лишние куски (видимо из-за ::before ::after)
                if method_extract == 'bs':
                    tag, attr_name, attr_value, child = self.feedly_open[name_i]['xpath'].split('|')
                    soup = BeautifulSoup(results_decode)
                    if child == 'all':
                        text = soup.find(tag, attrs={attr_name: attr_value}).text
                    else:
                        soup = soup.find(tag, attrs={attr_name: attr_value}).find_all(child)
                        text = ' '.join([single_tag.text for single_tag in soup])
                elif method_extract == 'bs_re':
                    tag, attr_name, attr_value = self.feedly_open[name_i]['xpath'].split('|')
                    soup = BeautifulSoup(results_decode)
                    text = soup.find(tag, attrs={attr_name: re.compile(rf'^{attr_value}')}).text
                else:
                    tree = html.fromstring(results_decode)
                    text = tree.xpath(self.feedly_open[name_i]['xpath'])
                    if len(text) == 0:
                        text = tree.xpath(self.feedly_open[name_i]['xpath2'])
                    try:
                        n_elem_same = int(self.feedly_open[name_i]['n_elem_same_xpath'])
                    except ValueError:
                        n_elem_same = 0
                    try:
                        text = etree.tostring(text[n_elem_same], method='xml', encoding='unicode')
                    except IndexError:
                        res.append('')
                        res_for_read.append('')
                        res_with_num.append('')
                        self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                                smi=name_i,
                                                problem=f"не найден данный xpath {self.feedly_open[name_i]['xpath']}",
                                                link=all_news_info.iloc[i]['hyperlink'], serious=1,
                                                error_group='xpath error')

                try:  # чистим и сохраняем текст в трех видах: res_for_read, res_with_num, res
                    cleared_text = re.sub('<.*?>|\n|&#13;|\t|\r|{([\s\S]+?)}|[^А-Яа-яёЁ0-9\s?!.,]|{.*?}', " ", text)
                    cleared_text = cleared_text.replace('ё', 'е')
                    cleared_text = cleared_text.replace('ВИДЕОРЕКЛАМА', '').replace('СЮЖЕТ', '')
                    cleared_text = cleared_text.replace('Ролик просмотрен', '').replace('\xa0', ' ').replace('↓↓↓', '')
                    cleared_text = cleared_text.split()
                    cleared_text = ' '.join(cleared_text)
                    cleared_text = cleared_text.replace(', . , , ,', '').replace('Загрузка... .123 ., ! ,', '')
                    cleared_text = cleared_text.strip()
                    cleared_text = cleared_text.replace('Если вы нашли ошибку пожалуйста выделите '
                                                        'фрагмент текста и нажмите', '')
                    res_for_read.append(cleared_text)
                    cleared_text = re.sub('[^А-Яа-я0-9\s]', " ", cleared_text)
                    cleared_text = cleared_text.split()
                    cleared_text = ' '.join(cleared_text)
                    res_with_num.append(cleared_text)
                    cleared_text = re.sub("[0-9]", "", cleared_text)
                    cleared_text = cleared_text.split()
                    cleared_text = ' '.join(cleared_text)
                    res.append(cleared_text)
                    self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                            smi=name_i,
                                            problem=f"OK",
                                            link=all_news_info.iloc[i]['hyperlink'], serious=2,
                                            error_group='OK')

                except IndexError:
                    res.append('')
                    res_for_read.append('')
                    res_with_num.append('')
                    self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                            smi=name_i,
                                            problem=f"не найден данный xpath {self.feedly_open[name_i]['xpath']}",
                                            link=all_news_info.iloc[i]['hyperlink'], serious=1,
                                            error_group='xpath error')
                except TypeError:
                    # res.append('')
                    # res_for_read.append('')
                    # res_with_num.append('')
                    pass

            except AttributeError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"по ссылке не получено новости",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1,
                                        error_group='http error(excessive)')
            except UnicodeDecodeError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"ошибка декодирования",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1, error_group='xpath error')
            except lxml.etree.XPathEvalError:
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                self._refresh_df_errors(time_news=all_news_info.iloc[i]['time'],
                                        smi=name_i,
                                        problem=f"Invalid predicate: {self.feedly_open[name_i]['xpath']}",
                                        link=all_news_info.iloc[i]['hyperlink'], serious=1, error_group='xpath error')
            except lxml.etree.ParserError:
                pass
                res.append('')
                res_for_read.append('')
                res_with_num.append('')
                # здесь ошибки связаны с получением html кода, они уже записаны на этапе обработки страницы в _get_data

        all_news_info['full_news'] = res
        all_news_info['news_for_reading'] = res_for_read
        all_news_info['full_news_with_nums'] = res_with_num
        all_news_info = all_news_info[all_news_info['full_news'] != '']
        del res, res_for_read, res_with_num

        return all_news_info

    def _errors_to_excel(self):
        """
        Функция записи ошибок в excel

        :return: эксель файл с записанными ошибками
        """
        try:
            start_row = self.writer.sheets['Sheet1'].max_row
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=start_row,
                                                  header=False)
        except ValueError:
            self.writer = pd.ExcelWriter(f'logs/errors_{self.gu}_{self.log_data}_{self.error_i}.xlsx',
                                         engine='openpyxl')
            self.error_i += 1
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=0)
        except KeyError:
            pd.DataFrame(self.df_errors).to_excel(self.writer, index=False, startrow=0)
        self.writer.save()
        self.df_errors = []

    def get_news_for_db(self, stop='', feedly_save=True, dump_sources=True):
        """
        Основная функция закачки. Все функции используются здесь или через другие функции внутри.

        :param dump_sources: save every source saved_feedlies in pkl format
        :param feedly_save: сохраняет полученные данные из feedly в excel
        :param stop: дата в формате '2020-01-01'. Если не задана, то качает новости включая сегодняшний день с самой
        последней даты
        :return: добавляет в базу новости
        """
        last_10_accuracy = 1
        failure = 0
        news = self.get_news(feedly_save=feedly_save, dump_sources=dump_sources)  # получаем новости
        news.sort_values(by=['time'], inplace=True)  # сортируем по времени, чтоб от старых добавлять
        parts = int(news.shape[0] / self.batch_size) + 1  # считаем количество батчей
        batch = 0
        print("Процесс скачивания новостей")
        start_time = time.time()
        first_batch = True
        num_new_todb = 0
        if news.shape[0] > 0:  # если новости есть
            for part_i in range(0, news.shape[0], self.batch_size):  # цикл по батчам
                batch += 1
                start_batch = time.time()
                part_df = news.iloc[part_i:part_i + self.batch_size]
                get_part = self._download_full_text(part_df, failure,
                                                    multiprocess_num=20)  # качаем новость и получаем текст
                if self.gu != 'VVGU' and self.gu != 'SZGU' and self.gu != 'UGU':
                    get_part = get_part[['title', 'feed_name', 'time', 'full_news', 'region',
                                         'full_news_with_nums', 'news_for_reading', 'key_word']]
                    get_part.columns = ['title', 'paper', 'crawled', 'fulltext', 'region',
                                        'fulltext_with_nums', 'news_for_reading', 'key_word']
                else:
                    get_part = get_lem_text(get_part, gu=self.gu)  # добавляем столбец с лемматизацией
                result_message = add_data_to_sql(get_part, first_batch=first_batch, gu=self.gu)  # кидаем в БД
                delta_all = time.time() - start_time
                delta_part = time.time() - start_batch

                if result_message == 0:
                    print('\nОшибка добавления данных в базу.')
                    return result_message
                elif result_message == 1:
                    print('\nБаза уже содержит актуальные данные')
                    return result_message
                else:
                    num_new_todb += len(get_part)
                failure += (self.batch_size - len(get_part))
                downloaded = part_i + self.batch_size
                if downloaded > news.shape[0]:
                    downloaded = news.shape[0]
                time_all = str(timedelta(seconds=delta_all)).split(".")[0]
                time_batch = str(timedelta(seconds=delta_part)).split(".")[0]
                stdout.write(f'\rСкачан {batch=} from {parts}. {downloaded=} from {news.shape[0]} ({failure=}). '
                             f'{time_all=} ({time_batch=}) in base: {len(get_part)}, (всего - {num_new_todb})')
                stdout.flush()
                first_batch = False
                # добавляю ошибки в excel файл append и очищаю self.df_errors, чтобы не копилось
                # в первой записи будут уже все ошибки с feedly и ошибки с закачкой тоже. будет записывать каждые 5
                # батчей, чтобы слишком часто не делать эти операции, но в тоже время не накапливать много ошибок в
                # памяти:
                if batch % 5 == 0:
                    self._errors_to_excel()
                    if self.acc_control and last_10_accuracy < 0.9:
                        return 'Bad_accuracy'
            if len(self.df_errors) > 0:
                self._errors_to_excel()
            self.writer.close()
            print(f'\nПроцент полученных новостей: {"{:.2%}".format(num_new_todb / news.shape[0])}')

            return 'OK'
        else:
            print(f'Пустой список новостей, но длина новостей = {len(news)}')
            return 0

