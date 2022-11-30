import json
import re  # чистка текста
from sys import stdout
import warnings
import logging  # почитать: https://khashtamov.com/ru/python-logging/
import locale  # локализация написания цифр, валют и пр (точка или запятая и др)
import os
import numpy as np
import pandas as pd
import tidytext  # токенизация и пр. работа с текстом
from src.sql_interection import add_data_to_dashboard_sql, get_sql_data, get_fed_data  # Наши ф-ции для дэшборда
from modules.preprocess import NER_dashboard  # Наша ф-ция для дэшборда -- каж., в PowerBI
from navec import Navec  # предобученный эмбеддинг, почитать: https://natasha.github.io/navec/
import pickle
from tensorflow.keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing.text import tokenizer_from_json
# from src.cosine import cosine_matrix  # для PowerBI -- пока не используется
from psycopg2.extensions import register_adapter, AsIs
import yaml


warnings.filterwarnings('ignore')
logging.getLogger('tensorflow').disabled = True
locale.setlocale(locale.LC_ALL, '')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'


with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


def addapt_numpy_array(numpy_array):
    """
    Чтобы не было сюрпризом для postgres

    :param numpy_array:
    :return: адаптируемый формат файла
    """
    return AsIs(tuple(numpy_array))


def get_pivot_count(name, date_update, gu):
    """
    Подсчет суммарного кол-ва новостей по региону (только для ВВГУ и СЗГУ)

    :param name: 'cbr', 'inflation', 'news'
    :param date_update: дата обновления
    :param gu: 'VVGU', 'SZGU'
    :return: df с регионами и соответствующим числом новостей в них
    """
    dict_region = pd.read_excel(config['DASHBOARD']['DICT_REGION'][gu])
    dict_region = dict(zip(dict_region.right_region, dict_region.region))
    df = pd.read_excel(f'data/gu_proc_files/{gu}/processed_files/{name}/{name} {date_update}.xlsx')
    df.replace({'region': dict_region}, inplace=True)
    if name == 'news':
        df = df.groupby('region').count()['date']  # группируем по региону и считаем, сколько новостей на каждую дату
    else:
        df = df.groupby('region').count()['crawled']  # аналогично, только название даты - crawled
    df.loc[rus_eng_gu(gu)] = df.sum()
    df = df.to_frame()

    return df


def rus_eng_gu(gu):
    if gu == 'VVGU':
        return 'ВВГУ'
    elif gu == 'SZGU':
        return 'СЗГУ'
    elif gu == 'UGU':
        return 'УГУ'
    elif gu == 'YUGU':
        return 'ЮГУ'
    elif gu == 'DGU':
        return 'ДГУ'
    elif gu == 'CFO':
        return 'ЦФО'
    elif gu == 'SGU':
        return 'СГУ'


def get_pivot_news(news, gu):
    """
    Подсчет количества новостей в регионе в категории (проды, непроды, услуги). Только для ВВГУ и СЗГУ

    :param news: 'cbr', 'inflation', 'news'
    :param gu: 'VVGU', 'SZGU'
    :return: df с кол-вом новостей в регионе по определенной категории
    """
    region_df = pd.read_excel(config['DASHBOARD']['DICT_REGION'][gu])
    dict_region = dict(zip(region_df.right_region, region_df.region))
    types = ['Продовольственные товары', 'Непродовольственные товары', 'Услуги']
    for kind in types:
        news_type = news[news['component'] == kind]  # берем срез новостей по категории
        news_type.replace({'region': dict_region}, inplace=True)
        news_type = news_type.groupby('region').count()['id']  # группируем по региону и считаем сколько новостей
        # в данной категории
        region_news = news_type.index
        for region in region_df.region:
            if region not in region_news:
                news_type.loc[region] = 0
        # news_type.sort_values('region', inplace=True)
        news_type.loc[rus_eng_gu(gu)] = news_type.sum()
        if 'df' in locals():
            news_type = news_type.to_frame()
            df[kind] = news_type
        else:
            news_type = news_type.to_frame()
            df = news_type
    df.rename(columns={'id': 'Продовольственные товары'}, inplace=True)

    return df


def create_folder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)


def vectorize_news(news_list):
    """
    Эмбеддинг новостей

    :param news_list: лист с новостями
    :return: лист с новостями, где слова - цифры (то есть на языке компьютера)
    """
    news_lem = []
    path = config['MODELS']['VECTORIZER']
    navec = Navec.load(path)  # эмбеддинг
    for i in range(0, len(news_list), 1000):  # срез по 1000 новостей
        news_batch = news_list[i:i + 1000].astype(str)
        text = ' div '.join(news_batch)  # объединение 1000 текстов в 1 для ускорения процесса
        # text = stem.lemmatize(text)
        text = ''.join(text)
        text = text.replace('\n', '')  # убираем переносы строк
        text = re.sub('[^A-Za-zА-Яа-я\s]', '', text)  # чистим от всего лишнего (знаки и пр)
        text = text.replace('  ', ' ')  # удаляем двойные пробелы
        text = text.split('div')  # разрезаем объединенный текст на отдельные новости
        news_lem.extend(text)
    news = [news.strip().lower().split(' ') for news in news_lem]  # разделяем новости на отдельные слова
    news = [np.mean([navec[word] for word in sent if word in navec], axis=0) for sent in news]  # эмбеддинг слов

    return news


# Основное
class dashboard_update:
    def __init__(self, gu):
        """
        Задаем все нужное для организации новостей и представления их в дашборде: модели, фичи и тд из config.yaml

        :param gu: 'VVGU', 'SZGU', 'fed'
        """
        self.gu = gu
        inflation_model = config['MODELS']['INFLATION']
        with open(inflation_model['INF_FILTER'], 'r', encoding='utf-8') as filt_file:
            self.filt = filt_file.read()
        with open(inflation_model['EXCLUSION_FILTER'], 'r', encoding='utf-8') as filt_file_model:
            self.filt_predict = filt_file_model.read()
        with open(inflation_model['STOP_WORDS'], 'r') as stops:
            self.stop_words = stops.read()
        with open(inflation_model['PROD_FILTER'], 'r', encoding='utf-8') as f:
            self.prod = f.read()
        with open(inflation_model['NPROD_FILTER'], 'r', encoding='utf-8') as f:
            self.nprod = f.read()
        with open(inflation_model['SERVICES_FILTER'], 'r', encoding='utf-8') as f:
            self.services = f.read()
        self.categories = pd.read_excel(inflation_model['CATEGORIES'])
        # model ----
        self.model = load_model(inflation_model['CNN'])
        with open(inflation_model['TOKENIZER']) as f:
            data = json.load(f)
            self.tokenizer = tokenizer_from_json(data)
        self.trash = pickle.load(open(inflation_model['TRASH_PHRASES'], 'rb'))
        self.trash_lem = pickle.load(open(inflation_model['TRASH_PHRASES_LEM'], 'rb'))
        if self.gu != 'fed':
            self.dict_region = pd.read_excel(config['DASHBOARD']['DICT_REGION'][self.gu])
            self.dict_region = dict(zip(self.dict_region.right_region, self.dict_region.region))
            self.region_shares = pd.read_excel(config['DASHBOARD']['REGION_SHARES'][self.gu])
        self.NER = NER_dashboard()  # from preprocess, каж. для PowerBI

    def get_period_data(self, start, finish):
        """
        Подсчет кол-ва новостей по регионам и ГУ за определенные даты (или дату)

        :param start: дата начала (задаем в main)
        :param finish: дата окончания (задаем в main)
        :return: два df со списками новостей по нужным датам и df с кол-вом новостей в разрезе регионов (и сумма по ГУ)
        """
        if self.gu == 'fed':
            df_sql = get_fed_data(start=start, finish=finish)  # Берет данные из БД и возвращает в формате датафрейма
            # count all
            df_sql_query = df_sql[['region', 'news_for_reading']]  # Берем только регион и новость
            df_sql_query = df_sql_query.groupby('region').count()['news_for_reading']  # Группируем по региону
            # и считаем сколько новостей в нем
            df_sql_query = df_sql_query.to_frame()
            df_sql_query['region'] = df_sql_query.index
        else:
            df_sql = get_sql_data(start=start, finish=finish, gu=self.gu)  # Берет данные из БД и возвращает
            # в формате датафрейма
            # count all ----
            df_sql_query = df_sql[['region', 'fulltext']]  # Берем только регион и полный текст новости
            df_sql_query = df_sql_query.groupby('region').count()['fulltext']  # Кол-во новостей по регионам
            df_sql_query.loc[rus_eng_gu(self.gu)] = df_sql_query.sum()  # получаем сумму по gu (суммируем кол-ва по рег)
            df_sql_query = df_sql_query.to_frame()
            df_sql_query['region'] = df_sql_query.index  # создаем колонку из индексов
            df_sql_query.replace({'region': self.dict_region}, inplace=True)
            df_sql_query.index = df_sql_query['region']
        df_sql_query.columns = ['all', 'region']
        df_sql_query['crawled'] = str(start)[0:7]

        # preparing files for dashboard ----
        df_sql['id'] = df_sql.index
        # убираю мусор, который ранее при обработке мог попасть в дэшборд:
        df_sql['stemmed'] = df_sql['stemmed'].str.replace(self.trash_lem, '', regex=True)  # Удаляем мусорные фразы
        # Из Мурманска куча ерунды а-ля архив новостей с описанием всякого
        df_sql = df_sql[~df_sql['news_for_reading'].str.contains('Архив новостей')]  # Удаляем новости в которых
        # "Архив новостей"

        return df_sql, df_sql_query  # Возвращает список новостей и количество новостей в разрезе регионов

    def prepare_predict_data(self, inf_news, data):
        """
        Выделяем части текста с инфляционными единицами (ИЕ). Отбираем новости, где число уникальных товарных групп
        в новости меньше 5 (другие новости бесполезны для нас)

        :param inf_news: датафрейм с новостями
        :param data: месяц и год обновления дашборда
        :return: 2 практически одинаковых датафрейма: df_predict (содержит столбец с кол-вом уникальных субов в новости)
        и merged_df без этого столбца, а все остальное такое же
        """
        filt_model = config['MODELS']['INFLATION']['PHRASE_CUT']
        prediction = inf_news['stemmed'].str.extractall(filt_model)  # Выделяем 1-50 символов вокруг ключевых слов
        # из config -- не просто символы, а слова, т.е. если полных слов на 53 символа, то целое последнее слово
        # удалиться, и тогда символов будет меньше 50

        # prediction.to_excel(f"models/extracts/extracts {data}.xlsx", index=False)
        prediction = prediction.droplevel(level=1)  # удаляется порядковый номер куска в новости
        prediction['id'] = prediction.index
        prediction.columns = ['filter', 'word_filt', 'id_filt']
        # добавляю к предложениям куски
        merge_df = pd.merge(inf_news, prediction, how='right', left_on='id',
                            right_on='id_filt', suffixes=('', '_filter'))  # Возвращаем кусок к полной новости, но
        # в отдельном столбце, т.е. показываем к какой полной новости относится данный кусок.
        # Убираем все фразы с переносным значением:
        del inf_news
        merge_df['filter'] = merge_df['filter'].fillna('')
        merge_df = tidytext.unnest_tokens(merge_df, 'words', 'filter', drop=False)  # Выносит каждое слово в отдельную
        # строку в новую колонку
        merge_df = pd.merge(merge_df, self.categories, how='left', left_on='words',
                            right_on='word')  # Добавляет категории к каждой новости по ключевым словам
        merge_df = merge_df[merge_df['sub'].notnull()]
        merge_df = merge_df[merge_df['sub'] != '']
        merge_df['id'] = merge_df.index

        table_count = merge_df.groupby(['id', 'sub'], as_index=False).count()  # Отбираем уникальные субы
        table_count = table_count[['id', 'sub', 'component']]  # отбираем только нужные столбцы
        table_count.columns = ['id', 'sub_2', 'n']  # переименовываем столбцы
        merge_df = pd.merge(merge_df, table_count, how='left', left_on=['id', 'sub'],
                            right_on=['id', 'sub_2'])  # весь файл с категориями сопоставляем с уникальными субы
        if self.gu == 'fed':
            merge_df = merge_df[['title', 'id', 'paper', 'crawled', 'region', 'sub', 'component',
                                 'news_for_reading', 'n', 'filter']]
            merge_df.drop_duplicates(subset=['news_for_reading', 'region', 'sub'], inplace=True)  # удаляем дубликаты
            # Фильтр на те новости, где просто все группы товаров подряд описывают. Примерно взял, больше 5 чаще будет
            # просто бесполезная статистика:
            subs_in_news = merge_df.groupby('news_for_reading', as_index=False).count()[['news_for_reading', 'sub']]
            # группируем таблицу по уникальным новостям, берем только столбцы с новостями и товарными группами
            subs_in_news = subs_in_news[subs_in_news['sub'] <= config['DASHBOARD']['IN_ROW']]  # собственно наш фильтр
            merge_df = pd.merge(merge_df, subs_in_news, how='right', left_on='news_for_reading',
                                right_on='news_for_reading')  # отбираем только новости, где <=5 групп товаров, из df
        else:  # для ВВГУ и СЗГУ
            merge_df = merge_df[['title', 'id', 'paper', 'crawled', 'fulltext', 'region', 'sub', 'component',
                                 'news_for_reading', 'n', 'filter']]
            merge_df.drop_duplicates(subset=['fulltext', 'region', 'sub'], inplace=True)  # Удаляем дубликаты
            # (где категории у кусков совпадают)
            subs_in_news = merge_df.groupby('fulltext', as_index=False).count()[['fulltext', 'sub']]
            subs_in_news = subs_in_news[subs_in_news['sub'] <= config['DASHBOARD']['IN_ROW']]  # Удаляем где кол-во
            # субов в 1 новости больше 5
            merge_df = pd.merge(merge_df, subs_in_news, how='right', left_on='fulltext', right_on='fulltext')  # Удалим
            # из исходных данных все новости где кол-во субов в 1 новости больше 5

        df_predict = merge_df.copy()
        df_predict['filter'] = df_predict['filter'].str.replace(self.filt_predict, ' ', regex=True)
        # df_predict = merge_df[~merge_df['filter'].str.contains(filt_predict, regex=True)]
        # убираю стоп-слова:
        df_predict['filter'] = df_predict['filter'].str.replace(self.stop_words, ' ', regex=True)

        if self.gu == 'fed':
            merge_df.columns = ['title', 'id', 'paper', 'date', 'region', 'sub', 'component',
                                'news_for_reading', 'n', 'inf_part', 'n_in_news']
            merge_df = merge_df[['title', 'id', 'paper', 'date', 'region', 'sub', 'component', 'news_for_reading',
                                 'n', 'inf_part']]  # без столбца кол-ва субов в новости
        else:  # для ВВГУ и СЗГУ
            merge_df.columns = ['title', 'id', 'paper', 'date', 'text', 'region', 'sub', 'component',
                                'news_for_reading', 'n', 'inf_part', 'n_in_news']
            merge_df = merge_df[['title', 'id', 'paper', 'date', 'text', 'region', 'sub', 'component',
                                 'news_for_reading', 'n', 'inf_part']]  # без столбца кол-ва субов в новости

        return df_predict, merge_df  # df предикт содержит доп. кол-во субов в новости

    def dashboard_newscount_update(self, news, data, inflation, inflation_parts, cbr, all_num):
        """
        Считаем кол-во новостей по отдельным категориям (ЦБ, инфл и пр) в разрезе и нет регионов

        :param news: фрейм с числом новостей в разрезе регионов
        :param data: дата новостей (берется общий месяц)
        :param inflation: фрейм с числом инфляционных новостей в разрезе регионов
        :param inflation_parts: фрейм с частью, где есть ИЕ
        :param cbr: фрейм с числом новостей про ЦБ в разрезе регионов
        :param all_num: фрейм с новостями с цифрами
        :return: добавление в БД данные для дашборда
        """
        if self.gu == 'fed':
            types = ['Продовольственные товары', 'Непродовольственные товары', 'Услуги']
            news_df = None
            for kind in types:
                news_type = news[news['component'] == kind]
                news_type = news_type.groupby('region').count()['id']  # считаем кол-во новостей в разрезе региона
                if news_df is not None:
                    news_type = news_type.to_frame()
                    news_df[kind] = news_type  # пополняем df
                else:
                    news_type = news_type.to_frame()
                    news_df = news_type  # записываем df
            news_df.rename(columns={'id': 'Продовольственные товары'}, inplace=True)

            del news_type
        else:  # для ВВГУ и СЗГУ
            news_df = get_pivot_news(news, self.gu)

        news_df['inflation'] = inflation
        news_df['inflation_parts'] = inflation_parts
        news_df['cbr'] = cbr
        news_df['all'] = all_num
        news_df['month'] = data
        if self.gu == 'fed':
            news_df['region'] = 'федеральный'
        else:
            news_df['region'] = news_df.index
        news_df = news_df[
            ['region', 'cbr', 'all', 'inflation', 'inflation_parts', 'Продовольственные товары',
             'Непродовольственные товары', 'Услуги']]  # только нужные столбцы
        news_df.fillna(0)  # заполняем пустые строки
        add_data_to_dashboard_sql(news_df, table_name='news_count', data=data, gu=self.gu)  # добавляем df в БД

    def dashboard_news_update(self, data):
        """
        Дополняем данные по новостям. Загружаем данные в дашборд

        :param data: дата обновления БД
        :return: 4 файла df: новости с добавлением месяца обновления и правильным наименованием регионов; число новостей
        в разрезе регионов по инфляционным новостям, новостям о ЦБ и новостям с ИЕ. А также загрузка в БД новости, прой-
        дящие эмбеддинг и очищенные
        """
        if self.gu == 'fed':
            inflation = pd.read_excel(f'data/gu_proc_files/{self.gu}/processed_files/inflation/inflation {data}.xlsx')
            inflation = inflation.groupby('region').count()['crawled']  # считаем кол-во инфляционных новостей
            # в разрезе регионов
            inflation = inflation.to_frame()

            inflation_parts = pd.read_excel(f'data/gu_proc_files/{self.gu}/processed_files/news/news {data}.xlsx')
            inflation_parts = inflation_parts.groupby('region').count()['date']  # считаем кол-во инфляционных частей в
            # разрезе регионов
            inflation_parts = inflation_parts.to_frame()

            # cbr = get_pivot_count('cbr', dates[i], gu=gu)
            cbr = pd.read_excel(f'data/gu_proc_files/{self.gu}/processed_files/cbr/cbr {data}.xlsx')
            cbr = cbr.groupby('region').count()['crawled']  # считаем кол-во новостей про ЦБ в разрезе регионов
            cbr = cbr.to_frame()
        else:  # для ВВГУ и СЗГУ
            inflation = get_pivot_count('inflation', data, gu=self.gu)  # запускаем ф-цию (см. выше)
            inflation_parts = get_pivot_count('news', data, gu=self.gu)
            cbr = get_pivot_count('cbr', data, gu=self.gu)

        news = pd.read_excel(f'data/gu_proc_files/{self.gu}/processed_files/news/news {data}.xlsx')
        news['period'] = pd.to_datetime(news['date']).dt.strftime("%B")
        if self.gu != 'fed':  # для ВВГУ и СЗГУ
            news.replace({'region': self.dict_region}, inplace=True)
        news_to_list_news = news[['paper', 'title', 'region', 'sub', 'news_for_reading', 'inf_part', 'class']]
        if self.gu == 'fed':
            news_to_list_news['data'] = data
            news_to_list_news.to_excel(f'data/gu_proc_files/{self.gu}/processed_files/news_fed_{data}.xlsx')

        trash_phrases = ['. 0 .', ' . ', ' , ', ',,', ' ,', '...', ' .', 'Поделиться ссылкой', 'Вернуться назад',
                         'бизнес Поделиться ссылкой Вернуться назад Если вы заметили опечатку, выделите часть текста с '
                         'ней и нажмите', '. , . . . ',
                         'Спасибо!',
                         'Пожалуйста, выделяйте фразу с опечаткой, а не только одно неверно написанное слово.',
                         'Тег не поддерживается вашим браузером.',
                         'Нашли ошибку в тексте? Выделите ее и нажмите версия для печати',
                         'Нашли ошибку в тексте?', 'Выделите ее и нажмите версия для печати', 'Фото']
        dict_trash = dict(zip(trash_phrases, ['' for _ in range(len(trash_phrases))]))
        for part, _ in dict_trash.items():  # чистим от мусорных фраз
            news_to_list_news['news_for_reading'] = news_to_list_news['news_for_reading'].str.replace(part, _,
                                                                                                      regex=False)
        news_to_list_news["news_for_reading"] = news_to_list_news["news_for_reading"].str.lstrip('0123456789.- ,')
        # Убрать в начале строки служебные символы

        # добавляю столбец с HTML кодом NER:
        news_to_list_news['NER'] = news_to_list_news.apply(self.NER.nlp_func, axis=1)
        news_to_list_news['vector'] = vectorize_news(news_to_list_news['news_for_reading'])

        add_data_to_dashboard_sql(news_to_list_news, table_name='news', data=data, gu=self.gu)

        return news, inflation, inflation_parts, cbr

    def dashboard_infshares_update(self, data, news, inflation_parts):
        """
        Только для 'fed'. Считаем кол-во долю новостей по субам в общем кол-ве новостей с ИЕ

        :param data: дата обновления дашборда (месяц и год)
        :param news: фрейм с новостями
        :param inflation_parts: фрейм с кол-вом всего новостей с ИЕ
        :return: обновление БД фреймом с указанием доли в феде новостей по субам
        """
        table = news.groupby(['region', 'sub'], as_index=False).count()[['region', 'sub', 'id']]  # считаем кол-во
        # новостей в разрезе регионов и субов
        if self.gu != 'fed':
            gu_table = table.groupby('sub', as_index=False)['id'].sum()[['sub', 'id']]  # только кол-во новостей
            # в разрезе субов, регион общий - фед
            gu_table['region'] = rus_eng_gu(self.gu)
            reg_table = table.groupby('region', as_index=False)['id'].sum()[['region', 'id']] # даже не используется
            table = pd.concat([table, gu_table])
        table = pd.merge(table, inflation_parts, left_on='region', right_on='region')
        table['share'] = table['id'] / table['date']  # Кол-во по субу делим на кол-во всего (название столбцов не то)
        table = table[['region', 'sub', 'share']]  # оставляем только нужное
        if self.gu == 'fed':
            table['data'] = data
            table.to_excel(f'data/gu_proc_files/{self.gu}/processed_files/sub_shares_fed_{data}.xlsx')

        add_data_to_dashboard_sql(table, table_name='inf_shares', data=data, gu=self.gu)

    def dashboard_ipi_update(self, data):
        """
        Функция считает IPI общий для фед и взвешенный через долю в ВРП 2020 IPI для ВВГУ и СЗГУ

        :param data: дата обновления дашборда (месяц и год)
        :return: обновление дашборда файлом с расчетом IPI
        """
        mood_types = ['mood', 'prod', 'nprod', 'services']
        ans = []
        for mood_type in mood_types:
            if mood_type != 'mood':
                mood_type = f'{mood_type}/{mood_type}'
            df_mood = pd.read_excel(f'data/gu_proc_files/{self.gu}/processed_files/mood/{mood_type} {data}.xlsx')
            # убираем пропущенные значения в части с ИЕ:
            df_mood = df_mood[~pd.isna(df_mood['values'])]
            df_mood = df_mood[df_mood['values'] != ""]
            if self.gu == 'fed':
                pivot_mood = pd.pivot_table(df_mood, values='values', index=['class'], columns=['region'],
                                            aggfunc='count')  # считаем кол-во новостей по категориям
                pivot_mood.fillna(0, inplace=True)
                pivot_mood.loc['sum'] = pivot_mood.sum()  # всего новостей
                pivot_mood.loc['IPI'] = (pivot_mood.loc[-1] - pivot_mood.loc[1]) / pivot_mood.loc['sum']  # считаем IPI:
                # доля число проинфляционных новостей - число дезинфляционных новостей в общем числе новостей
                ans.append(pivot_mood.loc['IPI'])  # добавляем в список
            else:  # для ВВГУ и СЗГУ
                df_mood.replace({'region': self.dict_region}, inplace=True)
                pivot_mood = pd.pivot_table(df_mood, values='values', index=['class'], columns=['region'],
                                            aggfunc='count')
                pivot_mood.fillna(0, inplace=True)
                pivot_mood.loc['sum'] = pivot_mood.sum()
                pivot_mood.loc['IPI'] = (pivot_mood.loc[-1] - pivot_mood.loc[1]) / pivot_mood.loc['sum']  # Кол-во про-
                # инфл новостей - кол-во дезинф новостей /кол-во всего
                pivot_mood = pivot_mood.loc[['IPI', 1, -1]]
                pivot_mood = pivot_mood.append(self.region_shares)  # Доля в ВРП за 2020 г, индекс 0 будет
                ipi_index = pivot_mood.loc[['IPI', 0]]
                ipi_index.loc[f'weighted {mood_type}'] = ipi_index.loc['IPI'] * ipi_index.loc[0]
                ipi_index[rus_eng_gu(self.gu)] = ipi_index.sum(axis=1)  # Для подсчета по всему ГУ с учетом долей в ВРП
                if mood_type == 'mood':
                    ipi_full = pd.DataFrame(ipi_index.loc[f'weighted {mood_type}'])  # IPI всего посчитанное из каждого
                    # региона больше не используется. Вместо этого расчет IPI происходит из компонентов ИПЦ
                    # в макросе эксель дашборда
                else:
                    ipi_full[f'weighted {mood_type}'] = pd.DataFrame(ipi_index.loc[f'weighted {mood_type}'])[
                        f'weighted {mood_type}']
        if self.gu == 'fed':
            ipi_full = pd.DataFrame(ans).T
            ipi_full.columns = ['mood_all', 'prod', 'nprod', 'services']
            ipi_full['region'] = 'федеральный'
        else:
            ipi_full.columns = ['mood_all', 'prod', 'nprod', 'services']
            ipi_full['region'] = ipi_full.index
        ipi_full.fillna(0)
        add_data_to_dashboard_sql(ipi_full, table_name='IPI', data=data, gu=self.gu)

    def dashboard_db_update(self, data, all_num):
        # обновляем дэшборд: таблица news, таблица новость, кол-во инфл единиц, кол-во новостей о ЦБ по регионам
        news, inflation, inflation_parts, cbr = self.dashboard_news_update(data)
        # обновляем дэшборд: таблицу news_count. Берет все числам по продам, непродам, услугам и сохр в базу
        # кол-во новостей по категориям
        self.dashboard_newscount_update(news, data, inflation, inflation_parts, cbr, all_num)
        # обновляем дэшборд таблицу inflation_parts: считается доля суба в новостях каждого региона
        self.dashboard_infshares_update(data, news, inflation_parts)
        # обновляем дэшборд таблицу IPI
        self.dashboard_ipi_update(data)

    def prepare_data_dashboard(self, start, finish):
        """
        Отбираем инфляционные новости, новости о ЦБ

        :param start: format 'yyyy-mm-dd'
        :param finish: format 'yyyy-mm-dd'
        :param gu: 'VVGU'
        :return:
        """
        register_adapter(np.ndarray, addapt_numpy_array)
        dates = [d.strftime('%Y-%m') for d in pd.date_range(start=start, end=finish, freq='MS')]
        for i, _ in enumerate(dates):
            if i == len(dates) - 1:
                break
            stdout.write(f'\rобработка даты {dates[i]}')
            stdout.flush()
            # create_folder(f'{gu}/for_dashboard/{dates[i]}/')

            data = str(dates[i])[0:7]
            df_sql, df_sql_query = self.get_period_data(start=f'{dates[i]}-01', finish=f'{dates[i + 1]}-01')
            all_num = df_sql_query['all']
            del df_sql_query

            # обработка с получением списка инфл. новостей ----
            df_sql = df_sql[~df_sql['stemmed'].isna()]
            df_sql['stemmed'] = df_sql['stemmed'].str.replace('в связь с ', '')  # попадает в категорию связи потому что
            inf_news = df_sql[df_sql['stemmed'].str.contains(self.filt, regex=True)]  # отбирает инфляционные новости:
            # если в новости есть слово из списка - значит инфляционная
            if self.gu == 'fed':
                inf_news = inf_news[['title', 'id', 'paper', 'stemmed', 'crawled', 'region', 'news_for_reading']]
            else:
                inf_news = inf_news[['title', 'id', 'paper', 'fulltext', 'stemmed', 'crawled', 'region',
                                     'news_for_reading']]
            inf_news.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/inflation/inflation {data}.xlsx",
                              index=False)  # сохранили инфл новости в эксель

            # обработка с получением списка цб новостей ----
            filt_cbr = ' цб |центробанк|центральный банк | банк россия|ключевой ставка|денежнокредитный политика'
            cbr_news = df_sql[df_sql['stemmed'].str.contains(filt_cbr, regex=True)]
            if self.gu == 'fed':
                cbr_news = cbr_news[['id', 'stemmed', 'crawled', 'region', 'news_for_reading']]
            else:
                cbr_news = cbr_news[['id', 'fulltext', 'stemmed', 'crawled', 'region', 'news_for_reading']]
            cbr_news['id'] = cbr_news.index
            cbr_news.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/cbr/cbr {data}.xlsx", index=False)
            del df_sql, cbr_news, filt_cbr

            # здесь отбираем куски, коннектим куски с текстом, чистим от стоп-слов
            df_predict, merge_df = self.prepare_predict_data(inf_news, data)

            # блок классификации ----
            X = self.tokenizer.texts_to_sequences(df_predict['filter'].to_list())  # Формируется матрица.
            # Присваиваем слову номер: первое -1 номер, второе -2 и тд
            X = pad_sequences(X, padding='post', maxlen=30)  # Увеличивает длину до 30, даже если было меньше
            y_pred = self.model.predict(X).argmax(axis=1)  # Добавляем к каждой новости классы

            df_predict['class'] = y_pred
            merge_df['class'] = y_pred

            df_predict.loc[df_predict['class'] == 2, 'class'] = -1
            if self.gu == 'fed':
                df_predict = df_predict[['filter', 'class', 'id', 'crawled', 'news_for_reading', 'region', 'component']]
            else:
                df_predict = df_predict[['filter', 'class', 'id', 'crawled', 'fulltext', 'region', 'component']]
            df_predict.columns = ['values', 'class', 'id', 'date', 'text', 'region', 'component']

            # блок фильтра новостей от мусора ----
            merge_df['news_for_reading'] = merge_df['news_for_reading'].str.replace(self.trash, '', regex=True)
            merge_df['news_for_reading'] = merge_df['news_for_reading'].str.lstrip(',. ')

            # сохраняем в эксель
            merge_df.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/news/news {data}.xlsx", index=False)
            df_predict.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/mood/mood {data}.xlsx", index=False)
            # разбиваем на категории
            df_prod = df_predict[df_predict['component'] == 'Продовольственные товары']
            df_nprod = df_predict[df_predict['component'] == 'Непродовольственные товары']
            df_services = df_predict[df_predict['component'] == 'Услуги']

            df_prod.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/mood/prod/prod {data}.xlsx", index=False)
            df_nprod.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/mood/nprod/nprod {data}.xlsx", index=False)
            df_services.to_excel(f"data/gu_proc_files/{self.gu}/processed_files/mood/services/services {data}.xlsx",
                                 index=False)

            del merge_df, df_predict, df_prod, df_nprod, df_services

            self.dashboard_db_update(data=dates[i], all_num=all_num)  # обновляем БД

