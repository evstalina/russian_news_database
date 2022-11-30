import pandas as pd
from datetime import datetime, timedelta
import time
from tqdm import tqdm  # Индикатор прогресса
import urllib3  # Для HTTP-запросов
# from sqlalchemy import create_engine
from sys import stdout
import warnings
from modules.feedly_news import feedly_proc  # Наш class feedly_proc с ф-ями загрузки с feedly
from src.custom_parsers import vedomosti, interfax, regnum, pravda, finanz  # Наши парсеры
from modules.econom_extractor import extractor  # Наш класс для выбора только эконом. новостей
from src.sql_interection import add_in_db  # Наш класс для добавления в БД новостей
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

warnings.filterwarnings('ignore')
urllib3.disable_warnings()


class fed_news(feedly_proc):
    def __init__(self, gu, method, data_begin=None, feedly_df=None):
        """
        Вводные данные, задаем конфигурацию, фичи и пр. Тут все также, как в feedly_proc

        :param gu:
        :param method:
        :param data_begin:
        :param feedly_df:
        # :param acc_control: пока удален
        """
        super().__init__(gu=gu, method=method, data_begin=data_begin, feedly_df=feedly_df)
        # берем init из наследуемого класса
        self.extractor = extractor()

    def check_amount_feedly(self, last_date, begin_db, count=1000, method='direct_get'):
        '''
        Сейчас не используется
        Вспомог. функция, использовал, чтобы посчитать сколько в день набегает новостей в Feedly по источнику

        :param last_date:
        :param begin_db:
        :param count:
        :param method:
        :return:
        '''
        len_res_full = 0
        d = dict()
        fed_count = []
        for source in tqdm(self.feedly_open.keys()):
            one_count = 0
            len_feedid = 0
            control = True  # True пока мы получаем новости датой больше, чем last_date, last_date получаем с базы
            continuation = '100L'  # для того, чтобы в api дать информацию, от какой новости по id выдавать результат,
            # это начальный id
            while control:
                result = self._feedly_query(feed_id=self.feedly_open[source]['feedId'], continuation=continuation)
                one_count += 1
                if result:
                    first_len = len(result)
                    continuation = result[-1]['id']
                    df = pd.DataFrame(result)
                    df['published'] = df['published'].astype(str)
                    df['published'] = pd.to_datetime(df['published'], unit='ms')
                    df.to_excel('text.xlsx')
                    df = df[df['published'] > last_date]
                    if df.shape[0] < first_len:
                        control = False
                    df = df[df['published'] < begin_db]
                    len_res_full += df.shape[0]
                    len_feedid += df.shape[0]
                else:
                    control = False
            fed_count.append(one_count)
            d[self.feedly_open[source]['feedId']] = len_feedid
        a = pd.DataFrame(d.items())
        a['feed_count'] = fed_count
        a.to_excel('count_fed_day.xlsx')

        return d

    def get_news_fed(self, last_date, begin_db, feedly_save=True):
        """
        Функция для получения новостей за определенный промежуток времени для фед. части из фидли

        :param last_date: берется дата начала из main, хотя по сути (аналогия из feedly_news) должна брать последнюю
        дату загрузки
        :param begin_db: если есть, для того, чтобы качать в другую сторону. Для обычной скачки это не нужно.
        :param feedly_save: True - сохраняем наш df со ссылками в excel
        :return: датафрейм со ссылками на все новости фидли
        """
        last_date = datetime.strptime(last_date, "%Y-%m-%d")
        begin_db = datetime.strptime(begin_db, "%Y-%m-%d")
        print('Получение списка актуальных новостей')
        df_full = None
        for source in tqdm(self.feedly_open.keys()):
            # Пробегаем по 1 СМИ из всего списка, загружаем ссылки на новости для него и добавляем в df
            df_source = self._get_news_one_source(self.feedly_open[source]['feedId'], source, last_date,
                                                  begin_db=begin_db)
            df_source['region'] = self.feedly_open[source]['region']
            df_source['feed_name'] = source
            # Убираем все новости, которые по итогу видео. Чаще всего в url это явно задано. Их все равно не спарсим,
            # так как видео:
            df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('video')]
            # Убираем и спорт, так как часто верстка у них особая, бесполезные новости для нас:
            df_source = df_source[~df_source['hyperlink'].astype(str).str.contains('sport')]
            if df_full is None:
                df_full = df_source
            else:
                df_full = df_full.append(df_source, ignore_index=True)
        del df_source
        if feedly_save:
            # special writer to avoid url record restriction (65 630 urls in one excel file):
            writer_feedly = pd.ExcelWriter(f'data/saved_feedlies/fed/feedly_{self.gu}_{str(begin_db)[:10]}_'
                                           f'{str(last_date)[:10]}.xlsx', engine='xlsxwriter',
                                           options={'strings_to_urls': False})
            df_full.to_excel(writer_feedly, sheet_name='feedly_result')
            writer_feedly.save()
        self._errors_to_excel()
        return df_full  # Датафрейм со всеми ссылками на новости

    def get_news_customs(self, start, end):
        """
        Часть получения списка новостей по тем источникам, которых нет в Feedly, но есть свой у нас парсер.
        Вызывает кастомные парсеры

        :param start: дата начала (задаем сами в main)
        :param end: дата окончания (задаем сами в main)
        :return: Лист листов со ссылками на новости с парсеров по 5 источников (посмотри, один из них выкл)
        """
        customs_config = config['CUSTOM_PARSERS']
        list_concat = []
        if customs_config['VEDOMOSTI']:
            stdout.write(f'\rВедомости')
            ved = vedomosti(start=start, end=end)
            list_concat.append(ved)
        if customs_config['INTERFAX']:
            stdout.write(f'\rИнтерфакс')
            inter = interfax(start=start, end=end)
            list_concat.append(inter)
        if customs_config['REGNUM']:
            stdout.write(f'\rРегнум')
            reg = regnum(start=start, end=end)
            list_concat.append(reg)
        if customs_config['PRAVDA']:
            stdout.write(f'\rПравда')
            pr = pravda(start=start, end=end)
            list_concat.append(pr)
        if customs_config['FINANZ']:
            stdout.write(f'\rFinanz')  # Финанз перестал архив свой собирать, с начала марта 2022
            fin = finanz(start=start, end=end)
            list_concat.append(fin)

        return pd.concat(list_concat)

    def save_batch(self, batch, first_batch, last_date, begin_db, part='all'):
        """
        Функция для сохранения данных в csv скачанных по фед. Просто было удобно передавать csv, можно переделать и
        сделать напрямую в postgres

        :param batch: размер батча (стоит 1000)
        :param first_batch: True/False - первый или нет батч
        :param last_date: берется дата начала из main, хотя по сути (аналогия из feedly_news) должна брать последнюю
        дату загрузки
        :param begin_db: если есть, для того, чтобы качать в другую сторону. Для обычной скачки это не нужно.
        :param part: all по умолчанию (то есть все)
        :return: csv-файл с данными
        """
        if first_batch:
            batch.to_csv(f'data/gu_proc_files/fed/processed_files/csvs/{part}/'
                            f'fedbase_{last_date}_{begin_db}.csv', encoding='utf-8-sig', sep=';')
        else:
            batch.to_csv(f'data/gu_proc_files/fed/processed_files/csvs/{part}/'
                            f'fedbase_{last_date}_{begin_db}.csv', mode='a', encoding='utf-8-sig', sep=';',
                            header=None)

    def news_to_csv(self, last_date, begin_db, news):
        """
        Вспомогательная функция для fed_news_update. Обеспечивает закачку новостей по ссылкам и сохранение в csv

        :param last_date: по аналогии выше
        :param begin_db: по аналогии выше
        :param news: документация со ссылками на новости
        :return: сохраненный csv файл
        """
        # last_10_accuracy = 1  # Пока убрали проверку на точность загрузки
        failure = 0  # счетчик незагруженных новостей
        news['time'] = pd.to_datetime(news['time'])
        news.sort_values(by=['time'], inplace=True)
        # номер части загрузки: делим длину все df со сслыками на размер батча (1000):
        parts = int(news.shape[0] / self.batch_size) + 1
        batch = 0  # счетчик батча
        print("Процесс скачивания новостей")
        start_time = time.time()
        first_batch = True
        downloaded = 0  # счетчик закачки
        if news.shape[0] > 0:
            for part_i in range(0, news.shape[0], self.batch_size):  # Берем по 1000 новостей и качаем их
                batch += 1
                start_batch = time.time()
                part_df = news.iloc[part_i:part_i + self.batch_size]  # Берем срез по 1000 новостей, с i по 1000+i
                get_part = self._download_full_text(part_df, failure,
                                                    multiprocess_num=20)  # по сути самая быстрая часть - закачка текста
                get_part = get_part[['title', 'feed_name', 'time', 'news_for_reading', 'region', 'key_word']]
                get_part.columns = ['title', 'paper', 'crawled', 'news_for_reading', 'region', 'key_word']
                self.save_batch(get_part, first_batch, last_date, begin_db)
                econom_get_part = self.extractor.three_step_econom(get_part.copy(deep=True))
                # deep=True - копирует, а не ссылается на др элемент
                self.save_batch(econom_get_part, first_batch, last_date, begin_db, part='econom')

                delta_all = time.time() - start_time
                delta_part = time.time() - start_batch

                downloaded += get_part.shape[0]
                failure += (part_df.shape[0] - get_part.shape[0])
                time_all = str(timedelta(seconds=delta_all)).split(".")[0]
                time_batch = str(timedelta(seconds=delta_part)).split(".")[0]
                stdout.write(f'\rСкачан {batch=} from {parts}. {downloaded=} from {news.shape[0]} ({failure=}). '
                             f'{time_all=} ({time_batch=}) in base: {get_part.shape[0]} from {part_df.shape[0]}')
                stdout.flush()
                first_batch = False
                # добавляю ошибки в excel файл append и очищаю self.df_errors, чтобы не копилось
                # в первой записи будут уже все ошибки с feedly и ошибки с закачкой тоже. будет записывать каждые 10
                # батчей, чтобы слишком часто не делать эти операции, но в тоже время не накапливать много ошибок в
                # памяти:
                if batch % 5 == 0:
                    self._errors_to_excel()
                    last_10_accuracy = failure / downloaded
                    if self.acc_control and last_10_accuracy < 0.9:
                        return 'Bad_accuracy'
            if len(self.df_errors) > 0:
                self._errors_to_excel()
            self.writer.close()
            # self.sqlite_connection.close()
            print(f'\nПроцент полученных новостей: {"{:.2%}".format(downloaded / news.shape[0])}')

            return 'OK'
        else:
            print(f'Пустой список новостей, но длина новостей = {len(news)}')
            return 0

    def fed_news_update(self, start, finish, fulls=False):
        """
        Основная вызывающаяся функция. Добавка к обработчику, все функции здесь совмещаются.

        :param start:дата начала (задаем сами в main)
        :param finish: дата окончания (задаем сами в main)
        :param fulls: получены ли данные с фидли и с кастомных парсеров
        :return: загрузка данных в БД
        """
        if not fulls:  # Если список со ссылками отсутствует
            res = self.get_news_fed(last_date=start, begin_db=finish)  # Ф-я получения с фидли
            res_customs = self.get_news_customs(start=start, end=finish)  # Ф-я получения с кастомных парсеров
            full_customs = pd.concat([res, res_customs], ignore_index=True)  # Объединяем полученные df в один фрейм
            del res, res_customs
            writer_feedly = pd.ExcelWriter(f'data/saved_feedlies/fed/'
                                           f'fulls_{start}_{finish}.xlsx', engine='xlsxwriter',
                                           options={'strings_to_urls': False})  # сохранение в эксель
            full_customs.to_excel(writer_feedly, sheet_name='feedly_result', index=False)
            writer_feedly.save()
            print('Список новостей сохранен в saved_feedlies/fed/fulls...')
        else:  # Если список со ссылками уже есть
            full_customs = pd.read_excel(f'data/saved_feedlies/fed/fulls_{start}_{finish}.xlsx')
        self.feedly_open = pd.read_excel(config['FEED_PARAMS']['FED'],
                                       index_col='feedname').to_dict(orient='index')  # открывает файл с 28 фед сми
        ans = self.news_to_csv(last_date=start, begin_db=finish, news=full_customs)  # сохраняем в csv
        if ans == 'OK':
            df_all = pd.read_csv(f'data/gu_proc_files/fed/processed_files/csvs/all/'
                                 f'fedbase_{start}_{finish}.csv', sep=';', encoding='utf-8-sig')  # считываем с csv
            add_in_db(df_all, table_name='fednews')  # добавляем в БД
            del df_all
            df_all = pd.read_csv(f'data/gu_proc_files/fed/processed_files/csvs/econom/'
                                 f'fedbase_{start}_{finish}.csv', sep=';', encoding='utf-8-sig')
            add_in_db(df_all, table_name='econom_news')
            del df_all, full_customs

        return ans


