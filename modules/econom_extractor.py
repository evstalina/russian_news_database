import pandas as pd
from pymystem3 import Mystem  # леммитизация https://habr.com/ru/post/503420/
import re  # чистка текста
from sklearn.pipeline import Pipeline
import pickle
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


class extractor:
    def __init__(self):
        """
        Подгружаем все для модели из config.yaml - открываем файлы pickle

        :return: все нужное для модели
        """
        self.stem = Mystem()
        self.stop_words = pickle.load(open(config['MODELS']['CLASS_MODEL']['STOP_WORDS'], 'rb'))
        self.model = pickle.load(open(config['MODELS']['CLASS_MODEL']['XGBOOST6CLASS'], 'rb'))
        self.tfidf = pickle.load(open(config['MODELS']['CLASS_MODEL']['TFIDF'], 'rb'))
        self.trash_phrases = pickle.load(open(config['MODELS']['CLASS_MODEL']['TRASH_PHRASES'], 'rb'))
        self.pipe = Pipeline([('tfidf', self.tfidf), ('xgboost', self.model)])  # объединяем в 1 модель
        self.marks = pd.read_excel(config['MODELS']['CLASS_MODEL']['MARKS'])

    def _proc(self, news_list):
        """
        Лемматизация текста

        :param news_list: список новостей (с парсеров и/или фидли)
        :return: список лемматизированных новостей
        """

        news_list = [re.sub(self.stop_words, ' ', news) for news in news_list]  # Убираем стоп-слова в новостях
        news_lem = []
        for i in range(0, len(news_list), 1000):  # По 1000 новостей объединяем для более быстрой обработки
            news_batch = news_list[i:i + 1000]
            text = ' div '.join(news_batch)  # объединяем 1000 новостей - так быстрее и продуктивнее, чем по 1
            text = self.stem.lemmatize(text)  # лемматизация -- см. функцию в __init__
            text = ''.join(text)
            text = re.sub(self.stop_words, " ", text)  # чистка от стоп-слов
            text = re.sub(self.trash_phrases, " ", text)  # чистка от слов-мусора (т.е. лишних и ненужных)
            text = text.replace('\n', '')
            text = re.sub('[^A-Za-zА-Яа-я\s]', '', text)  # делаем нормальный вид слов
            text = text.replace('  ', ' ')  # убираем двойные пробелы
            text = text.split('div')  # разбиваем на отдельные новости наши 1000 объединенных
            news_lem.extend(text)  # пополняем выходной список
        return [news.strip() for news in news_lem]

    def predict(self, news_lem):  # предсказываем по модели экономические новости. Экономические = 0
        return self.pipe.predict(news_lem)

    def three_step_econom(self, df):
        """
        Получаем на вход dataframe с новостями. На выход dataframe только из эконом. новостей
        1. Новости: Интерфак, Регнум, РБК экономика без фильтра (с feedly все новости экономические)
        2. Новости с ключевыми словами экономическими (с feedly где есть ключевые слова по экономике)
        3. Остальное по предсказанию модели (ф-я выше)

        :param df: файл с новостями
        :return: датафрейм с экономическими новостями
        """
        df['news_lem'] = self._proc(df['news_for_reading'].tolist())  # лемматизация

        df_first = df[df['paper'].isin(['Интерфакс', 'Regnum', 'РБК экономика'])]  # выделяем новости нужн. СМИ
        df.drop(df_first.index, inplace=True)  # удаляем уже сохраненные новости категории 1 (см. выше в опсании)
        econom_marks = self.marks['subtopics'][self.marks['mark'] == 0].tolist()  # выделяем эконом. ключ. слова
        df_second = df[df['key_word'].isin(econom_marks)]  # вычленяем новости с данными ключ. словами
        df.drop(df_second.index, inplace=True)  # удаляем уже сохраненные новости категории 2

        df['pred'] = self.predict(df['news_lem'])  # делаем прогнозы по эконом новостям (категория 3)
        df = df[df['pred'] == 0]  # выбираем только экономические новости (как нам сказала модель)
        df.drop(axis=1, columns='pred', inplace=True)  # удаляем уже сохраненное -- остаток - неэконом новости

        return pd.concat([df_first, df_second, df], ignore_index=True)  # объединяем эконом новости всех 3х кат в df
