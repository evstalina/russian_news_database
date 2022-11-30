import nltk
from nltk.corpus import stopwords
import pymorphy2
import re


def get_lem_text(result, gu):
    stop_list = stopwords.words('russian')
    stop_list.append('?')
    stop_list.append('это')
    text = list(result['full_news'])
    morph = pymorphy2.MorphAnalyzer()
    text_proc = []

    for news in text:
        news = news.lower()
        news = nltk.word_tokenize(news)
        words_lem = []
        for word in news:
            p = morph.parse(word)
            lem = p[0].normal_form
            # убираем мусор из стоп-листа и имена с фамилиями
            if lem not in stop_list:
                words_lem.append(lem)
        words_lem = ' '.join(words_lem)
        text_proc.append(words_lem)
    result['lemmatized'] = text_proc
    if gu == 'FED':
        result = result[['title', 'feed_name', 'time', 'full_news', 'lemmatized', 'region', 'full_news_with_nums',
                         'news_for_reading', 'key_word']]
        result.columns = ['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                          'news_for_reading', 'keywords']
    else:
        result = result[['title', 'paper', 'time', 'full_news', 'lemmatized', 'region', 'full_news_with_nums',
                         'news_for_reading']]
        result.columns = ['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                          'news_for_reading']
    return result


def get_lem_text_mystem(result, gu, stem):  # https://habr.com/ru/post/503420/ Скорее всего не используется
    stop_list = stopwords.words('russian')
    stop_list.append('это')
    stop_list = '|'.join(stop_list)
    stop_list = stop_list + '| +'
    text = list(result['full_news'])
    text = '/div/'.join(text)  # now string
    # text = text.lower()
    text = stem.lemmatize(text)
    text = ''.join(text)
    text = text.split('/div/')  # now list
    result['lemmatized'] = text
    result['lemmatized'] = result['lemmatized'].str.replace(stop_list, '')

    if gu == 'FED':
        result = result[['title', 'feed_name', 'time', 'full_news', 'lemmatized', 'region', 'full_news_with_nums',
                         'news_for_reading', 'key_word']]
        result.columns = ['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                          'news_for_reading', 'key_word']
    else:
        result = result[['title', 'paper', 'time', 'full_news', 'lemmatized', 'region', 'full_news_with_nums',
                         'news_for_reading']]
        result.columns = ['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                          'news_for_reading']
    return result
