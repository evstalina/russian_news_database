import pandas as pd
from modules.feedly_news import feedly_proc
import locale
from modules.update_dashboard import dashboard_update
from modules.FED_news import fed_news
from src.cosine import cosine_matrix
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)
locale.setlocale(locale.LC_ALL, '')


def update_news_dashboard(start, finish, list_update=None, list_download=None):
    """
    Основная функция, которая обновляет базу и готовить данные для дэшборда по СЗГУ, ВВГУ и фед.
    :param start: начало обновления, месяца.
    :param finish: конец обновления. Это все в формате YYYY-MM-DD,если напр. май, то start=2022-05-01, finish=2022-06-01
    :param list_update: список ГУ для обновы дэшборда. Если не передавать, то все.
    :param list_download: список ГУ для скачки. Если не передавать, то все.
    """
    if list_update is None:  # тут понятно, можно не передавать None и это будет означать, что код обработает все ГУ
        list_update = ['VVGU', 'SZGU', 'fed']
    if list_download is None:
        list_update = ['VVGU', 'SZGU', 'fed']
    if 'VVGU' in list_download:  # Если нашел ВВГУ в списке на скачку, то качает
        print(f'Обновляем ВВГУ')
        fp = feedly_proc(gu='VVGU', method=config['FEEDLY']['URL_METHOD'])  # иниц. класс, даем ему метод скачки и ГУ
        # метод скачки для ВВГУ и СЗГУ urlopen, так было изначально, потом добавил requests для фед, т.к. там были
        # проблемы (вроде бы надо было proxy передать и почище скачивать).
        ans_vvgu = fp.get_news_for_db(feedly_save=True)  # у класса вызываем функцию обновления базы, feedly_save=True
        # озн., что он сохранит в папку data/saved_feedlies и далее ГУ. Обновления сохр. в БД
        if ans_vvgu == 'Bad_accuracy':  # проверка на качество. Ее лучше сейчас не включать из-за нестабильности,
            # блокировок и проч. Он в config['FEEDLY']['ACC_CONTROL'] и он False.
            print('Необходимо проверить качество новостей ВВГУ. Скорее всего одно из СМИ сменило верстку и качество'
                  'упало ниже 90%')
            return 0
    if 'VVGU' in list_update:  # Если нашел ВВГУ в списке на обновление для дэшборда, то обрабатывает
        du = dashboard_update(gu='VVGU')  # класс обновления дэша, передаем ГУ.
        du.prepare_data_dashboard(start=start, finish=finish)  # передается старт и финиш обработки. Все сохр в БД
    if 'SZGU' in list_download:  # СЗГУ и Фед по аналогии
        print(f'Обновляем СЗГУ')
        fp = feedly_proc(gu='SZGU', method=config['FEEDLY']['URL_METHOD'])  # , feedly_df='today'
        ans_szgu = fp.get_news_for_db(feedly_save=True)
        if ans_szgu == 'Bad_accuracy':
            print('Необходимо проверить качество новостей СЗГУ. Скорее всего одно из СМИ сменило верстку и качество'
                  'упало ниже 90%')
            return 0
    if 'SZGU' in list_update:
        du = dashboard_update(gu='SZGU')
        du.prepare_data_dashboard(start=start, finish=finish)
    if 'fed' in list_download:
        print(f'Обновляем фед сми')
        fp_fed = fed_news(gu='FED_23', method=config['FEEDLY']['URL_METHOD_FED'], feedly_df=None)  # тут разве что
        # FED_23, т.к. без 5 кастомных (23 из Feedly), потом они присоединяются к скаченному
        ans = fp_fed.fed_news_update(start=start, finish=finish, fulls=False)
        if ans == 'Bad_accuracy':
            print('Необходимо проверить качество федеральных новостей. Скорее всего одно из СМИ сменило верстку и '
                  'качество упало ниже 90%')
            return 0
    if 'fed' in list_update:
        du = dashboard_update(gu='fed')
        du.prepare_data_dashboard(start=start, finish=finish)


START = '2022-10-01'  # начало месяца обновления для дэшборда (новости он качает просто от посл. даты в базе до
# нынешней)
FINISH = '2022-11-01'  # конец месяца
LIST_UPDATE = ['fed']  # по каким регионам обновлять для дэшборда: можно взять все ['VVGU', 'SZGU', 'fed'] или 1 отд.
LIST_DOWNLOAD = ['fed']  # по каким регионам закачивать и обрабатывать новости


if __name__ == '__main__':
    update_news_dashboard(START, FINISH, list_download=LIST_DOWNLOAD, list_update=LIST_UPDATE)
    dates = [d.strftime('%Y-%m') for d in pd.date_range(start=START, end=FINISH, freq='MS')]
    for i, dt in enumerate(dates):
        if i == len(dates) - 1:
            break
        cosine_matrix(dates[i], dates[i + 1])
