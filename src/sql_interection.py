import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import datedelta
import yaml

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


# ALTER TABLE my_table ADD COLUMN my_id serial;


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
    else:
        return gu


def add_data_to_dashboard_sql(df, table_name, data, gu): #Сохраняем в БД для дашборда
    gu = rus_eng_gu(gu)
    engine = create_engine(config['DASHBOARD']['DASHBOARD_BASE'])
    postgres_connection = engine.connect()
    df['data'] = datetime.strptime(data, '%Y-%m')
    df['gu'] = gu
    # убираем всякую хрень из версии новостей для чтения
    if 'news_for_reading' in df.columns:
        trash_phrases = ['. 0 .', ' . ', ' , ', ',,', ' ,', '...', ' .', 'Поделиться ссылкой', 'Вернуться назад',
                         'бизнес Поделиться ссылкой Вернуться назад Если вы заметили опечатку, выделите часть текста с '
                         'ней и нажмите', '. , . . . ',
                         'Спасибо!',
                         'Пожалуйста, выделяйте фразу с опечаткой, а не только одно неверно написанное слово.',
                         'Тег не поддерживается вашим браузером.',
                         'Нашли ошибку в тексте? Выделите ее и нажмите версия для печати',
                         'Нашли ошибку в тексте?', 'Выделите ее и нажмите версия для печати', 'Фото', 'Поделиться']
        dict_trash = dict(zip(trash_phrases, ['' for _ in range(len(trash_phrases))]))
        for part, _ in dict_trash.items():
            df['news_for_reading'] = df['news_for_reading'].str.replace(part, _, regex=False)
        # df.replace({"news_for_reading": dict_trash}, inplace=True)
        df["news_for_reading"] = df["news_for_reading"].str.lstrip('0123456789.- ,')
        df["news_for_reading"] = df["news_for_reading"].str.rstrip('0123456789.- ,!?')

    df.reset_index(inplace=True, drop=True)
    try:
        max_num = postgres_connection.execute(f"SELECT MAX(data) FROM {table_name} "
                                              f"WHERE gu='{gu}'").fetchall()
        max_num = max_num[0][0]
    except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.ProgrammingError):
        max_num = 0
    if max_num != 0 and max_num is not None:
        if datetime.strptime(data, '%Y-%m') <= datetime.strptime(str(max_num)[:7], '%Y-%m'):
            print(f'Последняя дата в базе - {max_num}. Данные не будут добавлены в базу, так как уже в ней '
                  f'присутствуют')
            return
        elif datetime.strptime(data, '%Y-%m') == datetime.strptime(str(max_num)[:7], '%Y-%m') + datedelta.MONTH:
            df.to_sql(table_name, postgres_connection, if_exists='append')
        else:
            print(f'Последняя дата в базе - {str(max_num)[:7]}. Данные не будут добавлены в базу, так как новые '
                  f'данные не являются следующим периодом последней даты в базе.')
            return
    else:
        df.to_sql(table_name, postgres_connection, if_exists='append')


def add_data_to_sql(df, first_batch=True, gu='VVGU'):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
    sqlite_connection = engine.connect()
    try:
        sqlite_table = "news_db"
        if first_batch:
            # вот тут
            try:
                # запрос стал в сотни раз быстрее
                max_num = sqlite_connection.execute("SELECT crawled from news_db "
                                                    "ORDER BY rowid DESC LIMIT 1").fetchall()
                # max_num = sqlite_connection.execute("SELECT ifnull(MAX(crawled),0) FROM news_db").fetchall()
                if max_num[0][0] != 0 and len(max_num) > 0:
                    sqlite_connection.execute(f"DELETE FROM news_db "
                                              f"where not(crawled <'{max_num[0][0][:10]} 00:00:00.000000')")
            except sqlalchemy.exc.OperationalError:
                pass  # ошибка возникает, если пустая база, просто идем дальше

        if gu != 'VVGU' and gu != 'SZGU':
            df = df[['title', 'paper', 'crawled', 'fulltext', 'region', 'fulltext_with_nums',
                     'news_for_reading', 'key_word']]
        else:
            df = df[['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                     'news_for_reading']]
        num_new = len(df)
        if num_new > 0:
            df.to_sql(sqlite_table, sqlite_connection, if_exists='append')
            sqlite_connection.close()
            return 2
        else:
            sqlite_connection.close()
            return 1
    except:
        sqlite_connection.close()
        return 0


def get_sql_data(start=None, finish=None, gu='VVGU'):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
    sqlite_connection = engine.connect()
    sqlite_connection.execute("DELETE FROM news_db WHERE length(stemmed) < 30")  # Всегда удалять пустые или почти
    # пустые новости
    if start is None and finish is None:
        table = sqlite_connection.execute("SELECT * FROM news_db").fetchall()
    elif start is not None and finish is None:
        try:
            datetime.strptime(start, "%Y-%m-%d")
            table = sqlite_connection.execute(f"SELECT * FROM news_db "
                                              f"WHERE crawled >='{start} 00:00:00.000000'").fetchall()
        except ValueError as err:
            print('Дата задана не в правильном формате. Дата должна быть в формате yyyy-mm-dd')
            return
    elif start is None and finish is not None:
        try:
            datetime.strptime(finish, "%Y-%m-%d")
            table = sqlite_connection.execute(f"SELECT * FROM news_db "
                                              f"WHERE crawled <'{finish} 00:00:00.000000'").fetchall()
        except ValueError as err:
            print('Дата задана не в правильном формате. Дата должна быть в формате yyyy-mm-dd')
            return
    else:
        try:
            datetime.strptime(finish, "%Y-%m-%d")
            datetime.strptime(start, "%Y-%m-%d")
            table = sqlite_connection.execute(f"SELECT * FROM news_db "
                                              f"WHERE crawled <'{finish} 00:00:00.000000'"
                                              f"AND crawled >='{start} 00:00:00.000000'").fetchall()
        except ValueError as err:
            print('Дата задана не в правильном формате. Дата должна быть в формате yyyy-mm-dd')
            return

    table = pd.DataFrame(table)
    table = table[[1, 2, 3, 4, 5, 6, 7, 8]]
    table.columns = ['title', 'paper', 'crawled', 'fulltext', 'stemmed', 'region', 'fulltext_with_nums',
                     'news_for_reading']
    sqlite_connection.close()
    return table


def get_papers_by_list(list_papers, gu='VVGU'):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
    sqlite_connection = engine.connect()
    joined = "', '".join(list_papers)
    joined = "'" + joined + "'"
    sql = f"SELECT * FROM news_db WHERE paper  in ({joined})"
    sqlite_connection.execute(sql).fetchall()


def delete_columns_from_base(base_name='VVGU'):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][base_name], echo=False)
    sqlite_connection = engine.connect()
    sqlite_connection.execute("CREATE TEMPORARY TABLE t1_backup(title, paper, crawled, fulltext, "
                              "region, key_word);")

    sqlite_connection.execute("INSERT INTO t1_backup SELECT title, paper, crawled, fulltext, "
                              "region, key_word FROM news_db;")
    sqlite_connection.execute("DROP TABLE news_db;")
    sqlite_connection.execute("CREATE TABLE news_db(title, paper, crawled, fulltext, "
                              "region, key_word);")
    sqlite_connection.execute("INSERT INTO news_db SELECT title, paper, crawled, fulltext, "
                              "region, key_word FROM t1_backup;")
    sqlite_connection.execute("DROP TABLE t1_backup;")


def unite_files():
    list_names = ['interfax', 'finanz', 'vedomosti', 'regnum']
    for name in list_names:
        pd.read_csv(f'{name}.csv').to_csv('fed.csv', mode='a', index=False, header=False)


def delete_carriage(gu):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
    sqlite_connection = engine.connect()
    sqlite_connection.execute(
        "UPDATE news_db SET title = REPLACE(REPLACE(REPLACE(title, '\r', ' '), '\t', ' '), '\n', ' ')")


def delete_empty(gu):
    engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
    sqlite_connection = engine.connect()
    sqlite_connection.execute("DELETE FROM news_db WHERE title='' AND news_for_reading=''")


def get_sample():
    list_gu = ['interfax']
    lst = []
    for gu in list_gu:
        engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
        sqlite_connection = engine.connect()
        table = sqlite_connection.execute("SELECT * FROM news_db LIMIT 1").fetchall()
        l = pd.DataFrame(table)[7].to_list()[0]
        lst.append(l)


def add_in_db(df, table_name):
    engine = create_engine(config['INFLATION_BASE']['POSTGRES']['FED'])
    if 'news_lem' in df.columns:
        df = df[['title', 'paper', 'crawled', 'news_for_reading', 'region', 'news_lem']]
        df.rename({'news_lem': 'lem_news'}, axis=1, inplace=True)
    else:
        df = df[['title', 'paper', 'crawled', 'news_for_reading', 'region', 'key_word']]
    df.to_sql(table_name, engine, if_exists='append', index=False)


def inflation_min_max_date(gu, data_begin):
    begin_db = None
    try:
        engine = create_engine(config['INFLATION_BASE']['SQLITE'][gu], echo=False)
        sqlite_connection = engine.connect()
        max_num = sqlite_connection.execute("SELECT crawled from news_db "
                                            "ORDER BY rowid DESC LIMIT 1").fetchall()
        # max_num = self.sqlite_connection.execute("SELECT ifnull(MAX(crawled),0) FROM news_db").fetchall() - Slow
        # если таблица БД пустая, беру первую необходимую, тут с начала 2020
        if max_num[0][0] == 0 or len(max_num) == 0:
            if data_begin is None:
                last_date = datetime.strptime('2020-01-01', "%Y-%m-%d")
            else:
                last_date = data_begin
        else:
            # если data_begin не пустая и меньше начала базы, то качаем новости в прошлое, если нет - то обычно
            if data_begin is not None:
                # в сотни раз быстрее
                min_num = sqlite_connection.execute("SELECT crawled from news_db ORDER BY rowid ASC LIMIT 1").fetchall()
                # min_num = self.sqlite_connection.execute("SELECT ifnull(MIN(crawled),0) FROM news_db").fetchall()
                begin_db = datetime.strptime(min_num[0][0][:10], "%Y-%m-%d")  # begin_db есть для ограничения сверху
                if data_begin < begin_db:
                    last_date = data_begin  # last_date имеется в виду для feedly, так как оттуда получаем
                    # новости с конца
                else:
                    last_date = datetime.strptime(max_num[0][0][:10], "%Y-%m-%d")
                    begin_db = None
            else:
                last_date = datetime.strptime(max_num[0][0][:10], "%Y-%m-%d")  # + timedelta(days=1)
    except sqlalchemy.exc.OperationalError:
        if data_begin is None:
            last_date = datetime.strptime('2020-01-01', "%Y-%m-%d")
        else:
            last_date = data_begin

    return last_date, begin_db


def get_fed_data(start, finish):
    engine = create_engine(config['INFLATION_BASE']['POSTGRES']['FED'])
    postgres_connection = engine.connect()
    df_sql = postgres_connection.execute(
        f"SELECT title, paper, crawled, news_for_reading, region, lem_news "
        f"FROM econom_news WHERE crawled >='{start} 00:00:00.000000'"
        f" AND crawled <'{finish} 00:00:00.000000'").fetchall()
    df_sql = pd.DataFrame(df_sql)
    df_sql.columns = ['title', 'paper', 'crawled', 'news_for_reading', 'region', 'stemmed']
    df_sql['region'] = 'федеральный'

    return df_sql


def get_vectors(start, finish):
    engine = create_engine(config['DASHBOARD']['DASHBOARD_BASE'])
    postgres_connection = engine.connect()
    data = postgres_connection.execute(f"SELECT id_news, vector FROM news WHERE data "
                                       f">='{start}-01 00:00:00.000000' AND data <'{finish}-01 00:00:00.000000'").fetchall()
    data = pd.DataFrame(data, columns=['id_news', 'vector'])

    return data


def post_vectors(sim_df):
    engine = create_engine(config['DASHBOARD']['DASHBOARD_BASE'])
    postgres_connection = engine.connect()
    sim_df.to_sql('top10_similarity', postgres_connection, if_exists='append')