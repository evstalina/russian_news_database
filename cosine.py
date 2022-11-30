import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import yaml
from src.sql_interection import get_vectors, post_vectors

with open(r'config/config.yaml', encoding='utf-8') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


def cosine_matrix(start, finish):
    data = get_vectors(start, finish)
    matrix = data['vector'].tolist()
    matrix_float = []
    for row in matrix:
        row = row.replace('(', '').replace(')', '').split(',')
        matrix_float.append(np.asarray([float(num) for num in row]))
    del matrix
    matrix_float = np.asarray(matrix_float)
    # m = matrix_float.astype('float32')
    cos = cosine_similarity(matrix_float, matrix_float)
    # cos = np.triu(cos)
    sim_df = pd.DataFrame(columns=['id_news', 'id_sim_news', 'cos_dist'])
    data_ids = data.id_news.tolist()
    for i, id_news in enumerate(data_ids):
        ind = cos[i].argsort()[-11:][::-1]  # 10 + сам элемент
        ind = ind[ind != i]  # убираем сам i элемент
        sim_ids = list(map(data_ids.__getitem__, ind))

        ans = pd.DataFrame([[id_news] * 10, sim_ids, cos[0, ind]]).T
        ans.columns = ['id_news', 'id_sim_news', 'cos_dist']
        sim_df = sim_df.append(ans)

    post_vectors(sim_df)


if __name__ == '__main__':
    start = '2022-03-01'
    finish = '2022-04-01'
    dates = [d.strftime('%Y-%m') for d in pd.date_range(start=start, end=finish, freq='MS')]
    for i, dt in enumerate(dates):
        if i == len(dates) - 1:
            break
        print(f'считаю {dt}')
        cosine_matrix(dates[i], dates[i+1])
