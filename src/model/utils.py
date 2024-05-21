import pandas as pd
import json
from typing import Optional


DATA_DIR = '../../data/'
JSON_FILE = 'feature_labels.json'


def read_json_data(path: Optional[str] = None):
    if not path:
        path = DATA_DIR + JSON_FILE

    with open(path, 'r') as json_file:
        json_data = json.load(json_file)

    table_data: dict = {
        'text': [],
        'home': [],
        'pervert': [],
        'polytical': [],
        'other': []
    }

    for i, item_ind in enumerate(json_data['items']):
        table_data['text'].append(json_data['items'][item_ind]['text'])
        table_data['home'].append(0)
        table_data['pervert'].append(0)
        table_data['polytical'].append(0)
        table_data['other'].append(0)

        for data in json_data['items'][item_ind]:
            if data == 'text':
                continue

            if data == 'пошлый':
                table_data['pervert'][i] = json_data['items'][item_ind][data]
            elif data == 'бытовой':
                table_data['home'][i] = json_data['items'][item_ind][data]
            elif data == 'политический':
                table_data['polytical'][i] = json_data['items'][item_ind][data]
            elif data == 'другой':
                table_data['other'][i] = json_data['items'][item_ind][data]

    return pd.DataFrame(table_data)


def read_csv_data(path: str):
    with open(path, 'r') as csv_file:
        csv_df = pd.read_csv(csv_file, sep='\t')

    namings = {
        'пошлый': 'pervert',
        'бытовой': 'home',
        'политический': 'polytical',
        'другой': 'other'
    }

    csv_df['class'] = csv_df['class'].apply(lambda s: namings[s])
    return csv_df


def multilabel(df: pd.DataFrame):
    HOME_LABEL = 1
    PERVERT_LABEL = 2
    POLYTICAL_LABEL = 3
    # OTHER_LABEL = 4

    df_new = df.copy(deep=True)
    df_new['labels'] = df['home'] + df['pervert'] + df['polytical']# + df['other']
    df_new = df_new[df_new['labels'] == 1]

    df_new['labels'] = df_new['home'] * HOME_LABEL \
                       + df_new['pervert'] * PERVERT_LABEL \
                       + df_new['polytical'] * POLYTICAL_LABEL
                       # + df['other'] * OTHER_LABEL
    df_new.drop(columns=['home', 'pervert', 'polytical', 'other'], inplace=True)

    return df_new
