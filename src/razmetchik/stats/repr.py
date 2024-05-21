import argparse
import json
from collections import namedtuple
import pandas as pd
import numpy as np
from itertools import product
from pprint import pprint


def get_table_dataset(json_contents: dict) -> list[tuple[str, str]]:
    lines: list[tuple[str, str]] = []
    ClassTuple = namedtuple('ClassTuple', ['name', 'count'])

    items: dict = json_contents['items']
    for ind in items:
        classes: list[ClassTuple] = []
        for val in items[ind]:
            if val == 'text':
                continue
            classes.append(ClassTuple(val, items[ind][val]))

        max_count: int = max([x.count for x in classes])
        max_classes: list[ClassTuple] = []
        for x in classes:
            if x.count == max_count:
                max_classes.append(x)

        for i in range(len(max_classes)):
            lines.append((items[ind]['text'], max_classes[i].name))
    
    return lines


def log_counts(dataset: pd.DataFrame):
    print(dataset['class'].value_counts())


def _log_before_merge(dataset: pd.DataFrame):
    text_info: dict = {}
    for index, row in dataset.iterrows():
        if not text_info.get(row['text']):
            text_info[row['text']] = []
        text_info[row['text']].append(row['class'])

    class_info: dict = {}
    for text, classes in text_info.items():
        if len(classes) == 1:
            if not class_info.get(classes[0]):
                class_info[classes[0]] = 1
            else:
                class_info[classes[0]] += 1
            continue

        for x in product(classes, classes):
            if x[0] == x[1]:
                continue
            if not class_info.get(x[0] + '-' + x[1]):
                class_info[x[0] + '-' + x[1]] = 1
            else:
                class_info[x[0] + '-' + x[1]] += 1
    
    pprint(class_info)


def log_overlapping(dataset: pd.DataFrame | dict):
    if isinstance(dataset, pd.DataFrame):
        _log_before_merge(dataset)
    else:
        raise NotImplementedError('json overlapping is not ready')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Evaluates represetnations of each class in the dataset')
    parser.add_argument('file', type=str,
                        help='path to the .json dataset file')
    parser.add_argument('-o', '--output', type=str,
                        help='path for csv output table')

    args = parser.parse_args()

    with open(args.file, 'r', encoding='utf-8') as json_file:
        contents: dict = json.load(json_file)

    lines = np.array(get_table_dataset(contents))
    df = pd.DataFrame({'text': lines[:, 0], 'class': lines[:, 1]})

    log_counts(df)
    log_overlapping(df)

    if args.output:
        df.to_csv(args.output + '/dataset.csv', sep='\t', index=False)
