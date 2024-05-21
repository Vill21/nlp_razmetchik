import redis
import couchdb
import argparse
from typing import (
    Dict
)
import json
import os

from razmetchik.config import (
    REDIS_HOST,
    REDIS_PORT,
    COUCH_URL
)
from razmetchik.task import (
    Stage,
    DecisionType
)

def label_number(db: couchdb.Database,
                 stage: Stage
                 ) -> int:
    """
    Counts labels with specified name in the database.

    :param db: couchdb database
    :type db: Database
    :param stage: stage to gather stats for
    :type stage: Stage
    :returns: number of labels
    :rtype: int
    """
    number_dict: Dict = {}
    if stage == Stage.DECISION:
        number_dict[f'count_{DecisionType.GOOD}'] = 0
        number_dict[f'count_{DecisionType.BAD}'] = 0
    if stage == Stage.FEATURE:
        number_dict['count'] = 0
    number_dict['items'] = {}

    for index in db:
        doc = db[index]
        for user in doc['voted']:
            if stage == Stage.DECISION and \
            (decision_value := doc['voted'][user].get(Stage.DECISION)) and decision_value != 0:
                if decision_value == 1:
                    number_dict[f'count_{DecisionType.GOOD}'] += 1
                    if not number_dict['items'].get(doc['_id']):
                        number_dict['items'][doc['_id']] = {DecisionType.GOOD: 1, DecisionType.BAD: 0}
                    else:
                        number_dict['items'][doc['_id']][DecisionType.GOOD] += 1
                else:
                    number_dict[f'count_{DecisionType.BAD}'] += 1
                    if not number_dict['items'].get(doc['_id']):
                        number_dict['items'][doc['_id']] = {DecisionType.GOOD: 0, DecisionType.BAD: 1}
                    else:
                        number_dict['items'][doc['_id']][DecisionType.BAD] += 1
            if stage == Stage.FEATURE and \
            (features := doc['voted'][user].get(Stage.FEATURE)):
                if not number_dict['items'].get(doc['_id']):
                    number_dict['items'][doc['_id']] = {}
                    number_dict['items'][doc['_id']]['text'] = doc['text']
                    number_dict['count'] += 1
                for feature in features:
                    if feature not in number_dict['items'][doc['_id']]:
                        number_dict['items'][doc['_id']][feature] = 1
                    else:
                        number_dict['items'][doc['_id']][feature] += 1

    return number_dict


if __name__ == '__main__':
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    couch = couchdb.Server(COUCH_URL)
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', type=str, choices=(Stage.DECISION, Stage.FEATURE),
                        help='stage to count stats for')
    parser.add_argument('--save', type=str,
                        help='name of json file')

    args = parser.parse_args()

    stats: Dict = label_number(couch['anek_dataset'], args.stage)
    if args.save:
        dir_path: str = os.path.dirname(os.path.abspath(__file__))
        with open(dir_path + f'/{args.save}', 'w', encoding='utf-8') as file_json:
            json.dump(stats, file_json, ensure_ascii=False, indent=4)
    else:
        print(stats)