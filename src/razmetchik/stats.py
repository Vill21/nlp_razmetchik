import redis
import couchdb
import argparse
import sys
from typing import (
    Optional,
    Sequence
)
from datetime import datetime
from config import (
    REDIS_HOST,
    REDIS_PORT,
    COUCH_URL
)
from task import (
    Stage
)


def label_number(db: couchdb.Database, name: str, /, from_users: Optional[Sequence] = None) -> int:
    """
    Counts labels with specified name in the database.

    :param db: couchdb database
    :type db: Database
    :param name: name of the label
    :type name: str
    :param from_users: (optional) sequence of users to count votes from
    :type from_users: Sequence
    :returns: number of labels
    :rtype: int
    """
    k = 0
    number_dict = {}

    for index in db:
        k += 1
        doc = db[index]
        print(doc)
        # number_dict[doc['voted']]
        if k == 5:
            break


def complete_number(r: redis.Redis, /, from_users: Optional[Sequence] = None) -> int:
    """
    Counts how many votes have been already given in current stage.

    :param r: redis instance
    :type r: Redis
    :param from_users: (optional) sequence of users to count votes from
    :type from_users: Sequence
    :returns: number of votes
    :rtype: int
    """
    if not from_users:
        return sum([r.scard(key) for key in r.keys('id:*')])
    else:
        return sum([r.scard(key) for key in ['id:' + str(x) for x in from_users]])


def top_voters(stage: Stage,
               start: datetime,
               end: datetime,
               redis: redis.Redis,
               db: couchdb.Database,
               /,
               limit: int = 10,
               from_users: Optional[Sequence] = None):
    """
    Return ordered list of voters in a specified time period.

    :param stage: task stage
    :type stage: Stage
    :param start: included begin time
    :type start: datetime
    :param end: included end time
    :type end: datetime
    :param redis: redis instance
    :type redis: Redis
    :param db: couchdb database
    :type db: Database
    :param limit: (default=10) returned number of voters
    :type limit: int
    :param from_users: (optional) sequence of users to count votes from
    :type from_users: Sequence
    :returns: descending ordered list
    :rtype: list
    """
    if not from_users:
        voter_pairs = [[voter, 0] for voter in redis.keys('id:*')]
    else:
        voter_pairs = [[voter, 0] for voter in ['id:' + str(x) for x in from_users]]
    for voter_pair in voter_pairs:
        for ind in redis.smembers(f'{voter_pair[0]}'):
            try:
                voted_date = db[f'anek_{ind}']['voted'][voter_pair[0].split(':')[1]][f'date_{stage}']
            except KeyError:
                continue
            voted_date = datetime.strptime(voted_date, '%d-%m-%Y %H:%M:%S')
            if start <= voted_date and voted_date <= end:
                voter_pair[1] += 1
    voter_pairs.sort(key = lambda x: x[1], reverse=True)
    voter_pairs = voter_pairs[0:limit]
    for voter_pair in voter_pairs:
        voter_id = voter_pair[0].split(':')[1]
        nickname = r.get(f'name:{voter_id}')
        if nickname:
            voter_pair[0] = nickname
    return voter_pairs


if __name__ == '__main__':
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    couch = couchdb.Server(COUCH_URL)
    parser = argparse.ArgumentParser()
    parser.add_argument('operation', choices=('count', 'top', 'label'),
                        help='choose an operation to run')
    parser.add_argument('--limit', type=int,
                        help='top list limit')
    parser.add_argument('--from_users', nargs='+', type=str,
                        help='users to gather stats for')

    args = parser.parse_args()
    if args.operation == 'count':
        print(complete_number(r, args.from_users))
    elif args.operation == 'top':
        top = top_voters(
            Stage.DECISION,
            datetime(2024, 3, 25, 12, 0),
            datetime.now(),
            r,
            couch['anek_dataset'],
            args.limit,
            args.from_users
        )
        longest_length = max([len(str(voter[1])) for voter in top])
        for voter in top:
            space_number = longest_length - len(str(voter[1]))
            print(f'`{voter[1]}  ' + space_number * ' ' + '`' + f'**{voter[0]}**')
    elif args.operation == 'label':
        label_number(couch['anek_dataset'], 'хорошо')
