import redis
import couchdb
import argparse
from typing import (
    Optional,
    Sequence,
    Union,
    List
)
from datetime import datetime
import csv

from razmetchik.config import (
    REDIS_HOST,
    REDIS_PORT,
    COUCH_URL
)
from razmetchik.task import (
    Stage
)


def top_voters(stage: Stage,
               start: datetime,
               end: datetime,
               redis: redis.Redis,
               db: couchdb.Database,
               /,
               limit: int = 10,
               from_users: Optional[Sequence[Union[str, int]]] = None
               ):
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


def read_csv(path: str):
    with open(path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        header: list[str] = next(reader)
        id_index: int = header.index('id')
        for row in reader:
            yield row[id_index]


if __name__ == '__main__':
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    couch = couchdb.Server(COUCH_URL)
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', type=str, choices=(Stage.DECISION, Stage.FEATURE),
                        help='specify stage to count stats for')
    parser.add_argument('start', type=str,
                        help='(format: dd-mm-yyyy) start date of reviewing period, enter "now" for current date')
    parser.add_argument('end', type=str,
                        help='(format: dd-mm-yyyy) end date of reviewing period, enter "now" for current date')
    parser.add_argument('--users', nargs='+', type=str,
                        help='users to gather stats for')
    parser.add_argument('--limit', type=int,
                        help='limit top list with specified number')
    parser.add_argument('--csv', nargs='+', type=str,
                        help='path to the csv file to gather users from')

    args = parser.parse_args()

    if args.start == 'now':
        start_period = datetime.now()
    else:
        start_period = datetime.strptime(args.start, '%d-%m-%Y')

    if args.end == 'now':
        end_period = datetime.now()
    else:
        end_period = datetime.strptime(args.end, '%d-%m-%Y')

    if args.users and args.csv:
        raise ValueError('Several user inputs were provided!')

    users: Union[List[str], None] = None
    if args.csv:
        for path in args.csv:
            users = [user for user in read_csv(path)]
    elif args.users:
        users = args.users

    top = top_voters(
        args.stage,
        start_period,
        end_period,
        r,
        couch['anek_dataset'],
        args.limit,
        users
    )
    longest_length = max([len(str(voter[1])) for voter in top])
    for voter in top:
        space_number = longest_length - len(str(voter[1]))
        print(f'`{voter[1]}  ' + space_number * ' ' + '`' + f'**{voter[0]}**')
