import requests
import json
import os
from tqdm import tqdm


admin = 'admin:210921'


if __name__ == '__main__':
    names = os.listdir('../data/couchdb')
    for i in tqdm(range(15905 + 15900 + 15902 + 15884 + 15894 + 15904 + 15902, len(names))):
        with open(f'../data/couchdb/{names[i]}', 'r', encoding='utf8') as file:
            json_data = json.load(file)

        url = f'http://{admin}@127.0.0.1:6006/anek_dataset'
        headers = {
            'Content-Type': 'application/json',
            # 'Accept-Charset': 'UTF-8'
        }

        response = requests.post(url, json=json_data, headers=headers)
        if not response.ok:
            print(names[i], response.reason)
