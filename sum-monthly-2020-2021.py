from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd


def make_cluster():
    try:
        cluster_db = MongoClient()
        return cluster_db
    except Exception as error:
        print(error)


def retrieve_collection(cluster_db, coll_year):
    collection_to_retrieve = f"tx_{coll_year}"
    try:
        db = cluster_db['transactions']
        collection_db = db[collection_to_retrieve]
        return collection_db
    except Exception as error:
        print(error)


year = 2020
month = 1
day = 1

cluster = make_cluster()
collection = retrieve_collection(cluster, year)

value_usd_leg = 0.0
value_usd_n_leg = 0.0
count_leg = 0
count_n_leg = 0

while True:
    try:
        file_name_ids = f'ids_{year}-{month}-{day}'
        data_file_name = f'file_{year}-{month}-{day}.tsv'
        with open(f"path/to/transactions_ids/{year}/{file_name_ids}", 'r') as file_ids:
            ids = file_ids.read()
            ids = ids.split('\n')
        txs_data = pd.read_csv(f'path/to/blockchair/files/{year}/{data_file_name}', compression='gzip', delimiter="\t")
        index = 0
        last_index = len(txs_data)
    except:
        if month <= 12:
            print(f'Month {month} completed!')
            with open(f'path/to/save/{year}/sum_values-{month}', 'w') as file:  # writing monthly sum
                file.write(f'{count_leg},{value_usd_leg}\n{count_n_leg},{value_usd_n_leg}')
            month += 1
            if month == 13:
                exit()
            day = 1
            value_usd_leg = 0.0
            value_usd_n_leg = 0.0
            count_leg = 0
            count_n_leg = 0
            file_name_ids = f'ids_{year}-{month}-{day}'
            with open(f"path/to/transactions_ids/{year}/{file_name_ids}", 'r') as file_ids:
                ids = file_ids.read()
                ids = ids.split('\n')
            data_file_name = f'file_{year}-{month}-{day}.tsv'
            txs_data = pd.read_csv(f'block_data/data/{year}/{data_file_name}', compression='gzip', delimiter="\t")
            index = 0
            last_index = len(txs_data)
        else:
            exit()

    ids.pop(len(ids) - 1)

    for tx_id in ids:
        for i in range(index, last_index):
            is_reward = txs_data['is_from_coinbase'][i]
            if is_reward == 0:
                tx_hash = txs_data['transaction_hash'][i]
                if tx_hash == tx_id:
                    current = tx_hash  # transaction_data['transaction_hash']
                    if txs_data['type'][i] != 'pubkeyhash':
                        tx_database = collection.find_one({'_id': current})

                        if len(tx_database['readable_scripts']) > 0:
                            value_usd_leg += txs_data['value_usd'][i]
                            count_leg += 1
                        else:
                            value_usd_n_leg += txs_data['value_usd'][i]
                            count_n_leg += 1

                    current_set = 1
                    try:
                        next_tx_hash = txs_data['transaction_hash'][i + 1]
                        if current != next_tx_hash and current_set == 1:
                            current_set = 0
                            index = i + 1
                            break
                    except:
                        pass
            else:
                index = i + 1
                break

    day += 1
