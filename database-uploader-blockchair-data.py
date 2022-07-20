from pymongo import MongoClient
import pandas as pd


day = 1
month = 1
year = 2020


def make_cluster():
    try:
        cluster_db = MongoClient()
        return cluster_db
    except Exception as error:
        print(error)


def retrieve_collection(cluster_db, coll_year):
    collection_to_retrieve = f"tx_{coll_year}"
    try:
        db = cluster_db['transaction']
        collection_db = db[collection_to_retrieve]
        return collection_db
    except Exception as error:
        print(error)


cluster = make_cluster()
collection = retrieve_collection(cluster, year)

while True:

    try:
        # file_name_data = "file_" + str(year) + "-" + str(month) + "-" + str(day) + ".tsv"
        # outputs_data = pd.read_csv(f'block_data/2020/{file_name_data}', compression='gzip', delimiter="\t")
        data_file_name = "file_" + str(year) + "-" + str(month) + "-" + str(day) + ".tsv"
        txs_data = pd.read_csv(f'block_data/2020/{data_file_name}', compression='gzip', delimiter="\t")
    except Exception as error:
        if month != 12:
            print(f"Month {month} completed!!!")
            month += 1
            day = 2
            data_file_name = "file_" + str(year) + "-" + str(month) + "-" + str(day) + ".tsv"
            txs_data = pd.read_csv(f'block_data/2020/{data_file_name}', compression='gzip', delimiter="\t")
        else:
            exit()

    file_name_ids = f"ids_{year}-{month}-{day}"

    with open(f"transactions_ids/{year}/{file_name_ids}", 'r') as file_ids:
        ids = file_ids.read()
        ids = ids.split('\n')
        for tx_id in ids:  # iteracao das ids
            readable_script = []
            not_readable_script = []
            output_values = []
            addresses_receivers = []
            ascii_script = []
            not_ascii = []
            current = ""
            current_set = 0

            for tx in txs_data.iterrows():
                transaction_data = tx[1]

                if tx_id == transaction_data['transaction_hash']:
                    current = transaction_data['transaction_hash']
                    current_set = 1
                    if transaction_data['script_hex'][:2] == "6a":
                        ascii_string = str(bytes.fromhex(transaction_data['script_hex'][4:]))
                        ascii_string = ascii_string[2:len(ascii_string)-1]

                        if "\\" not in ascii_string[:3]:
                            ascii_script.append(ascii_string)
                            readable_script.append(transaction_data['script_hex'][4:])
                        else:
                            not_readable_script.append(transaction_data['script_hex'][4:])
                            not_ascii.append(ascii_string)

                    elif transaction_data['index'] == 0:
                        output_values.append(transaction_data['value'])
                        addresses_receivers.append(transaction_data['recipient'])
                    else:
                        output_values.append(transaction_data['value'])
                        addresses_receivers.append(transaction_data['recipient'])

                if current != transaction_data['transaction_hash'] and current_set == 1:
                    current_set = 0
                    break

            if len(output_values) > 0:
                #upload to database
                post = {'_id': tx_id,
                        'readable_scripts': readable_script,
                        'ascii': ascii_script,
                        'not_readable_scripts': not_readable_script,
                        'addresses_receivers': addresses_receivers,
                        'values_outputs': output_values,
                        'month': month,
                        'day': day}
                try:
                    collection.insert_one(post)
                    print(f"{tx_id} uploaded to database, from file {file_name_ids}!")
                    pass
                except Exception as error:
                    print(f"Error: {error}")
                    print(f"Stopped on tx {tx_id}, from file {file_name_ids}")
                    exit()
    day += 1
    print(f"Completed {data_file_name}")
