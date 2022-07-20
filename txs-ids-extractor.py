#####Usado para extrair os ids (hashes) das transações
#####facilita as análises e o upload no database
import pandas as pd


transactions_ids = []
year = 2020
month = 1
day = 1


while True:
    try:
        data_file_name = "file_" + str(year) + "-" + str(month) + "-" + str(day) + ".tsv"
        #tentativa de abertura do arquivo tsv originado do Blockchair.com
        outputs_data = pd.read_csv(f'path/to/file/{year}/{data_file_name}', compression='gzip', delimiter="\t")
    except:
        if month != 12:
            print(f"Month {month} completed!!!")
            month += 1
            day = 1
            data_file_name = "file_" + str(year) + "-" + str(month) + "-" + str(day) + ".tsv"
            outputs_data = pd.read_csv(f'path/to/file/{year}/{data_file_name}', compression='gzip', delimiter="\t")
        else:
            exit()

    file_name_ids = f'ids_{data_file_name[5:len(data_file_name)-4]}'

    for line in outputs_data.iterrows():
        transaction_data = line[1]
        if transaction_data['script_hex'][:2] == '6a' and transaction_data['transaction_hash'] not in transactions_ids:
            try:
                with open(f'transactions_ids/{year}/{file_name_ids}', 'a') as file:
                    file.write(f"{transaction_data['transaction_hash']}\n")
            except Exception as error:
                print(f'ERROR: {error}')
                print(f'\n\ntransactions failed to be saved:')
                print(transactions_ids)
                exit()
            transactions_ids.append(transaction_data['transaction_hash'])
    transactions_ids = []
    day += 1
