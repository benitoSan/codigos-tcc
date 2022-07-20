#downloader for the files in https://gz.blockchair.com/bitcoin/outputs/
# url format: https://gz.blockchair.com/bitcoin/outputs/blockchair_bitcoin_transactions_YearMonthDay.tsv.gz
import requests


year = 2020
month = 1
day = 1

file_number = 1

url = "https://gz.blockchair.com/bitcoin/outputs/"

file_extension = ".tsv.gz"

file_extension_to_save = ".tsv"

while True:
    if month < 10:
        if day < 10:
            filename = "blockchair_bitcoin_outputs_" + str(year) + "0" + str(month) + "0" + str(day) + file_extension
        else:
            filename = "blockchair_bitcoin_outputs_" + str(year) + "0" + str(month) + str(day) + file_extension
    else:
        if day < 10:
            filename = "blockchair_bitcoin_outputs_" + str(year) + str(month) + "0" + str(day) + file_extension
        else:
            filename = "blockchair_bitcoin_outputs_" + str(year) + str(month) + str(day) + file_extension

    response = requests.get(url+filename)

    if response.status_code == 404:
        if month == 13:
            exit()
        else:
            month += 1
            day = 1

    else:
        try:
            open(f"path/to/save/{year}/file_{year}-{month}-{day}{file_extension_to_save}", 'wb').write(response.content)
            print(f"file saved! Data from {year}{month}{day} is now stored")
            day += 1
        except Exception as error:
            print(f"ERROR: {error}")
            print(f"Stopped in: {year}-{month}-{day}{file_extension_to_save}")
            exit()
