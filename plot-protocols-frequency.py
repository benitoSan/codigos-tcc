from yellowbrick.text import DispersionPlot, dispersion
from yellowbrick.datasets import load_hobbies

years = [2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]

op_return_by_year = []

for year in years:
    with open(f'to-analyses-op-return-{year}', 'r') as file:
        content_op_return = file.read().split('\n')
    op_return_by_year.append(content_op_return)

target_words = ['CNTRPRTY', 'PX2', 'omni', 'RSKBLOCK', 'PHOTECTOR', 'PEIRMOBILE','ion', 'THOR',
                'Safex1', 'Safex2', 'IDEA', 'POTX', 'POR', 'BERNSTEIN', 'PROOFSTACK', 'PPk',
                'Bitzlato', 'DOCPROOF']

visualizer = DispersionPlot(target_words, title="Frequência de protocolos")
visualizer.fit(op_return_by_year)
visualizer.show()
