import plotly.graph_objects as go

year_list = [2020, 2021]
month_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

data_values_leg = []
data_values_n_leg = []

median_leg = []
median_n_leg = []

for year in year_list:
    for month in month_list:
        with open(f'value_tx/{year}/sum_values-{month}', 'r') as file:
            values = file.read()

        values_leg = values.split('\n')

        values_n_leg = [values_leg[1]]
        values_leg.pop()

        for info_leg, info_n_leg in zip(values_leg, values_n_leg):
            data_values_leg.append(info_leg.split(','))
            data_values_n_leg.append(info_n_leg.split(','))

for data_leg, data_n_leg in zip(data_values_leg, data_values_n_leg):
    number_txs_leg = data_leg[0]
    tx_values_leg = data_leg[1]

    median_leg.append(float(tx_values_leg)/int(number_txs_leg))

    number_txs_n_leg = data_n_leg[0]
    tx_values_n_leg = data_n_leg[1]

    median_n_leg.append(float(tx_values_n_leg)/int(number_txs_n_leg))

print(len(median_leg), '\n', len(median_n_leg))

x = ['jan-2020', 'fev-2020', 'mar-2020', 'abr-2020', 'mai-2020', 'jun-2020', 'jul-2020', 'ago-2020',
     'set-2020', 'out-2020', 'nov-2020', 'dez-2020', 'jan-2021', 'fev-2021', 'mar-2021', 'abr-2021',
     'mai-2021', 'jun-2021', 'jul-2021', 'ago-2021', 'set-2021', 'out-2021', 'nov-2021', 'dez-2021']

fig = go.Figure()

fig.add_trace(go.Scatter(
        x=x,
        y=median_leg,
        name = '<b>Média de conteúdos legíveis</b>',
        connectgaps=False
))
fig.add_trace(go.Scatter(
    x=x,
    y=median_n_leg,
    name='<b>Média de conteúdos não legíveis</b>',
))

fig.update_layout(
        title='Média de transações OP_RETURN',
        xaxis_title='Mês',
        yaxis_title='Valor (US$)',
        font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
        )
)

fig.update_xaxes(title_font_family="Times New Roman")

fig.show()

