import plotly.graph_objects as go

x = ['2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']

fig = go.Figure()

fig.add_trace(go.Scatter(
        x=x,
        y=[0.0157, 0.2766, 0.53848, 1.2392, 3.8331, 14.0454, None, None],
        name = '<b>Coinsecrets</b>',
        connectgaps=False
))
fig.add_trace(go.Scatter(
    x=x,
    y=[None, None, None, None, None, None, 5.6452, 2.9339],
    name='<b>Blockchair</b>',
))

fig.update_layout(
        title='Porcentagem anual de transações com OP_RETURN',
        xaxis_title='Ano',
        yaxis_title='Porcentagem',
        font=dict(
                family="Courier New, monospace",
                size=18,
                color="RebeccaPurple"
        )
        #xaxis=dict(title='Ano', size=18)
)

fig.update_xaxes(title_font_family="Times New Roman")

fig.show()
