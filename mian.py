import logging
import os
import dash
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import psycopg2
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
from prophet import Prophet
from prophet.plot import plot_plotly

# pg_connect = {
#     'host': os.environ.get('PG_HOST'),
#     'user': os.environ.get('PG_USER'),
#     'password': os.environ.get('PG_PASSWORD')
# }

# Подключение к базе данных
conn = psycopg2.connect("""
                            host=*******
                            port=6432
                            sslmode=verify-full
                            dbname=db1
                            user=*******
                            password=********
                            target_session_attrs=read-write
                        """)

# Анализ динамики импорта и экспорта товаров
data1 = pd.read_sql("""
                        SELECT
                            s.period::date AS "Период",
                            s.napr AS "Направление",
                            SUM(s."Stoim") AS "Объем торгов",
                            SUM(s."Netto") AS "Суммарный вес",
                            SUM(s."Kol") AS "Суммарное количество товаров"
                        FROM stg.skfo s
                        GROUP BY s.period, s.napr
                        ORDER BY s.period, s.napr;
                    """, conn)

# Анализ структуры импорта и экспорта товаров
data2 = pd.read_sql("""
                        with
                        sum_im as (
                            select
                                SUM(s."Stoim") as s_import
                            from stg.skfo s
                            where s.napr = 'ИМ'
                        ),
                        sum_ex as (
                            select
                                SUM(s."Stoim") as s_export
                            from stg.skfo s
                            where s.napr = 'ЭК'
                        )
                        select
                            s.napr AS "Направление",
                            s.tnved AS "ТНВЭД",
                            t."NAME" AS "Товар",
                        CASE
                            WHEN s.napr = 'ИМ' THEN SUM(s."Stoim") / si.s_import * 100
                            WHEN s.napr = 'ЭК' THEN SUM(s."Stoim") / se.s_export * 100
                        ELSE 0
                        END AS "Доля, %"
                        from stg.skfo s
                        join stg.tnveds as t on s.tnved = t."KOD"
                        cross join sum_im si
                        cross join sum_ex se
                        GROUP BY s.napr, s.tnved, t."NAME", si.s_import, se.s_export
                        ORDER BY "Доля, %" desc
                    """, conn)

# Анализ географии импорта и экспорта товаров
data3 = pd.read_sql("""
                        SELECT
                            s.napr AS "Направление",
                            c."NAME" as "Страна направления",
                            SUM(s."Stoim") AS "Объем торгов"
                        FROM stg.skfo s
                        join stg.countries c on s.nastranapr = c."KOD"
                        GROUP BY s.napr, c."NAME"
                        ORDER BY SUM(s."Stoim") DESC;
                    """, conn)

# Анализ цен на импортируемые и экспортируемые товары
data4 = pd.read_sql("""
                        select distinct
                            s.tnved AS "ТНВЭД",
                            t."NAME" AS "Товар",
                            (avg(case when s.napr = 'ИМ' then s."Stoim" else 0 end) - avg(case when s.napr = 'ЭК' then s."Stoim" else 0 end)) / avg(CASE WHEN napr IN ('ИМ', 'ЭК') THEN s."Stoim" ELSE 0 END) * 100 as "Разница цен, %"
                        from stg.skfo s
                        join stg.tnveds as t on s.tnved = t."KOD"
                        group by s.tnved, t."NAME"
                    """, conn)
# Анализ баланса торговли
data5 = pd.read_sql( """
                        SELECT
                            s.tnved as "ТНВЭД",
                            t."NAME" AS "Товар",
                            SUM(CASE WHEN s.napr = 'ИМ' THEN -1 * s."Stoim" ELSE s."Stoim" END) AS "Разница",
                        CASE WHEN SUM(CASE WHEN s.napr = 'ИМ' THEN -1 * s."Stoim" ELSE s."Stoim" END) < 0 THEN 'Дефицит' ELSE 'Избыток' END AS "Баланс торговли"
                        FROM stg.skfo s
                        join stg.tnveds as t on s.tnved = t."KOD"
                        WHERE s.tnved IS NOT NULL
                        GROUP BY t."NAME", s.tnved
                        order by t."NAME"
                    """, conn)

# Список ТНВЭДов для выпадающего списка
tnveds_name = pd.read_sql("""
                            select distinct
                                t."NAME" as tnved_name
                            from stg.skfo s
                            join stg.tnveds t on s.tnved = t."KOD"
                            order by t."NAME" asc
                        """, conn)

# Список стран для перемещения товаров
countries_name = pd.read_sql("""
                                select distinct
                                    c."NAME" as country_name
                                from stg.skfo s
                                join stg.countries c on s.nastranapr = c."KOD"
                                order by c."NAME"
                            """, conn)

# Список федеральных округов
federal_districts_name = pd.read_sql("""
                                        select distinct
                                            SUBSTR(s."Region_s", 4) as federal_district
                                        from stg.skfo s
                                        order by SUBSTR(s."Region_s", 4)
                                    """, conn)

# Список регионов в округе
subjects_name = pd.read_sql("""
                                select distinct
                                    SUBSTR(s."Region", 9) as subject
                                from stg.skfo s
                                order by SUBSTR(s."Region", 9)
                            """, conn)

# Создание графиков
fig1 = px.line(data1, x='Период', y='Объем торгов', color="Направление")
fig1.update_layout(title='Анализ динамики импорта и экспорта товаров')

fig2 = px.bar(data2, x='Направление', y='Доля, %', text='Товар')
fig2.update_layout(title='Анализ структуры импорта и экспорта товаров')

fig3 = px.bar(data3, x='Направление', y='Объем торгов', text="Страна направления")
fig3.update_layout(title='Анализ географии импорта и экспорта товаров')

fig4 = px.bar(data4, x='ТНВЭД', y='Разница цен, %', text="Товар")
fig4.update_layout(title='Анализ цен на импортируемые и экспортируемые товары')

fig5 = px.bar(data5, x='ТНВЭД', y='Разница', text="Товар")
fig5.update_layout(title='Анализ баланса торговли')

# Создание объекта приложения
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.config.suppress_callback_exceptions = True

# Задание макета приложения
app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
        dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink("Аналитика", href="/analytics")),
            dbc.NavItem(dbc.NavLink("Прогнозирование", href="/forecast")),
        ],
        brand="Таможенная статистика",
        color="dark",
        dark=True,
        sticky="top",
    ),
    html.Div(id='page-content')
])

# Содержимое для вкладки "Аналитика"
tab1_content = dbc.Container([
    dbc.Row([
        dbc.Col(dcc.Graph(id='graph1', figure=fig1)),
        dbc.Col(dcc.Graph(id='graph2', figure=fig4)),
    ]),
    dbc.Row([
        dbc.Col(dcc.Graph(id='graph3', figure=fig3)),
        dbc.Col(dcc.Graph(id='graph4', figure=fig2)),
    ]),
    dbc.Row([
        dbc.Col(dcc.Graph(id='graph5', figure=fig5)),
    ]),
], fluid=True)

# Содержимое для вкладки "Прогнозирование"
tab2_content = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.Label('Выберите ТНВЭД'),
            dcc.Dropdown(
                id="tnved-dropdown",
                options=[
                {"label": col, "value": col} for col in tnveds_name['tnved_name']
                ],
                optionHeight=150,
                maxHeight=350
                # value=’russia’
            ),

            dbc.Label("Выберите страну для перемещения"),
            dcc.Dropdown(
                id="country-dropdown",
                options=[
                {"label": col, "value": col} for col in countries_name['country_name']
                ],
                # value=”sepal length (cm)",
            ),

            dbc.Label("Выберите федеральный округ"),
                dcc.Dropdown(
                id="fo-dropdown",
                options=[
                {"label": col, "value": col} for col in federal_districts_name['federal_district']
                ],
                optionHeight=80,
                value="СЕВЕРО-КАВКАЗСКИЙ ФЕДЕРАЛЬНЫЙ ОКРУГ",
            ),

            dbc.Label("Выберите субъект в округе"),
            dcc.Dropdown(
                id="subject-dropdown",
                options=[
                {"label": col, "value": col} for col in subjects_name['subject']
                ],
                optionHeight=80,
                value="СТАВРОПОЛЬСКИЙ КРАЙ",
            ),

            html.Label('Выберите направление перемещения'),
            dcc.RadioItems(
                id="napr-radio",
                options={
                    'ИМ': 'Импорт',
                    'ЭК': 'Экспорт'
                },
            ),
        ]),
        dbc.Col(dcc.Graph(id='predict-plot')),
    ]),
], fluid=True)

# Реактивное обновление шаблона страницы
@app.callback(
    Output('page-content', 'children'),
    [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/analytics':
        return tab1_content
    elif pathname == '/forecast':
        return tab2_content
    return tab1_content # по умолчанию показываем вкладку "Аналитика"

# Реактивное обновление графика прогноза
@app.callback(
    Output("predict-plot", "figure"),
    [Input("tnved-dropdown", "value")],
    [Input("country-dropdown", "value")],
    [Input("napr-radio", "value")],
    # prevent_initial_call=True
)
def update_predict_graph(tnved_name, country_name, napr_name):
    params = {
        'tnved_name': tnved_name,
        'country_name': country_name,
        'napr_name': napr_name
    }
    df_future = pd.read_sql("""
                                select
                                    s.period::date as date,
                                    sum(s."Stoim") as price
                                from stg.skfo s
                                join stg.tnveds t on s.tnved = t."KOD"
                                join stg.countries c on s.nastranapr = c."KOD"
                                where t."NAME" = %(tnved_name)s and c."NAME" = %(country_name)s and s.napr = %(napr_name)s
                                group by s.period::date
                                order by date asc
                            """, con=conn, params=params)

    df_future = df_future.rename(columns={'date': 'ds', 'price': 'y'})
    # Создание и обучение модели
    model = Prophet(yearly_seasonality='auto', weekly_seasonality='auto')
    model.fit(df_future)

    # Создание фрейма данных для прогнозирования на 3 месяца вперед
    future = model.make_future_dataframe(periods=3, freq='M')

    # Прогнозирование на 30 дней вперед
    forecast = model.predict(future)

    # Вывод результатов прогнозирования
    fig = plot_plotly(model, forecast)
    return fig

if __name__=='__main__':
    app.run_server(debug=True, port=3000)