import g4f
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta
import re

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

chat_id = '*'

my_token = '*'
bot = telegram.Bot(token=my_token) # получаем доступ
# Данные берутся из созданной ранее таблицы в Test, где хранятся срезы. Не зря же их делали:) Все данные верные, за то задание 10/10.
query_sliced_data = '''
select
  *
from
  test.kravets_iv
where
  event_date >= today() - 7
'''

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'iv-kravets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 21),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

connection = {'host': '*',
'database':'*',
'user':'*',
'password':'*'
}

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_full_report_kravets():

    @task
    def extract_sliced_data(query):
            df = ph.read_clickhouse(query, connection=connection)
            return df
        
    @task
    def prepare_slice(df, dimension: str):
        dimension_slice = df.loc[df['dimension']==dimension]
        dimension_slice['like_CTR'] = dimension_slice['likes'] / dimension_slice['views']
        dimension_slice['message_CTR'] = dimension_slice['messages_sent'] / dimension_slice['messages_recieved']
        df_report = dimension_slice[['event_date', 'dimension', 'dimension_value', 'like_CTR', 'message_CTR']]
        return dimension_slice
    
    @task
    def get_plot(df, metric: str):
        plt.figure(figsize=(12, 6))
        for group in df['dimension_value'].unique():
            group_data = df[df['dimension_value'] == group]
            plt.plot(group_data['event_date'], group_data[metric], marker='o', label=group)
        plt.xlabel('Date')
        plt.ylabel(metric)
        plt.title(metric)
        plt.xticks(rotation=45)
        plt.legend()
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = metric + '.png'
        plt.close()
        return plot_object
    
    @task
    def make_report(df):
        promt = f'''Проанализируй данные и сделай по ним вывод. {df}'''
        response = g4f.ChatCompletion.create(
            model = 'gpt-3.5-turbo',
            messages = [{'role': 'user', 'content': promt}],
            stream = True)
        return response
    
    @task
    def send_report(response, plot_list):
        for plot in plot_list:
            bot.sendPhoto(chat_id=chat_id, photo=plot)
        gpt_report = '''
        Анализ предоставлен GPT. Не является бизнес-рекомендацией
        -----------------------------
        
        '''
        for message in response:
            gpt_report += message

        bot.sendMessage(chat_id=chat_id, text=gpt_report, parse_mode='Markdown')
        
    df = extract_sliced_data(query=query_sliced_data)
    gender_sliced = prepare_slice(df, 'gender')
    os_sliced = prepare_slice(df, 'os')
    plot1 = get_plot(gender_sliced, 'like_CTR')
    plot2 = get_plot(gender_sliced, 'message_CTR')
    report = make_report(gender_sliced)
    if bool(re.search('[а-яА-Я]', response[0])): #изредка отчёт приходит на китайском
        report = make_report(sliced_df)
    send_report(report, [plot1, plot2])
    plot1 = get_plot(os_sliced, 'like_CTR')
    plot2 = get_plot(os_sliced, 'message_CTR')
    report = make_report(os_sliced)
    send_report(report, [plot1, plot2])
    
dag_full_report_kravets = dag_full_report_kravets()