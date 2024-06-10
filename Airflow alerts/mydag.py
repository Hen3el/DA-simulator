import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

chat_id = '*'
my_token = '*'
bot = telegram.Bot(token=my_token) # получаем доступ

default_args = {
    'owner': 'iv-kravets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 4, 28),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

connection = {'host': '*',
'database':'*',
'user':'*',
'password':'*'
}

query = '''
SELECT
  toStartOfDay(toDateTime(time)) as time,
  count(distinct user_id) as DAU,
  round(
    countIf(action = 'like') / countIf(action = 'view'),
    3
  ) AS CTR,
  countIf(action = 'like') as likes,
  countIf(action = 'view') as views
FROM
  simulator_20240320.feed_actions
WHERE
  toDate(time) >= today() - 7
  and toDate(time) < today()
GROUP BY
  time
ORDER BY
  time
'''

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_tg_bot():
    
    @task
    def extract_data(q):
        df = ph.read_clickhouse(query=q, connection=connection)
        return df
    
    @task
    def make_report(df):
        date = datetime.today().date() - timedelta(days=1)
        up = '\U0001F4C8'
        down = '\U0001F4C9'
        report = f'Сводка за {date.strftime("%d.%m.%y")}'
        for metric in df.columns[1:]:
            value = df[metric].iloc[-1]
            avg = round(df[metric].mean(), 2)
            week_growth = round((value - avg) / avg * 100, 2)
            day_growth = round((df[metric].pct_change() * 100)[6], 2)
            report += f'''

            <b>{metric}: {value}</b>,
            {up if week_growth>0 else down}{week_growth}% к среднему за неделю,
            {up if day_growth>0 else down}{day_growth}% к вчерашнему дню. '''
        
        return report
    
    @task
    def make_plot(df):
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(10, 8), sharex=True)

        # График для метрики CTR
        ax1.plot(df['time'], df['CTR'], color='green', label='CTR')
        ax1.set_ylabel('CTR', color='green')
        ax1.tick_params(axis='y', labelcolor='green')
        ax1.legend()

        # График для метрики DAU
        ax2.plot(df['time'], df['DAU'], color='blue', label='DAU')
        ax2.set_ylabel('DAU', color='blue')
        ax2.tick_params(axis='y', labelcolor='blue')
        ax2.legend()

        # График для метрик likes и views
        ax3.plot(df['time'], df['likes'], color='red', label='likes')
        ax3.plot(df['time'], df['views'], color='blue', label='views')
        ax3.set_ylabel('likes and views', color='red')
        ax3.tick_params(axis='y', labelcolor='red')
        ax3.legend()

        # Добавление подписей к осям и заголовка
        plt.xlabel('Дата')
        plt.xticks(rotation=45)
        plt.suptitle('Метрики')
        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        return plot_object
    
    @task
    def load_data_bot(report, image):
        bot.sendMessage(chat_id=chat_id, text=report, parse_mode='HTML')
        bot.sendPhoto(chat_id=chat_id, photo=image)
        
    
    df = extract_data(query)
    report = make_report(df)
    plot = make_plot(df)
    load_data_bot(report, plot)
    
dag_report_tg_bot = dag_report_tg_bot()