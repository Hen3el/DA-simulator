import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.neighbors import LocalOutlierFactor
from sklearn.model_selection import GridSearchCV
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.svm import OneClassSVM
from typing import Tuple
import seaborn as sns
import telegram
import pandahouse
from datetime import date

connection = {'host': '*',
'database':'*',
'user':'*',
'password':'*'
}

default_args = {
    'owner': 'iv-kravets',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2022, 5, 20),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

chat_id = '*'

my_token = '*'
bot = telegram.Bot(token=my_token)

metrics = [['likes', 'views'], ['messages_sent']]
slices = ['os', 'country', 'source']
#Выбор стран в срезе, исходя из максимальной аудитории. 
#В остальных, предполагаю, система будет постоянно репортить алерт из-за малой аудитории.
countries = ['Russia', 'Kazakhstan', 'Ukraine', 'Belarus']

q_test = '''
select
    toStartOfDay(time) as day,
    countIf(action = 'like') as likes
from simulator_20240320.feed_actions
where day >= today() - 14
group by
    day
order by
    day
'''

q_feed = f'''
select
  toStartOfFifteenMinutes(time) as fm,
  formatDateTime(fm, '%R') as hm,
  {', '.join(str(x) for x in slices)},
  countIf(action = 'like') as likes,
  countIf(action = 'view') as views
from simulator_20240320.feed_actions
where fm > yesterday() 
    and fm < toStartOfFifteenMinutes(now()) 
    and country in {*countries,}
group by fm, {', '.join(str(x) for x in slices)}
order by fm'''

q_message = f'''
select
  toStartOfFifteenMinutes(time) as fm,
  formatDateTime(fm, '%R') as hm,
  {', '.join(str(x) for x in slices)},
  count() as messages_sent
from simulator_20240320.message_actions
where fm > yesterday() 
    and fm < toStartOfFifteenMinutes(now())
    and country in {*countries,}
group by fm, {', '.join(str(x) for x in slices)}
order by fm'''

def slice_data(df: pd.DataFrame, slice_var: str) -> pd.DataFrame:
    try:
        sliced_df = df[['fm', 'hm', slice_var, 'likes', 'views']]
    except KeyError:
        sliced_df = df[['fm', 'hm', slice_var, 'messages_sent']]
    return sliced_df

def group_sliced_data(df: pd.DataFrame, slice_var: str) -> pd.DataFrame:
    try:
        ch = df[['fm', 'hm', slice_var, 'likes', 'views']]
    except KeyError:
        ch = df[['fm', 'hm', slice_var, 'messages_sent']]
    grouped_df = ch.groupby(['fm', 'hm', slice_var]).sum().reset_index()
    values = *pd.unique(grouped_df[slice_var]),
    return grouped_df, values

def make_stat_tests(vals: list) -> Tuple[bool, bool]:
    s_rule = False
    tukey = False
    current = vals.pop(0)
    avg, std = np.mean(vals), np.std(vals)
    if current < avg - 2 * std or current > avg + 2 * std: # 95% интервал
        s_rule = True
    p_25, p_75 = np.percentile(vals, 25), np.percentile(vals, 75)
    iqr = p_75 - p_25
    k = 2 # нечто среднее между консервативным коэффициентом 1.5 и более агрессивным 3
    if current < p_25 - k * iqr or current > p_75 + k * iqr:
        tukey = True
    return s_rule, tukey

def prepare_vals(vals: list) -> list:
    scaler = StandardScaler()
    res = np.array(vals).reshape(-1, 1)
    return scaler.fit_transform(res)
                    
def dbs_svm_tests(vals: list, dbs_eps: float = 0.3, svm_nu: float = 0.01) -> Tuple[bool, bool]:
    '''Выявление аномалий методами DBSCAN и OneClassSVM'''
    normalized = prepare_vals(vals)
    min_samples = 5  # Минимальное количество точек в окрестности
    dbscan = DBSCAN(eps=dbs_eps, min_samples=min_samples)
    dbs_result = dbscan.fit_predict(normalized)[-1]
    
    svm = OneClassSVM(nu = svm_nu)
    svm_result = svm.fit_predict(normalized)[-1] 
    
    return dbs_result == -1, svm_result == -1

def LOF_test(vals: list) -> bool:
    '''Выявление аномалии методом LocalOutlierFactor'''
    normalized = prepare_vals(vals)
    lof = LocalOutlierFactor(n_neighbors=5)
    return lof.fit_predict(normalized)[-1] == -1

def simple_check(df: pd.DataFrame, metric: str, threshold: float=0.3) -> bool:
   
    current_ts = df['fm'].max()
    day_ago_ts = current_ts - pd.DateOffset(days=1)

    current_value = df[df['fm'] == current_ts][metric].iloc[0]
    day_ago_value = df[df['fm'] == day_ago_ts][metric].iloc[0]
    # вычисляем отклонение
    if current_value <= day_ago_value:
        diff = abs(current_value / day_ago_value - 1)
    else:
        diff = abs(day_ago_value / current_value - 1)
    return diff > threshold

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def auto_alert_kravets(query):
    
    @task
    def extract_data(query: str):
        return pandahouse.read_clickhouse(query, connection=connection)
    
    @task
    def search_anomaly(df: pd.DataFrame, slices: list, mtrcs: list) -> pd.DataFrame:
        result = pd.DataFrame(columns=[
            'slice', 'value', 'metric',
            'sigma_test', 'tukey_test', 'dbs_test', 'svm_test', 'lof_test', 'simple_test'
        ])
        for slice_name in slices:
            sliced = slice_data(df, slice_name)
            grouped, slice_values = group_sliced_data(sliced, slice_name)
            for value in slice_values:
                for metric in mtrcs:
                    simple_test = simple_check(grouped[grouped[slice_name] == value], metric)
                    metric_vals = list(grouped[grouped[slice_name] == value][metric].values)
                    sigma_test, tukey_test = make_stat_tests(metric_vals)
                    dbs_test, svm_test = dbs_svm_tests(metric_vals)
                    lof_test = LOF_test(metric_vals)
                    row = [
                        slice_name, value, metric, 
                        sigma_test, tukey_test, dbs_test, svm_test, lof_test, simple_test
                    ]
                    result.loc[len(result)] = row
        return result
    
    @task            
    def report_alert(df: pd.DataFrame):
        for index, row in df.iterrows():
            alert = False
            message = f'''
            Метрика: {row['metric']},
            Срез: {row['slice']} в значении {row['value']}.
            '''
            if (row['sigma_test'] or row['tukey_test']) and row['simple_test']:
                # сработал один из статистических тестов + простая проверка
                message += "\U00002754 Выявлена аномалия, уровень угрозы: *Подозрительный*"
                alert = True
            elif (row['sigma_test'] or row['tukey_test'] or row['simple_test']) and row['lof_test']:
                # сработал один из статистических тестов или простая проверка + метрический
                message += "\U00002755 Выявлена аномалия, уровень угрозы: *Опасный*"
                alert = True
            elif (row['sigma_test'] or row['tukey_test']) and (row['dbs_test'] or row['svm_test']) and row['lof_test'] and row['simple_test']:
                # сработал хотя бы 1 тест в каждой группе
                message += "\U00002757 Выявлена аномалия, уровень угрозы: *Подтверждённый*"
                alert = True

            if alert:
                message += '''
                    ссылка на дашборд: http://superset.lab.karpov.courses/r/5422'''
                bot.sendMessage(chat_id=chat_id, text=message, parse_mode='Markdown')
                
    data = extract_data(query)
    try:
        res = search_anomaly(data, slices, metrics[0])
    except KeyError:
        res = search_anomaly(data, slices, metrics[1])
    report_alert(res)

dag_feed = auto_alert_kravets(q_feed)
dag_message = auto_alert_kravets(q_message)