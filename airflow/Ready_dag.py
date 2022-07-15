from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
import numpy as np

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator'
}
connection_2 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': '656e2b0c9c',
    'user': 'student-rw',
    'database': 'test'
}

default_args = {
'owner': 'a.lan',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 5, 23),
}

schedule_interval = '0 12 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_lan():
    
    @task()
    def feed():         
        q_feed = """
        select sum(action = 'like') as likes,
            sum(action = 'view') as views,
            user_id, 
            toDate(time) as event_day,
            gender,
            age,
            os
        from simulator_20220420.feed_actions
        where toDate(time) = yesterday()
        group by user_id, 
            event_day, 
            gender,
            age,
            os """
        df_feed = ph.read_clickhouse(q_feed, connection=connection)
        return df_feed
    
    
    @task()
    def msg():    
        q_msg = """
        select l.message_sent,
            l.user_id,
            l.users_sent,
            r.messages_received,
            r.users_received,
            gender,
            age,
            os,
            l.event_day
        from
            (select count(*) as message_sent, 
                user_id,
                count(distinct(reciever_id)) as users_sent,
                toDate(time) as event_day,
                gender,
                age,
                os
            from simulator_20220420.message_actions
            where toDate(time) = yesterday()
            group by user_id, event_day, gender, age, os) as l

            left join

            (select count(*) as messages_received, 
                reciever_id, 
                toDate(time) as event_day, 
                count(distinct(reciever_id)) as users_received
            from simulator_20220420.message_actions
            where toDate(time) = yesterday()
            group by reciever_id, event_day) as r
            on l.user_id = r.reciever_id"""

        df_msg = ph.read_clickhouse(q_msg, connection=connection)
        return df_msg

    @task
    def merge_table(df_feed,df_msg):
        df_merge = df_feed.merge(df_msg, how='outer')
        return df_merge  
    
    @task
    def sum_metric_age(df_merge):
        df_metric_age = df_merge.groupby(['age','event_day'], as_index = False)\
                        [['likes', 'views', 'message_sent','users_sent','messages_received','users_received']].agg('sum').\
                        rename(columns = {'age':'metric'})
        return df_metric_age
    
    @task
    def sum_metric_gender(df_merge):
        df_metric_gender = df_merge.groupby(['gender','event_day'], as_index = False)\
                        [['likes', 'views', 'message_sent','users_sent','messages_received','users_received']].agg('sum').\
                        rename(columns = {'gender':'metric'})
        df_metric_gender.loc[(df_metric_gender.metric ==0), 'metric'] = 'famale'
        df_metric_gender.loc[(df_metric_gender.metric ==1), 'metric'] = 'male'
        return df_metric_gender
    
    @task
    def sum_metric_os(df_merge):
        df_metric_os = df_merge.groupby(['os','event_day'], as_index = False)\
                    [['likes', 'views', 'message_sent','users_sent','messages_received','users_received']].agg('sum').\
                    rename(columns = {'os':'metric'})
        return df_metric_os
    
    @task
    def ready_table(df_metric_age, df_metric_gender, df_metric_os):
        df_ready = pd.concat([df_metric_age, df_metric_gender, df_metric_os])
        df_ready[['likes','views','message_sent','users_sent','messages_received','users_received']] =\
            df_ready[['likes','views','message_sent','users_sent','messages_received','users_received']].astype('int')
        df_ready['event_day']=df_ready['event_day'].apply(lambda x: datetime.isoformat(x))
        return df_ready
    
    @task
    def record_table(df_ready):
        q ="""
        CREATE or replace TABLE test.anlan4(
            metric TEXT,
            event_day DateTime,
            likes INTEGER,
            views INTEGER,
            message_sent INTEGER,
            users_sent INTEGER,
            messages_received INTEGER,
            users_received INTEGER
            )
        ENGINE = MergeTree 
        ORDER BY (event_day)
        """
        ph.execute(q, connection=connection_2)
        ph.to_clickhouse(df = df_ready, table = 'anlan4', index=False, connection=connection_2)  
        
    df_feed = feed()
    df_msg = msg()
    df_merge = merge_table(df_feed,df_msg)
    df_metric_age = sum_metric_age(df_merge)
    df_metric_gender = sum_metric_gender(df_merge)
    df_metric_os = sum_metric_os(df_merge)
    df_ready = ready_table(df_metric_age, df_metric_gender, df_metric_os)
    record_table(df_ready)

dag_lan = dag_lan()        