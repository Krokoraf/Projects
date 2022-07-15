import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse
import os
import pandahouse


def test_report():
    bot = telegram.Bot(token = '1799868897:AAG2yb9QMlM_yByoQG2uu_b2FGlARAOjfHA')
    chat_id = -799682816



    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator'
    }

    q = 'SELECT user_id, action FROM simulator_20220420.feed_actions where toDate(time) = today()-1'

    df = pandahouse.read_clickhouse(q, connection=connection)

    DAU = df.user_id.value_counts().count()
    Views = df.query('action =="view"').action.count()
    Likes = df.query('action =="like"').action.count()
    CTR = round((df.query('action =="like"').action.count()/df.query('action =="view"').action.count())*100,2)

    msg = f'DAU: {DAU}\nПросмотры: {Views}\nЛайки: {Likes}\nCTR: {CTR}'
    bot.sendMessage(chat_id = chat_id, text = msg )

    q = """SELECT user_id, action, toDate(time) as time
    FROM simulator_20220420.feed_actions where toDate(time)<=today()-1 and toDate(time)>=today()-7"""

    df = pandahouse.read_clickhouse(q, connection=connection)

    sns.set(
        font_scale =1,
        style      ='whitegrid',
        rc         ={'figure.figsize':(15,6)})

    day_active_users=df.groupby('time', as_index=False).agg({'user_id':'nunique'})
    view = df.query('action == "view"').groupby('time', as_index=False).agg({'user_id':'count'})
    like = df.query('action == "like"').groupby('time', as_index=False).agg({'user_id':'count'})
    ctr_df = like.merge(view, on ='time')
    ctr_df['ctr'] = ctr_df['user_id_x']/ctr_df['user_id_y']

    sns.lineplot(x = day_active_users.time, y = day_active_users.user_id)
    plt.title('DAU last week')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'DAU last week.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    sns.lineplot(x = view.time, y = view.user_id)
    plt.title('View last week')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'View last weekt.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    sns.lineplot(x = like.time, y = like.user_id)
    plt.title('Like last week')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'Like last week.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    sns.lineplot(x = ctr_df.time, y = ctr_df.ctr)
    plt.title('CTR last week')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'Like last week.png'
    plt.close()
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

try:
    test_report()
except Exception as e:
    print(e)