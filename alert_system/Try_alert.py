import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import telegram
import pandahouse
from datetime import date
import io
from read_db.CH import Getch
import sys
import os


chat_id = -691600284
bot = telegram.Bot(token='1799868897:AAG2yb9QMlM_yByoQG2uu_b2FGlARAOjfHA')
dashboard = 'https://superset.lab.karpov.courses/superset/dashboard/791/'

#т.к. число пользователей постоянно растет, считаю целесообразным брать данные за месяц, а не за более ранний период, напрмиер , неделю
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , uniqExact(user_id) as users_lenta
            FROM simulator_20220420.feed_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) 
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'Активные пользователи в ленте'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x = our_time.users_lenta.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений
x1 = our_time.users_lenta.quantile(0.25) - (our_time.users_lenta.quantile(0.75) - our_time.users_lenta.quantile(0.25))*3
x2 = our_time.users_lenta.quantile(0.75) + (our_time.users_lenta.quantile(0.75) - our_time.users_lenta.quantile(0.25))*3


if x < x1:
    diff = round(abs(x/our_time.users_lenta.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.users_lenta.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         

if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)

    chart = "http://superset.lab.karpov.courses/r/960"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , uniqExact(user_id) as users_message
            FROM simulator_20220420.message_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) 
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'Активные пользователи в мессенджере'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x = our_time.users_message.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений
x1 = our_time.users_message.quantile(0.25) - (our_time.users_message.quantile(0.75) - our_time.users_message.quantile(0.25))*3
x2 = our_time.users_message.quantile(0.75) + (our_time.users_message.quantile(0.75) - our_time.users_message.quantile(0.25))*3

if x < x1:
    diff = round(abs(x/our_time.users_message.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.users_message.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         

if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)
    chart = "http://superset.lab.karpov.courses/r/962"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , count(user_id) as users_view
            FROM simulator_20220420.feed_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) and action = 'view'
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'Просмотры'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x = our_time.users_view.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений
x1 = our_time.users_view.quantile(0.25) - (our_time.users_view.quantile(0.75) - our_time.users_view.quantile(0.25))*2.5
x2 = our_time.users_view.quantile(0.75) + (our_time.users_view.quantile(0.75) - our_time.users_view.quantile(0.25))*3

if x < x1:
    diff = round(abs(x/our_time.users_view.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.users_view.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         
    
if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)

    chart = "http://superset.lab.karpov.courses/r/963"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , count(user_id) as users_like
            FROM simulator_20220420.feed_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) and action = 'like'
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'Лайки'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x = our_time.users_like.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений.
x1 = our_time.users_like.quantile(0.25) - (our_time.users_like.quantile(0.75) - our_time.users_like.quantile(0.25))*2.5
x2 = our_time.users_like.quantile(0.75) + (our_time.users_like.quantile(0.75) - our_time.users_like.quantile(0.25))*3

if x < x1:
    diff = round(abs(x/our_time.users_like.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.users_like.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         

if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)

    chart = "http://superset.lab.karpov.courses/r/964"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , round(countIf(user_id, action='like')/countIf(user_id, action='view'),2) as CTR
            FROM simulator_20220420.feed_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) 
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'CTR'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x = our_time.CTR.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений.
x1 = round(our_time.CTR.quantile(0.25) - ((our_time.CTR.quantile(0.75) - our_time.CTR.quantile(0.25))*3),2)
x2 = round(our_time.CTR.quantile(0.75) + ((our_time.CTR.quantile(0.75) - our_time.CTR.quantile(0.25))*3),2)

if x < x1:
    diff = round(abs(x/our_time.CTR.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.CTR.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         

if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    chart = "http://superset.lab.karpov.courses/r/969"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    
data = Getch(''' SELECT
                  toStartOfFifteenMinutes(time) as ts
                , toDate(ts) as date
                , formatDateTime(ts, '%R') as hm
                , count(user_id) as count_message
            FROM simulator_20220420.message_actions
            WHERE ts >=  today() - 8 and ts < toStartOfFifteenMinutes(now()) 
            GROUP BY ts, date, hm
            ORDER BY ts ''').df

metric = 'Кол-во отправленных сообщений'

our_time = data[data.hm.iloc[-1] == data.hm]
#Записываем значение, которое хотим сравнить, в переменную x
x =our_time.count_message.iloc[-1]
our_time.drop(index = our_time.tail(1).index,inplace=True)

#определим границы допустимых значений
x1 = our_time.count_message.quantile(0.25) - (our_time.count_message.quantile(0.75) - our_time.count_message.quantile(0.25))*3
x2 = our_time.count_message.quantile(0.75) + (our_time.count_message.quantile(0.75) - our_time.count_message.quantile(0.25))*3

if x < x1:
    diff = round(abs(x/our_time.count_message.median()-1),2)
elif x > x2:
    diff = round(abs(our_time.count_message.median()/x-1),2)

# если отклонение больше, то вернем 1, в противном случае 0                                            
if x < x1  or x > x2:
    is_well = 1 
else:
    is_well = 0         

if is_well:     
    msg = '''Метрика {metric}:\nтекущее значение = {current_value}\nотклонение от медианного значения = {diff}'''.format(metric=metric, current_value = x,diff=diff)
    #отправляем алерт
    bot.sendMessage(chat_id=chat_id, text=msg)

    chart = "http://superset.lab.karpov.courses/r/970"
    msg_chart = "Ссылка на график - {chart}\nСсылка на дашборд - {dashboard}".format(chart = chart, dashboard = dashboard)
    bot.sendMessage(chat_id=chat_id, text=msg_chart)

    