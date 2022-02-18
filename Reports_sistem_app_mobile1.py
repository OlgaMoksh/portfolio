import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import EngFormatter
from matplotlib.pyplot import figure
import seaborn as sns
import telegram
from read_db.CH import Getch
import io
import os

dir_path = '/analyst_simulator'

def test_reports_feed(chat = None):
    chat_id = chat or  -708816698 #534279539
    bot = telegram.Bot(token = '5049099789:AAEfyY541Q_LQDUvbCqcKkDOkQCNdBIMkZQ')
    #собираем из БД ключевые метрики за вчера
    metrics1d = Getch('''
    select toDate(time) day,
            uniq(user_id) DAU, 
            countIf(user_id, action = 'view') as Views, 
           countIf(user_id, action = 'like') as Likes, 
           round(100*countIf(user_id, action = 'like') / countIf(user_id, action = 'view'),2) as CTR
    from {db}.feed_actions 
    where toDate(time) = date_sub(day, 1, today())
       group by toDate(time)
    ''').df
    metrics1d['day'] = metrics1d['day'].dt.date
    #формируем сообщение для бота
    msg = "<b>Ключевые метрики по ленте новостей за вчерашний день ({0}):</b>\n\n   DAU ={1}k\n Просмотры = {2}k\n  Лайки = {3}k\n  CTR = {4}%".format(metrics1d.iat[0,0], round(metrics1d.iat[0,1]/1000), round(metrics1d.iat[0,2]/1000), round(metrics1d.iat[0,3]/1000), round(metrics1d.iat[0,4]))
    bot.sendMessage(chat_id = chat_id, text = msg, parse_mode="html") #отправляем сообщение через бота
    
    #собираем из БД ключевые метрики за последние 7 дней
    metrics7d = Getch('''
    select toDate(time) day,
           uniq(user_id) DAU, 
           countIf(user_id, action = 'view') as Views, 
           countIf(user_id, action = 'like') as Likes, 
           100*countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR
    from {db}.feed_actions 
    where day between date_sub(day, 7, today()) AND date_sub(day, 1, today())
    group by day
    ''').df
    metrics7d = metrics7d.set_index('day')
    sns.set() #сетка графика
    fig = plt.figure(figsize=(10, 12))

    #строим график DAU
    ax = plt.subplot(3, 1, 1)
    sns.lineplot(x =metrics7d.index , y =metrics7d['DAU'],  label="DAU", color = '#8f1402') 
    fig.autofmt_xdate(rotation=45) #размещаем даты на оси x
    ax.yaxis.set_major_formatter(ticker.EngFormatter()) #задаем инженерный формат подписей на оси y
    
    #строим график лайков и просмотров
    ax1 = plt.subplot(3, 1, 2)
    ax2 = ax1.twinx() #добавляем вспомогательную ось
    
    ax1.plot(metrics7d.index,metrics7d['Views'], 'b-')
    ax2.plot(metrics7d.index,metrics7d['Likes'],'g-')
    ax1.set_xlabel("day")
    fig.autofmt_xdate(rotation=45)
    #форматируем оси
    ax1.set_ylabel("Просмотры",color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    ax1.yaxis.set_major_formatter(ticker.EngFormatter())

    ax2.set_ylabel("Лайки",color='g')
    ax2.tick_params(axis='y', labelcolor='g')
    ax2.yaxis.set_major_formatter(ticker.EngFormatter())
    fig.tight_layout() #чтобы правый край не обрезался
    
        
    #строим график CTR
    ax = plt.subplot(3,1,3)
    sns.lineplot(x = metrics7d.index, y = metrics7d['CTR'], label="CTR", color = '#8f1402') 
    ax.yaxis.set_major_formatter(ticker.PercentFormatter(decimals=1)) #задаем процентный формат с 1 знаком для подписей оси y
    plt.xticks(rotation=45)
    
    #формируем файл
    plot_object2 = io.BytesIO()
    plt.savefig(plot_object2)
    plot_object2.name = 'plots.png'
    plot_object2.seek(0) #чтобы увидеть картинку, возвращаем курсор в начало
    plt.close()
    bot.sendPhoto(chat_id = chat_id, photo = plot_object2)
    

try:
    test_reports_feed()
    
except Exception as e:
    print(e)
    
