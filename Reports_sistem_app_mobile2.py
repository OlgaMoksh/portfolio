import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.ticker import EngFormatter
from matplotlib import rc
import seaborn as sns
import telegram
from telegram import InputMediaPhoto
from read_db.CH import Getch
import io
import os

sns.set()
dir_path = '/reports_feed_automatization'

def test_reports_feed_msg(chat = None):
    chat_id = chat or  -708816698 #534279539
    bot = telegram.Bot(token = '5049099789:AAEfyY541Q_LQDUvbCqcKkDOkQCNdBIMkZQ')
    
    #собираем DAU  с ленты новостей и мессенджера
    uniq_users_7d = Getch('''
        select t1.day day, DAU_feed, DAU_feed_msg
    from  (select toDate(time) day, uniq(user_id) DAU_feed_msg
            from simulator.feed_actions fa
            join simulator.message_actions ma on ma.user_id = fa.user_id
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()) 
            group by day) t1 
    join 
        (select toDate(time) day, uniq(user_id) DAU_feed
            from simulator.feed_actions fa
            left join simulator.message_actions ma on ma.user_id = fa.user_id
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()) and ma.user_id = 0
            group by day) t2 
        on t2.day = t1.day
    order by t1.day
            ''').df
    #изменение типа данных даты
    uniq_users_7d['day'] = uniq_users_7d['day'].dt.date

    #собираем stickiness  с ленты новостей и мессенджера
    stickiness_7d = Getch('''
    select fm.day day, stickiness_feed, stickiness_feed_msg
from  (
        select t1.day, 100*dau/mau as stickiness_feed_msg
        from (select toDate(time) day, uniqExact(user_id) dau
                from simulator.feed_actions fa
                join simulator.message_actions ma on ma.user_id = fa.user_id
                where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()) 
                group by day) t1
        join (select day, uniqExact(user_id) mau
                from (select day+n as day, user_id
                        from 
                            (select user_id, toDate(time) day
                                from simulator.feed_actions fa
                                join simulator.message_actions ma on ma.user_id = fa.user_id
                                ) 
            array join range(30) as n )
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today())
            group by day) t2 
            on t2.day = t1.day
     ) fm
join (
        select t1.day, 100*dau/mau as stickiness_feed
        from  (select toDate(time) day, uniqExact(user_id) dau
                from simulator.feed_actions fa
                left join simulator.message_actions ma on ma.user_id = fa.user_id
                where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()) and ma.user_id = 0
                group by day) t1
        join (select day, uniqExact(user_id) mau
                from (select day+n as day, user_id
                        from 
                            (select user_id, toDate(time) day
                                from simulator.feed_actions fa
                                left join simulator.message_actions ma on ma.user_id = fa.user_id
                                where ma.user_id = 0
                                ) 
            array join range(30) as n )
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today())
            group by day) t2 
            on t2.day = t1.day
     ) f on f.day = fm.day       
''').df

    #изменение типа данных даты
    stickiness_7d['day'] = stickiness_7d['day'].dt.date
    
    #собираем сведения по количеству новых публикаций постов, общему количеству событий в ленте (все просмотры+лайки), количесту чатов
    count_posts_7d = Getch('''
    select publication_date, count_posts, count_events_feed, count_chats
    from (select count(post_id) count_posts, toDate(day) publication_date
            from (select post_id, min(time) day
                    from simulator.feed_actions
                    group by post_id)
            where publication_date between date_sub(day, 7, today()) AND date_sub(day, 1, today())
            group by publication_date) t1
    join (select count(user_id) count_events_feed, toDate(time) day
            from simulator.feed_actions
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today())
            group by day) t2 on t2.day = t1.publication_date
    join (select toDate(time) day, uniq(user_id) count_chats
            from simulator.message_actions
            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today())
            group by day) t3 on t3.day = t1.publication_date   
            ''').df
    
    #Сообщение для бота
    msg = "<b>Ключевые метрики за вчерашний день ({0}):</b>\n\nDAU(only feed) = {1}k\nDAU(msg+feed) = {2}k\n\nStickiness(only feed) = {3}%\nStickiness(msg+feed) = {4}%\n\n<b>Общие сведения:</b>\nКоличество новых публикаций = {5}\nКоличество событий в ленте = {6}k\nКоличество чатов = {7}k".format(uniq_users_7d.iat[6,0], round(uniq_users_7d.iat[6,1]/1000), round(uniq_users_7d.iat[6,2]/1000), round(stickiness_7d.iat[6,1]), 
round(stickiness_7d.iat[6,2]), count_posts_7d.iat[6,1], round(count_posts_7d.iat[6,2]/1000), round(count_posts_7d.iat[6,3]/1000) )
    
    #отправка сообщения ботом
    bot.sendMessage(chat_id = chat_id, text = msg, parse_mode="html")
    
    #Построение графиков DAU с дополнительной осью
    plt.rcParams["figure.figsize"] = (8,7)
    sns.set()
    #fig = plt.figure(figsize = (12,10))
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx() 

    ax1.plot(uniq_users_7d['day'], uniq_users_7d['DAU_feed'],'o-m', label = 'DAU_feed')
    ax2.plot(uniq_users_7d['day'], uniq_users_7d['DAU_feed_msg'], 'o-g', label = 'DAU_feed_msg' )
    

    ax1.set_xlabel("Day")
    ax1.legend(loc = 8)
    fig.autofmt_xdate(rotation=45)


    ax1.set_ylabel("DAU_feed",color='m')
    ax1.tick_params(axis='y', labelcolor='m')
    ax1.yaxis.set_major_formatter(ticker.EngFormatter())

    ax2.set_ylabel("DAU_feed_msg",color='g')
    ax2.tick_params(axis='y', labelcolor='g')
    ax2.yaxis.set_major_formatter(ticker.EngFormatter())
    ax2.legend(loc=3)
    fig.tight_layout() #чтобы не обрезался правый край

    #сохраняем картинку
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'DAU.png'
    plot_object.seek(0)
    plt.close()
    
    #Отправка графика боту
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    #Построение графиков stickiness с дополнительной осью
    plt.rcParams["figure.figsize"] = (8,7)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx() 

    ax1.plot(stickiness_7d['day'], stickiness_7d['stickiness_feed'],'o-m', label = 'Stickiness_feed')
    ax2.plot(stickiness_7d['day'], stickiness_7d['stickiness_feed_msg'], 'o-g', label = 'Stickiness_feed_msg')
    
    ax1.set_xlabel("Day")
    ax1.legend(loc=8)
    fig.autofmt_xdate(rotation=45)

    ax1.set_ylabel("Stickiness_feed",color='m')
    ax1.tick_params(axis='y', labelcolor='m')
    ax1.yaxis.set_major_formatter(ticker.PercentFormatter(decimals=1))


    ax2.set_ylabel("Stickiness_feed_msg",color='g')
    ax2.tick_params(axis='y', labelcolor='g')
    ax2.yaxis.set_major_formatter(ticker.PercentFormatter(decimals=1))
    ax2.legend(loc=3)
    fig.tight_layout() #чтобы не обрезался правый край

    #сохраняем картинку
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.name = 'Stickiness.png'
    plot_object.seek(0)
    plt.close()
    
    #Отправка графика боту
    bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
    #построение графика динамики новых публикаций
    plt.rcParams["figure.figsize"] = (8,7)
    fig, ax = plt.subplots()

    sns.lineplot(x = count_posts_7d['publication_date'], y = count_posts_7d['count_posts'], label="Count_posts", color = '#7e1e9c')
    plt.xticks(rotation=45)

    #сохраняем картинку
    plot_object1 = io.BytesIO()
    plt.savefig(plot_object1)
    plot_object1.name = 'Posts.png'
    plot_object1.seek(0)
    plt.close()

    #Построение графиков данных по количеству событий с дополнительной осью

    plt.rcParams["figure.figsize"] = (8,7)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx() 

    ax1.plot(count_posts_7d['publication_date'], count_posts_7d['count_events_feed'],'o-m', label = 'count_events_feed')
    ax2.plot(count_posts_7d['publication_date'], count_posts_7d['count_chats'], 'o-b', label = 'count_chats')

    ax1.set_xlabel("Day")
    ax1.legend()
    fig.autofmt_xdate(rotation=45)

    ax1.set_ylabel("Count_events_feed",color='m')
    ax1.tick_params(axis='y', labelcolor='m')
    ax1.yaxis.set_major_formatter(ticker.EngFormatter())


    ax2.set_ylabel("Count_chats",color='b')
    ax2.tick_params(axis='y', labelcolor='b')
    ax2.yaxis.set_major_formatter(ticker.EngFormatter())
    ax2.legend()
    fig.tight_layout() #чтобы не обрезался правый край

    #сохраняем картинку
    plot_object2 = io.BytesIO()
    plt.savefig(plot_object2)
    plot_object2.name = 'Events.png'
    plot_object2.seek(0)
    plt.close()
        
    #отправка графиков одним сообщением
    bot.sendMediaGroup(chat_id, media =[ InputMediaPhoto(plot_object1), InputMediaPhoto(plot_object2)    ])
    
    #составляем топ постов каждого дня недели
    top_posts_7d = Getch('''
    select day, post, max_count_events
    from (  select max(count_events) max_count_events, day
            from (select post_id, count(post_id) count_events, day
                        from (
                                select post_id, toDate(time) day
                                        from simulator.feed_actions
                                        where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()))
                group by post_id, day) 
            group by day) t1
    join (  select post_id post, count(post_id) count_events, day
            from (
                    select post_id, toDate(time) day
                            from simulator.feed_actions
                            where day between date_sub(day, 7, today()) AND date_sub(day, 1, today()))
                    group by post_id, day) t2 
        on t1.day = t2.day and t1.max_count_events = t2.count_events  ''').df
    
    #создание csv и отправка ботом
    file_object = io.StringIO()
    top_posts_7d.to_csv(file_object)
    file_object.seek(0)

    file_object.name = 'top_posts_7days.csv'
    bot.sendDocument(chat_id = chat_id, document = file_object)

try:
    test_reports_feed_msg()
except Exception as e:
    print(e)
