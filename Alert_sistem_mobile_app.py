import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt
from matplotlib import rc
import seaborn as sns
import telegram
from telegram import InlineKeyboardMarkup
from telegram import InlineKeyboardButton
from read_db.CH import Getch
import io

dir_path = '/Test_alerts_feed'

def test_alerts(chat = None):
    
        chat_id = chat or -664596965 #chat_id =   534279539
        bot = telegram.Bot(token = '5049099789:AAEfyY541Q_LQDUvbCqcKkDOkQCNdBIMkZQ')

        #формируем список анализируемых метрик с учетом разных коэффициентов для детектирования аномалий
        metric_list_a1 = ['users_feed','CTR','users_msg','msg']
        metric_list_a2 = ['likes','views']

        
        #ссылки на графики ['users_feed', 'likes','views', 'CTR', 'users_msg','msg']
        chart_urls = {'users_feed':'http://superset.lab.karpov.courses/r/229', 'likes':'http://superset.lab.karpov.courses/r/231', \
        'views':'http://superset.lab.karpov.courses/r/230', 'CTR':'http://superset.lab.karpov.courses/r/227', \
        'users_msg':'http://superset.lab.karpov.courses/r/226', 'msg':'http://superset.lab.karpov.courses/r/225'}

        #кнопка с дашбордом
        button = InlineKeyboardButton(text = 'Посмотреть дашборд', url = 'http://superset.lab.karpov.courses/r/190')
        keyboard = InlineKeyboardMarkup([[button]])

        #собираем данные по медианным и квартильным метрикам из ленты новостей в разрезе 15 минут за неделю
        info_feed_week = Getch('''
        select 
                formatDateTime(15mininterval, '%R') time_, 
                quantile(0.1)(users_feed) lower_users_feed,
                quantile(0.5)(users_feed) median_users_feed,
                quantile(0.9)(users_feed) upper_users_feed,
                quantile(0.1)(likes) lower_likes,
                quantile(0.5)(likes) median_likes,
                quantile(0.9)(likes) upper_likes,
                quantile(0.1)(views) lower_views,
                quantile(0.5)(views) median_views,
                quantile(0.9)(views) upper_views,
                quantile(0.1)(CTR) lower_CTR,
                quantile(0.5)(CTR) median_CTR,
                quantile(0.9)(CTR) upper_CTR
        from (select 
                        toStartOfFifteenMinutes(time) 15mininterval,
                        uniqExact(user_id) users_feed,
                        countIf(user_id, action = 'like') likes,
                        countIf(user_id, action = 'view') views,
                        100*countIf(user_id, action = 'like') / countIf(user_id, action = 'view') CTR
                from simulator.feed_actions 
                where 15mininterval  >= today()-8  AND 15mininterval < today()
                group by 15mininterval)
        group by time_
        order by time_
                ''').df
        #собираем данные по метрикам из ленты новостей в разрезе 15 минут за сегодня
        info_feed_today = Getch('''
        select toStartOfFifteenMinutes(time) 15mininterval,
                formatDateTime(15mininterval, '%R') time_,
                uniqExact(user_id) users_feed,
                countIf(user_id, action = 'like') likes,
                countIf(user_id, action = 'view') views,
                100*countIf(user_id, action = 'like') / countIf(user_id, action = 'view') CTR
        from simulator.feed_actions 
        where 15mininterval  >= today()  AND 15mininterval < toStartOfFifteenMinutes(now())
        group by 15mininterval
                ''').df
        #собираем данные по медианным и квартильным метрикам из мессенджера в разрезе 15 минут за прошлую неделю
        info_msg_week = Getch('''
        select formatDateTime(15mininterval, '%R') time_, 
                quantile(0.1)(users_msg) lower_users_msg,
                quantile(0.5)(users_msg) median_users_msg,
                quantile(0.9)(users_msg) upper_users_msg,
                quantile(0.1)(msg) lower_msg,
                quantile(0.5)(msg) median_msg,
                quantile(0.9)(msg) upper_msg
        from (
                select toStartOfFifteenMinutes(time) 15mininterval,
                        uniqExact(user_id) users_msg,
                        count(user_id) msg
                from simulator.message_actions
                where 15mininterval  >= today()-8  AND 15mininterval < today()
                group by 15mininterval)
        group by time_
        order by time_
        ''').df
        #собираем данные по метрикам из мессенджера в разрезе 15 минут за сегодня
        info_msg_today = Getch('''
        select toStartOfFifteenMinutes(time) 15mininterval,
                formatDateTime(15mininterval, '%R') time_,
                uniqExact(user_id) users_msg,
                count(user_id) msg
        from simulator.message_actions
        where 15mininterval  >= today()  AND 15mininterval < toStartOfFifteenMinutes(now())
        group by 15mininterval
        ''').df

        #формируем рабочий датафрейм
        info_feed = info_feed_week.merge(info_feed_today, how='left', on='time_')
        info_msg = info_msg_week.merge(info_msg_today, how='left', on='time_')
        data = info_feed.merge(info_msg, how='inner', on='time_')
        data.drop('15mininterval_y', axis=1, inplace=True)
        #размещаем колонки 
        data = data.reindex(columns=['15mininterval_x','time_', 'lower_users_feed', 'median_users_feed', 'upper_users_feed', 'users_feed',\
                        'lower_likes', 'median_likes', 'upper_likes', 'likes', \
                        'lower_views','median_views', 'upper_views', 'views', \
                        'lower_CTR', 'median_CTR', 'upper_CTR', 'CTR', \
                        'lower_users_msg', 'median_users_msg', 'upper_users_msg', 'users_msg', \
                        'lower_msg', 'median_msg', 'upper_msg', 'msg'])

        #создаем мультииндексы для удобства
        data.columns = pd.MultiIndex.from_tuples(zip(['time', 'time', 'users_feed', 'users_feed', 'users_feed', 'users_feed', \
        'likes','likes','likes','likes', 'views','views', 'views','views', 'CTR', 'CTR','CTR', 'CTR', \
                                        'users_msg','users_msg', 'users_msg', 'users_msg', 'msg', 'msg', 'msg', 'msg'], data.columns))

        #переименовываем колонки для удобства
        data.rename(columns={'lower_users_feed': 'lower', 'median_users_feed': 'median', 'upper_users_feed': 'upper',\
                'lower_likes':'lower', 'median_likes': 'median', 'upper_likes' : 'upper', \
                'lower_views': 'lower', 'median_views':'median','upper_views': 'upper', \
                'lower_CTR': 'lower', 'median_CTR':'median','upper_CTR': 'upper',\
                'lower_users_msg': 'lower', 'median_users_msg':'median','upper_users_msg': 'upper',\
                'lower_msg': 'lower', 'median_msg':'median','upper_msg': 'upper'}, inplace=True)

        #Отделяем текущую 15минутку от исторических данных 
        info_current = data[data.loc[:, ('time', '15mininterval_x')] == data.loc[:, ('time', '15mininterval_x')].max()]

        #создаем функцию для отправки сообщения
        def message(metric, chat_id1 = -664596965):
            alert = ''' <b>!!! Метрика {metric} в срезе {group}.</b> 
            \nТекущее значение: {value}. Отклонение {x}%.
            \nCсылка на риалтайм чарт: {url_chart}
            \nМедианное значение за неделю: {mediana} 
            \n@OlgaMokshina посмотри, пожалуйста.'''.format(metric = metric, \
                    group = info_current.loc[:, ('time', '15mininterval_x')].iloc[0], \
                    value = round(info_current.loc[:, (metric, metric)].iloc[0]), \
            x = round(abs(100*(1 - (info_current.loc[:, (metric, metric)].iloc[0] /  info_current.loc[:, (metric, 'median')].iloc[0])))),                                                    url_chart = chart_urls[metric], mediana = round(info_current.loc[:, (metric, 'median')].iloc[0]))
            bot.send_message(chat_id=chat_id1, text = alert, reply_markup=keyboard, parse_mode="html")


        #создаем функцию для отрисовки и отправки графика метрики текущего дня с доверительным интервалом и метрики в среднем за неделю
        def plot_send(metric, a, data1 = data, chat_id1 = -664596965):
            q_lower = data1.loc[:, (metric, 'lower')]
            q_upper = data1.loc[:, (metric, 'upper')]        
            iqr = q_upper - q_lower
            y_min = q_lower - a*iqr
            y_max = q_upper + a*iqr
            #plt.rcParams["figure.figsize"] = (10,12)
            sns.set()
            fig, ax = plt.subplots(figsize = (10,12))
            plt.fill_between(x = data1.loc[:, ('time', 'time_')] , y1 = y_min, y2 = y_max, edgecolor='g', facecolor='g', alpha=0.3)
            sns.lineplot(x = data1.loc[:, ('time', 'time_')], y = data1.loc[:, (metric, metric)], label = 'Сегодня', palette = 'magma')
            sns.lineplot( x = data1.loc[:, ('time', 'time_')] , y = data1.loc[:, (metric, 'median')] , label = 'В среднем (медиана) за неделю', palette = 'magma')  
            ax.set_xlabel('Время')
            ax.set_ylabel(metric) 
            ax.set_xticks((np.arange(0, len(data.loc[:, ('time', 'time_')])+1, 5)))
            fig.autofmt_xdate(rotation=45) 
            plot_obj = io.BytesIO()#записываем в файл
            plt.savefig(plot_obj)
            plot_obj.name='plot.png'
            plot_obj.seek(0) #перенoc курсора в начало файла 
            plt.close()
            bot.sendPhoto(chat_id=chat_id1, photo = plot_obj)

        #создаем функцию для проверки на аномальность
        def anomaly_detection(alpha, metric_list_a):
            a1 = alpha
            #формируем датафрейм с найденными аномалиями
            #global data_fulfilled 
            #data_fulfilled = pd.DataFrame(columns = ['day', 'time', 'metric', 'value'] )
            for metric1 in metric_list_a:
                q_lower = info_current.loc[:, (metric1, 'lower')].iloc[0] 
                q_upper = info_current.loc[:, (metric1, 'upper')].iloc[0]
                iqr = q_upper - q_lower
                if info_current.loc[:, (metric1, metric1)].iloc[0] < q_lower - a1 * iqr or info_current.loc[:, (metric1, metric1)].iloc[0] > q_upper + a1 * iqr:
                        message(metric1)
                        plot_send(metric1, a1)
                #         new_row = pd.Series({"day": info_current.loc[:, ('time', '15mininterval_x')].iloc[0].strftime('%Y-%m-%d'), "time": info_current.loc[:, ('time', '15mininterval_x')].iloc[0].strftime('%H:%M'),\
                #                      "metric": metric1, "value": info_current.loc[:, (metric1, metric1)].iloc[0]})
                # data_fulfilled = data_fulfilled.append(new_row, ignore_index=True)
                # else:
                #         text = '''Метрика {metric} в порядке'''.format(metric = metric1)
                #         bot.send_message(chat_id=chat_id, text = text)

        #проверяем на аномальность текущее значение метрики 
        anomaly_detection(3, metric_list_a1)
        anomaly_detection(1.5, metric_list_a2)
        
try:
    test_alerts()
except Exception as e:
    print(e)
