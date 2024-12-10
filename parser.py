import telebot
import requests
import pandas as pd
import pymysql
import time
import threading


DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASSWORD = ''
DB_NAME = 'debil'


API_TOKEN = '7297492916:AAF4PPmENpnozCMRQJpFFrsOFTarMnH3jTk'
bot = telebot.TeleBot(API_TOKEN)
parsing_in_progress = False

prev_buy_volume = 0
prev_sell_volume = 0

def get_db_connection():
    return pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def get_trades(start_date, end_date):
    url = "https://api.exchange.coinbase.com/products/OXT-USD/trades"
    params = {
        'start': start_date,
        'end': end_date,
        'limit': 1000
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        trades = response.json()
        df = pd.DataFrame(trades)
        df['time'] = pd.to_datetime(df['time'])
        df['size'] = pd.to_numeric(df['size'])
        df['price'] = pd.to_numeric(df['price'])
        df['time'] = df['time'].dt.tz_localize(None)
        return df
    else:
        print(f"Ошибка запроса: {response.status_code}")
        return None


def check_volume_and_send_to_telegram(chat_id):
    global prev_buy_volume, prev_sell_volume

    df = get_trades('2024-12-01T00:00:00Z', '2024-12-10T23:59:59Z')

    if df is not None:
        buy_trades = df[df['side'] == 'buy']
        sell_trades = df[df['side'] == 'sell']

        buy_volume = buy_trades['size'].sum()
        sell_volume = sell_trades['size'].sum()

        if buy_volume > sell_volume and prev_buy_volume <= prev_sell_volume:
            message = (
                f"Объем на покупку стал больше, чем на продажу!\n"
                f"Объем на покупку: {buy_volume:.2f}\n"
                f"Объем на продажу: {sell_volume:.2f}"
            )
            bot.send_message(chat_id, message)
            prev_buy_volume = buy_volume
            prev_sell_volume = sell_volume

        elif sell_volume > buy_volume and prev_sell_volume <= prev_buy_volume:
            message = (
                f"Объем на продажу стал больше, чем на покупку!\n"
                f"Объем на продажу: {sell_volume:.2f}\n"
                f"Объем на покупку: {buy_volume:.2f}"
            )
            bot.send_message(chat_id, message)
            prev_buy_volume = buy_volume
            prev_sell_volume = sell_volume


def auto_parse(chat_id):
    global parsing_in_progress
    parsing_in_progress = True
    while parsing_in_progress:
        check_volume_and_send_to_telegram(chat_id)
        time.sleep(15)


def main_menu(chat_id):
    markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(
        telebot.types.KeyboardButton("Старт"),
        telebot.types.KeyboardButton("Стоп"),
        telebot.types.KeyboardButton("Информация по текущим показателям"),
        telebot.types.KeyboardButton("Добавить данные")
    )
    bot.send_message(chat_id, "Выбери действие:", reply_markup=markup)


@bot.message_handler(commands=['start'])
def start(message):
    main_menu(message.chat.id)
    bot.send_message(message.chat.id, "Бот запущен")


@bot.message_handler(func=lambda message: message.text == "Старт")
def start_parsing(message):
    global parsing_in_progress
    if not parsing_in_progress:
        parsing_in_progress = True
        threading.Thread(target=auto_parse, args=(message.chat.id,)).start()
        bot.send_message(message.chat.id, "Парсинг запущен")
    else:
        bot.send_message(message.chat.id, "Парсинг уже запущен")


@bot.message_handler(func=lambda message: message.text == "Стоп")
def stop_parsing(message):
    global parsing_in_progress
    if parsing_in_progress:
        parsing_in_progress = False
        bot.send_message(message.chat.id, "Парсинг остановлен")
    else:
        bot.send_message(message.chat.id, "Парсинг не был запущен")


@bot.message_handler(func=lambda message: message.text == "Информация по текущим показателям")
def send_current_info(message):
    df = get_trades('2024-12-01T00:00:00Z', '2024-12-10T23:59:59Z')
    if df is not None:
        buy_trades = df[df['side'] == 'buy']
        sell_trades = df[df['side'] == 'sell']

        buy_volume = buy_trades['size'].sum()
        sell_volume = sell_trades['size'].sum()

        total_volume = buy_volume + sell_volume

        if total_volume > 0:
            buy_percentage = (buy_volume / total_volume) * 100
            sell_percentage = (sell_volume / total_volume) * 100
        else:
            buy_percentage = sell_percentage = 0

        info_message = (
            f"Текущая информация:\n"
            f"Объем на покупку: {buy_volume:.2f} ({buy_percentage:.2f}%)\n"
            f"Объем на продажу: {sell_volume:.2f} ({sell_percentage:.2f}%)"
        )
        bot.send_message(message.chat.id, info_message)
    else:
        bot.send_message(message.chat.id, "Не удалось получить данные")


@bot.message_handler(func=lambda message: message.text == "Добавить данные")
def add_data_start(message):
    bot.send_message(message.chat.id, "Введите данные в формате: имя, значение")
    bot.register_next_step_handler(message, add_data_to_db)

def add_data_to_db(message):
    try:
        text_content = message.text.strip()  

        if not text_content:  
            bot.send_message(message.chat.id, "Ошибка: текст не может быть пустым.")
            return

        
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = "INSERT INTO actions (text_content) VALUES (%s)"
            cursor.execute(query, (text_content,))
            connection.commit()

        bot.send_message(message.chat.id, "Текст успешно добавлен в базу данных")
    except Exception as e:
        bot.send_message(message.chat.id, f"Ошибка при добавлении текста: {str(e)}")
    finally:
        if 'connection' in locals() and connection:
            connection.close()

bot.polling(none_stop=True)
