# /usr/bin/python3

# Author        : Daniel
# Date          : 2024-01-17
import requests
from flask import Flask, request

app = Flask(__name__)

# Telegram Bot Token，需要在 BotFather 那里获取
TELEGRAM_BOT_TOKEN = '6559646803:AAFTKQmzFnx1dzbDT9z3mkqU_RzF2lBw_Fs'
# Chat ID，可以通过与 BotFather 对话获取
TELEGRAM_CHAT_ID = '-972839729'


def send_telegram_message(message):
    telegram_api_url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message
    }
    response = requests.post(telegram_api_url, json=payload)
    if response.status_code != 200:
        print(f'发送Telegram 消息失败:{response.text}')


# 设置 Flask 的 Webhook 路径，例如 /webhook
WEBHOOK_PATH = '/webhook'


# 注册处理器函数到 Flask 应用
@app.route(WEBHOOK_PATH, methods=['POST'])
def forward_to_telegram():
    # 直接将接收到的 POST 请求数据转发给 Telegram Bot
    message = request.get_data(as_text=True)
    send_telegram_message(message)
    return 'OK，消息已发送至telegram'


if __name__ == '__main__':
    # 启动 Flask 应用
    app.run(host='0.0.0.0', port=5000, debug=True)
