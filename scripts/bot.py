import telebot
import os
import dotenv

dotenv.load_dotenv()

TOKEN = os.environ.get('TOKEN_TELEGRAM')  # Substitua com o token do seu bot

bot = telebot.TeleBot("TELEGRAM", parse_mode=None) # You can set parse_mode by default. HTML or MARKDOWN

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "Olá, bem vindo ao bot de automação de relatório de faturamento. Digite /start para iniciar")


@bot.message_handler(func=lambda m: True)
def echo_all(message):
    bot.reply_to(message, message.text)

bot.infinity_polling()
