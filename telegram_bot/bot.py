from typing import Final
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from calcs import get_grouped_products, get_one_product, get_departments

TOKEN: Final = '6390262039:AAEC4RLBuCQacUok14wTxkELxVyl5hqSyDM'
BOT_USER_NAME: Final = 'thisisatestingbot_bot'


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('This is an information bot you can get some simple data aggs!')


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('I have limited functions, I can give aggregations based on products, departments '
                                    'or sub-departments! Use /commands for functions')


async def commands_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('My commands are: start, help, commands, get_grouped_products, get_product, get_departments')

async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Sorry, I don't understand that command. Type /commands for a list of available commands.")




if __name__ == '__main__':
    app = Application.builder().token(TOKEN).build()

    # Commands
    app.add_handler(CommandHandler('start', start_command))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('commands', commands_command))
    app.add_handler(CommandHandler('get_grouped_products', get_grouped_products))
    app.add_handler(CommandHandler('get_product', get_one_product))
    app.add_handler(CommandHandler('get_departments', get_departments))
    app.add_handler(MessageHandler(filters.COMMAND | filters.TEXT, unknown_command))


    app.run_polling(poll_interval=3)
