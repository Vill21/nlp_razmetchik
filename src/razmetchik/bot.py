import couchdb
import redis
import random
from telebot import (
    TeleBot,
    types
)
from datetime import datetime
from config import (
    TOKEN,
    COUCH_URL,
    REDIS_HOST,
    REDIS_PORT
)
from task import (
    Stage,
    DecisionType,
    MarkupFactory
)
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(filename='bot.log', level=logging.INFO)


# couchdb constants
couch = couchdb.Server(COUCH_URL)
db = couch['anek_dataset']
# redis constants
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
# telebot constants
bot = TeleBot(TOKEN)
# possible indices
anek_inds = set([str(x) for x in range(0, 121658)])
# current stage
current_stage = Stage.FEATURE
# filtered good aneks
good_aneks = r.smembers('item:filtered')


@bot.message_handler(commands=['start'])
def start_message(message):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton('следующий'))

    bot.send_message(
        message.chat.id,
        'Привет! Я Разметчик, помогаю своему создателю ' +
        'размечать данные для датасета. Сейчас мы работаем над анекдотами. ' +
        'Сначала нужно вычленить из них хорошие. Затем - разбить анекдоты на категории. ' +
        'Когда захочешь поучаствовать, жми на кнопку *следующий*, и я вышлю тебе анекдот! ' +
        'Если захочешь отменить свой выбор, нажми на кнопку с ним ещё раз!',
        parse_mode='Markdown',
        reply_markup=markup
    )


@bot.message_handler(commands=['name'])
def name_message(message):
    try:
        name = message.text.split()[1]

        markup = types.InlineKeyboardMarkup()
        btn_1 = types.InlineKeyboardButton('Да', callback_data=f'name_{name}_да')
        btn_2 = types.InlineKeyboardButton('Нет', callback_data=f'name_{name}_нет')
        markup.row(btn_1, btn_2)

        bot.send_message(
            message.chat.id,
            f'Хотите, чтобы вашим ником стал {name}? ' + 
            'Это удобно, если вы хотите видеть себя в таблице лидеров-разметчиков.',
            parse_mode='Markdown',
            reply_markup=markup
        )
    except IndexError:
        bot.send_message(
            message.chat.id,
            'Похоже, вы применили команду, но не указали аргумент. Введите его после через пробел.',
            parse_mode='Markdown',
        )


@bot.callback_query_handler(func=lambda c: c.data.split('_')[0] == 'name')
def callback_name(call: types.CallbackQuery):
    _, name, decision = call.data.split('_')
    if decision == 'да':
        r.set(f'name:{call.message.chat.id}', name)
        bot.reply_to(
            message=call.message,
            text=f'Хорошо! Будем знакомы, {name}. На всякий случай, можешь менять его, когда захочешь!',
            parse_mode='Markdown'
        )
    else:
        bot.reply_to(
            message=call.message,
            text=f'Ладненько. Если что, то это всегда можно!',
            parse_mode='Markdown'
        )


@bot.message_handler(commands=['status'])
def status_message(message):
    voted_for_anek_ids = r.smembers(f'id:{message.chat.id}')
    nickname = r.get(f'name:{message.chat.id}')
    task = ''
    if current_stage == Stage.DECISION:
        task = 'разметка хороших анекдотов'
    elif current_stage == Stage.FEATURE:
        task = 'разметка категорий анекдотов'

    if nickname:
        bot.send_message(
            message.chat.id,
            f'В данный момент решается задача: *{task}*. Вы разметили: *{len(voted_for_anek_ids)}*. Ваш ник: {nickname}.',
            parse_mode='Markdown',
        )
    else:
        bot.send_message(
            message.chat.id,
            f'В данный момент решается задача: *{task}*. Вы разметили: *{len(voted_for_anek_ids)}*.',
            parse_mode='Markdown',
        )


@bot.message_handler(commands=['remove_keyboard'])
def status_message(message):
    bot.send_message(
        message.chat.id,
        f'Хорошо! Клавиатура испарилась!',
        reply_markup=types.ReplyKeyboardRemove()
    )


@bot.callback_query_handler(func=lambda c: c.data.split('_')[0] in [Stage.DECISION, Stage.FEATURE])
def callback_vote(call: types.CallbackQuery):
    stage, vote, ind = call.data.split('_')

    json_document = db[f'anek_{ind}']

    chat_id = str(call.message.chat.id)
    if chat_id not in json_document['voted'].keys():
        json_document['voted'][chat_id] = {Stage.DECISION: 0, Stage.FEATURE: []}
        if stage == Stage.DECISION:
            json_document['voted'][chat_id][f'date_{Stage.DECISION}'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        else:
            json_document['voted'][chat_id][f'date_{Stage.FEATURE}'] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

    if stage == Stage.DECISION:
        if vote == DecisionType.GOOD:
            json_document['voted'][chat_id][Stage.DECISION] = 1
        else:
            json_document['voted'][chat_id][Stage.DECISION] = -1
    elif stage == Stage.FEATURE:
        if vote in json_document['voted'][chat_id][Stage.FEATURE]:
            json_document['voted'][chat_id][Stage.FEATURE].remove(vote)
        else:
            json_document['voted'][chat_id][Stage.FEATURE].append(vote)

    try:
        db.save(json_document)

        logging.debug(f'{chat_id} {vote} {ind}')
        logging.debug(f'{json_document}')

        if stage == Stage.FEATURE:
            reply_vote = ', '.join(['*' + str(x) + '*' for x in json_document["voted"][chat_id][Stage.FEATURE]])
        elif stage == Stage.DECISION:
            reply_vote = vote
        logging.debug(f'{reply_vote}')

        bot.reply_to(
            message=call.message,
            text=f'Спасибо! Для вас анекдот {reply_vote}.',
            parse_mode='Markdown'
        )
    except Exception as e:
        logging.exception(f'{e}')

        bot.reply_to(
            message=call.message,
            text=f'Ой, что-то случилось. Не смог записать ваш выбор, простите.',
            parse_mode='Markdown'
        )


@bot.message_handler(func=lambda message: message.text == 'следующий')
def next_anek(message):
    voted_for_anek_ids = r.smembers(f'id:{message.chat.id}')
    logging.debug(str(voted_for_anek_ids))

    if current_stage == Stage.FEATURE:
        vacant_ids = good_aneks.difference(voted_for_anek_ids)
        if not vacant_ids:
            vacant_ids = anek_inds.difference(voted_for_anek_ids)
    elif current_stage == Stage.DECISION:
        vacant_ids = anek_inds.difference(voted_for_anek_ids)

    ind = random.choice(list(vacant_ids))
    json_document = db[f'anek_{ind}']

    if current_stage == Stage.DECISION:
        markup = MarkupFactory.stageDecision(ind)
    elif current_stage == Stage.FEATURE:
        markup = MarkupFactory.stageFeature(ind)

    bot.send_message(
        message.chat.id,
        json_document['text'],
        reply_markup=markup
    )

    r.sadd(f'id:{message.chat.id}', ind)


logger.info('Started infinity pooling')
bot.infinity_polling()
