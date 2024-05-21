from enum import StrEnum
from telebot import types


class Stage(StrEnum):
    DECISION = 'decision',
    FEATURE = 'feature'


class DecisionType(StrEnum):
    GOOD = 'хороший',
    BAD = 'плохой'


class FeatureType(StrEnum):
    POLITICAL = 'политический',
    PERVERT = 'пошлый',
    EVERYDAY = 'бытовой'
    DIFFERENT = 'другой'


class MarkupFactory:
    @staticmethod
    def stageDecision(ind: int):
        """
        Makes markup with buttons for deciding good jokes.

        :param ind: index of the joke in a dataset
        :type ind: int
        :returns: markup for telebot
        :rtype: InlineKeyboardMarkup
        """
        markup = types.InlineKeyboardMarkup()
        btn_1 = types.InlineKeyboardButton(str(DecisionType.GOOD), callback_data=f'{Stage.DECISION}_{DecisionType.GOOD}_{ind}')
        btn_2 = types.InlineKeyboardButton(str(DecisionType.BAD), callback_data=f'{Stage.DECISION}_{DecisionType.BAD}_{ind}')

        markup.row(btn_1, btn_2)
        return markup

    @staticmethod
    def stageFeature(ind: int):
        """
        Makes markup with buttons for jokes feature labelling.

        :param ind: index of the joke in a dataset
        :type ind: int
        :returns: markup for telebot
        :rtype: InlineKeyboardMarkup
        """
        markup = types.InlineKeyboardMarkup()
        btn_1 = types.InlineKeyboardButton(str(FeatureType.PERVERT), callback_data=f'{Stage.FEATURE}_{FeatureType.PERVERT}_{ind}')
        btn_2 = types.InlineKeyboardButton(str(FeatureType.POLITICAL), callback_data=f'{Stage.FEATURE}_{FeatureType.POLITICAL}_{ind}')
        btn_3 = types.InlineKeyboardButton(str(FeatureType.EVERYDAY), callback_data=f'{Stage.FEATURE}_{FeatureType.EVERYDAY}_{ind}')
        btn_4 = types.InlineKeyboardButton(str(FeatureType.DIFFERENT), callback_data=f'{Stage.FEATURE}_{FeatureType.DIFFERENT}_{ind}')

        markup.row(btn_1, btn_2)
        markup.row(btn_3, btn_4)
        return markup