'''
Created on 15-Sep-2018

@author: srinivasan
'''
import re
import string
from unicodedata import normalize
import unicodedata


class DataCleaning:
    
    def __init__(self):
        self.__ingoresymbol = [i for i in set(string.punctuation) if i not in ['(', ')']]
        self.__symbol = re.compile(r'[\"\#\&\(\)\*\+\/\<\=\>\[\\\]\^\`\{\|\}\~\£¡§µ\!\?\;]+')
        # self.__charcter_unicodes = re.compile(r'(^| ).( |$)')

    def clean(self, text):
        text = self.__normalizeStr(text)
        text = self.__ignoreAscii(text)
        if(self.__isDigit(text)):
            return text.strip()
        # text = self.__charcter_unicodes.sub(' ', text)
        # text = text.strip("".join(self.__ingoresymbol))
        # text = self.__remove_punctuation(text)
        # text = self.__replaceMultipleDots(text)
        # text = self.__replaceMultipleComma(text)
        # text = self.__replacedollar(text)
        # text = self.__removeMutipleDoubleQuotes(text)
        # text = self.__removeMutipleSingleQuotes(text)
        # text = self.__remove_double_spaces(text)
        # text = self.__remove_double_spaces(text)
        # text = re.sub(r"'", "", text)
        text = re.sub(r'"', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    def __isDigit(self, text):
        remove_spec = lambda x: ''.join(e for e in x if e.isalnum())
        __text = remove_spec(text.strip("".join(self.__ingoresymbol)))
        try:
            float(__text)
            return True
        except ValueError:
            return __text.isdigit()
    
    def __remove_double_spaces(self, text):
        return " ".join(text.split())
    
    def __remove_punctuation(self, text):
        return text.translate(string.punctuation)
    
    def __replaceMultipleComma(self, text):
        return re.sub(r'\,+', ",", text) 

    def __replaceMultipleDots(self, text):
        return re.sub(r'\.+', ".", text)
    
    def __replacedollar(self, text):
        return re.sub(r'\$+', "$", text)
    
    def __removeMutipleDoubleQuotes(self, text):
        return re.sub(r'\"+', '"', text)
    
    def __removeMutipleSingleQuotes(self, text):
        return re.sub(r'\'+', "'", text) 
    
    def __normalizeStr(self, text):
        return self.__shave_marks_latin(self.__shave_marks(normalize('NFC', text)))
    
    def __shave_marks(self, txt):
        norm_txt = unicodedata.normalize('NFD', txt)
        shaved = ''.join(c for c in norm_txt
                         if not unicodedata.combining(c))
        return unicodedata.normalize('NFC', shaved)
    
    def __ignoreAscii(self, text):
        text = text.encode().decode('unicode_escape').encode('ascii', 'ignore').decode()
        nkfd_form = unicodedata.normalize('NFKD', text)
        return nkfd_form.encode('ASCII', 'ignore').decode('ASCII')
    
    def __shave_marks_latin(self, txt):
        norm_txt = unicodedata.normalize('NFD', txt)
        latin_base = False
        keepers = []
        for c in norm_txt:
            if unicodedata.combining(c) and latin_base:
                continue
            keepers.append(c)
            if not unicodedata.combining(c):
                latin_base = c in string.ascii_letters
        shaved = ''.join(keepers)
        return unicodedata.normalize('NFC', shaved)