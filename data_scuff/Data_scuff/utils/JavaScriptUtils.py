'''
Created on 24-Jun-2018

@author: srinivasan
'''


class JavaScriptUtils:
    
    @staticmethod
    def getLinkFromWindowOpen(text):
        arguments = text[text.find('window.open('):text.find(');')]
        return arguments.split(',')[0].split('(')[1].replace("'", "")
    
    @staticmethod
    def getLinkFromDocument_loc(text):
        return text[text.find('=') + 1:].strip("'")
    
    @staticmethod
    def getValueFromViewDetail(text):
        arguments = text[text.find('javascript: ViewDetail('):text.find(')')]
        return arguments.split('(')[1].replace("'", "")
    
    @staticmethod
    def getValuesFromdoPost(text):
        arguments = text[text.find('javascript:__doPostBack('):text.find(')')].split(',')
        return {'__EVENTTARGET':arguments.pop(0).split('(')[1].replace("'", ""),
       '__EVENTARGUMENT':arguments.pop(0).replace("'", "")}
