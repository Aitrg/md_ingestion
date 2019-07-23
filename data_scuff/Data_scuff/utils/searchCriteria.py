'''
Created on 16-Jun-2018

@author: srinivasan
'''
import itertools
import string


class SearchCriteriaException(Exception):
    pass


class SearchCriteria:
    
    @staticmethod
    def rangeAtoZ():
        return [i for i in string.ascii_uppercase]
    
    @staticmethod
    def rangeAAtoZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product([i for i in string.ascii_uppercase],
                                                [i for i in string.ascii_uppercase]))]

    @staticmethod
    def rangeAAAtoZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAtoZZ()))]

    @staticmethod
    def rangeAAAAtoZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAtoZZZ()))]
    
    @staticmethod
    def rangeAAAAAtoZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAtoZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAtoZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAtoZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAtoZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAtoZZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAAtoZZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAAtoZZZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAAAtoZZZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAAAtoZZZZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAAAAtoZZZZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAAAAtoZZZZZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAAAAAtoZZZZZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAAAAAtoZZZZZZZZZZ()))]
    
    @staticmethod
    def rangeAAAAAAAAAAAAtoZZZZZZZZZZZZ():
        return [str(v[0]) + str(v[1]) for v in 
                         list(itertools.product(SearchCriteria.rangeAtoZ(),
                                      SearchCriteria.rangeAAAAAAAAAAAtoZZZZZZZZZZZ()))]
    
    @staticmethod
    def strRange(start, end_or_len):

        def strRange(start, end_or_len, sequence='ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            start = start.upper()
            if not end_or_len.isdigit():
                end_or_len = end_or_len.upper()
            seq_len = len(sequence)
            start_int_list = [sequence.find(c) for c in start]
            if isinstance(end_or_len, int):
                end_int_list = list(start_int_list)
                i = len(end_int_list) - 1
                end_int_list[i] += end_or_len - 1
                while end_int_list[i] >= seq_len:
                    j = end_int_list[i] // seq_len
                    end_int_list[i] = end_int_list[i] % seq_len
                    if i == 0:
                        end_int_list.insert(0, j - 1)
                    else:
                        i -= 1
                        end_int_list[i] += j
            else:
                end_int_list = [sequence.find(c) for c in end_or_len]
            while len(start_int_list) < len(end_int_list) or\
                 (len(start_int_list) == len(end_int_list) and start_int_list <= end_int_list):
                yield ''.join([sequence[i] for i in start_int_list])
                i = len(start_int_list) - 1
                start_int_list[i] += 1
                while start_int_list[i] >= seq_len:
                    start_int_list[i] = 0
                    if i == 0:
                        start_int_list.insert(0, 0)
                    else:
                        i -= 1
                        start_int_list[i] += 1

        vals = list(strRange(start, end_or_len))
        if(len(vals) == 0):
            raise SearchCriteriaException("String Range Search Criteria is wrong(start-{},end-{})".format(
                start, end_or_len))
        return vals
    
    @staticmethod
    def dateRange(start, end, freq='1D', formatter='%Y-%m-%d'):
        import pandas as pd
        date_rng = pd.date_range(str(start), str(end), freq=freq, closed=None)
        vals = date_rng.format(formatter=lambda x: x.strftime(formatter))
        if(len(vals) == 0):
            raise SearchCriteriaException("Date Range Search Criteria is wrong(start-{},end-{})".format(
                start, end))
        if 'M' in freq:
            if(len(vals) >= 2):
                vals.insert(0, ''.join([vals.pop(0)[:-2],
                                        str(start)[-2:]]))
                vals.append(''.join([vals.pop(-1)[:-2],
                                        str(end)[-2:]]))
        return vals 
    
    @staticmethod
    def numberRange(start, end, step=1, zeropadSize=None):
        if not zeropadSize:
            zeropadSize = len(str(end))
        zeropad = '{0:0' + str(zeropadSize) + '}'
        vals = [str(i) if zeropad == '' else '{}'.format(zeropad).format(i) 
            for i in range(int(start), int(end) + 1, int(step))]
        if(len(vals) == 0):
            raise SearchCriteriaException("Number Range Search Criteria is wrong(start-{},end-{})".format(
                start, end))
        return vals
