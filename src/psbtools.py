""" moduł """
import sys
import re
import roman as romenum
from wikibaseintegrator.datatypes import Time, Item
from wikibaseintegrator.wbi_enums import WikibaseDatePrecision


class DateBDF:
    """ obługa daty urodzenia, śmierci lub flourit """

    P_SOURCING_CIRCUMSTANCES = 'P502'
    P_REFINE_DATE = 'P490'
    P_DATE_OF_BIRTH = 'P422'
    P_DATE_OF_DEATH = 'P423'
    P_EARLIEST_DATE = 'P432'
    P_LATEST_DATE = 'P464'
    P_INFORMATION_STATUS = 'P458'
    P_FLORUIT = 'P444'

    # uaktualnić dla instancji testowej!
    Q_CIRCA = 'Q37979'
    Q_FIRST_HALF = 'Q40688'
    Q_SECOND_HALF = 'Q41336'
    Q_BEGINNING_OF = 'Q41337'
    Q_MIDDLE_OF = 'Q41338'
    Q_END_OF = 'Q41339'
    Q_FIRST_QUARTER = 'Q49427'

    def __init__(self, text:str, typ:str = '') -> None:
        """ init, typ - B data urodzenia, D - data śmierci """
        self.text_org = text
        self.text = text.strip().lower()
        self.type = typ
        self.date = ''
        self.date_2 = ''
        self.about = False
        self.between = False
        self.or_date = False
        self.turn = False
        self.before = False
        self.after = False
        self.certain = False
        self.first_half = False
        self.second_half = False
        self.beginning_of = False
        self.middle_of = False
        self.end_of = False
        self.first_quarter = False
        self.somevalue = False
        self.roman = self.roman_numeric()
        if not self.type:
            self.find_type()
        self.find_uncertainty()
        self.find_date()
        if not self.certain and (self.before or self.after or self.between):
            self.somevalue = True


    def find_type(self):
        """ ustala typ daty """
        if 'zm.' in self.text or 'zmarł' in self.text:
            self.type = 'D'
        elif 'um.' in self.text:
            self.type = 'D'
        elif 'ur.' in self.text:
            self.type = 'B'
        else:
            self.type = 'F'


    def find_uncertainty(self):
        """ ustala czy jest i jaka niepewność dla daty """

        tmp = self.text.replace('zm.', '').replace('um.', '').replace('ur.', '').strip()
        if tmp.isnumeric():
            self.certain = True

        if 'prawdopodobnie' in self.text:
            self.about = True

        if 'ok.' in self.text or 'około' in self.text or 'ok ' in self.text:
            self.about = True

        if 'między' in self.text or 'miedzy' in self.text:
            self.between = True

        if 'w okresie II wojny światowej' in self.text_org:
            self.between = True

        if 'lub nieco później' in self.text:
            self.after = True

        if 'w/przed' in self.text:
            self.before = True

        if 'przed lub w' in self.text:
            self.before = True

        if 'w lub po' in self.text or 'po lub w' in self.text:
            self.after = True

        if ('prawdopodobnie' in self.text or 'zapewne' in self.text
            or 'rzekomo' in self.text):
            self.about = True

        if ('lub' in self.text and not 'po ' in self.text
               and not ' w ' in self.text and not 'przed ' in self.text
               and not 'nieco później' in self.text):
            self.or_date = True

        if 'przed ' in self.text:
            self.before = True

        if 'po ' in self.text:
            self.after = True

        if 'nie później niż' in self.text:
            self.before = True

        if 'najpóźniej' in self.text:
            self.before = True

        # niepewność
        if '?' in self.text:
            self.about = True

        # przełom lat
        match = re.search(r'\d{3,4}/\d{1,2}', self.text)
        if match:
            self.turn = True

        if '1 poł.' in self.text or 'i poł.' in self.text or '1. poł.' in self.text:
            self.first_half = True
        elif '2 poł.' in self.text or 'ii poł.' in self.text or '2. poł.' in self.text:
            self.second_half = True
        elif '1. ćwierć' in self.text:
            self.first_quarter = True
        elif 'pocz.' in self.text:
            self.beginning_of = True
        elif 'koniec' in self.text or 'w końcu' in self.text:
            self.end_of = True
        elif 'poł.' in self.text:
            self.middle_of = True


    def roman_numeric(self) -> bool:
        """ czy liczba rzymska oznaczająca wiek?
            (ale nie część daty np. 3 V 1458)
        """
        # jeżeli to specyficzny zapis:
        if 'II wojny' in self.text_org:
            return False

        pattern_test = r'\d{1,2}\s+[IVX]{1,4}\s+\d{4}'
        match = re.search(pattern_test, self.text_org)
        if match:
            return False

        pattern = r'[IVX]{1,5}\s+w\.{0,1}'
        match = re.search(pattern, self.text_org)
        if not match:
            pattern = r'[IVX]{1,5}'
            match = re.search(pattern, self.text_org)

        return bool(match)

    def find_date(self):
        """ wyszukuje daty """
        # XVI w.
        if self.roman:
            matches = [x.group() for x in re.finditer(r'[IVX]{1,5}', self.text_org)]
            if len(matches) == 1:
                self.date = str(romenum.fromRoman(matches[0]))
            elif len(matches) == 2:
                matches = [str(romenum.fromRoman(x)) for x in matches]
                self.date = matches[0]
                self.date_2 = matches[1]
                if '/' in self.text:
                    self.between = True
                    self.somevalue = True
        # 1523/4
        elif self.turn:
            match = re.search(r'\d{3,4}/\d{1,2}', self.text)
            if match:
                v_list = match.group().split('/')
                v_list1 = v_list[0].strip()
                v_list2 = v_list1[:len(v_list1)-len(v_list[1].strip())] + v_list[1].strip()
                self.date = v_list1
                self.date_2 = v_list2
        # zwykłe daty (jeszcze obsługa dat dziennych i miesięcznych do zrobienia)
        else:
            if 'w okresie II wojny światowej' in self.text_org:
                self.date = '1939'
                self.date_2 = '1945'
            else:
                # test czy to nie data dzienna
                pattern_test = r'\d{1,2}\s+[IVX]{1,4}\s+\d{4}'
                match = re.search(pattern_test, self.text_org)
                if match:
                    t_match = match.group().split(' ')
                    y = t_match[2]
                    m = str(romenum.fromRoman(t_match[1]))
                    d = t_match[0]
                    self.date = f'{y}-{m.zfill(2)}-{d.zfill(2)}'
                else:
                    # a może jest podany miesiąc?
                    m = []
                    months = {'stycz':'01', 'luty':'02', 'maj':'05',
                              'marzec':'03', 'marcem':'03', 'sierpn':'08',
                              'wrześniem':'09', 'kwie':'04', 'listo':'11',
                              'czerw':'06', 'lip':'07', 'paźdz':'10',
                              'grud':'12'}
                    for key, value in months.items():
                        if key in self.text_org:
                            m.append(value)

                    pattern = r'\d{3,4}'
                    matches = [x.group() for x in re.finditer(pattern, self.text)]
                    if len(matches) == 1:
                        self.date = matches[0]
                        if len(m) == 1:
                            self.date += '-'+ m[0]
                    elif len(matches) > 1:
                        self.date = matches[0]
                        if len(m) > 1:
                            self.date += '-'+ m[0]
                        self.date_2 = matches[1]
                        if len(m) > 1:
                            self.date += '-'+ m[1]



    def _format_date(self, value: str) -> str:
        """ formatuje datę na sposób oczekiwany przez Wikibase ale z precyzją na końcu do przekształcenia
            na osobny parametr
            np. +1839-00-00T00:00:00Z/11
        """
        result = ''
        if len(value) == 4:                          # tylko rok np. 1525
            result = f"+{value}-00-00T00:00:00Z/9"
        elif len(value) == 3:                        # tylko rok np. 980
            result = f"+{value.zfill(4)}-00-00T00:00:00Z/9"
        elif len(value) == 7 and '-' in value:       # rok i miesiąc
            result = f"+{value}-00T00:00:00Z/10"
        elif len(value) == 10:                       # dokłada data
            if value.endswith('00'):                 # jeżeli brak daty dziennej
                result = f"+{value}T00:00:00Z/10"
            else:
                result = f"+{value}T00:00:00Z/11"
        elif len(value) == 2 and value.isnumeric():  # wiek
            result = f"+{str(int(value)-1)}01-00-00T00:00:00Z/7"
        elif len(value) == 1 and value.isnumeric():  # wiek np. X
            value = str(int(value)-1)
            result = f"+{value.zfill(2)}01-00-00T00:00:00Z/7"

        return result

    def time_from_string(self, value:str, prop: str, ref:list=None, qlf_list:list=None) -> Time:
        """ przekształca datę na time oczekiwany przez wikibase """

        if value == 'somevalue':
            return Time(prop_nr=prop, time=None, snaktype='somevalue',
                        references=ref, qualifiers=qlf_list)
        else:
            precision = None
            if value.startswith('+'):
                value = value[1:]
                pos = value.find(r'/')
                if pos != -1:
                    precision_str = value[pos + 1:].strip()

                    pos = value.find('T')
                    if pos != -1:
                        value = value[:pos]

                    if precision_str == '7':
                        precision = WikibaseDatePrecision.CENTURY
                    elif precision_str == '8':
                        precision = WikibaseDatePrecision.DECADE
                    elif precision_str == '9':
                        precision = WikibaseDatePrecision.YEAR
                    elif precision_str == '10':
                        precision = WikibaseDatePrecision.MONTH
                    elif precision_str == '11':
                        precision = WikibaseDatePrecision.DAY
                else:
                    print('ERROR: time_from_string - ', value)
                    sys.exit(1)

            tmp = value.split('-')
            year = tmp[0].zfill(4)
            month = tmp[1]
            day = tmp[2]

            if not precision:
                precision = WikibaseDatePrecision.YEAR
                if day != '00':
                    precision = WikibaseDatePrecision.DAY
                elif day == '00' and month != '00':
                    precision = WikibaseDatePrecision.MONTH
                    day = '01'
                else:
                    day = month = '01'
            else:
                if day == '00':
                    day = '01'
                if month == '00':
                    month = '01'

            format_time =  f'+{year}-{month}-{day}T00:00:00Z'

            print(format_time)
            print(precision)
            return Time(prop_nr=prop, time=format_time, precision=precision,
                    references=ref, qualifiers=qlf_list)


    def prepare_st(self, ref=None) -> Time:
        """ tworzy deklaracje (statements) na podstawie daty"""
        print_date = print_date_2 = print_kw_date = print_kw_date_2 = ''

        if self.somevalue:
            print_date = 'somevalue'
            print_kw_date = self._format_date(self.date)
            print_kw_date_2 = self._format_date(self.date_2)
        else:
            print_date = self._format_date(self.date)
            if self.or_date or self.turn:
                print_date_2 = self._format_date(self.date_2)

        if self.type == 'B':
            print_type = self.P_DATE_OF_BIRTH
        elif self.type == 'D':
            print_type = self.P_DATE_OF_DEATH
        elif self.type == 'F':
            print_type = self.P_FLORUIT
        else:
            print('ERROR: nieokreślony typ daty:', self.date, self.date_2)
            sys.exit(1)

        qualifier_list = []
        statement = statement_2 = None

        if self.about:
            qualifier = Item(value=self.Q_CIRCA, prop_nr=self.P_SOURCING_CIRCUMSTANCES)
            qualifier_list.append(qualifier)
        if self.or_date:
            qualifier_2 = None
            if self.about:
                qualifier_2 = [Item(value=self.Q_CIRCA, prop_nr=self.P_SOURCING_CIRCUMSTANCES)]

            statement_2 = self.time_from_string(print_date_2,
                                          print_type,
                                          ref=ref,
                                          qlf_list=qualifier_2)
        if self.turn:
            qualifier_2 = None
            if self.about:
                qualifier_2 = [Item(value=self.Q_CIRCA, prop_nr=self.P_SOURCING_CIRCUMSTANCES)]
            statement_2 = self.time_from_string(print_date_2,
                                          print_type,
                                          ref=ref,
                                          qlf_list=qualifier_2)
        if self.after:
            qualifier = self.time_from_string(print_kw_date, self.P_EARLIEST_DATE)
            qualifier_list.append(qualifier)

        if self.before and self.after: # dla opisu: zm. w lub po 1458 a przed 1467
            qualifier = self.time_from_string(print_kw_date_2, self.P_LATEST_DATE)
            qualifier_list.append(qualifier)
        elif self.before:
            qualifier = self.time_from_string(print_kw_date, self.P_LATEST_DATE)
            qualifier_list.append(qualifier)

        if self.between:
            qualifier = self.time_from_string(print_kw_date, self.P_EARLIEST_DATE)
            qualifier_list.append(qualifier)
            qualifier = self.time_from_string(print_kw_date_2, self.P_LATEST_DATE)
            qualifier_list.append(qualifier)
        if self.beginning_of:
            qualifier = Item(value=self.Q_BEGINNING_OF, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)
        if self.middle_of:
            qualifier = Item(value=self.Q_MIDDLE_OF, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)
        if self.end_of:
            qualifier = Item(value=self.Q_END_OF, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)
        if self.first_half:
            qualifier = Item(value=self.Q_FIRST_HALF, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)
        if self.second_half:
            qualifier = Item(value=self.Q_SECOND_HALF, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)
        if self.first_quarter:
            qualifier = Item(value=self.Q_FIRST_QUARTER, prop_nr=self.P_REFINE_DATE)
            qualifier_list.append(qualifier)

        statement = self.time_from_string(print_date,
                                          print_type,
                                          ref=ref,
                                          qlf_list=qualifier_list)
        return statement, statement_2
