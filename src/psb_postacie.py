""" skrypt do importu postaci z PSB
    uwaga: wymaga biblioteki WikibaseIntegrator w wersji 0.12 lub nowszej
"""
import os
import sys
import time
import json
import logging
from logging import Logger
import warnings
from pathlib import Path
from dotenv import load_dotenv
from wikibaseintegrator import WikibaseIntegrator
from wikibaseintegrator.wbi_config import config as wbi_config
from wikibaseintegrator import wbi_login
from wikibaseintegrator.datatypes import ExternalID, Time, MonolingualText, Item, URL, String
from wikibaseintegrator import wbi_helpers
from wikibaseintegrator.wbi_enums import WikibaseDatePrecision
from wikibaseintegrator.wbi_exceptions import MWApiError
from wikibaseintegrator.wbi_enums import ActionIfExists, WikibaseSnakType
from psbtools import DateBDF
import roman as romenum

# czy zapis do wikibase czy tylko test
WIKIBASE_WRITE = True

warnings.filterwarnings("ignore")

# adresy wikibase
wbi_config['SPARQL_ENDPOINT_URL'] = 'https://prunus-208.man.poznan.pl/bigdata/sparql'
wbi_config['MEDIAWIKI_API_URL'] = 'https://prunus-208.man.poznan.pl/api.php'
wbi_config['WIKIBASE_URL'] = 'https://prunus-208.man.poznan.pl'
wbi_config['USER_AGENT'] = 'MyWikibaseBot/1.0'

# login i hasło ze zmiennych środowiskowych
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# OAuth
WIKIDARIAH_CONSUMER_TOKEN = os.environ.get('WIKIDARIAH_CONSUMER_TOKEN')
WIKIDARIAH_CONSUMER_SECRET = os.environ.get('WIKIDARIAH_CONSUMER_SECRET')
WIKIDARIAH_ACCESS_TOKEN = os.environ.get('WIKIDARIAH_ACCESS_TOKEN')
WIKIDARIAH_ACCESS_SECRET = os.environ.get('WIKIDARIAH_ACCESS_SECRET')

# właściwości w testowej instancji wikibase
P_VIAF = 'P517'
P_DATE_OF_BIRTH = 'P422'
P_DATE_OF_DEATH = 'P423'
P_PLWABN_ID = 'P484'
P_STATED_AS = 'P505'
P_INSTANCE_OF = 'P459'
P_REFERENCE_URL = 'P399'
P_STATED_IN = 'P506'
P_PAGES = 'P479'
P_VOLUME = 'P518'
P_AUTHOR = 'P404'
P_AUTHOR_STR = 'P405'
P_SOURCING_CIRCUMSTANCES = 'P502'
P_EARLIEST_DATE = 'P432'
P_LATEST_DATE = 'P464'
P_INFORMATION_STATUS = 'P458'
P_FLORUIT = 'P444'
P_INCIPIT = 'P443'
P_RETRIEVED = 'P494'
P_WIKIDATA_ID = 'P398'
P_DESCRIBED_BY_SOURCE = 'P425'
P_PUBLICATION_DATE = 'P488'

# elementy definicyjne w testowej instancji wikibase
Q_HUMAN = 'Q229050'
Q_CIRCA = 'Q233831'
Q_UNCERTAIN = 'Q233828'

# QID elementu PSB
Q_PSB_ITEM = 'Q315332'

# data pobrania danych z BN
DATE_BN = '+2023-02-07T00:00:00Z'
# data pobrania danych z wikidata.org
DATE_WIKIDATA = '+2023-06-15T00:00:00Z'


class Postac:
    """ dane postaci PSB """

    def __init__(self, postac_dict:dict, logger_object:Logger,
                 login_object:wbi_login.OAuth1, wbi_object: WikibaseIntegrator) -> None:

        self.identyfikator = postac_dict['ID']
        self.name = postac_dict['name']
        # identyfikator wikibase QID
        self.qid = postac_dict.get('QID', '')
        # lata życia z listy BB
        self.years = postac_dict.get('years', '')

        self.description_pl = postac_dict.get('description_pl', '').strip()
        self.description_en = postac_dict.get('description_en', '').strip()

        # aliasy z BN, czyli referencja w 'stated as' będzie do BN, nie PSB
        self.aliasy = postac_dict.get('bn_400', [])

        # dane z dokładnymi datami są niepewne
        self.date_of_birth = postac_dict.get('date_of_birth', '')
        self.date_of_death = postac_dict.get('date_of_death', '')

        self.years_start = ''
        self.years_end = ''
        self.years = self.years.replace('(','').replace(')','').strip()
        tmp = self.years.split('-')
        if len(tmp) != 2:
            if 'zm' in self.years or 'um.' in self.years:
                self.years_end = self.years.replace('zm.','').replace('um.','').strip()
            elif 'ur.' in self.years:
                self.years_start = self.years.replace('ur.','').strip()
        else:
            self.years_start = tmp[0].strip()
            self.years_end = tmp[1].strip()

        # jeżeli nie udało się sprowadzić tekstu do samego roku
        if self.years_start and not self.years_start.isnumeric():
            self.years_start = ''
        if self.years_end and not self.years_end.isnumeric():
            self.years_end = ''

        # lata życia postaci z deskryptora BN
        self.bn_years = postac_dict.get('bn_years', '')
        self.bn_years = self.bn_years.replace('(','').replace(')','')

        # informacje do utworzenia referencji do PSB
        self.volume = postac_dict.get('volume', '').strip()
        self.publ_year = postac_dict.get('publ_year', '').strip()
        self.page = postac_dict.get('page', '').strip()
        if self.page and 's.' in self.page:
            self.page = self.page.replace('s.','').strip()
        self.autor = postac_dict.get('autor', [])
        self.incipit = postac_dict.get('incipit', '').strip()

        # identyfikatory
        self.plwabn_id = str(postac_dict.get("id_bn", '')).strip()
        self.id_bn_a = str(postac_dict.get("id_bn_a", '')).strip()
        viaf = str(postac_dict.get('viaf', '')).strip()
        if 'https' in viaf:
            self.viaf = viaf.replace('https://viaf.org/viaf/','').replace(r'/','')
        else:
            self.viaf = viaf.replace('http://viaf.org/viaf/','').replace(r'/','')
        self.wikidata = str(postac_dict.get("wikidata", '')).strip()

        # pola techniczne
        self.wb_item = None                # element
        self.logger = logger_object        # logi
        self.login_instance = login_object # login instance
        self.wbi = wbi_object              # WikibaseIntegratorObject
        self.reference_psb = None          # referencje do PSB
        self.reference_bn = None           # referencje do Biblioteki Narodowej

        # referencja do elementu PSB (tomu?), do podpięcia dla daty urodzin i śmierci
        if self.volume and self.publ_year:
            self.reference_psb = self.create_psb_reference()
        if self.plwabn_id:
            self.reference_bn = self.create_bn_reference()
        if self.wikidata:
            self.reference_wiki = self.create_wiki_reference()


    def prepare_authors(self) -> list:
        """ metoda tworzy listę autorów biogramu """
        lista = []
        if self.autor:
            for item in self.autor:
                autor_name = item.get('autor_name','')
                autor_years = item.get('autor_years','')
                as_string = item.get('as_string','')
                if as_string == '1':
                    lista.append(String(value=autor_name, prop_nr=P_AUTHOR_STR))
                else:
                    autor_qid = self.find_autor(autor_name, autor_years)
                    if autor_qid:
                        lista.append(Item(value=autor_qid, prop_nr=P_AUTHOR))
                    else:
                        print('ERROR:', autor_name, autor_years)

        return lista


    def create_psb_reference(self) -> list:
        """ metoda tworzy referencję do tomu PSB """
        author_list = self.prepare_authors()

        # jeżeli jeden element PSB
        result = [Item(value=Q_PSB_ITEM, prop_nr=P_STATED_IN),
                   String(value=self.volume, prop_nr=P_VOLUME),
                   String(value=self.page, prop_nr=P_PAGES),
                   MonolingualText(text=self.incipit, language='pl', prop_nr=P_INCIPIT)
                ]

        if author_list:
            result += author_list

        return [result]


    def create_bn_reference(self) -> list:
        """ metoda tworzy referencję do deskryptora BN """
        result = None
        if self.id_bn_a:
            adres = f'https://dbn.bn.org.pl/descriptor-details/{self.id_bn_a}'
            result = [[ URL(value=adres, prop_nr=P_REFERENCE_URL),
                        Time(prop_nr=P_RETRIEVED, time=DATE_BN, precision=WikibaseDatePrecision.DAY)]]

        return result


    def create_wiki_reference(self) -> list:
        """ metoda tworzy referencję do wikidata.org """
        result = None
        if self.wikidata:
            adres = f'https://www.wikidata.org/wiki/{self.wikidata}'
            result = [[ URL(value=adres, prop_nr=P_REFERENCE_URL),
                        Time(prop_nr=P_RETRIEVED, time=DATE_WIKIDATA, precision=WikibaseDatePrecision.DAY)]]

        return result


    def time_from_string(self, value:str, prop: str, ref:list=None, qlf_list:list=None) -> Time:
        """ przekształca datę na time oczekiwany przez wikibase """

        if value == 'somevalue':
            return Time(prop_nr=prop, time=None, snaktype=WikibaseSnakType.UNKNOWN_VALUE,
                        references=ref, qualifiers=qlf_list)

        if len(value) == 10 and value.endswith('XX'):
            value = value.replace('XX','00')

        year = value[:4]
        month = value[5:7]
        day = value[8:]

        if year.endswith('..') or year.endswith('uu') or year.endswith('XX'):
            year = year.replace('..','01').replace('uu','01').replace('XX','01')
            month = '01'
            day = '01'
            precision = WikibaseDatePrecision.CENTURY
        else:
            precision = WikibaseDatePrecision.YEAR
            if day != '00':
                precision = WikibaseDatePrecision.DAY
            elif day == '00' and month != '00':
                precision = WikibaseDatePrecision.MONTH
                day = '01'
            else:
                day = month = '01'

        format_time =  f'+{year}-{month}-{day}T00:00:00Z'

        return Time(prop_nr=prop, time=format_time, precision=precision,
                    references=ref, qualifiers=qlf_list)


    def date_from_bn(self):
        """ metoda przetwarza lata życia z deskryptora BN na daty do pól
            date of birth, date of death
            Daty niepewne lub przybliżone w deskryptorach BN są zapisywane
            w formacie EDTF (Extended Date/Time Format) Level 1,
            ale tylko jeżeli pochodzą z pola MARC21 '046' (mniejszość), jeżeli
            pochodzą z pola '100d' (zdecydowana większość) wówczas stosowane są
            opisy słowne np. ok., urodzony, zmarł, czynny, ?, przed, po, lub,
            ca, post, non ante i inne.
        """
        if self.bn_years.count('-') > 1:
            tmp = self.bn_years.split(' - ')
        else:
            tmp = self.bn_years.split('-')

        b_statement = d_statement = None

        if len(tmp) == 1 and 'fl. ca' in self.bn_years:
            b_date = self.bn_years.replace('fl. ca','').strip()
            if len(b_date) == 5 and b_date.endswith('%'): # fl. ca 1800%
                b_date[:4] += '-%%-%%'
            elif len(b_date) == 4: # fl. ca 1860
                b_date += '-00-00'
            b_statement = self.time_from_string(value=b_date, prop=P_FLORUIT, ref=self.reference_bn)
            return b_statement, d_statement

        if 'czynny ok.' in self.bn_years:
            b_date = self.bn_years.replace('czynny ok.','').strip()
            qualifier = None
            if len(b_date) == 5 and b_date.endswith('%'): # czynny ok. 1800%
                b_date[:4] += '-%%-%%'
            elif len(b_date) == 4: # czynny ok. 1860
                b_date += '-00-00'
            elif '-' in b_date: # czynny ok. 1772-1780
                tmp = b_date.split('-')
                earliest = tmp[0].strip()
                if len(earliest) == 4:
                    earliest += '-00-00'
                latest = tmp[1].strip()
                if len(latest) == 4:
                    latest += '-00-00'
                b_date = 'somevalue'
                qualifier = [self.time_from_string(value=earliest, prop=P_EARLIEST_DATE),
                             self.time_from_string(value=latest, prop=P_LATEST_DATE)]
            b_statement = self.time_from_string(value=b_date, prop=P_FLORUIT, ref=self.reference_bn, qlf_list=qualifier)
            return b_statement, d_statement

        b_date = tmp[0].strip()
        if b_date == '?':
            b_date = ''
        d_date = tmp[1].strip()
        if d_date == '?':
            d_date = ''

        if b_date.endswith('.?'):
            b_date = b_date.replace('.?','..')

        if d_date.endswith('.?'):
            d_date = d_date.replace('.?','..')

        if len(b_date) == 4 and b_date.isnumeric():
            b_date += '-00-00'
        if '??' in b_date:
            b_date = b_date.replace('??','..')

        if len(b_date) == 10  and b_date.count('-') == 2:
            b_statement = self.time_from_string(value=b_date, prop=P_DATE_OF_BIRTH, ref=self.reference_bn)
        else:
            if '?' in b_date or '~' in b_date or 'ca' in b_date or 'ok.' in b_date:
                qualifier = [Item(value=Q_CIRCA, prop_nr=P_SOURCING_CIRCUMSTANCES)]
                b_date = b_date.replace('?', '').replace('~','').replace('ca','').replace('ok.','').strip()
                if len(b_date) == 4:
                    b_date += '-00-00'
                elif len(b_date) == 3:
                    b_date = '0' + b_date + '-00-00'
                b_statement = self.time_from_string(value=b_date, prop=P_DATE_OF_BIRTH,
                                                    ref=self.reference_bn,
                                                    qlf_list=qualifier)
            elif 'non post' not in b_date and 'nie po' not in b_date and ('po' in b_date or 'post' in b_date or 'non ante' in b_date):
                b_date = b_date.replace('post','').replace('po','').replace('non ante','').strip()
                if len(b_date) == 4:
                    b_date += '-00-00'
                qualifier = [self.time_from_string(value=b_date, prop=P_EARLIEST_DATE)]
                b_statement = Time(time=None, prop_nr=P_DATE_OF_BIRTH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)
            elif 'przed' in b_date or 'ante' in b_date or 'non post' in b_date or 'nie po' in b_date:
                b_date = b_date.replace('ante','').replace('przed','').replace('non post','').replace('nie po','').strip()
                if len(b_date) == 4:
                    b_date += '-00-00'
                qualifier = [self.time_from_string(value=b_date, prop=P_LATEST_DATE)]
                b_statement = Time(time=None, prop_nr=P_DATE_OF_BIRTH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)
            elif r'/' in b_date:
                tmp = b_date.split(r'/')
                earliest = tmp[0].strip()
                if len(earliest) == 4:
                    earliest += '-00-00'
                latest = tmp[1].strip()
                if len(latest) == 4:
                    latest += '-00-00'
                elif len(latest) == 2:
                    latest = earliest[:2] + latest + '-00-00'
                elif len(latest) == 1:
                    latest = earliest[:3] + latest + '-00-00'
                qualifier = [self.time_from_string(value=earliest, prop=P_EARLIEST_DATE),
                             self.time_from_string(value=latest, prop=P_LATEST_DATE)]
                b_statement = Time(time=None, prop_nr=P_DATE_OF_BIRTH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)

        if len(d_date) == 4 and d_date.isnumeric():
            d_date += '-00-00'
        if '??' in d_date:
            d_date = d_date.replace('??','..')

        if len(d_date) == 10 and d_date.count('-') == 2:
            d_statement = self.time_from_string(value=d_date, prop=P_DATE_OF_DEATH, ref=self.reference_bn)
        else:
            if '?' in d_date or '~' in d_date or 'ca' in d_date or 'ok.' in d_date:
                qualifier = [Item(value=Q_CIRCA, prop_nr=P_SOURCING_CIRCUMSTANCES)]
                d_date = d_date.replace('?', '').replace('~','').replace('ca','').replace('ok.','').strip()
                if len(d_date) == 4:
                    d_date += '-00-00'
                elif len(d_date) == 3:
                    d_date = '0' + d_date + '-00-00'
                d_statement = self.time_from_string(value=d_date, prop=P_DATE_OF_DEATH,
                                                    ref=self.reference_bn,
                                                    qlf_list=qualifier)
            elif 'non post' not in d_date and 'nie po' not in d_date and ('po' in d_date or 'post' in d_date or 'non ante' in d_date):
                d_date = d_date.replace('post','').replace('po','').replace('non ante','').strip()
                if len(d_date) == 4:
                    d_date += '-00-00'
                qualifier = [self.time_from_string(value=d_date, prop=P_EARLIEST_DATE)]
                d_statement = Time(time=None, prop_nr=P_DATE_OF_DEATH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)
            elif 'przed' in d_date or 'ante' in d_date or 'non post' in d_date or 'nie po' in d_date:
                d_date = d_date.replace('ante','').replace('przed','').replace('non post','').replace('nie po','').strip()
                if len(d_date) == 4:
                    d_date += '-00-00'
                qualifier = [self.time_from_string(value=d_date, prop=P_LATEST_DATE)]
                d_statement = Time(time=None, prop_nr=P_DATE_OF_DEATH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)
            elif r'/' in d_date:
                tmp = d_date.split(r'/')
                earliest = tmp[0].strip()
                if len(earliest) == 4:
                    earliest += '-00-00'
                latest = tmp[1].strip()
                if len(latest) == 4:
                    latest += '-00-00'
                elif len(latest) == 2:
                    latest = earliest[:2] + latest + '-00-00'
                elif len(latest) == 1:
                    latest = earliest[:3] + latest + '-00-00'
                qualifier = [self.time_from_string(value=earliest, prop=P_EARLIEST_DATE),
                             self.time_from_string(value=latest, prop=P_LATEST_DATE)]
                b_statement = Time(time=None, prop_nr=P_DATE_OF_BIRTH, snaktype=WikibaseSnakType.UNKNOWN_VALUE, qualifiers=qualifier)


        return b_statement, d_statement


    def create_item(self, update_qid=None):
        """ przygotowuje nowy element do dodania """
        if not update_qid:
            self.wb_item = self.wbi.item.new()
            self.wb_item.labels.set(language='pl', value=self.name)
            self.wb_item.labels.set(language='en', value=self.name)
            self.wb_item.descriptions.set(language='pl', value=self.description_pl)
            self.wb_item.descriptions.set(language='en', value=self.description_en)
        else:
            self.wb_item = self.wbi.item.get(entity_id=update_qid)
            description = self.wb_item.descriptions.get(language='pl')
            if not description or description == '-' or description != self.description_pl:
                self.wb_item.descriptions.set(language='pl', value=self.description_pl)
                self.wb_item.descriptions.set(language='en', value=self.description_en)

        # VIAF
        if self.viaf:
            statement = ExternalID(value=self.viaf, prop_nr=P_VIAF)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # Wikidata ID
        if self.wikidata:
            statement = ExternalID(value=self.wikidata, prop_nr=P_WIKIDATA_ID, references=self.reference_wiki)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # W celu uzyskania daty urodzenia i śmierci przetwarzane są lata życia z nawiasów.
        # Dane z datami dokładnymi zawierają błędy (daty dzienne połączone z przypadkowymi rocznymi,
        # prawidłowe roczne połączone z przypadkowymi dziennymi, lub brak istniejących dat
        # rocznych) dlatego na razie je pomijamy, będą w przyszłości wyciągane przez GPT

        separator = ',' if ',' in self.years else '-'
        date_of_1 = date_of_2 = None
        # jeżeli zakres dat
        if separator in self.years:
            tmp = self.years.split(separator)
            date_of_1 = DateBDF(tmp[0].strip(), 'B')
            date_of_2 = DateBDF(tmp[1].strip(), 'D')
        # jeżeli tylko jedna z dat lub ogólny opis np. XVII wiek
        else:
            if self.years:
                date_of_1 = DateBDF(self.years, '')

        if date_of_1:
            statement_1, statement_2 = date_of_1.prepare_st(ref=self.reference_psb)
            if statement_1:
                self.wb_item.claims.add([statement_1], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
            if statement_2:
                self.wb_item.claims.add([statement_2], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if date_of_2:
            statement_1, statement_2 = date_of_2.prepare_st(ref=self.reference_psb)
            if statement_1:
                self.wb_item.claims.add([statement_1], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
            if statement_2:
                self.wb_item.claims.add([statement_2], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # lata życia z deskryptora Biblioteki Narodowej
        # specjalna metoda do przetwarzania dat z BN, daty z BN są dodawane osobno, nawet jak są takie
        # same jak daty z PSB, dlatego że docelowo nie będą takie same - będziemy mieć daty dzienne z PSB
        # generalnie nie widzę wartości w dodawaniu dat z BN, to nie jest źródło historyczne...
        if self.bn_years:
            bn_birth_statement, bn_death_statement = self.date_from_bn()
            if bn_birth_statement:
                self.wb_item.claims.add([bn_birth_statement], action_if_exists=ActionIfExists.FORCE_APPEND)
            if bn_death_statement:
                self.wb_item.claims.add([bn_death_statement], action_if_exists=ActionIfExists.FORCE_APPEND)

        # PLWABN ID
        if self.plwabn_id:
            statement = ExternalID(value=self.plwabn_id, prop_nr=P_PLWABN_ID)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # ALIASY i STATED AS
        if self.aliasy:
            # wszystkie aliasy do j. polskiego, w aliasach naszej instancji testowej nie ma języka 'und'?
            self.wb_item.aliases.set(language='pl', values=self.aliasy, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)
            # stated as
            for alias in self.aliasy:
                # dodawać statement z językiem 'und' lub 'mul'
                statement = MonolingualText(text=alias.strip(), language='und',
                                            prop_nr=P_STATED_AS, references=self.reference_bn)
                self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.FORCE_APPEND)

        statement = Item(value=Q_HUMAN, prop_nr=P_INSTANCE_OF)
        self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # DESCRIBED BY SOURCE
        author_list = self.prepare_authors()

        psb_qualifiers = [String(value=self.volume, prop_nr=P_VOLUME),
                          String(value=self.page, prop_nr=P_PAGES),
                          MonolingualText(text=self.incipit, language='pl', prop_nr=P_INCIPIT)
                          ]

        if author_list:
            psb_qualifiers += author_list


        statement = Item(value=Q_PSB_ITEM,
                         prop_nr=P_DESCRIBED_BY_SOURCE,
                         qualifiers=psb_qualifiers,
                         references=None)
        self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)


    def find_autor(self, value:str, years:str) -> str:
        """ wyszukuje autora w wikibase, zwraca QID """
        result = ''

        items = wbi_helpers.search_entities(search_string=value,
                                                language='pl',
                                                search_type='item')
        for item in items:
            wbi_item = self.wbi.item.get(entity_id=item)
            item_label = wbi_item.labels.get(language='pl')
            item_description = wbi_item.descriptions.get(language='pl')

            if item_description:
                years = years.replace('(','').replace(')', '').strip()
                tmp = years.split('-')
                y_start = y_end = ''
                y_start = tmp[0].strip()
                if len(tmp) == 2:
                    y_end = tmp[1].strip()
                if (years in item_description or
                    ((not y_start or y_start in item_description) and
                    (not y_end or y_end in item_description))):
                    result = item
                    break
            else:
                if value == item_label:
                    result = item
                    break

        return result


    def appears_in_wikibase(self) -> bool:
        """ proste wyszukiwanie elementu w wikibase, dokładna zgodność etykiety i opisu
        """

        items = wbi_helpers.search_entities(search_string=self.name,
                                             language='pl',
                                             search_type='item')
        for item in items:
            wbi_item = self.wbi.item.get(entity_id=item)
            item_label = wbi_item.labels.get(language='pl')
            item_description_pl = wbi_item.descriptions.get(language='pl')

            if (item_label == self.name and item_description_pl and item_description_pl == self.description_pl):
                self.qid = item
                return True

        return False


    def write_or_exit(self):
        """ zapis danych do wikibase lub zakończenie programu """
        loop_num = 1
        while True:
            try:
                new_id = self.wb_item.write()
                break
            except MWApiError as wb_error:
                err_code = wb_error.code
                err_message = wb_error.messages
                self.logger.error(f'ERROR: {err_code}, {err_message}')

                # jeżeli jest to problem z tokenem to próba odświeżenia tokena i powtórzenie
                # zapisu, ale tylko raz, w razie powtórnego błędu bad token, skrypt kończy pracę
                if err_code in ['assertuserfailed', 'badtoken']:
                    if loop_num == 1:
                        self.logger.error('błąd "badtoken", odświeżenie poświadczenia...')
                        self.login_instance.generate_edit_credentials()
                        loop_num += 1
                        continue
                # jeżeli błąd zapisu dto druga próba po 5 sekundach
                elif err_code in ['failed-save']:
                    if loop_num == 1:
                        self.logger.error('błąd zapisu, czekam 5 sekund...')
                        loop_num += 1
                        continue

                sys.exit(1)

        self.qid = new_id.id


def set_logger(path:str) -> Logger:
    """ utworzenie loggera """
    logger_object = logging.getLogger(__name__)
    logger_object.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(message)s')
    # log w konsoli
    c_handler = logging.StreamHandler()
    c_handler.setFormatter(log_format)
    c_handler.setLevel(logging.DEBUG)
    logger_object.addHandler(c_handler)

    # zapis logów do pliku
    f_handler = logging.FileHandler(path)
    f_handler.setFormatter(log_format)
    f_handler.setLevel(logging.INFO)
    logger_object.addHandler(f_handler)

    return logger_object


# ------------------------------------------------------------------------------
if __name__ == '__main__':

    # pomiar czasu wykonania
    start_time = time.time()

    # tworzenie obiektu loggera
    file_log = Path('..') / 'log' / 'psb_postacie.log'
    logger = set_logger(file_log)

    logger.info('POCZĄTEK IMPORTU')

    # zalogowanie do instancji wikibase
    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                      consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                      access_token=WIKIDARIAH_ACCESS_TOKEN,
                                      access_secret=WIKIDARIAH_ACCESS_SECRET)

    wbi = WikibaseIntegrator(login=login_instance)

    # realne dane
    input_path = Path("..") / "data" / "postacie.json"

    # próbka do testów
    #input_path = '/home/piotr/ihpan/psb_import/data/probka_postacie_2.json'

    # ścieżka do pliku json z danymi postaci z przypisanymi identyfikatorami wikibase (QID)
    # ten plik stanie się nową wersją pliku postacie.json i ułatwi późniejsze uzupełnianie danych
    # w wikibase (uwaga: wersje dla wiki testowej i produkcyjnej będą miały inne identyfikatory)
    output_path = Path("..") / "data" / "postacie_qid.json"

    # ścieżka do pliku tymczasowego do zapisu identyfikatorów QID w razie przerwania skryptu
    # (brak prądu, problemy sieciowe itp.), na podstawie tego pliku mozna uzupełnić dane w postacie.json
    output_tmp_path = Path("..") / "data" / "tmp_qid_list.csv"

    with open(input_path, "r", encoding='utf-8') as f:
        json_data = json.load(f)
        for i, postac_record in enumerate(json_data['persons']):

            # przetwarzanie partiami po 1000
            if (i < 25001):
                continue

            # utworzenie instancji obiektu postaci
            postac = Postac(postac_record, logger_object=logger, login_object=login_instance,
                          wbi_object=wbi)

            # jeżeli nie ma postaci w wikibase
            if not postac.qid and not postac.appears_in_wikibase():
                postac.create_item()
                if WIKIBASE_WRITE:
                    postac.write_or_exit()
                else:
                    postac.qid = 'TEST'

                message = f'({i}) Dodano element: # [https://prunus-208.man.poznan.pl/wiki/Item:{postac.qid} {postac.name}]'
            # jeżeli jest to próba uzupełnienia danych
            else:
                message = f'({i}) Element istnieje: # [https://prunus-208.man.poznan.pl/wiki/Item:{postac.qid} {postac.name}]'
                postac.create_item(update_qid=postac.qid)
                if WIKIBASE_WRITE:
                    postac.write_or_exit()

            postac_record['QID'] = postac.qid

            # zapis do pliku tekstowego w razie przerwania skryptu - do uzupełnienia w postacie.json
            # przed ponownym uruchomieniem skryptu!
            with open(output_tmp_path, 'a', encoding='utf-8') as f_tmp:
                f_tmp.write(f'{postac.identyfikator}@{postac.qid}\n')

            # zapis w logu
            logger.info(message)

    # zapis pliku json z identyfikatorami wikibase (QID)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=4, ensure_ascii=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    message = f'Czas wykonania programu: {time.strftime("%H:%M:%S", time.gmtime(elapsed_time))} s.'
    logger.info(message)
