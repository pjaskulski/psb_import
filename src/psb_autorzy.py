""" skrypt do importu autorów biogramów PSB
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
from wikibaseintegrator.wbi_enums import ActionIfExists

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
P_VOLUME = 'P518'
P_PAGES = 'P479'

# elementy definicyjne w instancji wikibase
Q_HUMAN = 'Q229050'
Q_PSB = 'Q315332'


class Autor:
    """ dane autora PSB """

    def __init__(self, author_dict:dict, logger_object:Logger,
                 login_object:wbi_login.OAuth1, wbi_object: WikibaseIntegrator) -> None:

        self.identyfikator = author_dict['ID']
        self.name = author_dict['name']

        self.description_pl = author_dict.get('years', '')
        self.description_en = author_dict.get('years', '')

        # opis polski złożony z lat życia i informacji o autorze z BN,
        # bez kropki na końcu opisu
        tmp_description_pl = author_dict.get('bn_opis', '')
        if tmp_description_pl:
            first_char = tmp_description_pl[0].lower()
            tmp_description_pl = first_char + tmp_description_pl[1:]
            if tmp_description_pl.endswith('.'):
                tmp_description_pl = tmp_description_pl[:-1]
            self.description_pl += ' ' + tmp_description_pl
            self.description_pl = self.description_pl.strip()

        # opis angielski złożony z lat życia i informacji o autorze z BN przetłumaczonych
        # automatycznie, bez kropki na końcu opisu
        tmp_description_en = author_dict.get('description_en', '')
        if tmp_description_en:
            first_char = tmp_description_en[0].lower()
            tmp_description_en = first_char + tmp_description_en[1:]
            if tmp_description_en.endswith('.'):
                tmp_description_en = tmp_description_en[:-1]
            self.description_en += ' ' + tmp_description_en
            self.description_en = self.description_en.strip()

        self.aliasy = author_dict.get('aliasy', [])

        self.date_of_birth = author_dict.get('date_of_birth', '')
        self.date_of_death = author_dict.get('date_of_death', '')

        self.http_viaf = str(author_dict.get('viaf', '')).strip()
        if 'https' in self.http_viaf:
            self.viaf = self.http_viaf.replace('https://viaf.org/viaf/','').replace(r'/','')
        else:
            self.viaf = self.http_viaf.replace('http://viaf.org/viaf/','').replace(r'/','')

        self.plwabn_id = author_dict.get('plwabn_id', '')
        self.psb_volume = author_dict.get('volume', '')
        self.psb_pages = author_dict.get('pages', '')

        self.wb_item = None                # element
        self.qid = ''                      # znaleziony lub utworzony QID
        self.logger = logger_object        # logi
        self.login_instance = login_object # login instance
        self.wbi = wbi_object              # WikibaseIntegratorObject
        self.references = None             # referencje
        self.references_psb = None         # referencja do PSB dla wariantów nazwiska autora
        # referencja do VIAF dla daty urodzenia, daty śmierci
        if self.http_viaf:
            self.references = [[ URL(value=self.http_viaf, prop_nr=P_REFERENCE_URL) ]]
        # referencja do tomu PSB
        if self.psb_volume:
            self.references_psb = [
                [Item(value=Q_PSB, prop_nr=P_STATED_IN),
                 String(value=self.psb_volume, prop_nr=P_VOLUME),
                 String(value=self.psb_pages, prop_nr=P_PAGES)
                 ]
                                ]
        # referencja URL do deksryptora Biblioteki Narodowej (tylko do plwabn id)
        self.id_bn_a = author_dict.get('id_bn_a', '')
        self.references_bn = None
        if self.id_bn_a:
            adres = f'https://dbn.bn.org.pl/descriptor-details/{self.id_bn_a}'
            self.references_bn = [[ URL(value=adres, prop_nr=P_REFERENCE_URL) ]]


    def time_from_string(self, value:str, prop: str) -> Time:
        """ przekształca datę z json na time oczekiwany przez wikibase """
        year = value[:4]
        month = value[5:7]
        day = value[8:]

        precision = WikibaseDatePrecision.YEAR
        if day != '00':
            precision = WikibaseDatePrecision.DAY
        elif day == '00' and month != '00':
            precision = WikibaseDatePrecision.MONTH
            day = '01'
        else:
            day = month = '01'

        format_time =  f'+{year}-{month}-{day}T00:00:00Z'

        # referencja do VIAF
        return Time(prop_nr=prop, time=format_time, precision=precision,
                    references=self.references)


    def create_new_item(self):
        """ przygotowuje nowy element do dodania """
        self.wb_item = self.wbi.item.new()

        self.wb_item.labels.set(language='pl', value=self.name)
        self.wb_item.labels.set(language='en', value=self.name)

        self.wb_item.descriptions.set(language='pl', value=self.description_pl)
        self.wb_item.descriptions.set(language='en', value=self.description_en)

        if self.viaf:
            statement = ExternalID(value=self.viaf, prop_nr=P_VIAF)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.date_of_birth:
            statement = self.time_from_string(self.date_of_birth, P_DATE_OF_BIRTH)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.date_of_death:
            statement = self.time_from_string(self.date_of_death, P_DATE_OF_DEATH)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.plwabn_id:
            plwabn_reference = None
            if self.references_bn:
                plwabn_reference = self.references_bn
            statement = ExternalID(value=self.plwabn_id, prop_nr=P_PLWABN_ID, references=plwabn_reference)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        # stated as dla podstawowego imienia i nazwiska (label)
        # dodawany tylko jeżeli mamy jakieś referencje, do PSB, lub do VIAF
        statement_references = None
        if self.references_psb:
            statement_references = self.references_psb
        elif self.references:
            statement_references = self.references

        if statement_references:
            statement = MonolingualText(text=self.name, language='pl',
                                        prop_nr=P_STATED_AS,
                                        references=statement_references)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.FORCE_APPEND)

        # aliasy
        if self.aliasy:
            alias_names_list = []

            for alias in self.aliasy:
                alias_references = None
                alias_name, alias_volume, alias_pages = alias

                alias_names_list.append(alias_name)

                if alias_volume:
                    alias_references = [
                        [Item(value=Q_PSB, prop_nr=P_STATED_IN),
                         String(value=alias_volume, prop_nr=P_VOLUME),
                         String(value=alias_pages, prop_nr=P_PAGES)
                        ]]
                # else:
                    # # jeżeli nie ma nr tomu, czy zakładamy że alias pochodzi z VIAF,
                    # a tylko jeżeli nie ma VIAF, nie ma referencji ???
                    # if self.http_viaf:
                    #     alias_references = [[ URL(value=self.http_viaf, prop_nr=P_REFERENCE_URL) ]]
                    # else:
                    #     alias_references = None

                # deklaracja jest dodawana tylko jeżeli mamy jakąkolwiek referencję
                if alias_references:
                    statement = MonolingualText(text=alias_name, language='pl',
                                                prop_nr=P_STATED_AS,
                                                references=alias_references)
                    self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

            self.wb_item.aliases.set(language='pl', values=alias_names_list)

        statement = Item(value=Q_HUMAN, prop_nr=P_INSTANCE_OF)
        self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)


    def update_item(self, update_qid:str):
        """ aktualizacja istniejącego elementu """

        self.wb_item = self.wbi.item.get(entity_id=update_qid)
        description = self.wb_item.descriptions.get(language='pl')
        if not description or description == '-':
            self.wb_item.descriptions.set(language='pl', value=self.description_pl)
            self.wb_item.descriptions.set(language='en', value=self.description_en)

        if self.viaf:
            statement = ExternalID(value=self.viaf, prop_nr=P_VIAF)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.date_of_birth:
            statement = self.time_from_string(self.date_of_birth, P_DATE_OF_BIRTH)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.date_of_death:
            statement = self.time_from_string(self.date_of_death, P_DATE_OF_DEATH)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        if self.plwabn_id:
            plwabn_reference = None
            if self.references_bn:
                plwabn_reference = self.references_bn
            statement = ExternalID(value=self.plwabn_id, prop_nr=P_PLWABN_ID, references=plwabn_reference)
            self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

        claims_value = []
        claims = self.wb_item.claims.get_json()
        if P_STATED_AS in claims:
            claims_value = [x.mainsnak.datavalue['value']['text'] for x in self.wb_item.claims.get(P_STATED_AS)]

        # stated as dla podstawowego imienia i nazwiska (label)
        # dodawany tylko jeżeli mamy jakieś referencje, do PSB, lub do VIAF
        statement_references = None
        if self.references_psb:
            statement_references = self.references_psb
        elif self.references:
            statement_references = self.references

        if statement_references:
            if self.name not in claims_value:
                statement = MonolingualText(text=self.name, language='pl',
                                            prop_nr=P_STATED_AS,
                                            references=statement_references)
                self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.FORCE_APPEND)


        if self.aliasy:
            alias_names_list = []

            for alias in self.aliasy:
                alias_references = None
                alias_name, alias_volume, alias_pages = alias

                alias_names_list.append(alias_name)

                if alias_volume:
                    alias_references = [
                        [Item(value=Q_PSB, prop_nr=P_STATED_IN),
                         String(value=alias_volume, prop_nr=P_VOLUME),
                         String(value=alias_pages, prop_nr=P_PAGES)
                        ]]
                # else:
                #     # jeżeli nie ma nr tomu, czy zakładamy że alias pochodzi z VIAF,
                #       a tylko jeżeli nie ma VIAF, nie ma referencji ???
                #     if self.http_viaf:
                #         alias_references = [[ URL(value=self.http_viaf, prop_nr=P_REFERENCE_URL) ]]
                #     else:
                #         alias_references = None

                # deklaracja jest dodawana tylko jeżeli mamy referencję
                if alias_references:
                    statement = MonolingualText(text=alias_name, language='pl',
                                                prop_nr=P_STATED_AS,
                                                references=alias_references)
                    self.wb_item.claims.add([statement], action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

            self.wb_item.aliases.set(language='pl', values=alias_names_list)


    def appears_in_wikibase(self) -> bool:
        """ proste wyszukiwanie elementu w wikibase, tylko dokładna zgodność imienia i nazwiska """
        f_result = False

        items = wbi_helpers.search_entities(search_string=self.name,
                                             language='pl',
                                             search_type='item')
        for item in items:
            wbi_item = self.wbi.item.get(entity_id=item)
            item_label = wbi_item.labels.get(language='pl')

            if item_label == self.name:
                f_result = True
                self.qid = item
                break

        return f_result


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
                # jeżeli błąd zapisu to druga próba po 5 sekundach
                elif err_code in ['failed-save']:
                    if loop_num == 1:
                        self.logger.error('błąd zapisu, czekam 5 sekund...')
                        time.sleep(5.0)
                        loop_num += 1
                        continue

                sys.exit(1)

        self.qid = new_id.id


def set_logger(path:str) -> Logger:
    """ utworzenie loggera """
    logger_object = logging.getLogger(__name__)
    logger_object.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s - %(message)s')
    c_handler = logging.StreamHandler()
    c_handler.setFormatter(log_format)
    c_handler.setLevel(logging.DEBUG)
    logger_object.addHandler(c_handler)
    # zapis logów do pliku tylko jeżeli skrypt uruchomiono z zapisem do wiki
    if WIKIBASE_WRITE:
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
    file_log = Path('..') / 'log' / 'psb_autorzy.log'
    logger = set_logger(file_log)

    logger.info('POCZĄTEK IMPORTU')

    # zalogowanie do instancji wikibase
    login_instance = wbi_login.OAuth1(consumer_token=WIKIDARIAH_CONSUMER_TOKEN,
                                      consumer_secret=WIKIDARIAH_CONSUMER_SECRET,
                                      access_token=WIKIDARIAH_ACCESS_TOKEN,
                                      access_secret=WIKIDARIAH_ACCESS_SECRET)

    wbi = WikibaseIntegrator(login=login_instance)

    # realne dane
    input_path = Path("..") / "data" / "autorzy.json"
    # dane z modyfikacjami
    output_path = Path("..") / "data" / "autorzy_qid.json"
    # lub testowe dane
    # input_path = '/home/piotr/ihpan/psb_import/data/probka.json'
    # output_path = '/home/piotr/ihpan/psb_import/data/probka_qid.json'

    with open(input_path, "r", encoding='utf-8') as f:
        json_data = json.load(f)
        for i, autor_record in enumerate(json_data['authors']):
            # utworzenie instancji obiektu autora
            autor = Autor(autor_record, logger_object=logger, login_object=login_instance,
                          wbi_object=wbi)

            if not autor.appears_in_wikibase():
                autor.create_new_item()
                if WIKIBASE_WRITE:
                    autor.write_or_exit()
                else:
                    autor.qid = 'TEST'

                # uzupełnienie danych autora o nadane QID
                autor_record['QID'] = autor.qid

                message = f'Dodano element: # [https://prunus-208.man.poznan.pl/wiki/Item:{autor.qid} {autor.name}]'
            else:
                message = f'Element istnieje: # [https://prunus-208.man.poznan.pl/wiki/Item:{autor.qid} {autor.name}]'
                autor.update_item(autor.qid)
                if WIKIBASE_WRITE:
                    autor.write_or_exit()

            logger.info(message)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=4, ensure_ascii=False)

    end_time = time.time()
    elapsed_time = end_time - start_time
    message = f'Czas wykonania programu: {time.strftime("%H:%M:%S", time.gmtime(elapsed_time))} s.'
    logger.info(message)
