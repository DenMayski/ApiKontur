import json
import time
import io

import requests

from DAL import DAL


class ApiBilly:
    """
    Класс для работы с API Kontur CRM

    Основной URL API  KONTUR.CRM

    https://api-crm-billing.kontur.ru


    Атрибуты
    --------
    REQ_PARAMS : dict
        Параметры запроса GET
    HEADERS_AUTH : dict
        Заголовки запросов
    BODY_JSON : dict
        Тело запроса POST
    stages : dict
        Значения этапов продаж
    methods : dict
        Методы для работы с API
    result : request.resp
        Результат запроса
    cursor : pymysql.Connection
        Курсор для работы с БД

    Методы
    --------
    def UpdateTimeStamp()
        Метод обновления метки времени в параметрах и БД
    def CheckStage(stage, idPs)
        Метод для смены этапа ПП
    def POST(url)
        Метод запроса POST
    def GET(url, param=None)
        Метод запроса GET
    """

    def __init__(self):
        """
        Конструктор класса, при инициализации записывает метку времени
        """

        self.cursor = DAL()

        f = open("Info.txt", "r")
        # Заголовок с ключом токеном для подключения
        self.HEADERS_AUTH = {"x-Auth-CustomToken": f.readlines()[0][:-1]}
        f.close()

        if self.cursor.row_count("lasttimestamp"):
            self.REQ_PARAMS['from'] = self.cursor.SELECT_ALL("lasttimestamp").fetchone()[0]
        else:
            self.REQ_PARAMS['from'] = int(
                self.GET(self.methods['lasttimestamp']).text)
        self.GET(self.methods['News'], param=self.REQ_PARAMS)

    # Параметры для поиска новостей
    REQ_PARAMS = {
        "from": 1283342357,
        "count": 30
    }

    # Тело для POST запроса SwitchStage
    BODY_JSON = {
        "StageId": "{Staged}",
        "NewState": 2,
        "ManagerName": "Репняков Лев Константинович"
    }

    # Результат запросов
    result = None

    # Курсор для работы с БД
    cursor = None

    # Этапы потенциальной продажи
    stages = {
        1: "47583553-8582-49d5-8337-d66a75001530",  # Вышли на ЛПР
        2: "9d3d7591-d41d-4c5e-9897-316383892dfc",  # Сформировано предложение
        3: "84756de0-5632-4e7c-b85a-13351d67ce87",  # Согласован счет
        4: "f4b727c0-04ff-45eb-8d23-8f2379ff00b1",  # Получена оплата
        5: "46195862-f1f3-4814-ab8c-3da3d60cb0b8",  # Предоставлен доступ
        6: "a8d93fdb-896a-44c5-a75d-d63fe06b075f",  # Подписаны документы
        7: "12293cee-56c6-4956-bea2-0b1383c524f1"  # Подписаны документы
    }

    # Методы поиска новых пп, возвращает ID ПП, временную метку следующей ПП и есть ли еще
    methods = {
        "News": "https://api-crm-billing.kontur.ru/prospectivesales/news",
        "Find": "https://api-crm-billing.kontur.ru/prospectivesales/{id}/find",
        "SwitchStage": "https://api-crm-billing.kontur.ru/prospectivesales/{id}/SwitchStage",
        "lasttimestamp": "https://api-crm-billing.kontur.ru/prospectivesales/lasttimestamp",
        "Clients": "https://api-crm-billing.kontur.ru/clients/find",
    }

    def UpdateTimeStamp(self, NextTimeStamp):
        """
        Метод для обновления метки последнего времени в параметрах запроса и БД
        :param int NextTimeStamp: Метка времени
        """

        self.REQ_PARAMS['from'] = NextTimeStamp
        self.cursor.EXECUTE(
            f"{'UPDATE' if self.cursor.row_count('lasttimestamp') else 'INSERT INTO '} " +
            f"lasttimestamp SET lasttimestamp = {NextTimeStamp}")

    def GET(self, url, param=None):
        """
        Метод для GET запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict param: Параметры которые необходимо передать в GET запросе
        :return:  Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.get(url=url, headers=self.HEADERS_AUTH, params=param, timeout=30)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504
        finally:
            return self.result

    def POST(self, url):
        """
        Метод для POST запроса
        :param str url: Адрес на который будет совершен запрос
        :return: Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.post(url=url, headers=self.HEADERS_AUTH, json=self.BODY_JSON)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504

    def ClientsFind(self, Inn, Kpp, OrgType):
        """
        Метод поиска клиента по ИНН, КПП и типу организации
        :param str Inn: ИНН организации
        :param str Kpp: КПП организации
        :param int OrgType: Тип организации (1-3)
        :return:
        """
        self.BODY_JSON = {"Inn": Inn, "Kpp": Kpp if Kpp != "Null" else "", "ClientType": OrgType}
        try:
            self.POST(self.methods['Clients'])
            return self.result.status_code == 200
        except Exception as msg:
            print("Error", msg)
            return False

    def CheckStage(self, stage, idPs):
        """
        Метод смены этапа ПП
        :param int stage: Номер этапа
        :param str idPs: Идентификатор ПП
        """
        self.BODY_JSON = {"StageId": stage, "NewState": 2, "ManagerName": "Репняков Лев Константинович"}
        self.POST(url=self.methods['SwitchStage'].replace("{id}", idPs))
        if self.result.status_code == 200:
            print("Ok")
        else:
            print("Error")


class ApiBitrix:
    """
    Класс для работы с API Битрикс24

    Основной URL API  Битрикс

    https://shturmanit.bitrix24.ru


    Атрибуты
    --------
    stages : dict
        Соответствие этапов Билли этапам Битрикса
    typeSale : dict
        Тип продажи
    Headers : dict
        Заголовки для GET запроса
    result : dict
        Результат POST/GET запросов
    methods : dict
        Методы для работы с API
    result : request.resp
        Результат запроса

    Методы
    --------
    def GetInfo(self, inn)
        Метод для получения основной информации о компании
    def UpdateComp(self, inn)
        Метод для создания и обновления информации о компании
    def FindContact(self, email=None, phone=None)
        Метод поиска и проверка существования контакта
    def GET(self, url, param=None)
        Метод для совершения GET запроса
    """

    def __init__(self):
        f = open("Info.txt", "r")
        self.URL_bitrix = f.readlines()[1][:-1]
        f.close()

    stages = {
        "47583553-8582-49d5-8337-d66a75001530": "C2:NEW",
        "9d3d7591-d41d-4c5e-9897-316383892dfc": "C2:FORMED_OFFER",
        "84756de0-5632-4e7c-b85a-13351d67ce87": "C2:INVOICED",
        "f4b727c0-04ff-45eb-8d23-8f2379ff00b1": "C2:PAYMENT_RECEIVED",
        "46195862-f1f3-4814-ab8c-3da3d60cb0b8": "C2:GRANTED_ACCESS",
        "a8d93fdb-896a-44c5-a75d-d63fe06b075f": "C2:WON",
        "12293cee-56c6-4956-bea2-0b1383c524f1": "C2:WON"
    }

    typeSale = {
        1: "SALE",
        2: "COMPLEX",
        3: "GOODS",
        4: "SERVICES",
        5: "SERVICE"
    }

    URL_dadata = "http://ke29.ru/dadata-get.php"

    Headers = {'Accept': 'application/json', 'Content-Type': 'application/json;charset=utf-8'}

    result = None

    def GetInfo(self, inn):
        """
        Получение информации по компании ИНН
        :param str inn: ИНН компании
        """
        time.sleep(0.5)
        self.result = requests.get(self.URL_dadata + f"?inn={inn}")
        return self.result

    def UpdateComp(self, inn):
        """
        Создание или обновление информации компании по ИНН
        :param str inn: ИНН
        """
        time.sleep(0.5)
        self.result = requests.get(url=f"http://ke29.ru/comp.php?inn={inn}", timeout=30)

    def FindContact(self, email=None, phone=None):
        """
        Метод поиска контакта в Битрикс24
        :param str email: Адрес электронной почты
        :param dict phone: Номер телефона
        :return: Возвращает значение 0 (не найден), 3 (найден) или id контакта (не все данные есть)
        :rtype: int
        """
        #
        if phone != email:
            count = 0
            self.GET(f"crm.contact.list?select[]=PHONE&filter[PHONE]={phone}")
            count += self.result.json()['total']
            self.GET(f"crm.contact.list?select[]=EMAIL&filter[EMAIL]={email}")
            count += self.result.json()['total']
            is_find = bool(count)
        else:
            is_find = True

        return is_find

    def GET(self, url, param=None):
        """
        Метод для GET запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict param: Параметры которые необходимо передать в GET запросе
        :return:  Возвращает результат запроса
        :rtype: requests.Response
        """
        time.sleep(0.6)
        self.result = requests.get(url=self.URL_bitrix + url, headers=self.Headers, params=param, timeout=30)
        return self.result


class ApiExternal:
    """
    Класс для работы с API Битрикс24

    Основной URL API  Битрикс

    https://apinew.iecp.ru/api/external/v2/


    Атрибуты
    --------
    products : dict
        Список продуктов
    URL_external : str
        URL доступа к API
    result : dict
        Объект хранящий результат POST запроса
    BODY_JSON : dict
        Объект хранящий данные для запросов
    stages : dict
        Список хранящий соответствие этапов

    Методы
    --------
    def POST(self, url, json_Body=None)
        Метод для POST запросов к API

    """

    def __init__(self):

        self.URL_external = "https://apinew.iecp.ru/api/external/v2/"
        self.result = None
        # Получение данных для подключения
        f = open("Info.txt", "r")
        connectionData = f.readlines()[2:]
        self.BODY_JSON = {
            "login": connectionData[0][:-1],
            "pass": connectionData[1][:-1]
        }
        f.close()
        self.stages = {
            1: "C3:PREPAYMENT_INVOICE",
            4: "C3:WON",
            5: "C5:LOST",
        }
        self.POST("products")
        # Получение продуктов АЦ
        self.products = self.result.json()['products']
        self.products += [
            {
                "id": 3336,
                "name": "Создание и выдача квалифицированного сертификата ключа проверки электронной "
                        "подписи (КСКПЭП) юридического лица 1 500 ₽",
                "price": {
                    "fl": 1500,
                    "ip": 1500,
                    "ur": 1500
                }
            },
            {
                "id": 3346,
                "name": "Создание и выдача квалифицированного сертификата ключа проверки электронной "
                        "подписи (КСКПЭП) ИП",
                "price": {
                    "fl": 1500,
                    "ip": 1500,
                    "ur": 1500
                }
            }
        ]

    def POST(self, url, json_Body=None):
        if json_Body is None:
            json_Body = dict()
        try:
            json_Body.update(self.BODY_JSON)
            self.result = requests.post(url=self.URL_external + url, json=json_Body)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(self.URL_external + url, json_Body, sep="\n")
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504


class ApiOrder:

    def __init__(self):
        """
        Конструктор класса, при инициализации записывает метку времени
        """
        # Заголовок с ключом токеном для подключения
        self.HEADERS_AUTH = {"x-Auth-CustomToken": "9ab474b4-c588-4d5f-887e-4cd5b583ad92"}
        self.Url = 'https://billy-publicapi.kontur.ru/'

    # Результат запросов
    result = None

    def GET(self, url, param=None):
        """
        Метод для GET запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict param: Параметры которые необходимо передать в GET запросе
        :return:  Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.get(url=self.Url + url, headers=self.HEADERS_AUTH, params=param, timeout=30)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504
        finally:
            return self.result

    def POST(self, url, bodyJSON):
        """
        Метод для POST запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict bodyJSON: Тело запроса 
        :return: Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.post(url=url, headers=self.HEADERS_AUTH, json=bodyJSON)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504

    def UpdateClient(self, requisite=None, Inn=None, ClientType=None):
        """
        Метод для обновления данных по клиенту. В карточку клиента дописывает AbonentId и записывает BillyId в реквизит
        :param dict requisite: Реквизиты клиента
        :param str Inn: Инн клиента передается в паре с ClientType
        :param int ClientType: Тип клиента 3 - Физ. лицо, 4 - Организация (ИП или ЮЛ)
        :rtype: dict
        :return: Возвращает пару результат и сообщение
        """
        # Проверка переданных параметров
        bit = ApiBitrix()

        if requisite is None:
            # Проверка заполненности ИНН и типа
            if Inn and ClientType:
                # Проверка типа клиента
                if ClientType == 3 or ClientType == 4:
                    # Поиск в битриксе реквизита
                    bit.GET(f"crm.requisite.list?select[]=ID&select[]=ENTITY_TYPE_ID&select[]=ENTITY_ID&"
                            f"select[]=PRESET_ID&select[]=RQ_INN&select[]=RQ_KPP&select[]=UF_CRM_BILLY&"
                            f"filter[ENTITY_TYPE_ID]={ClientType}&filter[RQ_INN]={Inn}")
                    # Проверка результата поиска
                    if bit.result.status_code == 200:
                        if bit.result.json()['total'] == 1:
                            requisite = bit.result.json()['result'][0]
                        elif bit.result.json()['total'] == 0:
                            return {'result': False, 'message': "По переданным ИНН и типу клиента ничего не найдено"}
                        else:
                            return {'result': False,
                                    'message': f"Найдено {bit.result.json()['total']} реквизитов по ИНН и типу"}
                    else:
                        return {'result': False, 'message': f"Ошибка {bit.result.status_code} {bit.result.text}"}
                else:
                    return {'result': False, 'message': f"Тип клиента должен быть 3 (ФЛ) или 4 (ИП или ЮЛ)"}
            else:
                return {'result': False, 'message': f"Ошибка не указан ИНН или тип клиента"}
        # Проверка наличия всех полей
        if not {'ID', 'ENTITY_TYPE_ID', 'ENTITY_ID', 'PRESET_ID',
                'RQ_INN', 'RQ_KPP', 'UF_CRM_BILLY'} <= set(requisite.keys()):
            return {'result': False, 'message': f"В переданных реквизитах {requisite} не хватает полей"}
        # Проверяем реквизит на соответствие шаблонам
        if requisite['ENTITY_TYPE_ID'] == '4':
            method = "company"
            # Компания ЮЛ
            if requisite['PRESET_ID'] == '1':
                if requisite['RQ_KPP'] == '':
                    return {'result': False, 'message': f"У Юридического лица {requisite['ENTITY_ID']} не указан КПП. "
                                                        f"Измените тип на ИП или заполните КПП"}
            # Компания ИП
            elif requisite['PRESET_ID'] == '3':
                if requisite['RQ_KPP']:
                    return {'result': False,
                            'message': f"У Индивидуального предпринимателя {requisite['ENTITY_ID']} указан КПП. "
                                       f"Измените тип на ЮЛ или заполните КПП"}
            else:
                return {'result': False,
                        'message': f"У компании {requisite['ENTITY_ID']} указан неверный тип реквизита. "
                                   f"Измените тип на ЮЛ или ИП"}
        elif requisite['ENTITY_TYPE_ID'] == '3':
            # Клиент ФЛ
            if requisite['PRESET_ID'] == '5':
                method = "contact"
            else:
                return {
                    False: f"У контакта {requisite['ENTITY_ID']} указан не правильный тип реквизита. "
                           f"Измените тип на ФЛ."}
        else:
            return {'result': False,
                    'message': f"Переданный реквизит {requisite} не относится к компании или контакту. "
                               "Проверьте корректность передаваемого реквизита"}
        # Поиск клиента по Id
        bit.GET(f"crm.{method}.get?id={requisite['ENTITY_ID']}")
        if bit.result.status_code == 200:
            client = bit.result.json()['result']
            # Поиск абонента в Биллинге
            if requisite['PRESET_ID'] == '5':
                # Проверка, что контакт является клиентом
                if client['TYPE_ID'] == "CLIENT":
                    self.GET(f"abonents/v0/abonents?inn={requisite['RQ_INN']}").json()
                else:
                    return {'result': False,
                            'message': f"Переданный контакт {requisite['ENTITY_ID']} не является \"Клиентом\""}
            elif requisite['PRESET_ID'] == '1' or requisite['PRESET_ID'] == '3':
                # Проверка, что компания является клиентом
                if client['COMPANY_TYPE'] == "CUSTOMER":
                    self.GET(f"abonents/v0/abonents?inn={requisite['RQ_INN']}&"
                             f"kpp={requisite['RQ_KPP'] if str(requisite['RQ_KPP']) else ''}").json()
                else:
                    return {'result': False,
                            'message': f"Переданная компания {requisite['ENTITY_ID']} не является \"Клиентом\""}
            # Если абонент клиента найден
            if self.result.status_code == 200:
                abonents = self.result.json()
                reqHead = ''
                # Перебор абонентов
                for j in abonents:
                    reqStr = ''
                    # Если клиент компания
                    if requisite['ENTITY_TYPE_ID'] == '4':
                        # Если компания ЮЛ
                        if requisite['PRESET_ID'] == '1':
                            # Проверка является ли абонент головной организацией
                            if j['requisites']['kpp']['organizationKppType'] == 1:
                                # BillyId головы
                                reqHead = j['requisites']['requisiteId']
                        # Проверка заполненности BillyId
                        if requisite['UF_CRM_BILLY'] is None:
                            if len(abonents) == 1:
                                # Присвоить значение BillyId если всего 1 абонент связан с клиентом
                                requisite['UF_CRM_BILLY'] = j['requisites']['requisiteId']
                            else:
                                return {'result': False, 'message': 'У клиента не указан requisiteId биллинга'}
                        # Проверка соответствия BillyId абонента и клиента
                        if j['requisites']['requisiteId'] == requisite['UF_CRM_BILLY'][-36:]:
                            # Формирование строки для обновления реквизита
                            reqStr = f"crm.requisite.update?id={requisite['ID']}&" \
                                     f"fields[UF_CRM_BILLY]={j['requisites']['requisiteId']}&"
                            #
                            reqStr += f"fields[RQ_KPP]={j['requisites']['kpp']['value']}" if requisite[
                                                                                                 'PRESET_ID'] == '1' \
                                else f"fields[RQ_KPP]={j['requisites']['kpp']}"
                            bit.GET(reqStr)
                    else:
                        if j['requisites']['clientType'] == 3:
                            # Обновление реквизита
                            reqStr = f"crm.requisite.update?id={requisite['ID']}&" \
                                     f"fields[UF_CRM_BILLY]={j['requisites']['requisiteId']}"
                            bit.GET(reqStr)
                    # Проверка на обновление реквизита
                    if reqStr:
                        # Формирование строки на обновление клиента
                        reqStr = f"crm.{method}.update?id={client['ID']}&fields[UF_CRM_ABONENTID]={j['abonentId']}"
                        # Проверка на то что компания является филиалом
                        if requisite['PRESET_ID'] == '1' and requisite['RQ_KPP'][4:6] != '01' and reqHead:
                            bit.GET(f"crm.requisite.list?filter[?UF_CRM_BILLY]={reqHead}")
                            if bit.result.json()['total'] == 1:
                                reqStr += f"&fields[UF_CRM_PARENTCOMPANY_ID]=" \
                                          f"{bit.result.json()['result'][0]['ENTITY_ID']}"
                        # Обновление карточки клиента
                        bit.GET(reqStr)
        else:
            return {'result': False, 'message': f"У клиента {requisite} не найден абонент"}
        return {'result': True, 'message': f"Обновление прошло успешно. "
                                           f"Id реквизита: {requisite['ID']}, "
                                           f"Тип сущности: {requisite['ENTITY_TYPE_ID']}, "
                                           f"Id сущности: {requisite['ENTITY_ID']}"}


class ApiDocuments:
    """
    Класс для работы с API Kontur CRM

    Основной URL API  KONTUR.CRM

    https://api-crm-billing.kontur.ru


    Атрибуты
    --------
    REQ_PARAMS : dict
        Параметры запроса GET
    HEADERS_AUTH : dict
        Заголовки запросов
    stages : dict
        Значения этапов продаж
    methods : dict
        Методы для работы с API
    result : request.resp
        Результат запроса
    cursor : pymysql.Connection
        Курсор для работы с БД

    Методы
    --------
    def UpdateTimeStamp()
        Метод обновления метки времени в параметрах и БД
    def CheckStage(stage, idPs)
        Метод для смены этапа ПП
    def POST(url)
        Метод запроса POST
    def GET(url, param=None)
        Метод запроса GET
    """

    def __init__(self):
        """
        Конструктор класса, при инициализации записывает метку времени
        """

        self.cursor = DAL()
        f = open("Info.txt", "r")
        # Заголовок с ключом токеном для подключения
        self.HEADERS_AUTH = {"x-Auth-CustomToken": f.readlines()[0][:-1]}
        self.result = None
        self.URL = "https://billy-publicapi.kontur.ru/"
        f.close()
        self.DocType = {
            1: "Счет - оферта",
            2: "Счет",
            5: "Акт",
            6: "Счет - фактура",
            7: "Счет - фактура на аванс",
            8: "Договор",
            9: "Приложение к договору",
            10: "Сублицензионный договор",
            11: "Спецификация",
            12: "Список лицензиаров",
            13: "Лицензия криптопровайдера",
            14: "Дополнительное соглашение к договору",
            15: "Лицензионный договор",
            16: "УПД"
        }

    def GET(self, url, param=None):
        """
        Метод для GET запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict param: Параметры которые необходимо передать в GET запросе
        :return:  Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.get(url=self.URL + url, headers=self.HEADERS_AUTH, params=param, timeout=30)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
            self.result.status_code = 400
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
            self.result.status_code = 504
        finally:
            return self.result

    def POST(self, url, body):
        """
        Метод для POST запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict body: Тело POST запроса
        :return: Возвращает результат запроса
        :rtype: requests.Response
        """
        try:
            self.result = requests.post(url=self.URL + url, headers=self.HEADERS_AUTH, json=body)
        except requests.exceptions.ConnectionError as ConErr:
            print("Connection Error", ConErr)
            print(url)
        except requests.exceptions.Timeout as TimeOut:
            print("TimeOut", TimeOut)
        return self.result

    def DocumentInfo(self, billId):
        self.GET(f"documents/v2/bills/{billId}/documents/package")
        if self.result.status_code == 200:
            doc = self.result.json()['Documents']
            body = list()
            for i in doc:
                body.append(i['DocumentKey'])
            # body = json.dumps(body)
            self.POST(f"documents/v2/bills/{billId}/documents/info", body)
