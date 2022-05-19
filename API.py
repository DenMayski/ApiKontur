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
        self.HEADERS_AUTH = {"x-Auth-CustomToken": "d8738ad8-d36b-11e7-baf5-77141aa05f0f"}
        self.Url = 'https://api-billy.testkontur.ru/'

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
