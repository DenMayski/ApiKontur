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
        if self.cursor.row_count("lasttimestamp"):
            self.REQ_PARAMS["from"] = self.cursor.SELECT_ALL("lasttimestamp").fetchone()[0]
        else:
            self.REQ_PARAMS["from"] = int(
                self.GET(self.methods["lasttimestamp"]).text)
        self.GET(self.methods["News"], param=self.REQ_PARAMS)

    # Заголовок с ключом токеном для подключения
    HEADERS_AUTH = {"x-Auth-CustomToken": "9ab474b4-c588-4d5f-887e-4cd5b583ad92"}

    # Параметры для поиска новостей
    REQ_PARAMS = {
        "from": 1283342357,
        "count": 500
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
        6: "a8d93fdb-896a-44c5-a75d-d63fe06b075f"  # Подписаны документы
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
            self.POST(self.methods["Clients"])
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
        self.POST(url=self.methods["SwitchStage"].replace("{id}", idPs))
        if self.result.status_code == 200:
            print("Ok")
        else:
            print("Error")


class ApiBitrix:
    URL_bitrix = "https://shturmanit.bitrix24.ru/rest/1/pv07bkvp2y83yn07/"

    URL_dadata = "http://ke29.ru/dadata-get.php"

    Headers = {'Accept': 'application/json', 'Content-Type': 'application/json;charset=utf-8'}

    result = None

    def GetInfo(self, inn):
        return self.GET(self.URL_dadata + f"?inn={inn}")

    def GET(self, url, param=None):
        """
        Метод для GET запроса
        :param str url: Адрес на который будет совершен запрос
        :param dict param: Параметры которые необходимо передать в GET запросе
        :return:  Возвращает результат запроса
        :rtype: requests.Response
        """
        self.result = requests.get(url=url, headers=self.Headers, params=param, timeout=30)
        return self.result
