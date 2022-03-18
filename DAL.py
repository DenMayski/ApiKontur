import pymysql
import io


class DAL:
    """
    Data access layer - слой доступа к данным



    Атрибуты
    --------
    conn : pymysql.Connection
        Коннектор к БД
    cur : pymysql.Connection
        Курсор для работы с БД

    Методы
    --------
    def row_count(self, query, isTable=True)
        Метод для получения количества строк в таблице
    def SELECT(self, query)
        Метод для получения выборки из таблицы
    def SELECT_ALL(self, tablename)
        Метод для получения всех данных из таблицы
    def EXECUTE(self, sql)
        Метод для запроса на изменение таблицы
    """

    def __init__(self):
        try:
            """
            self.con = pymysql.connect(host='localhost', user='root', password='Fish_Warden99', database='shturman_it')
            """
            f = open("DB.txt", "r")
            conn_data = [x[:-1] for x in f.readlines()]
            self.con = pymysql.connect(
                host=conn_data[0],
                user=conn_data[1],
                password=conn_data[2],
                database=conn_data[3]
            )
        except pymysql.err.MySQLError:
            self.cursor = None
            print("Ошибка подключения к БД")
        else:
            self.cursor = self.con.cursor()

    # Коннектор к базе mysql
    con = None
    # Курсор для работы
    cursor = None

    def row_count(self, sql_query, isTable=True):
        """
        Метод возвращает количество строк в таблице
        :param str sql_query: Имя таблицы или SQL-запрос
        :param bool isTable: передано имя таблицы
        :return: количество строк
        :rtype: int
        """

        if isTable:
            self.SELECT_ALL(sql_query)
        else:
            self.SELECT(sql_query)
        return self.cursor.rowcount

    def Upd(self, isUpdate, tablename, field_value, where=None):
        """
        Метод на запись или обновление данных
        :param bool isUpdate: Является ли запрос - запросом на обновление
        :param str tablename: Название таблицы
        :param str|dict field_value: Поля и значения под запись
        :param str where: Условие по которому ведется запись
        """
        if type(field_value) is dict:
            field_value = ', '.join([f"{key}='{value}'" for key, value in field_value.items()])
        # Обновление или Добавление записи в таблицу
        self.EXECUTE(f"{'UPDATE' if isUpdate else 'INSERT INTO'} "
                     f"{tablename} "
                     f"SET {field_value} "
                     f"{f'WHERE {where}' if where else ''}")

    def SELECT(self, sql_query):
        """
        Запрос SELECT к таблице в БД
        :param str sql_query: SQL-запрос
        :return:  Результат SQL запроса
        :rtype: pymysql.Connection.cursorclass
        """
        try:
            self.cursor.execute(sql_query)
        except pymysql.err.ProgrammingError as err:
            print(err)
        return self.cursor

    def SELECT_ALL(self, tablename):
        """
        Запрос SELECT к таблице БД, без условий
        :param str tablename: Имя таблицы
        :return: Результат SQL-запроса
        :rtype: pymysql.Connection.cursor-class
        """
        self.SELECT(f"SELECT * FROM {tablename}")
        return self.cursor

    def EXECUTE(self, sql):
        """
        Метод для выполнения запроса на изменение к БД
        :param str sql: SQL-запрос
        """
        try:
            self.cursor.execute(sql)
            self.con.commit()
        except pymysql.err.ProgrammingError as err:
            print(err)
            print(sql)
        except pymysql.err.MySQLError as err:
            print(err)
            print(sql)
