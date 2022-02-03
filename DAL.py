import pymysql


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

    # Коннектор к базе mysql
    con = pymysql.connect(host='localhost', user='root', password='Fish_Warden99', database='shturman_it')

    # Курсор для работы
    cursor = con.cursor()

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
        return  self.cursor

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