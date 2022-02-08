# ApiKontur
Проект по связи API контура и Битрикса24.\
На данный момент существует промежуточная MySQL БД на локальном компе

Потенциальная продажа - ПП

В текущей версии реализовано:
1) Работа с api Billy контура
   1) Поиск новостей по всем ПП
   2) Поиск ПП
   3) Переключение этапов ПП
   4) Поиск метки времени
   5) Поиск клиента по ИНН, КПП и типу клиента
2) Работа с БД
   1) Подсчет строк в таблице по определенному параметру
   2) Получение выборки из таблиц по условию
   3) Получение выборки из всех строк таблицы
   4) Выполнение SQL команд
   5) Реализован (но не протестирован) метод "Upd" по обновлению данных таблицы
3) Работа с api Bitrix24
   1) Метод GetInfo относится к сервису dadata
   2) Поиск контакта по email и номеру телефона (необходимо дописать сравнение по имени)
   3) Get запрос к сервису с задержкой, т.к. у Битрикса ограничение не более 2 запросов в секунду