# count = 0
#     with open('data_file', 'w', encoding='utf-8') as write_file:
#         s=""
#         for row in cur.SELECT_ALL("prospectivesales").fetchall():
#             s += row[7].replace("\n", "\\n")
#             count += 1
#             if count < cur.cursor.rowcount:
#                 s+=(",")
#         json.dump(s, write_file, ensure_ascii=False, indent=0)
#     raise Exception
import datetime
import json
import time
import sys
import os
import traceback

import numpy as np

# import time
from API import ApiBilly, ApiBitrix
from DAL import DAL

cur = DAL()  # Экземпляр класса DAL для работы с БД
resp_api = ApiBilly()  # Экземпляр класса API для работы с Api
bitrix = ApiBitrix()
parameters = sys.argv[1:]
action = {
    1: "Проверить новости",
    2: "Проверить закрытые продажи",
    3: "Получить информацию по компаниям",
    4: "Выделить организации",
    5: "Выход"
}
s_action = '\n'.join([f"{key}) {value}" for key, value in action.items()]) + '\n'

if not parameters:
    while True:
        parameters.append(input(s_action))
        # Если значение целое число и в промежутке от 1 до 5, то программа идет дальше
        if parameters[0].isdigit():
            if 0 < int(parameters[0]) < 6:
                break
        print(f"Значение <{parameters.pop()}> не корректно, попробуйте ввести значение заново\n")

for i in range(len(parameters)):
    print()
    if not parameters[i].isdigit():
        print(f"Значение <{parameters[i]}> не корректно, попробуйте ввести значение заново\n")
        while True:
            parameters[i] = input(s_action)
            # Если значение целое число и в промежутке от 1 до 5, то программа идет дальше
            if parameters[i].isdigit():
                if 0 < int(parameters[i]) < 6:
                    break

    ch = int(parameters[i])

    print(f"Вы выбрали {action[ch]}")
    # os.system("CLS")

    try:
        if ch == 1:
            print(f"Время начала: {datetime.datetime.now().strftime('%d %b - %H:%M:%S')}")
            result_requests = []  # Список ПП
            i = 0
            j = 0

            # Запрос к API Контура
            while True:
                # Проверка статуса запроса
                if resp_api.GET(resp_api.methods['News'], resp_api.REQ_PARAMS).status_code == 200:
                    # result_requests.append(resp_api.result.json())  # Запись результата в список
                    News = resp_api.result.json()
                    for fields in News["News"]:
                        # Получение основных данных ПП
                        params = {
                            "idPs": fields['Id'],
                            "VersPs": fields['Version'],
                            "TypePs": fields['Type'],
                            "InnOrg": fields['Organization']['Inn'],
                            "KppOrg": fields['Organization']['Kpp'],
                            "IdOrg": fields['Organization']['ClientId'],
                            "TypeOrg": fields['Organization']['Type']
                        }

                        for key in params:
                            if not params[key]:
                                params[key] = 'Null'

                        if params["InnOrg"]:
                            if params["idPs"]:
                                if resp_api.ClientsFind(params["InnOrg"], params["KppOrg"], params["TypeOrg"]):
                                    params["idPs"] = resp_api.result.json()["ClientId"]

                            s_exec = ""  # Строка запроса
                            # # Проверка статуса ПП
                            # if fields["Status"]["State"] == 2:
                            #     # Строка удаления завершенной ПП из БД
                            #     # s_exec = f"DELETE FROM prospectivesales WHERE idProspectiveSales='{params['idPs']}'"
                            #     pass
                            # else:
                            # Строка запроса к БД с условием
                            query = f"SELECT * FROM prospectivesales " \
                                    f"WHERE idProspectivesales = \"{params['idPs']}\" "
                            # Проверка наличия ПП
                            if cur.row_count(query, False):
                                oldVers = cur.cursor.fetchone()[1]  # Номер версии ПП из БД
                                # Сравнение версий ПП
                                if np.int64(oldVers) < np.int64(params['VersPs']):
                                    # Строка обновления ПП в таблице
                                    s_exec = f"UPDATE prospectivesales SET " \
                                             f"Version = \"{params['VersPs']}\", " \
                                             f"TypeSale = {params['TypePs']}, " \
                                             f"INN = \"{params['InnOrg']}\", " \
                                             f"KPP = {params['KppOrg']}, " \
                                             f"IdOrganization = \"{params['IdOrg']}\", " \
                                             f"ClientType = {params['TypeOrg']}, " \
                                             f"JSON = '" \
                                             + json.dumps(fields).replace("\\", "\\\\").replace("'", "\\\'") + \
                                             f"' WHERE idProspectivesales = '{params['idPs']}'"
                            else:
                                # Строка записи ПП в таблицу
                                s_exec = f"INSERT INTO prospectivesales SET  " \
                                         f"idProspectivesales = \"{params['idPs']}\", " \
                                         f"Version = \"{params['VersPs']}\", " \
                                         f"TypeSale = {params['TypePs']}, " \
                                         f"INN = \"{params['InnOrg']}\", " \
                                         f"KPP = {params['KppOrg']}, " \
                                         f"IdOrganization = \"{params['IdOrg']}\", " \
                                         f"ClientType = {params['TypeOrg']}, " \
                                         f"JSON = '" + json.dumps(fields).replace("\\", "\\\\").replace("'",
                                                                                                        "\\\'") + "'"
                            # Проверка на наличие запроса
                            # print(s_exec)
                            if s_exec:
                                cur.EXECUTE(s_exec)  # Выполнение запроса к БД
                                j += 1
                                print(f"Обработано {j} новостей")
                    resp_api.UpdateTimeStamp(News["NextTimestamp"])  # Обновление параметров запроса

                    i += 1
                    # print(f"Количество запросов {i}")

                    # Проверка на наличие новых данных
                    if not News['HasMore']:
                        break

                else:
                    print(f'Error: {resp_api.result.status_code} code')
                    break

            print("Последняя метка", resp_api.REQ_PARAMS['from'])
            print(f"Время окончания {datetime.datetime.now().strftime('%d %b - %H:%M:%S')}")
            print('All is complete')

        elif ch == 2:
            completed = []  # Список завершенных ПП
            # Перебор всех записей таблицы
            for row in cur.SELECT_ALL("prospectivesales"):
                resp_api.GET(resp_api.methods["Find"].replace("{id}", row[0]))  # Поиск информации о ПП
                # Проверка статуса ПП
                if resp_api.result.json()["Status"]["State"] == 2:
                    completed.append(row[0])
                # Проверка канала продажи
                if resp_api.result.json()["SalesChannel"] == 2:
                    # Проверка количества счетов
                    if len(resp_api.result.json()["Bills"]):
                        isPaid = True  # Флаг оплачено
                        # Перебор всех счетов
                        for bill in resp_api.result.json()["Bills"]:
                            isPaid = isPaid and (bill["State"] == 1 or bill["State"] == 3)  # Проверка статуса счета
                        # Проверка оплаты счета
                        if isPaid:
                            stages = resp_api.result.json()["Stages"]  # Список этапов
                            # Проверка выделения этапа
                            if resp_api.stages[4] in [list(i.values())[0] for i in stages]:
                                print("++")
                            else:
                                print("??", row[0])
                        else:
                            print("+-", row[0])
                    else:
                        print("--", row[0])
            for row in completed:
                # cur.EXECUTE(f"DELETE FROM prospectivesales WHERE idProspectiveSales='{row}'")
                pass

        elif ch == 3:
            table = cur.SELECT("Select * from Clients where ClientType = 3")
            for row in table.fetchall():
                try:
                    if not row[4]:
                        bitrix.GET(bitrix.URL_bitrix + f"/crm.requisite.list"
                                                       f"?select[]=ENTITY_ID&filter[RQ_INN]={row[1]}")
                        time.sleep(0.6)
                        if bitrix.result.json()['total'] == 1:
                            cur.EXECUTE(f"UPDATE clients SET "
                                        f"ReqID = '{bitrix.result.json()['result'][0]['ENTITY_ID']}' "
                                        f"WHERE guid = '{row[0]}'")
                            print(row[1], "in database +")
                        else:

                            cur.SELECT(f"SELECT * FROM prospectivesales WHERE idOrganization = '{row[0]}'")
                            rowProspective = cur.cursor.fetchone()

                            js = json.loads(rowProspective[7])
                            s_message = ""

                            if len(js["Contacts"]):
                                name = js["Organization"]["Name"]
                                email = "None"
                                number = "None"

                                if "контакты" not in js["Contacts"][0]["Name"].lower() and \
                                        js["Contacts"][0]["Name"] != "":
                                    if len(js["Contacts"][0]["Name"].lower().split(" ")) == 3:
                                        name = js["Contacts"][0]["Name"]

                                name = str(name)

                                if name:
                                    name = name.title()
                                else:
                                    name = "None"

                                if len(js["Contacts"][0]["Emails"]):
                                    email = js["Contacts"][0]["Emails"][0]["Address"]
                                if len(js["Contacts"][0]["Phones"]):
                                    number = js["Contacts"][0]["Phones"][0]["Number"]
                                    if js["Contacts"][0]["Phones"][0]["AdditionalNumber"]:
                                        number += f'({js["Contacts"][0]["Phones"][0]["AdditionalNumber"]})'

                                s_message = f"Необходимо создать контакт " \
                                            f"Имя: {name:40}" \
                                            f"email: {email:35}" \
                                            f"Номер телефона: {number}"

                                if email == number:
                                    s_message = ""

                            print(
                                s_message if s_message else row[1] + " not found -")
                except Exception:
                    print(Exception)
            """
            for row in cur.cursor.fetchall():
                js = json.loads(row[7])
                org = js["Organization"]

                s = f"INSERT INTO clients SET Inn = '{org['Inn']}', " \
                    f"Kpp = %kpp, Type = {org['Type']}, " \
                    f"Guid = '{org['ClientId']}'"
                s.replace("%kpp", "'" + org["Kpp"] + "'" if org["Kpp"] else "Null")
                if js["Organization"]["Type"] < 3:
                    print(js["Organization"])
                    res = bitrix.GetInfo(js["Organization"]["Inn"])
                    if res.text:
                        print(res.json())
                time.sleep(1.1)
            """

        elif ch == 4:
            cur.EXECUTE("INSERT INTO clients (guid, inn, kpp, ClientType)"
                        "SELECT p.IdOrganization, p.INN, p.KPP, p.ClientType "
                        "FROM Clients c RIGHT JOIN prospectivesales p ON c.guid = p.IdOrganization "
                        "WHERE c.guid IS NULL AND NOT p.IdOrganization IS null")

            print(f"На данный момент количество организаций в БД: {cur.row_count('Clients')}")
        elif ch == 5:
            break
        else:
            print("Такого варианта нет")
    except Exception as mes:
        print("Something is wrong ", mes.args[0])
    # os.system("pause")
print("Bye-Bye")
