import datetime
import json
import sys
# import time
# import os
# import traceback

import numpy as np

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
            # Выбор всех строк, где Тип клиента 3
            table = cur.SELECT("Select * from Clients where ClientType = 3")
            for row in table.fetchall():
                # Проверка заполненности ReqId в таблице
                if not row[4]:
                    # Запрос битриксу на поиск клиента по ИНН
                    bitrix.GET(bitrix.URL_bitrix + f"/crm.requisite.list"
                                                   f"?select[]=ENTITY_ID&filter[RQ_INN]={row[1]}")
                    # Если данные найдены, то внести информацию в БД
                    if bitrix.result.json()['total'] == 1:
                        cur.EXECUTE(f"UPDATE clients SET "
                                    f"ReqId = '{bitrix.result.json()['result'][0]['ENTITY_ID']}' "
                                    f"WHERE guid = '{row[0]}'")
                        print(f"{row[1]} have ReqId: {bitrix.result.json()['result'][0]['ENTITY_ID']}")
                    # Если данные не найдены, то необходимо его создать
                    else:
                        # Выбираем ПП по идентификатору организации GUID == idOrganization
                        cur.SELECT(f"SELECT * FROM prospectivesales WHERE idOrganization = '{row[0]}'")
                        js = json.loads(cur.cursor.fetchone()[7])

                        # Наименование организации
                        name = str(js["Organization"]["Name"]).title()

                        # Если есть контакты, то
                        if len(js["Contacts"]):
                            # Заполнение ФИО, Email и номера телефона
                            s_req = f"FIELDS[OPENED]=Y&" \
                                    f"FIELDS[ASSIGNED_BY_ID]=1&" \
                                    f"FIELDS[TYPE_ID]=CLIENT&" \
                                    f"FIELDS[SOURCE_ID]=SELF&" \
                                    f"PARAMS[REGISTER_SONET_EVENT]&" \
                                    f"FIELDS[LAST_NAME]={name.split()[0]}"
                            # Проверка на наличие имени и отчества
                            if len(name.split()) > 2:
                                s_req += f"&FIELDS[NAME]={name.split()[1]}&" \
                                         f"FIELDS[SECOND_NAME]={' '.join(name.split()[2:])}"

                            email = ""
                            if len(js["Contacts"][0]["Emails"]):
                                email = js['Contacts'][0]['Emails'][0]['Address']
                                s_req += f"&FIELDS[EMAIL][0][VALUE]=WORK&FIELDS[EMAIL][0][VALUE]={email}"
                            phone = ""
                            if len(js["Contacts"][0]["Phones"]):
                                phone = js['Contacts'][0]['Phones'][0]['Number']
                                s_req += f"&FIELDS[PHONE][0][VALUE]=WORK&FIELDS[PHONE][0][VALUE]={phone}"
                                # if js["Contacts"][0]["Phones"][0]["AdditionalNumber"]:
                                #     phone += f'({js["Contacts"][0]["Phones"][0]["AdditionalNumber"]})'

                            print(f"{name:40}|{email:35}|{phone}")
                            method = bitrix.FindContact(email, phone)
                            if method == 0:
                                print(f"{name} добавлен в контакты")
                                bitrix.GET(f"{bitrix.URL_bitrix}crm.contact.add?{s_req}")
                            elif method == 3:
                                print(f"{name} уже существует поиск по {phone:15} | {email}")
                            elif method > 3:
                                print(f"NEED UPDATE {name}")
                                # s_req = f"{bitrix.URL_bitrix}crm.contact.update?{s_req}&id={method}"

                            # print(s_req)
                            # bitrix.GET(s_req)
                            # else:
                            #     print(row[0], "не хватает данных")
                        else:
                            print(f"У организации {name} - {row[0]} нет контактов")
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
