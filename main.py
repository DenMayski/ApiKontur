import datetime
import json
import re
import sys
# import time
# import os
# import traceback

import numpy as np

from API import ApiBilly, ApiBitrix
from DAL import DAL

cur = DAL()  # Экземпляр класса DAL для работы с БД

if cur:
    resp_api = ApiBilly()  # Экземпляр класса API для работы с Api
    bitrix = ApiBitrix()

    parameters = sys.argv[1:]
    action = {
        1: "Проверить новости",
        2: "Проверить закрытые продажи",
        3: "Получить информацию по компаниям",
        4: "Выделить организации",
        5: "Выборка физиков",
        6: "Исправление неправильных реквизитов",
        7: "Выборка компаний",
        8: "Выход"
    }

    s_action = '\n'.join([f"{key}) {value}" for key, value in action.items()]) + '\n'

    if not parameters:
        while True:
            parameters.append(input(s_action))
            # Если значение целое число и в промежутке от 1 до 5, то программа идет дальше
            if parameters[0].isdigit():
                if 0 < int(parameters[0]) < len(action.keys()) + 1:
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
                    if 0 < int(parameters[i]) < len(action.keys()) + 1:
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
                                # Строка запроса к БД с условием
                                query = f"SELECT * FROM prospectivesales " \
                                        f"WHERE idProspectivesales = \"{params['idPs']}\" "
                                # Проверка наличия ПП
                                if cur.row_count(query, False):
                                    oldVers = cur.cursor.fetchone()[1]  # Номер версии ПП из БД
                                    # Сравнение версий ПП
                                    if np.int64(oldVers) < np.int64(params['VersPs']):
                                        # Строка обновления ПП в таблице
                                        upd_fields = {
                                            "Version": params['VersPs'],
                                            "TypeSale": params['TypePs'],
                                            "INN": params['InnOrg'],
                                            "KPP": params['KppOrg'],
                                            "IdOrganization": params['IdOrg'],
                                            "ClientType": params['TypeOrg'],
                                            "JSON": json.dumps(fields).replace("\\", "\\\\").replace("'", "\\\'"),
                                        }
                                        cur.Upd(
                                            True,
                                            "prospectivesales",
                                            upd_fields,
                                            f"idProspectivesales = '{params['idPs']}'"
                                        )
                                else:
                                    # Строка записи ПП в таблицу
                                    ins_fields = {
                                        "idProspectivesales": params['idPs'],
                                        "Version": params['VersPs'],
                                        "TypeSale": params['TypePs'],
                                        "INN": params['InnOrg'],
                                        "KPP": params['KppOrg'],
                                        "IdOrganization": params['IdOrg'],
                                        "ClientType": params['TypeOrg'],
                                        "JSON": json.dumps(fields).replace("\\", "\\\\").replace("'", "\\\'") + "'"
                                    }
                                    # ХЗ ПРОВЕРЯЙ МБ БАГ
                                    cur.Upd(
                                        False,
                                        "prospectivesales",
                                        ', '.join([f"{key}='{value}'" for key, value in ins_fields.items()])[:-1]
                                    )
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
                exist = 0
                add = 0
                upd = 0
                for row in table.fetchall():
                    # Проверка заполненности ReqId в таблице
                    if not row[4]:
                        # Запрос битриксу на поиск клиента по ИНН
                        bitrix.GET(f"crm.requisite.list?"
                                   f"filter[ENTITY_TYPE_ID]=3&"
                                   f"filter[RQ_INN]={row[1]}")
                        # Если данные найдены, то внести информацию в БД
                        if bitrix.result.json()['total'] == 1:
                            Req = bitrix.result.json()['result'][0]
                            cur.Upd(
                                True,
                                "clients",
                                f"ReqId = '{Req['ID']}'",
                                f"guid = '{row[0]}'")
                            print(f"{row[1]} have ReqId: {bitrix.result.json()['result'][0]['ID']}")
                        elif bitrix.result.json()['total'] > 1:
                            f = open("Need_to_see.txt", "a")
                            f.writelines(f"\n\n{row[0]:30} | {row[1]:15} HAVE MANY REQ\n\n")
                            f.close()
                            print(f"{row[1]} уже существует")
                            print("https://shturmanit.bitrix24.ru/crm/contact/details/" +
                                  bitrix.result.json()['result'][0]['ENTITY_ID'] + "/")
                        # Если данные не найдены, то необходимо его создать
                        else:
                            # Выбираем ПП по идентификатору организации GUID == idOrganization
                            cur.SELECT(f"SELECT * FROM prospectivesales WHERE idOrganization = '{row[0]}'")
                            js = json.loads(cur.cursor.fetchone()[7])

                            # Наименование организации
                            if resp_api.ClientsFind(js["Organization"]["Inn"], "", 3):
                                name = resp_api.result.json()["Name"]
                            else:
                                name = str(js["Organization"]["Name"]).title()

                            email = ""
                            phone = ""
                            s_req = ""

                            # Словарь с полями контакта
                            cont_fields = {
                                "OPENED": "Y",
                                "ASSIGNED_BY_ID": 1,
                                "TYPE_ID": "CLIENT",
                                "SOURCE_ID": "SELF",
                                "LAST_NAME": name.split()[0]
                            }

                            # Проверка на наличие имени и отчество
                            if len(name.split()) > 1:
                                cont_fields["NAME"] = name.split()[1]
                                if len(name.split()) > 2:
                                    cont_fields["SECOND_NAME"] = ' '.join(name.split()[2:])

                            # Если есть контакты, то
                            if len(js["Contacts"]):
                                # Заполнение Email и номера телефона
                                if len(js["Contacts"][0]["Emails"]):
                                    email = js['Contacts'][0]['Emails'][0]['Address']
                                if len(js["Contacts"][0]["Phones"]):
                                    phone = js['Contacts'][0]['Phones'][0]['Number']
                                    if js["Contacts"][0]["Phones"][0]["AdditionalNumber"]:
                                        phone += f'({js["Contacts"][0]["Phones"][0]["AdditionalNumber"]})'

                            # Поиск клиента с таким телефоном и email на битриксе
                            is_find = bitrix.FindContact(name, email, phone)
                            # Если номер телефона и email в битриксе не существуют
                            if not is_find:
                                cont_fields["EMAIL][0][VALUE_TYPE"] = "WORK"
                                cont_fields["EMAIL][0][VALUE"] = email
                                cont_fields["PHONE][0][VALUE_TYPE"] = "WORK"
                                cont_fields["PHONE][0][VALUE"] = phone
                                add += 1
                                print(f"{name} добавлен в контакты")
                            else:
                                upd += 1
                                f = open("Need_to_see.txt", "a")
                                f.writelines(f"Имя: {name:40}| Inn: {row[1]:15}| Guid: {row[0]}\n")
                                f.close()

                            s_req = f"crm.contact.add?PARAMS[REGISTER_SONET_EVENT]&" + \
                                    '&'.join([f'fields[{key}]={value}' for key, value in cont_fields.items()])
                            bitrix.GET(s_req)
                            print(
                                f"https://shturmanit.bitrix24.ru/crm/contact/details/{bitrix.result.json()['result']}/")
                            req_fields = {
                                "ENTITY_TYPE_ID": 3,
                                "ENTITY_ID": bitrix.result.json()['result'],
                                "PRESET_ID": 5,
                                "RQ_INN": row[1],
                                "RQ_NAME": name,
                                "RQ_LAST_NAME": name.split()[0],
                                "UF_CRM_BILLY": row[0],
                                "NAME": "Физ. лицо",
                                "ACTIVE": "Y"
                            }

                            if len(name.split()) > 1:
                                req_fields["RQ_FIRST_NAME"] = name.split()[1]
                                if len(name.split()) > 2:
                                    req_fields["RQ_SECOND_NAME"] = ' '.join(name.split()[2:])

                            s_req = f"crm.requisite.add?" + \
                                    '&'.join([f'fields[{key}]={value}' for key, value in req_fields.items()])
                            bitrix.GET(s_req)
                            cur.Upd(
                                True,
                                "clients",
                                f"ReqId = {bitrix.result.json()['result']}",
                                f"guid = '{row[0]}'"
                            )
                            # s_req = f"{bitrix.URL_bitrix}crm.contact.update?{s_req}&id={method}"

                            # print(s_req)
                            # bitrix.GET(s_req)
                            # else:
                            #     print(row[0], "не хватает данных")

                print(f"Add {add}\nNeed update {upd}\nExist {exist}")
            elif ch == 4:
                cur.EXECUTE("INSERT INTO clients (guid, inn, kpp, ClientType)"
                            "SELECT p.IdOrganization, p.INN, p.KPP, p.ClientType "
                            "FROM Clients c RIGHT JOIN prospectivesales p ON c.guid = p.IdOrganization "
                            "WHERE c.guid IS NULL AND NOT p.IdOrganization IS null")

                print(f"На данный момент количество организаций в БД: {cur.row_count('Clients')}")
            elif ch == 5:
                next_id = 0
                while True:
                    bitrix.GET(f"crm.requisite.list?filter[ENTITY_TYPE_ID]=3&start={next_id}")
                    for client in bitrix.result.json()['result']:
                        if int(client['PRESET_ID']) != 5:
                            print("https://shturmanit.bitrix24.ru/crm/contact/details/" +
                                  client['ENTITY_ID'] + "/")

                        if cur.row_count(f"SELECT * FROM physic WHERE Id = {client['ID']}", False) == 0:
                            if not cur.row_count(f"SELECT * FROM physic WHERE ID = '{client['ID']}'", False):
                                upd_fields = {
                                    'id': client['ID'],
                                    'inn': client['RQ_INN'],
                                    'name': f"{client['RQ_LAST_NAME']} "
                                            f"{client['RQ_FIRST_NAME']} "
                                            f"{client['RQ_SECOND_NAME']}",
                                    'Entity_id': client['ENTITY_ID'],
                                }
                                if client['UF_CRM_BILLY']:
                                    pattern = "[a-z\d]{8}-[a-z\d]{4}-[a-z\d]{4}-[a-z\d]{4}-[a-z\d]{12}"
                                    res = re.findall(pattern, client['UF_CRM_BILLY'])
                                    upd_fields['billyid'] = res[0] if res else ""
                                cur.Upd(
                                    False,
                                    "physic",
                                    upd_fields
                                )
                                print("Данные занесены Id - ", client['ID'])
                    if "next" in bitrix.result.json():
                        next_id = bitrix.result.json()['next']
                    else:
                        break
            elif ch == 6:
                next_id = 0
                # Цикл на удаление реквизитов контактов созданных по шаблону "Организация"
                while True:
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[ENTITY_TYPE_ID]=3&"
                               f"filter[PRESET_ID]=1&"
                               f"select[]=ID&"
                               f"start = {next_id}")
                    res = bitrix.result

                    for reqId in res.json()['result']:
                        bitrix.GET(f"crm.requisite.delete?id={reqId['ID']}")
                        print(f"Реквизит {reqId['ID']} удален")

                    if "next" in res.json():
                        next_id = bitrix.result.json()["next"]
                    else:
                        break

                """
                Не работает, вопросы к битриксу
                next_id = 0
                # Цикл на обновление реквизитов контактов созданных по шаблону "ИП"
                while True:
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[ENTITY_TYPE_ID]=3&"
                               f"filter[PRESET_ID]=3&"
                               f"select[]=ID&"
                               f"start = {next_id}")
                    res = bitrix.result

                    for reqId in res.json()['result']:
                        bitrix.GET(f"crm.requisite.update?field[PRESET_ID]=5")
                        print(f"Реквизит {reqId['ID']} обновлен")

                    if "next" in bitrix.result.json():
                        next_id = bitrix.result.json()["next"]
                    else:
                        break
                    """
                next_id = 0
                while True:
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[ENTITY_TYPE_ID]=4&"
                               f"filter[PRESET_ID]=1&"
                               f"filter[RQ_KPP]=&"
                               f"start={next_id}")

                    res = bitrix.result.json()
                    for row in res["result"]:
                        if not row["RQ_KPP"] or not row["RQ_OGRN"]:
                            dadata = ApiBitrix.GET_dadata({"inn": row["RQ_INN"]})
                            bitrix.GET(f"crm.requisite.update?id={row['ID']}&"
                                       f"fields[RQ_KPP]={dadata['suggestions'][0]['data']['kpp']}&"
                                       f"fields[RQ_OGRN]={dadata['suggestions'][0]['data']['ogrn']}")
                            print(f"Организации {row['RQ_INN']} добавлены КПП и ОГРН")

                    if "next" in res:
                        next_id = res["next"]
                    else:
                        break
            elif ch == 7:
                next_id = 0
                EntityID = []
                while True:
                    bitrix.GET(f"crm.requisite.list?filter[ENTITY_TYPE_ID]=4&filter[PRESET_ID]=1&start={next_id}")
                    res = bitrix.result.json()
                    for row in res['result']:
                        ins_fields = {
                            "id": row["ENTITY_ID"],
                            "INN": row["RQ_INN"],
                            "KPP": row["RQ_KPP"],
                            "OGRN": row["RQ_OGRN"]
                        }

                        if ins_fields['id'] in EntityID:
                            bitrix.GET(f"crm.requisite.delete?id={row['ID']}")
                            print(f"Удален реквизит с id {row['ID']}")
                        else:
                            EntityID.append(ins_fields['id'])
                            cur.Upd(False, "company", ins_fields)
                            print(f"{row['RQ_INN']} добавлена в БД")

                    if "next" in res:
                        next_id = res['next']
                    else:
                        break

            elif ch == 8:
                break
            else:
                print("Такого варианта нет")
        except Exception as mes:
            print("Something is wrong ", mes.args[0], mes.args[1])
        # os.system("pause")
print("Bye-Bye")
