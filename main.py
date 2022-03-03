import datetime
import json
import os
import sys
# import operator
# import re
# import time
# import os
# import traceback

import numpy as np

from API import ApiBilly, ApiBitrix
from DAL import DAL


def FieldsString(dictionary):
    """
    Функция генерирует строку для запроса
    :param dict dictionary: Словарь для запроса
    :return: Строка fields[key]=value
    """
    return '&'.join([f'fields[{key}]={value}' for key, value in dictionary.items()])


def BillyToBitrix(ProspectiveSales):
    """
    Метод для создания сделки в битрикс24
    :param dict ProspectiveSales: Потенциальная продажа
    """

    # Поиск компании из ПП
    bitrix.GET(f"crm.requisite.list?"f"filter[RQ_INN]={ProspectiveSales['Organization']['Inn']}&"
               f"filter[ENTITY_TYPE_ID]={3 if ProspectiveSales['Organization']['Type'] == 3 else 4}")
    # Если компания не найдена, то необходимо ее создать
    if not bitrix.result.json()['total']:
        # Создание компании
        if ProspectiveSales['Organization']['Type'] == 1 or ProspectiveSales['Organization']['Type'] == 2:
            try:
                bitrix.UpdateComp(ProspectiveSales['Organization']['Inn'])
                # Поиск реквизита по ИНН
                bitrix.GET(f"crm.requisite.list?"
                           f"filter[RQ_INN]={ProspectiveSales['Organization']['Inn']}&"
                           f"filter[RQ_KPP]={ProspectiveSales['Organization']['Kpp']}")
                if not bitrix.result.json()['total']:
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[RQ_INN]={ProspectiveSales['Organization']['Inn']}")
                    company_id = bitrix.result.json()['result'][0]['ID']
                    if ProspectiveSales['Organization']['Type'] == 1:
                        if ProspectiveSales['Organization']['Kpp'][4:6] != "01":
                            bitrix.GET(f"crm.requisite.add?"
                                       f"fields[ENTITY_ID]={company_id}&"
                                       f"fields[PRESET_ID]={bitrix.result.json()['result'][0]['PRESET_ID']}&"
                                       f"fields[RQ_INN]={ProspectiveSales['Organization']['Inn']}&"
                                       f"fields[ENTITY_TYPE_ID]="
                                       f"{3 if ProspectiveSales['Organization']['Type'] == 3 else 4}&"
                                       f"fields[RQ_KPP]={ProspectiveSales['Organization']['Kpp']}".replace('None', ''))
                else:
                    company_id = bitrix.result.json()['result'][0]['ENTITY_ID']
            except Exception:
                comp_fields = {
                    "TITLE": ProspectiveSales['Organization']['Name'],
                    "COMPANY_TYPE": "CUSTOMER",
                    "INDUSTRY": "OTHER",
                    "EMPLOYEES": "EMPLOYEES_1",
                    "CURRENCY_ID": "RUB",
                    "OPENED": "Y",
                    "ASSIGNED_BY_ID": 1
                }
                bitrix.GET("crm.company.add?params[REGISTER_SONET_EVENT]=N&" + FieldsString(comp_fields))
                requisite_fields = {
                    "ENTITY_TYPE_ID": 4,
                    "ENTITY_ID": bitrix.result.json()['result'],
                    "PRESET_ID": 1 if ProspectiveSales['Organization']['Type'] == 1 else 3,
                    "RQ_INN": ProspectiveSales['Organization']['Inn'],
                    "UF_CRM_BILLY": ProspectiveSales['Organization']['ClientId'],
                    "NAME": "Организация",
                    "ACTIVE": "Y"
                }
                if ProspectiveSales['Organization']['Type'] == 1:
                    requisite_fields['RQ_KPP'] = ProspectiveSales['Organization']['Kpp']
                else:
                    fullname = ProspectiveSales['Organization']['Name'].split(' ')
                    if "ИП" in fullname:
                        fullname.pop(fullname.index("ИП"))
                    requisite_fields['RQ_FIRST_NAME'] = fullname[1]
                    requisite_fields['RQ_SECOND_NAME'] = fullname[2]
                    requisite_fields['RQ_LAST_NAME'] = fullname[0]
                bitrix.GET("crm.requisite.add?" + FieldsString(requisite_fields))
                company_id = bitrix.result.json()['result']

        # Создание контакта
        elif ProspectiveSales['Organization']['Type'] == 3:
            company_id = CreateContact(ProspectiveSales)
        else:
            print("Проверьте корректность потенциальной продажи")
            return False
    else:
        # Идентификатор компании
        company_id = bitrix.result.json()['result'][0]['ENTITY_ID']

    # Формирование наименования сделки
    name = f"{ProspectiveSales['Product']['Name']} / {ProspectiveSales['Organization']['Name']}"
    # Если канал продажи ПП - "Онлайн"
    if ProspectiveSales['SalesChannel'] == 1:
        category = 5
        managerId = 1
        stageId = "C5:NEW"
    else:
        category = 2
        # Если у ПП есть менеджер
        if ProspectiveSales['Manager']:
            bitrix.GET(f"user.get?filter[UF_USR_USERINBILLY]={ProspectiveSales['Manager']['Code']}")
            managerId = bitrix.result.json()['result'][0]['ID']
        else:
            managerId = 1
        # Определение этапа ПП
        if len(ProspectiveSales['Stages']):
            stageId = bitrix.stages[ProspectiveSales['Stages'][len(ProspectiveSales['Stages']) - 1]['StageId']]
        else:
            stageId = "C2:NEW"

    Amount = 0
    # Сумма по сделке
    for bills in ProspectiveSales['Bills']:
        Amount += bills['Amount'] if bills['State'] != 3 else 0
    # Продукт Контура
    SkbProd = cur.SELECT(f"SELECT id_Bitrix FROM products WHERE id_Billy='{ProspectiveSales['Product']['Id']}'")
    # Бриф сделки
    brief = ProspectiveSales['Brief']['Name'] if ProspectiveSales['Brief'] is not None else ''
    # Комментарий по сделке
    if ProspectiveSales['Comments']['SourceComment'] is not None:
        comment = ProspectiveSales['Comments']['SourceComment']['Text']
    else:
        comment = ''
    # Температура сделки
    temperature = {
        0: None,
        1: 611,
        2: 613,
        3: 615
    }
    # Поля для создания сделки
    deal_fields = {
        "TITLE": name,
        "TYPE_ID": bitrix.typeSale[ProspectiveSales['Type']],
        "STAGE_ID": stageId,
        "CURRENCY_ID": "RUB",
        "OPPORTUNITY": Amount,
        "BEGINDATE": ProspectiveSales['CreateTime'],
        "COMMENTS": comment,
        "ASSIGNED_BY_ID": managerId,
        "ADDITIONAL_INFO": "Test",
        "CATEGORY_ID": category,
        "OPENED": "Y",
        "UF_CRM_SKBPRODUCT": SkbProd.fetchone()[0],
        "UF_CRM_CODPP": "https://billy-partners.kontur.ru/prospectivesale/" + ProspectiveSales['Id'],
        "UF_CRM_BRIEF": brief,
        "UF_CRM_CODSC": ProspectiveSales['Partner']['Code'],
        "UF_CRM_ISTOCHNIKVIGRUZKI": ProspectiveSales['Source']['Name'],
        "UF_CRM_PAGENT": ProspectiveSales['Supplier']['PartnerCode'],
        "UF_CRM_TEMPERATURE": temperature[ProspectiveSales['Temperature']]
    }
    # Дополнительные поля сделки по счету
    if ProspectiveSales['Bills'] is None:
        deal_fields["UF_CRM_LINKBILL"] = "https://billy-partners.kontur.ru/billinfo/" + \
                                         ProspectiveSales['Bills'][0]['BillId']
        deal_fields["UF_CRM_NUMBER_BILL"] = ProspectiveSales['Bills'][0]['Number']
        deal_fields["UF_CRM_DATE_BILL"] = ProspectiveSales['Bills'][0]['CreateDate']
        deal_fields["UF_CRM_DATE_PAY"] = ProspectiveSales['Bills'][0]['PaymentDate']
        deal_fields["UF_CRM_AUTO_BILL"] = ProspectiveSales['Bills'][0]['PaymentDate']
    # Дополнительные поля сделки в случае если ПП завершена
    if ProspectiveSales['Status'] == 4 or ProspectiveSales['Status'] == 2:
        deal_fields['CLOSED'] = 'Y'
        deal_fields["CLOSEDATE"] = ProspectiveSales['Status']['PostponedToDate']
    # Поле организации или контакта сделки
    if ProspectiveSales['Organization']['Type'] == 1 or ProspectiveSales['Organization']['Type'] == 2:
        deal_fields["COMPANY_ID"] = company_id
    elif ProspectiveSales['Organization']['Type'] == 3:
        deal_fields['CONTACT_ID'] = company_id

    # Определение метода, добавление или обновление сделки
    bitrix.GET(f"crm.deal.list?filter[%UF_CRM_CODPP]={ProspectiveSales['Id']}")
    if bitrix.result.json()['total']:
        method = "update"
        deal_fields['IS_NEW'] = 'N'
    else:
        method = "add"
        deal_fields['IS_NEW'] = 'Y'
    # Формирование строки запроса
    s_req = f"crm.deal.{method}?params[REGISTER_SONET_EVENT]=N&" + \
            FieldsString(deal_fields) + \
            (f"&id={bitrix.result.json()['result'][0]['ID']}" if method == 'update' else '')
    if len(s_req) > 2048:
        s_req = s_req.replace('fields[COMMENTS]=' + deal_fields['COMMENTS'] + '&', "")
    bitrix.GET(s_req)
    if bitrix.result.status_code == 200:
        CreateComments(ProspectiveSales)
    else:
        print(ProspectiveSales['Id'], "ошибка создания ", bitrix.result)
        f = open("Error_PotentialSales.txt", "a")
        f.writelines(f"{ProspectiveSales['Id']}, ошибка создания, {bitrix.result}\n")
        f.close()


def CreateComments(ProspectiveSales):
    comments = sorted(ProspectiveSales['Comments']['PartnerComments'], key=lambda x: x['Date'])
    bitrix.GET(f"crm.deal.list?select[]=*&select[]=UF_*&filter[%UF_CRM_CODPP]={ProspectiveSales['Id']}")
    deal = bitrix.result.json()['result'][0]
    dateCom = deal['UF_CRM_LAST_PARTNERCOMMENT'][:-6]
    id = next((x for x in comments if x['Date'] == dateCom), None)
    id = comments.index(id) + 1 if id else 0
    for com in comments[id:]:
        name = com['Author'].split()
        bitrix.GET(
            f"user.get?"
            f"filter[LAST_NAME]={name[0]}&"
            f"filter[NAME]={name[1]}&"
            f"filter[SECOND_NAME]={name[2]}"
        )
        if len(bitrix.result.json()['result']):
            author_id = bitrix.result.json()['result'][0]['ID']
        else:
            author_id = 1

        fields = {
            "AUTHOR_ID": author_id,
            "COMMENT": f"[TABLE]"
                       f"[TR][TD][B][COLOR=blue]Билли[/COLOR]: [/B][I]{com['Date'].replace('T', ' ')}[/I][/TD][/TR]"
                       f"[TR][TD]{com['Text']}[/TD][/TR]"
                       f"[/TABLE]",
            "ENTITY_TYPE": "deal",
            "ENTITY_ID": deal['ID']
            # datetime.datetime.strptime(comments[0]['Date'], "%Y-%m-%dT%H:%M:%S")
        }
        bitrix.GET("crm.timeline.comment.add?" + FieldsString(fields))
        bitrix.GET(f"crm.deal.update?id={deal['ID']}&"
                   f"fields[UF_CRM_LAST_PARTNERCOMMENT]={com['Date']}")


def CreateContact(ProspectiveSales):
    # Наименование организации
    if resp_api.ClientsFind(ProspectiveSales["Organization"]["Inn"], "", 3):
        name = resp_api.result.json()["Name"]
    else:
        name = str(ProspectiveSales["Organization"]["Name"]).title()

    email = ""
    phone = ""

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
    if len(ProspectiveSales["Contacts"]):
        # Заполнение Email и номера телефона
        if len(ProspectiveSales["Contacts"][0]["Emails"]):
            email = ProspectiveSales['Contacts'][0]['Emails'][0]['Address']
        if len(ProspectiveSales["Contacts"][0]["Phones"]):
            phone = ProspectiveSales['Contacts'][0]['Phones'][0]['Number']

            if ProspectiveSales["Contacts"][0]["Phones"][0]["AdditionalNumber"]:
                phone += f'({ProspectiveSales["Contacts"][0]["Phones"][0]["AdditionalNumber"]})'

    # Поиск клиента с таким телефоном и email на битриксе
    isFind = bitrix.FindContact(email, phone)
    # Если номер телефона и email в битриксе не существуют
    if not isFind:
        cont_fields["EMAIL][0][VALUE_TYPE"] = "WORK"
        cont_fields["EMAIL][0][VALUE"] = email
        cont_fields["PHONE][0][VALUE_TYPE"] = "WORK"
        cont_fields["PHONE][0][VALUE"] = phone

    # Создание контакта в Битриксе
    bitrix.GET(f"crm.contact.add?PARAMS[REGISTER_SONET_EVENT]&" + FieldsString(cont_fields))
    contact_Id = bitrix.result.json()['result']
    # Заполнение реквизита
    req_fields = {
        "ENTITY_TYPE_ID": 3,
        "ENTITY_ID": bitrix.result.json()['result'],
        "PRESET_ID": 5,
        "RQ_INN": ProspectiveSales['Organization']['Inn'],
        "RQ_NAME": name,
        "RQ_LAST_NAME": name.split()[0],
        "UF_CRM_BILLY": ProspectiveSales['Id'],
        "NAME": "Физ. лицо",
        "ACTIVE": "Y"
    }
    if len(name.split()) > 1:
        req_fields["RQ_FIRST_NAME"] = name.split()[1]
        if len(name.split()) > 2:
            req_fields["RQ_SECOND_NAME"] = ' '.join(name.split()[2:])

    # Создание реквизита для контакта
    bitrix.GET(f"crm.requisite.add?" + FieldsString(req_fields))
    # Обновление данных в БД
    cur.Upd(
        True,
        "clients",
        f"ReqId = {bitrix.result.json()['result']}",
        f"guid = '{ProspectiveSales['Id']}'"
    )
    return contact_Id


cur = DAL()  # Экземпляр класса DAL для работы с БД

if cur:
    resp_api = ApiBilly()  # Экземпляр класса API для работы с Api
    bitrix = ApiBitrix()
    external = ApiExternal()

    parameters = sys.argv[1:]
    action = {
        1: "Проверить новости",
        2: "Проверить закрытые продажи",
        3: "Создать физ лица",
        4: "Удаление и обновление реквизитов в битриксе",
        5: "Удаление дублирующихся реквизитов у компаний",
        6: "Создание или обновление 1 сделки",
        7: "Разбор АЦ УЦ",
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
            # Выборка ПП
            if ch == 1:
                start_time = datetime.datetime.now().strftime('%d %b - %H:%M:%S')
                print(f"Время начала: {start_time}")
                print(f"Метка времени {resp_api.REQ_PARAMS['from']}")
                result_requests = []  # Список ПП
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
                                        s = f"idProspectivesales = '{params['idPs']}'"
                                        cur.Upd(True, "prospectivesales", upd_fields, s)
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
                                        # ins_fields
                                        ', '.join([f"{key}='{value}'" for key, value in ins_fields.items()])[:-1]
                                    )
                                if fields['Type'] < 4:
                                    BillyToBitrix(fields)
                                j += 1
                                print(f"Обработано {j} новостей")

                        resp_api.UpdateTimeStamp(News["NextTimestamp"])  # Обновление параметров запроса
                        # Проверка на наличие новых данных
                        if not News['HasMore']:
                            break
                    else:
                        print(f'Error: {resp_api.result.status_code} code')
                        break

                print("Последняя метка", resp_api.REQ_PARAMS['from'])
                end_time = datetime.datetime.now().strftime('%d %b - %H:%M:%S')
                print(f"Время окончания {end_time}")

                f = open(os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') + "\\Logs.txt", "a")
                f.write(f"start: {start_time:20}\t| end: {end_time:20}\t| lasttimestamp: {resp_api.REQ_PARAMS['from']}\n")
                f.close()
                # Объединение таблиц чтобы в таблице клиенты всегда были актуальные данные
                cur.EXECUTE("INSERT INTO clients (guid, inn, kpp, ClientType)"
                            "SELECT p.IdOrganization, p.INN, p.KPP, p.ClientType "
                            "FROM Clients c RIGHT JOIN prospectivesales p ON c.guid = p.IdOrganization "
                            "WHERE c.guid IS NULL AND NOT p.IdOrganization IS null")

                print(f"На данный момент количество организаций в БД: {cur.row_count('Clients')}")

            # Проверка статусов ПП
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

            # Создание контактов с реквизитами
            elif ch == 3:
                # Выбор всех строк, где Тип клиента 3
                table = cur.SELECT("Select * from Clients where ClientType = 3")
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

                            CreateContact(js)

            # Удаление и обновление реквизитов в битриксе
            elif ch == 4:
                next_id = 0
                # Цикл на удаление реквизитов контактов созданных по шаблону "Организация"
                while True:
                    # Получение выборки Id реквизитов, созданных по шаблону "Организация"
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[ENTITY_TYPE_ID]=3&"
                               f"filter[PRESET_ID]=1&"
                               f"select[]=ID&"
                               f"start = {next_id}")
                    res = bitrix.result
                    # Перебор результатов выборки
                    for reqId in res.json()['result']:
                        bitrix.GET(f"crm.requisite.delete?id={reqId['ID']}")
                        print(f"Реквизит {reqId['ID']} удален")
                    # Проверка на наличие новых реквизитов
                    if "next" in res.json():
                        next_id = bitrix.result.json()["next"]
                    else:
                        # Обнуление next_id
                        nex_id = 0
                        break

                # Цикл на обновление реквизитов у которых отсутствуют КПП и ОГРН
                while True:
                    # Получение выборки реквизитов с пустым КПП
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[ENTITY_TYPE_ID]=4&"
                               f"filter[PRESET_ID]=1&"
                               f"filter[RQ_KPP]=&"
                               f"start={next_id}")
                    res = bitrix.result.json()
                    # Перебор значений из выборки
                    for row in res["result"]:
                        # Получение информации из да-даты
                        dadata = bitrix.GetInfo(row["RQ_INN"])
                        # Обновление данных реквизита
                        bitrix.GET(f"crm.requisite.update?id={row['ID']}&"
                                   f"fields[RQ_KPP]={dadata['suggestions'][0]['data']['kpp']}&"
                                   f"fields[RQ_OGRN]={dadata['suggestions'][0]['data']['ogrn']}")

                        print(f"Организации {row['RQ_INN']} добавлены КПП и ОГРН")
                    # Проверка на наличие новых реквизитов
                    if "next" in res:
                        next_id = res["next"]
                    else:
                        break

            # Удаление дублирующихся реквизитов у компаний
            elif ch == 5:
                next_id = 0
                # Список сущностей
                EntityID = []
                while True:
                    bitrix.GET(f"crm.requisite.list?filter[ENTITY_TYPE_ID]=4&filter[PRESET_ID]=1&start={next_id}")
                    res = bitrix.result.json()
                    for row in res['result']:
                        # Если у сущности уже есть реквизит
                        if row["ENTITY_ID"] in EntityID:
                            # Удаляем дубликаты из битрикса
                            bitrix.GET(f"crm.requisite.delete?id={row['ID']}")
                            print(f"Удален реквизит с id {row['ID']}")
                        else:
                            # Добавляем номер сущности в список
                            EntityID.append(row["ENTITY_ID"])
                            # Проверка наличия номера реквизита у клиента
                            if not cur.row_count(f"SELECT * FROM clients "
                                                 f"WHERE Inn={row['RQ_INN']} and Kpp={row['RQ_KPP']}", False):
                                # Обновление данных БД
                                upd_fields = {
                                    "ReqId": row["ID"],
                                }
                                cur.Upd(True, "clients", upd_fields, f"Inn={row['RQ_INN']} and Kpp={row['RQ_KPP']}")
                                print(f"{row['RQ_INN']} добавлена в БД")
                    # Проверка на наличие реквизитов
                    if "next" in res:
                        next_id = res['next']
                    else:
                        break

            # Создание сделок
            elif ch == 6:
                codpp = input("Введите код ПП")
                ProspectiveSales = resp_api.GET(
                    resp_api.methods['Find'].replace("{id}", codpp)).json()
                # print(ProspectiveSales)
                if  "errors" not in ProspectiveSales:
                    BillyToBitrix(ProspectiveSales)
                else:
                    print("Ничего не найдено")
            elif ch == 7:
                start = datetime.datetime.strptime("07.07.2021", "%d.%m.%Y")
                days = datetime.timedelta(days=1)
                while (start + days).date() <= datetime.date.today():
                    days = datetime.timedelta(days.days+1)

            elif ch == 8:
                break
            else:
                print("Такого варианта нет")
        except Exception:
            print("Something is wrong ", Exception)
        # os.system("pause")
print("Bye-Bye")
