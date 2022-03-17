import datetime
import json
import os
import sys
import time
import traceback
# import operator
# import re
# import time
# import os
# import traceback

# import numpy as np

from API import ApiBilly, ApiBitrix, ApiExternal
from DAL import DAL


def FieldsString(dictionary):
    """
    Функция генерирует строку для запроса
    :param dict dictionary: Словарь для запроса
    :return: Строка fields[key]=value
    """
    return '&'.join([f'fields[{key}]={value if value is not None else ""}' for key, value in dictionary.items()])


def BillyToBitrix(ProspectiveSale):
    """
    Метод для создания сделки в битрикс24
    :param dict ProspectiveSale: Потенциальная продажа
    """

    company_id = None
    contact_id = None
    # Если сделка была взята из АЦ УЦ
    if ProspectiveSale['SalesChannel'] == -1:
        # Создаем или обновляем информацию о компании если ИП или ЮЛ
        if ProspectiveSale['Organization']['Type'] < 3:
            bitrix.UpdateComp(ProspectiveSale['Organization']['Inn'])
            bitrix.GET(f"crm.requisite.list?"
                       f"filter[RQ_INN]={ProspectiveSale['Organization']['Inn']}&"
                       f"filter[ENTITY_TYPE_ID]=4")
            company_id = bitrix.result.json()['result'][0]['ENTITY_ID']

        # Создаем или обновляем информацию о контакте
        contact_id = CreateContact(ProspectiveSale, company_id)
    else:
        # Поиск компании из ПП
        bitrix.GET(f"crm.requisite.list?"f"filter[RQ_INN]={ProspectiveSale['Organization']['Inn']}&"
                   f"filter[ENTITY_TYPE_ID]={3 if ProspectiveSale['Organization']['Type'] == 3 else 4}")
        # Если компания не найдена, то необходимо ее создать
        if not bitrix.result.json()['total']:
            # Создание компании
            if ProspectiveSale['Organization']['Type'] == 1 or ProspectiveSale['Organization']['Type'] == 2:
                # Создание компании через сервис Дадата
                try:
                    bitrix.UpdateComp(ProspectiveSale['Organization']['Inn'])
                    # Поиск реквизита по ИНН
                    bitrix.GET(f"crm.requisite.list?"
                               f"filter[RQ_INN]={ProspectiveSale['Organization']['Inn']}&"
                               f"filter[RQ_KPP]={ProspectiveSale['Organization']['Kpp']}")
                    # Проверка создания реквизита с указанными ИНН и КПП
                    if not bitrix.result.json()['total']:
                        # Создание реквизита с ИНН и КПП из ПП
                        bitrix.GET(f"crm.requisite.list?"
                                   f"filter[RQ_INN]={ProspectiveSale['Organization']['Inn']}")
                        company_id = bitrix.result.json()['result'][0]['ID']

                        # Проверка, что организация - Юридическое лицо и не головная
                        if ProspectiveSale['Organization']['Type'] == 1:
                            if ProspectiveSale['Organization']['Kpp'][4:6] != "01":
                                bitrix.GET(f"crm.requisite.add?"
                                           f"fields[ENTITY_ID]={company_id}&"
                                           f"fields[PRESET_ID]={bitrix.result.json()['result'][0]['PRESET_ID']}&"
                                           f"fields[RQ_INN]={ProspectiveSale['Organization']['Inn']}&"
                                           f"fields[ENTITY_TYPE_ID]="
                                           f"{3 if ProspectiveSale['Organization']['Type'] == 3 else 4}&"
                                           f"fields[RQ_KPP]="
                                           f"{ProspectiveSale['Organization']['Kpp']}".replace('None', ''))
                    else:
                        company_id = bitrix.result.json()['result'][0]['ENTITY_ID']
                except Exception:
                    comp_fields = {
                        "TITLE": ProspectiveSale['Organization']['Name'],
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
                        "PRESET_ID": 1 if ProspectiveSale['Organization']['Type'] == 1 else 3,
                        "RQ_INN": ProspectiveSale['Organization']['Inn'],
                        "UF_CRM_BILLY": ProspectiveSale['Organization']['ClientId'],
                        "NAME": "Организация",
                        "ACTIVE": "Y"
                    }
                    if ProspectiveSale['Organization']['Type'] == 1:
                        requisite_fields['RQ_KPP'] = ProspectiveSale['Organization']['Kpp']
                    else:
                        fullname = ProspectiveSale['Organization']['Name'].split(' ')
                        if "ИП" in fullname:
                            fullname.pop(fullname.index("ИП"))
                        requisite_fields['RQ_FIRST_NAME'] = fullname[1]
                        requisite_fields['RQ_SECOND_NAME'] = fullname[2]
                        requisite_fields['RQ_LAST_NAME'] = fullname[0]
                    bitrix.GET("crm.requisite.add?" + FieldsString(requisite_fields))
                    company_id = bitrix.result.json()['result']

            # Создание контакта
            elif ProspectiveSale['Organization']['Type'] == 3:
                contact_id = CreateContact(ProspectiveSale)
            else:
                print("Проверьте корректность потенциальной продажи")
                return False
        else:
            # Id компании
            company_id = bitrix.result.json()['result'][0]['ENTITY_ID']

    # Словарь с полями сделки
    deal_fields = dict()
    # Действия в зависимости от сделки
    if ProspectiveSale['SalesChannel'] == -1:
        # Формирование наименования сделки
        name = f"АЦ-{ProspectiveSale['Id']} / {ProspectiveSale['Organization']['Name']}"
        # Категория сделки
        category = 3
        # Ответственный за сделку
        managerId = 1
        # Этап сделки
        stageId = external.stages[ProspectiveSale['Stages']] \
            if ProspectiveSale['Stages'] in external.stages else "C3:EXECUTING"
        # ID продукта
        SkbProd = 0
        # Комментарий к сделке
        comment = '|\n\n'.join(prod['Name'] for prod in ProspectiveSale['Product'])
        # Ссылка на сделку
        link = f"https://lk.iecp.ru/application/{ProspectiveSale['Id']}"

    else:
        #  Формирование наименования сделки
        name = f"{ProspectiveSale['Product']['Name']} / {ProspectiveSale['Organization']['Name']}"
        # Ссылка на сделку
        link = f"https://billy-partners.kontur.ru/prospectivesale/{ProspectiveSale['Id']}"

        # Если канал продажи ПП - "Онлайн"
        if ProspectiveSale['SalesChannel'] == 1:
            # Категория сделки
            category = 5
            # Ответственный за сделку
            managerId = 1
            # Этап сделки
            stageId = "C5:NEW"
        else:
            # Категория сделки
            category = 2

            # Выставление ответственного за менеджера
            if ProspectiveSale['Manager']:
                # Получить код ответственного по сделке по коду Биллинга
                bitrix.GET(f"user.get?filter[UF_USR_USERINBILLY]={ProspectiveSale['Manager']['Code']}")
                # Если есть номер
                if bitrix.result.json()['result']:
                    # Ответственный за сделку
                    managerId = bitrix.result.json()['result'][0]['ID']
                else:
                    managerId = 1
            else:
                managerId = 1

            # Определение этапа ПП
            if len(ProspectiveSale['Stages']):
                stageId = bitrix.stages[ProspectiveSale['Stages'][len(ProspectiveSale['Stages']) - 1]['StageId']]
            else:
                stageId = "C2:NEW"

        # Id продукта в Битрикс24 по Id Биллинга
        SkbProd = cur.SELECT(f"SELECT id_Bitrix "
                             f"FROM products "
                             f"WHERE id_Billy='{ProspectiveSale['Product']['Id']}'").fetchone()[0]

        # Комментарий по сделке
        if ProspectiveSale['Comments']['SourceComment'] is not None:
            comment = ProspectiveSale['Comments']['SourceComment']['Text']
        else:
            comment = ""

    # Сумма по сделке
    Amount = 0
    for bills in ProspectiveSale['Bills']:
        Amount += bills['Amount'] if bills['State'] != 3 else 0

    # Бриф сделки
    brief = ProspectiveSale['Brief']['Name'] if ProspectiveSale['Brief'] is not None else ''

    # Температура сделки
    temperature = {
        0: None,
        1: 611,
        2: 613,
        3: 615
    }

    # Поля для создания сделки
    # Наименование сделки
    deal_fields['TITLE'] = name
    # Тип сделки
    deal_fields['TYPE_ID'] = bitrix.typeSale[ProspectiveSale['Type']]
    # Этап сделки
    deal_fields['STAGE_ID'] = stageId
    # Валюта сделки
    deal_fields['CURRENCY_ID'] = "RUB"
    # Стоимость сделки
    deal_fields['OPPORTUNITY'] = Amount
    # Дата создания
    deal_fields['BEGINDATE'] = ProspectiveSale['CreateTime']
    # Комментарий
    deal_fields['COMMENTS'] = comment
    # Ответственный за сделку
    deal_fields['ASSIGNED_BY_ID'] = managerId
    # Дополнительная информация
    deal_fields['ADDITIONAL_INFO'] = ""
    # Категория сделки
    deal_fields['CATEGORY_ID'] = category
    # Открытая ли сделка
    deal_fields['OPENED'] = "Y"
    # Продукт Биллинга
    deal_fields['UF_CRM_SKBPRODUCT'] = SkbProd
    # Ссылка на ПП
    deal_fields['UF_CRM_CODPP'] = link
    # Бриф сделки
    deal_fields['UF_CRM_BRIEF'] = brief
    # Код сервисного центра
    deal_fields['UF_CRM_CODSC'] = ProspectiveSale['Partner']['Code']
    # Источник ПП
    deal_fields['UF_CRM_ISTOCHNIKVIGRUZKI'] = ProspectiveSale['Source']['Name']
    # Код партнера
    deal_fields['UF_CRM_PAGENT'] = ProspectiveSale['Supplier']['PartnerCode']
    # Температура сделки
    deal_fields['UF_CRM_TEMPERATURE'] = temperature[ProspectiveSale['Temperature']]
    # Сделка по АЦ или Биллинг
    deal_fields['ORIGINATOR_ID'] = "АЦ" if ProspectiveSale['SalesChannel'] == -1 else "Billy"
    # Код ПП или сделки АЦ
    deal_fields['ORIGIN_ID'] = ProspectiveSale['Id']

    # Дополнительные поля сделки по счету
    if ProspectiveSale['Bills'] and ProspectiveSale['SalesChannel'] != -1:
        # Ссылка на счет
        deal_fields['UF_CRM_LINKBILL'] = "https://billy-partners.kontur.ru/billinfo/" + \
                                         ProspectiveSale['Bills'][0]['BillId']
        # Номер счета
        deal_fields['UF_CRM_NUMBER_BILL'] = ProspectiveSale['Bills'][0]['Number']
        # Дата счета
        deal_fields['UF_CRM_DATE_BILL'] = ProspectiveSale['Bills'][0]['CreateDate']
        # Дата оплаты
        deal_fields['UF_CRM_DATE_PAY'] = ProspectiveSale['Bills'][0]['PaymentDate']
        deal_fields['UF_CRM_AUTO_BILL'] = ProspectiveSale['Bills'][0]['PaymentDate']

    # ID компании связанной со сделкой
    if company_id is not None:
        deal_fields['COMPANY_ID'] = company_id
    # ID контакта связанного со сделкой
    if contact_id is not None:
        deal_fields['CONTACT_ID'] = contact_id

    # Определение метода, добавление или обновление сделки
    bitrix.GET(f"crm.deal.list?filter[%UF_CRM_CODPP]={ProspectiveSale['Id']}")
    # Если поиск вернул результат
    if bitrix.result.json()['total']:
        # Метод на обновление
        method = "update"
        # Если сделка по АЦ
        if ProspectiveSale['SalesChannel'] == -1:
            # Проверка соответствия этапа
            if deal_fields['STAGE_ID'] not in external.stages.values():
                # Выставить заранее проставленное значение
                deal_fields['STAGE_ID'] = bitrix.result.json()['STAGE_ID']
        deal_fields['IS_NEW'] = 'N'
    else:
        # Поиск по оригинатору
        bitrix.GET(f"crm.deal.list?filter[ORIGIN_ID]={ProspectiveSale['Id']}")
        if bitrix.result.json()['total']:
            # Метод на обновление
            method = "update"
            deal_fields['IS_NEW'] = 'N'
        else:
            # Метод на добавление
            method = "add"
            deal_fields['IS_NEW'] = 'Y'

    # Формирование строки запроса
    s_req = f"crm.deal.{method}?params[REGISTER_SONET_EVENT]=N&" + \
            FieldsString(deal_fields) + \
            (f"&id={bitrix.result.json()['result'][0]['ID']}" if method == 'update' else '')
    # Ограничение на количество символов запроса
    if len(s_req) > 2048:
        s_req = s_req.replace('fields[COMMENTS]=' + deal_fields['COMMENTS'] + '&', "")
    # Выполнение запроса
    bitrix.GET(s_req)
    # Если сделка по Биллингу
    if deal_fields['ORIGINATOR_ID'] == "Billy":
        # Если запрос выполнен успешно
        if bitrix.result.status_code == 200:
            # Создание комментариев
            CreateComments(ProspectiveSale)
        else:
            # Вывод сообщение об ошибке выполнения запроса
            print(ProspectiveSale['Id'], "ошибка создания ", bitrix.result)
            f = open("Error_PotentialSales.txt", "a")
            f.writelines(f"{ProspectiveSale['Id']}, ошибка создания, {bitrix.result}\n")
            f.close()


def CreateContact(ProspectiveSales, company_id=None):
    """
    Метод создания контакта
    :param dict ProspectiveSales: Потенциальная продажа
    :param int company_id: Id компании
    :return: Id контакта
    :rtype: int
    """

    # Если сделка по АЦ
    if ProspectiveSales['SalesChannel'] == -1:
        # Имя заявителя
        name = ProspectiveSales['Organization']['ClaimantName']
    # Иначе сделка Биллинга
    else:
        # Поиск по сочетанию типа компании и ИНН
        if resp_api.ClientsFind(ProspectiveSales['Organization']['Inn'], "", ProspectiveSales['Type']):
            name = resp_api.result.json()['Name']
        else:
            name = str(ProspectiveSales['Organization']['Name']).title()

    # Приведение строки имени к корректному виду
    name = name.title()
    # Электронный адрес контакта
    email = ""
    # Номер телефона контакта
    phone = ""

    # Словарь с полями контакта
    cont_fields = dict()

    # Открытый контакт
    cont_fields["OPENED"] = "Y"
    # Ответственный за контакт
    cont_fields["ASSIGNED_BY_ID"] = 1
    # Тип контакта
    cont_fields["TYPE_ID"] = "CLIENT"
    # Источник контакта
    cont_fields["SOURCE_ID"] = "SELF"
    # Фамилия контакта
    cont_fields["LAST_NAME"] = name.split()[0]

    # Проверка на наличие имени и отчество
    if len(name.split()) > 1:
        # Имя контакта
        cont_fields['NAME'] = name.split()[1]
        if len(name.split()) > 2:
            # Фамилия контакта
            cont_fields['SECOND_NAME'] = ' '.join(name.split()[2:])

    # Если есть контакты, то
    if len(ProspectiveSales['Contacts']):
        # Заполнение Email и номера телефона
        if len(ProspectiveSales['Contacts'][0]['Emails']):
            email = ProspectiveSales['Contacts'][0]['Emails'][0]['Address']
        if len(ProspectiveSales['Contacts'][0]['Phones']):
            phone = ProspectiveSales['Contacts'][0]['Phones'][0]['Number']
            if ProspectiveSales['Contacts'][0]['Phones'][0]['AdditionalNumber']:
                phone += f"({ProspectiveSales['Contacts'][0]['Phones'][0]['AdditionalNumber']})"

    # # Поиск клиента с таким телефоном и email на битриксе
    # isFind = bitrix.FindContact(email, phone)
    # # Если номер телефона и email в битриксе не существуют
    # if not isFind:

    # Если продажа из АЦ
    if ProspectiveSales['SalesChannel'] == -1:
        # Дата рождения
        cont_fields['BIRTHDATE'] = ProspectiveSales['Documents']['Birthdate']
        # Пол
        cont_fields['HONORIFIC'] = ProspectiveSales['Documents']['Gender']

    contact_id = 0
    # Поиск по компании
    bitrix.GET(f"crm.requisite.list?"
               f"filter[RQ_INN]={ProspectiveSales['Organization']['Inn']}&"
               f"filter[ENTITY_TYPE_ID]=3")

    if bitrix.result.json()['result']:
        # Получение Id реквизита
        reqId = bitrix.result.json()['result'][0]['ID']
        # Получение Id контакта
        contact_id = bitrix.result.json()['result'][0]['ENTITY_ID']

        bitrix.GET(f"crm.contact.list?select[]=*&select[]=PHONE&select[]=EMAIL&filter[ID]={contact_id}")
        if bitrix.result.json()['result'][0]['EMAIL']:
            HasCont = False
            cont_fields['EMAIL'] = list()
            for emails in bitrix.result.json()['result'][0]['EMAIL']:
                # Тип и значение электронной почты
                cont_fields['EMAIL'].append(
                    {
                        'VALUE_TYPE': emails['VALUE_TYPE'],
                        'VALUE': emails['VALUE']
                    }
                )
                if email == emails['VALUE']:
                    HasCont = True
            if HasCont and email:
                cont_fields['EMAIL'].append(
                    {
                        'VALUE_TYPE': "WORK",
                        'VALUE': email
                    }
                )

        if bitrix.result.json()['result'][0]['PHONE']:
            HasCont = False
            cont_fields['PHONE'] = list()
            for phones in bitrix.result.json()['result'][0]['PHONE']:
                # Тип и значение электронной почты
                cont_fields['PHONE'].append(
                    {
                        'VALUE_TYPE': phones['VALUE_TYPE'],
                        'VALUE': phones['VALUE']
                    }
                )
                if phone == phones['VALUE']:
                    HasCont = True

            if HasCont and "7405405" not in phone and phone:
                cont_fields['PHONE'].append(
                    {
                        'VALUE_TYPE': "WORK",
                        'VALUE': phone
                    }
                )

        # Обновление информации о контакте
        bitrix.GET(f"crm.contact.update?id={contact_id}&PARAMS[REGISTER_SONET_EVENT]&" + FieldsString(cont_fields))
    else:
        # Обнуление Id реквизита
        reqId = 0

        # Если есть Id компании, то поиск по ФИО
        if company_id is not None:
            # Поиск всех контактов с ФИО
            bitrix.GET(f"crm.contact.list?select[]=*&select[]=PHONE&select[]=EMAIL&"
                       f"filter[LAST_NAME]={name.split()[0]}&"
                       f"filter[NAME]={name.split()[1] if len(name.split()) > 1 else ''}&"
                       f"filter[SECOND_NAME]={' '.join(name.split()[2:]) if len(name.split()) > 2 else ''}")
            contacts = bitrix.result.json()['result']
            # Флаг поиска
            isFind = False
            # Перебор всех контактов
            for cont in contacts:
                # Поиск всех компаний связанных с контактом
                bitrix.GET(f"crm.contact.company.items.get?ID={cont['ID']}")
                companies = bitrix.result.json()['result']

                # Перебор всех компаний
                for company in companies:
                    # Если Id компании совпало с переданным Id
                    if company['COMPANY_ID'] == company_id:
                        # Флаг поиска меняется
                        isFind = True
                        break

                # Проверка флага
                if isFind:
                    # Получение Id контакта
                    contact_id = cont['ID']

                    if cont['EMAIL']:
                        HasCont = False
                        cont_fields['EMAIL'] = list()
                        for emails in cont['EMAIL']:
                            # Тип и значение электронной почты
                            cont_fields['EMAIL'].append(
                                {
                                    'VALUE_TYPE': emails['VALUE_TYPE'],
                                    'VALUE': emails['VALUE']
                                }
                            )
                            if email == emails['VALUE']:
                                HasCont = True

                        if HasCont and email:
                            cont_fields['EMAIL'].append(
                                {
                                    'VALUE_TYPE': "WORK",
                                    'VALUE': email
                                }
                            )

                    if cont['PHONE']:
                        HasCont = False
                        cont_fields['PHONE'] = list()
                        for phones in cont['PHONE']:
                            # Тип и значение электронной почты
                            cont_fields['PHONE'].append(
                                {
                                    'VALUE_TYPE': phones['VALUE_TYPE'],
                                    'VALUE': phones['VALUE']
                                }
                            )
                            if phone == phones['VALUE']:
                                HasCont = True

                        if HasCont and "7405405" not in phone and phone:
                            cont_fields['PHONE'].append(
                                {
                                    'VALUE_TYPE': "WORK",
                                    'VALUE': phone
                                }
                            )

                    # Обновление информации о контакте
                    bitrix.GET(
                        f"crm.contact.update?id={contact_id}&PARAMS[REGISTER_SONET_EVENT]&" + FieldsString(cont_fields))
                    # Остановка перебора компаний
                    break

            # Если контакт не был найден
            if not isFind:
                # Создание контакта
                bitrix.GET(f"crm.contact.add?PARAMS[REGISTER_SONET_EVENT]&" + FieldsString(cont_fields))
                # Id контакта
                contact_id = bitrix.result.json()['result']
        # Если Id компании не был передан
        else:
            # Создание контакта
            bitrix.GET(f"crm.contact.add?PARAMS[REGISTER_SONET_EVENT]&" + FieldsString(cont_fields))
            # Id контакта
            contact_id = bitrix.result.json()['result']

    if contact_id:
        # Если был передан Id компании
        if company_id is not None:
            # Запрос на связывание контакта и компании
            bitrix.GET(f"crm.contact.company.add?id={contact_id}&"
                       f"fields[COMPANY_ID]={company_id}")

        # Словарь с полями реквизита
        req_fields = dict()

        # Тип сущности связанной с реквизитом
        req_fields['ENTITY_TYPE_ID'] = 3
        # Id сущности
        req_fields['ENTITY_ID'] = contact_id
        # Шаблон реквизита
        req_fields['PRESET_ID'] = 5
        # Действительный реквизит
        req_fields['ACTIVE'] = "Y"
        # Название реквизита
        req_fields['NAME'] = "Физ. лицо"
        # ФИО
        req_fields['RQ_NAME'] = name
        # Фамилия
        req_fields['RQ_LAST_NAME'] = name.split()[0]
        if len(name.split()) > 1:
            # Имя
            req_fields['RQ_FIRST_NAME'] = name.split()[1]
            if len(name.split()) > 2:
                # Отчество
                req_fields['RQ_SECOND_NAME'] = ' '.join(name.split()[2:])

        # Если сделка из АЦ
        if ProspectiveSales['SalesChannel'] == -1:
            # ИНН
            req_fields['RQ_INN'] = ProspectiveSales['Organization']['PersonInn']
            # Серия паспорта
            req_fields['RQ_IDENT_DOC_SER'] = ProspectiveSales['Documents']['Serial']
            # Номер паспорта
            req_fields['RQ_IDENT_DOC_NUM'] = ProspectiveSales['Documents']['Number']
            # Дата выдачи паспорта
            req_fields['RQ_IDENT_DOC_DATE'] = ProspectiveSales['Documents']['Date']
            # Код подразделения выдавшего паспорт
            req_fields['RQ_IDENT_DOC_DEP_CODE'] = ProspectiveSales['Documents']['Code']
            # Снилс
            req_fields['UF_CRM_SNILS'] = ProspectiveSales['Documents']['Snils']
        else:
            # ИНН
            req_fields['RQ_INN'] = ProspectiveSales['Organization']['Inn']
            # Id Биллинга
            req_fields['UF_CRM_BILLY'] = ProspectiveSales['Id']

        # Создание реквизита для контакта
        if reqId == 0:
            bitrix.GET(f"crm.requisite.add?" + FieldsString(req_fields))
        else:
            bitrix.GET(f"crm.requisite.update?id={reqId}" + FieldsString(req_fields))

        # Обновление данных в БД
        # cur.Upd(
        #     True,
        #     "clients",
        #     f"ReqId = {bitrix.result.json()['result']}",
        #     f"guid = '{ProspectiveSales['Id']}'"
        # )

    return contact_id


def CreateComments(ProspectiveSale):
    """
    Метод на создание комментариев по сделке
    :param dict ProspectiveSale: Потенциальная продажа
    :return:
    """
    # Поиск сделки по Id ПП
    bitrix.GET(f"crm.deal.list?select[]=*&select[]=UF_*&filter[ORIGIN_ID]={ProspectiveSale['Id']}")
    # Сделка из битрикса
    deal = bitrix.result.json()['result'][0]

    if ProspectiveSale['SalesChannel'] != -1:
        # Отсортированный список комментариев из ПП
        comments = sorted(ProspectiveSale['Comments']['PartnerComments'], key=lambda x: x['Date'])

        # Дата последнего комментария
        dateCom = deal['UF_CRM_LAST_PARTNERCOMMENT'][:-6]
        # Номер комментария
        id = next((x for x in comments if x['Date'] == dateCom), None)
        id = comments.index(id) + 1 if id else 0
        # Перебор комментариев
        for com in comments[id:]:
            # Автор комментария
            name = com['Author'].split()
            # Поиск пользователя по ФИО
            bitrix.GET(
                f"user.get?"
                f"filter[LAST_NAME]={name[0]}&"
                f"filter[NAME]={name[1]}&"
                f"filter[SECOND_NAME]={name[2]}"
            )

            # Id автора комментария
            if len(bitrix.result.json()['result']):
                author_id = bitrix.result.json()['result'][0]['ID']
            else:
                author_id = 1
            # Поля комментария
            fields = dict()
            # Id автора комментария
            fields["AUTHOR_ID"] = author_id
            # Комментарий
            fields["COMMENT"] = f"[TABLE][TR][TD][B][COLOR=blue]Билли[/COLOR]: [/B][I]" \
                                f"{com['Date'].replace('T', ' ')}[/I][/TD][/TR]" \
                                f"[TR][TD]{com['Text']}[/TD][/TR][/TABLE]"
            # Тип сущности куда пишется комментарий
            fields["ENTITY_TYPE"] = "deal"
            # Id сделки
            fields["ENTITY_ID"] = deal['ID']
            # Запрос создания комментария
            bitrix.GET("crm.timeline.comment.add?" + FieldsString(fields))
            # Запрос на обновление метки времени последнего комментария
            bitrix.GET(f"crm.deal.update?id={deal['ID']}&fields[UF_CRM_LAST_PARTNERCOMMENT]={com['Date']}")
    else:
        if ProspectiveSale['Comments']['Text']:
            # Получение все комментарии по сделке
            bitrix.GET(f"crm.timeline.comment.list?filter[ENTITY_ID]={deal['ID']}&filter[ENTITY_TYPE]=deal")
            # Флаг поиска
            isFind = False
            # Перебор всех комментариев
            for comments in bitrix.result.json()['result']:
                # Если текст комментария совпадает с существующими комментариями
                if ProspectiveSale['Comments']['Text'] in comments['COMMENT']:
                    isFind = True
                    break
            # Если комментарий не найден
            if not isFind:
                # Поля для запроса на создание комментария
                fields = {
                    "AUTHOR_ID": 1,
                    "COMMENT": f"[TABLE][TR][TD][B][COLOR=blue]АЦ УЦ[/COLOR]:[/B][/TD][/TR]"
                               f"[TR][TD]{ProspectiveSale['Comments']['Text']}[/TD][/TR][/TABLE]",
                    "ENTITY_TYPE": "deal",
                    "ENTITY_ID": deal['ID']
                }
                # Запрос создания комментария
                bitrix.GET("crm.timeline.comment.add?" + FieldsString(fields))


# Экземпляр класса DAL для работы с БД
cur = DAL()
isBreak = False
isConnection = False
start_time = datetime.datetime.now().strftime("%d %b - %H:%M:%S")
addInf = ""

if cur:
    # Экземпляры классов API
    resp_api = ApiBilly()
    bitrix = ApiBitrix()
    external = ApiExternal()

    # Действия доступные пользователю
    action = {
        1: "Проверить новости",
        2: "Проверить закрытые продажи",
        3: "Создать физ лица",
        4: "Удаление и обновление реквизитов в битриксе",
        5: "Удаление дублирующихся реквизитов у компаний",
        6: "Создание или обновление 1 сделки",
        7: "Разбор АЦ УЦ",
        8: "Разбор АЦ УЦ по конкретной дате",
        9: "Выход"
    }
    # Список параметров при запуске программы
    parameters = sys.argv[1:]
    # Строка с действиями
    s_action = '\n'.join([f"{key}) {value}" for key, value in action.items()]) + '\n'

    # Если при запуске программы не были переданы параметры
    if not parameters:
        while True:
            # Выбор действия
            parameters.append(input(s_action))
            # Если значение целое число и в промежутке от 1 до 5, то программа идет дальше
            if parameters[0].isdigit():
                if 0 < int(parameters[0]) < len(action.keys()) + 1:
                    break
            print(f"Значение <{parameters.pop()}> не корректно, попробуйте ввести значение заново\n")

    # Перебор всех действий
    for i in range(len(parameters)):
        print()
        # Если значение не целое число, то требуем корректировки значения
        if not parameters[i].isdigit():
            print(f"Значение <{parameters[i]}> не корректно, попробуйте ввести значение заново\n")
            while True:
                parameters[i] = input(s_action)
                # Если значение целое число и в промежутке от 1 до 5, то программа идет дальше
                if parameters[i].isdigit():
                    if 0 < int(parameters[i]) < len(action.keys()) + 1:
                        break

        # Номер действия
        ch = int(parameters[i])
        print(f"Вы выбрали {action[ch]}")

        try:
            # Выборка ПП
            if ch == 1:
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
                        for fields in News['News']:
                            # Получение основных данных ПП
                            # params = {
                            #     "idPs": fields['Id'],
                            #     "VersPs": fields['Version'],
                            #     "TypePs": fields['Type'],
                            #     "InnOrg": fields['Organization']['Inn'],
                            #     "KppOrg": fields['Organization']['Kpp'],
                            #     "IdOrg": fields['Organization']['ClientId'],
                            #     "TypeOrg": fields['Organization']['Type']
                            # }
                            #
                            # # Замена пустых значений на Null
                            # for key in params:
                            #     if not params[key]:
                            #         params[key] = 'Null'

                            # Проверка наличия ИНН
                            if fields['Organization']['Inn']:

                                '''
                                if params['idPs']:
                                    if resp_api.ClientsFind(params['InnOrg'], params['KppOrg'], params['TypeOrg']):
                                        params['idPs'] = resp_api.result.json()['ClientId']
                                
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
                                '''

                                # Если тип ПП Подключение, Продление или Допродажа
                                if fields['Type'] < 4:
                                    # То создание сделки
                                    BillyToBitrix(fields)
                                    # Счетчик сделок
                                    j += 1
                                    print(f"Обработано {j} новостей")
                        # Обновление параметра временной метки запроса
                        resp_api.UpdateTimeStamp(News['NextTimestamp'])
                        # Проверка на наличие новых данных
                        if not News['HasMore']:
                            break
                    else:
                        print(f'Error: {resp_api.result.status_code} code')
                        break

                # Вывод сообщения с последней меткой
                print("Последняя метка", resp_api.REQ_PARAMS['from'])
                end_time = datetime.datetime.now().strftime('%d %b - %H:%M:%S')
                print(f"Время окончания {end_time}")

                f = open(os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') + "\\Logs.txt", "a")
                f.write(
                    f"start: {start_time:20}\t| end: {end_time:20}\t| lasttimestamp: {resp_api.REQ_PARAMS['from']}\n")
                f.close()

                '''
                # Объединение таблиц чтобы в таблице клиенты всегда были актуальные данные
                cur.EXECUTE("INSERT INTO clients (guid, inn, kpp, ClientType)"
                            "SELECT p.IdOrganization, p.INN, p.KPP, p.ClientType "
                            "FROM Clients c RIGHT JOIN prospectivesales p ON c.guid = p.IdOrganization "
                            "WHERE c.guid IS NULL AND NOT p.IdOrganization IS null")
                print(f"На данный момент количество организаций в БД: {cur.row_count('Clients')}")
                '''

            # Проверка статусов ПП
            elif ch == 2:
                completed = []  # Список завершенных ПП
                # Перебор всех записей таблицы
                for row in cur.SELECT_ALL("prospectivesales"):
                    resp_api.GET(resp_api.methods['Find'].replace("{id}", row[0]))  # Поиск информации о ПП
                    # Проверка статуса ПП
                    if resp_api.result.json()['Status']['State'] == 2:
                        completed.append(row[0])
                    # Проверка канала продажи
                    if resp_api.result.json()['SalesChannel'] == 2:
                        # Проверка количества счетов
                        if len(resp_api.result.json()['Bills']):
                            isPaid = True  # Флаг оплачено
                            # Перебор всех счетов
                            for bill in resp_api.result.json()['Bills']:
                                isPaid = isPaid and (bill['State'] == 1 or bill['State'] == 3)  # Проверка статуса счета
                            # Проверка оплаты счета
                            if isPaid:
                                # Список этапов
                                stages = resp_api.result.json()['Stages']
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
                            # Выбираем ПП по id организации GUID == idOrganization
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
                        next_id = bitrix.result.json()['next']
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
                    for row in res['result']:
                        # Получение информации из да-даты
                        dadata = bitrix.GetInfo(row['RQ_INN'])
                        # Обновление данных реквизита
                        bitrix.GET(f"crm.requisite.update?id={row['ID']}&"
                                   f"fields[RQ_KPP]={dadata['suggestions'][0]['data']['kpp']}&"
                                   f"fields[RQ_OGRN]={dadata['suggestions'][0]['data']['ogrn']}")

                        print(f"Организации {row['RQ_INN']} добавлены КПП и ОГРН")
                    # Проверка на наличие новых реквизитов
                    if "next" in res:
                        next_id = res['next']
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
                        if row['ENTITY_ID'] in EntityID:
                            # Удаляем дубликаты из битрикса
                            bitrix.GET(f"crm.requisite.delete?id={row['ID']}")
                            print(f"Удален реквизит с id {row['ID']}")
                        else:
                            # Добавляем номер сущности в список
                            EntityID.append(row['ENTITY_ID'])
                            # Проверка наличия номера реквизита у клиента
                            if not cur.row_count(f"SELECT * FROM clients "
                                                 f"WHERE Inn={row['RQ_INN']} and Kpp={row['RQ_KPP']}", False):
                                # Обновление данных БД
                                upd_fields = {
                                    "ReqId": row['ID'],
                                }
                                cur.Upd(True, "clients", upd_fields, f"Inn={row['RQ_INN']} and Kpp={row['RQ_KPP']}")
                                print(f"{row['RQ_INN']} добавлена в БД")
                    # Проверка на наличие реквизитов
                    if "next" in res:
                        next_id = res['next']
                    else:
                        break

            # Создание сделки
            elif ch == 6:
                # Код ПП
                codpp = input("Введите код ПП")
                # Получение ПП
                ProspectiveSales = resp_api.GET(resp_api.methods['Find'].replace("{id}", codpp)).json()
                # Если ПП найдена, то создание сделки
                if "errors" not in ProspectiveSales:
                    BillyToBitrix(ProspectiveSales)
                else:
                    print("Ничего не найдено")

            # Разбор заявок АЦ
            elif ch == 7:
                # Начало разбора заявок
                start = datetime.datetime.strptime("07.07.2021", "%d.%m.%Y")
                # Дельта времени
                days = datetime.timedelta(days=0)
                # Дата поиска
                createDate = (start + days).date()

                # Получение продуктов АЦ
                external.POST("products")
                products = external.result.json()['products']
                products += [
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
                # Счетчик
                i = 0
                # Перебор дат от начала до сегодняшнего дня
                while createDate <= datetime.date.today():
                    # Получение заявок по дате
                    external.POST("request/list",
                                  {
                                      "filter":
                                          {
                                              "createdate": datetime.datetime.strftime(createDate, "%d.%m.%Y")
                                          }
                                  }
                                  )
                    # Проверка на наличие данных
                    if external.result.json()['info']:
                        # print(createDate.strftime("%d.%m.%Y"), external.result.json(), sep='\n')
                        # Перебор всех заявок
                        for row in external.result.json()['info']:
                            # Преобразование номера телефона к строке
                            row['phone'] = str(row['phone'])
                            # Приведение заявки к виду ПП
                            ProspectiveSale = {
                                "Id": row['requestId'],
                                "Organization":
                                    {
                                        # Инн организации
                                        "Inn": row['inn'],
                                        # КПП организации
                                        "Kpp": row['kpp'],
                                        # Тип организации
                                        "Type": 3 if row['type'] == 1 else 1 if row['type'] == 3 else 2,
                                        # Имя организации
                                        "Name": row['company'] if row['company'] else
                                        f"{row['lastName']} {row['firstName']} {row['middleName']}",
                                        # Id клиента
                                        'ClientId': None,
                                        # Имя заявителя
                                        "ClaimantName": f"{row['lastName']} {row['firstName']} {row['middleName']}",
                                        # Инн заявителя
                                        "PersonInn": row['personInn'] if row['personInn'] else row['inn'],
                                        # Имя директора
                                        "Director":
                                            f"{row['headLastName']} {row['headFirstName']} {row['headMiddleName']}"
                                            if row['headLastName'] else
                                            f"{row['lastName']} {row['firstName']} {row['middleName']}"
                                    },
                                "Product":
                                    [
                                        {
                                            "Id": i,
                                            "Name": next((x for x in products if x['id'] == i), None)['name']
                                        } for i in row['products']
                                    ],
                                "SalesChannel": -1,
                                "Manager": "0680",
                                "Bills":
                                    [
                                        {
                                            "Amount":
                                                next((x for x in products if x['id'] == i), None)['price'][
                                                    'fl' if row['type'] == 1 else "ip" if row['type'] == 2 else "ur"],
                                            "State": 1
                                        } for i in row['products']
                                    ],
                                "Brief": None,
                                "Comments":
                                    {
                                        "Text": row['comment']
                                    },
                                "Type": 1,
                                "CreateTime": row['createDate'],
                                "Partner":
                                    {
                                        "Code": None
                                    },
                                "Source":
                                    {
                                        "Name": None
                                    },
                                "Supplier":
                                    {
                                        "PartnerCode": None,
                                    },
                                "Temperature": 0,
                                "Status":
                                    {
                                        "State": row['statusId'],
                                        "PostponedToDate": datetime.datetime.today()
                                    },
                                "Contacts":
                                    [
                                        {
                                            "Emails":
                                                [
                                                    {
                                                        "Address": row['email']
                                                    }
                                                ],
                                            "Phones":
                                                [
                                                    {
                                                        "Number": ("8" if len(row['phone']) else "") + row['phone'],
                                                        "AdditionalNumber": None
                                                    }
                                                ]
                                        }
                                    ],
                                "Stages": row['statusId'],
                                "Documents":
                                    {
                                        "Serial": row['passportSerial'],
                                        "Number": row['passportNumber'],
                                        "Date": row['passportDate'],
                                        "Code": row['passportCode'],
                                        "Birthdate": row['birthDate'],
                                        "Gender": f"HNR_RU_{1 if row['gender'] == 'M' else 2}",
                                        "Snils": row['snils'],
                                        "Ogrn": row['ogrn'] if row['ogrn'] else "",
                                    }
                            }
                            # Создание сделки
                            BillyToBitrix(ProspectiveSale)
                            i += 1
                            print(f"Обработано сделок {i}", row['requestId'])
                    days = datetime.timedelta(days=days.days + 1)
                    createDate = (start + days).date()

            elif ch == 8:
                i = 0
                # Получение продуктов АЦ
                external.POST("products")
                products = external.result.json()['products']
                products += [
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
                # Получение заявок по дате
                external.POST("request/list",
                              {
                                  "filter":
                                      {
                                          "createdate": input("Введите дату\n")
                                      }
                              }
                              )
                # Проверка на наличие данных
                if external.result.json()['info']:
                    # print(createDate.strftime("%d.%m.%Y"), external.result.json(), sep='\n')
                    # Перебор всех заявок
                    for row in external.result.json()['info']:
                        # Преобразование номера телефона к строке
                        row['phone'] = str(row['phone'])
                        # Приведение заявки к виду ПП
                        ProspectiveSale = {
                            "Id": row['requestId'],
                            "Organization":
                                {
                                    # Инн организации
                                    "Inn": row['inn'],
                                    # КПП организации
                                    "Kpp": row['kpp'],
                                    # Тип организации
                                    "Type": 3 if row['type'] == 1 else 1 if row['type'] == 3 else 2,
                                    # Имя организации
                                    "Name": row['company'] if row['company'] else
                                    f"{row['lastName']} {row['firstName']} {row['middleName']}",
                                    # Id клиента
                                    'ClientId': None,
                                    # Имя заявителя
                                    "ClaimantName": f"{row['lastName']} {row['firstName']} {row['middleName']}",
                                    # Инн заявителя
                                    "PersonInn": row['personInn'] if row['personInn'] else row['inn'],
                                    # Имя директора
                                    "Director":
                                        f"{row['headLastName']} {row['headFirstName']} {row['headMiddleName']}"
                                        if row['headLastName'] else
                                        f"{row['lastName']} {row['firstName']} {row['middleName']}"
                                },
                            "Product":
                                [
                                    {
                                        "Id": i,
                                        "Name": next((x for x in products if x['id'] == i), None)['name']
                                    } for i in row['products']
                                ],
                            "SalesChannel": -1,
                            "Manager": "0680",
                            "Bills":
                                [
                                    {
                                        "Amount":
                                            next((x for x in products if x['id'] == i), None)['price'][
                                                'fl' if row['type'] == 1 else "ip" if row['type'] == 2 else "ur"],
                                        "State": 1
                                    } for i in row['products']
                                ],
                            "Brief": None,
                            "Comments":
                                {
                                    "SourceComment": None,

                                },
                            "Type": 1,
                            "CreateTime": row['createDate'],
                            "Partner":
                                {
                                    "Code": None
                                },
                            "Source":
                                {
                                    "Name": None
                                },
                            "Supplier":
                                {
                                    "PartnerCode": None,
                                },
                            "Temperature": 0,
                            "Status":
                                {
                                    "State": row['statusId'],
                                    "PostponedToDate": datetime.datetime.today()
                                },
                            "Contacts":
                                [
                                    {
                                        "Emails":
                                            [
                                                {
                                                    "Address": row['email']
                                                }
                                            ],
                                        "Phones":
                                            [
                                                {
                                                    "Number": ("8" if len(row['phone']) else "") + row['phone'],
                                                    "AdditionalNumber": None
                                                }
                                            ]
                                    }
                                ],
                            "Stages": row['statusId'],
                            "Documents":
                                {
                                    "Serial": row['passportSerial'],
                                    "Number": row['passportNumber'],
                                    "Date": row['passportDate'],
                                    "Code": row['passportCode'],
                                    "Birthdate": row['birthDate'],
                                    "Gender": f"HNR_RU_{1 if row['gender'] == 'M' else 2}",
                                    "Snils": row['snils'],
                                    "Ogrn": row['ogrn'] if row['ogrn'] else "",
                                }
                        }
                        # Создание сделки
                        BillyToBitrix(ProspectiveSale)
                        i += 1
                        print("Обработано", i, ProspectiveSale['Id'])
            elif ch == 9:
                break
            else:
                print("Такого варианта нет")
        except ConnectionError:
            print("Connection Error", ConnectionError)
            f = open(os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') + "\\Logs.txt", "a")
            f.write(f"Exit with Connection Error: {datetime.datetime.now()}\t{ConnectionError}\n")
            f.close()
            isBreak = True
            isConnection = True
            addInf = traceback.format_exc()
        except Exception:
            print("Something is wrong ", Exception)
            f = open(os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop') + "\\Logs.txt", "a")
            f.write(f"Exit with Error: {datetime.datetime.now()}\t{Exception}\n")
            f.close()
            isBreak = True
            addInf = Exception
        finally:
            logs_field = dict()
            logs_field['DateStart'] = start_time
            logs_field['DateStop'] = datetime.datetime.strftime(datetime.datetime.now(), "%d %b - %H:%M:%S")
            logs_field['EndResult'] = isBreak
            logs_field['AdditionalInfo'] = addInf
            cur.Upd(False, "Logs", logs_field)

print("Bye-Bye")
time.sleep(2)
