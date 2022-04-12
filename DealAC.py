import sys
import requests
import os
import urllib.parse
import cgitb
import cgi
import time
import datetime

from API import ApiBitrix, ApiExternal


def print_json(js, ch=""):
    if isinstance(js, dict):
        print(f"{ch}{'{'}")
        for key, val in js.items():
            if isinstance(val, dict):
                print(f"{ch}\t{key} :")
                print_json(val, "\t" + ch)
            elif isinstance(val, list):
                print(f"{ch}\t{key} :")
                print(f"{ch}\t[")
                for v in val:
                    print_json(v, "\t" + ch)
                print(f"{ch}\t]")
            else:
                print(f"{ch}\t{key} : {val};")
        print(f"{ch}{'}'}")
    else:
        print(f"{ch}\t{js};")


bitrix = ApiBitrix()
external = ApiExternal()
"""
cgitb.enable()

print("Content-Type: text/html; charset=Windows-1251")
print("")
print("<!DOCTYPE html><html><head></head><body>")

form = cgi.FieldStorage()
id = form.getvalue("DealId")
"""
# ИП
id = 13407

# ФЛ
id = 12455

# ЮЛ
id = 12317
mes = ""
try:
    id = int(input("Введите номер сделки в битрикс: "))
    deal = bitrix.GET(f"crm.deal.get?id={id}").json()['result']
    extDeal = dict()

    if deal['COMPANY_ID'] != "0":
        company = bitrix.GET(f"crm.company.get?id={deal['COMPANY_ID']}").json()['result']

        # ИНН директора компании
        req = bitrix.GET(f"crm.requisite.list?"
                         f"filter[ENTITY_ID]={company['ID']}&"
                         f"filter[ENTITY_TYPE_ID]=4").json()['result'][0]
        res = bitrix.GetInfo(req['RQ_INN']).json()['suggestions'][0]
        # Тип организации
        extDeal['type'] = 3 if req['PRESET_ID'] == '1' else 2

        # Название организации
        extDeal['company'] = res['value']
        if extDeal['type'] == 2:
            # Фамилия руководителя ИП
            extDeal['headLastName'] = res['data']['fio']['surname']
            # Имя руководителя ИП
            extDeal['headFirstName'] = res['data']['fio']['name']
            # Отчество руководителя ИП
            extDeal['headMiddleName'] = res['data']['fio']['patronymic']
            # Должность руководителя ИП
            extDeal['headPosition'] = 'Индивидуальный предприниматель'

        else:
            fio = res['data']['management']['name']
            # Фамилия руководителя ЮЛ
            extDeal['headLastName'] = fio.split()[0]
            # Имя руководителя ЮЛ
            extDeal['headFirstName'] = fio.split()[1] if len(fio.split()) > 1 else ""
            # Отчество руководителя ЮЛ
            extDeal['headMiddleName'] = " ".join(fio.split()[2:]) if len(fio.split()) > 2 else ""
            # Должность руководителя ЮЛ
            extDeal['headPosition'] = res['data']['management']['post']
            # Номер телефона компании
            extDeal['companyPhone'] = company['PHONE'][0]['VALUE'][-10:]

        # ИНН организации
        extDeal['inn'] = req['RQ_INN']
        # ОГРН организации
        extDeal['ogrn'] = req['RQ_OGRN'] if extDeal['type'] == 3 else req['RQ_OGRNIP']
        # external['ogrnip'] = req['RQ_OGRNIP'] if external['type'] == 2 else None
        # КПП организации
        extDeal['kpp'] = req['RQ_KPP'] if extDeal['type'] == 3 else ""
        address = bitrix.GET(f"crm.address.list?"
                             f"filter[ENTITY_TYPE_ID]=8&"
                             f"filter[TYPE_ID]=6&"
                             f"filter[ENTITY_ID]={req['ID']}").json()['result'][0]

        if extDeal['type'] == 2:
            # Регион компании
            extDeal['region'] = extDeal['ogrn'][3:5]
            # Город компании
            extDeal['city'] = address['CITY'] if address['CITY'] else address['PROVINCE']
            # Адрес компании
            extDeal['address'] = f"{address['ADDRESS_1']}" \
                                  f"{(', ' + address['ADDRESS_2']) if address['ADDRESS_2'] else ''}".strip()
        else:
            # Юридический регион компании
            extDeal['regionLaw'] = extDeal['ogrn'][3:5] if extDeal['type'] == 3 else extDeal['ogrnip'][3:5]
            # Юридический город компании
            extDeal['cityLaw'] = address['CITY'] if address['CITY'] else address['PROVINCE']
            # Юридический адрес компании
            extDeal['addressLaw'] = f"{address['ADDRESS_1']}" \
                                     f"{(', ' + address['ADDRESS_2']) if address['ADDRESS_2'] else ''}".strip()
            extDeal['index'] = address['POSTAL_CODE']
    else:
        extDeal['type'] = 1
        extDeal['address'] = ""
        extDeal['region'] = 29

    contacts = bitrix.GET(f"crm.deal.contact.items.get?id={id}").json()['result']

    if len(contacts) > 2:
        raise Exception("Количество контактов не может быть больше 2: Заявитель и контактное лицо")

    # Проверка контактов ИП
    if extDeal['type'] == 2:
        # Получаем первый контакт
        c = bitrix.GET(f"crm.contact.get?id={contacts[0]['CONTACT_ID']}").json()['result']
        # Проверяем является ли он директором
        if c['NAME'] != extDeal['headFirstName'] or c['LAST_NAME'] != extDeal['headLastName'] or c['SECOND_NAME'] != \
                extDeal['headMiddleName']:
            # Если контакт не ИП, то переставить контакты местами
            bitrix.GET(f"crm.deal.contact.items.set?id={id}&"
                       f"ITEMS[0][CONTACT_ID]={contacts[1]['CONTACT_ID']}&"
                       f"ITEMS[0][IS_PRIMARY]=Y&"
                       f"ITEMS[0][SORT]=10&"
                       f"ITEMS[1][CONTACT_ID]={contacts[0]['CONTACT_ID']}&"
                       f"ITEMS[1][IS_PRIMARY]=N&"
                       f"ITEMS[1][SORT]=20")
            contacts = bitrix.GET(f"crm.deal.contact.items.get?id={id}").json()['result']
    i = 0
    for cont in contacts:
        contact = bitrix.GET(f"crm.contact.get?id={cont['CONTACT_ID']}").json()['result']
        if contact['HONORIFIC'] and i == 0:
            bitrix.GET(f"crm.requisite.list?"
                       f"filter[ENTITY_ID]={contact['ID']}&"
                       f"filter[ENTITY_TYPE_ID]=3")
            if len(bitrix.result.json()['result']) == 0:
                raise Exception(
                    f"У контакта {contact['LAST_NAME']} {contact['NAME']} {contact['SECOND_NAME']} отсутствует реквизит")
            req = bitrix.result.json()['result'][0]
            if extDeal['type'] == 2:
                if contact['NAME'] != extDeal['headFirstName'] or contact['LAST_NAME'] != extDeal['headLastName'] or \
                        contact['SECOND_NAME'] != extDeal['headMiddleName']:
                    raise Exception("В сделке с ИП заявителем может быть только директор")

            # Первый контакт в сделке это заявитель
            if i == 0:
                # Имя заявителя
                extDeal['firstName'] = contact['NAME']
                # Фамилия заявителя
                extDeal['lastName'] = contact['LAST_NAME']
                # Отчество заявителя
                extDeal['middleName'] = contact['SECOND_NAME']
                # ИНН заявителя
                extDeal['personInn'] = req['RQ_INN']
                # Серия паспорта
                extDeal['passportSerial'] = req['RQ_IDENT_DOC_SER']
                # Номер паспорта
                extDeal['passportNumber'] = req['RQ_IDENT_DOC_NUM']
                # Дата выдачи паспорта
                extDeal['passportDate'] = req['RQ_IDENT_DOC_DATE']
                # Код подразделения
                extDeal['passportCode'] = req['RQ_IDENT_DOC_DEP_CODE']
                # Кто выдал паспорт
                extDeal['passportDivision'] = ""
                # Пол
                extDeal['gender'] = "M" if "1" in contact['HONORIFIC'] else "F"
                if contact['BIRTHDATE']:
                    # Дата рождения
                    extDeal['birthDate'] = datetime.datetime.strftime(
                        datetime.datetime.strptime(contact['BIRTHDATE'][:contact['BIRTHDATE'].index("T")], "%Y-%m-%d"),
                        "%d.%m.%Y"
                    )
                else:
                    raise Exception("Укажите дату рождения")
                # СНИЛС заявителя
                extDeal['snils'] = req['UF_CRM_SNILS'] if req['UF_CRM_SNILS'] else '00000000000'
                if extDeal['type'] != 1:
                    # Если ФИО заявителя совпадает с ФИО директора
                    if extDeal['headLastName'] == extDeal['lastName'] and \
                            extDeal['headFirstName'] == extDeal['firstName'] and \
                            extDeal['middleName'] == extDeal['headMiddleName']:
                        extDeal['position'] = extDeal['headPosition']
                    else:
                        # Должность заявителя
                        extDeal['position'] = contact['POST']

                # Есть присоединение к оферте
                extDeal['offerJoining'] = True
                # Способ идентификации 0 - Лично, 1 - по ЭЦП
                extDeal['identificationKind'] = 0

            i += 1
            # Проверка, что контакт последний
            if len(contacts) == i:
                if contact['EMAIL']:
                    # Email контакта
                    extDeal['email'] = contact['EMAIL'][0]['VALUE']
                else:
                    raise Exception("Укажите email у контактного лица")

                if contact['PHONE']:
                    # Телефон контакта
                    extDeal['phone'] = contact['PHONE'][0]['VALUE'][-10:]
                else:
                    raise Exception("Укажите номер телефона у контактного лица")
        else:
            raise Exception(f"Укажите обращение к контакту: "
                            f"{contact['LAST_NAME']} {contact['NAME']} {contact['SECOND_NAME']}")
    extDeal['products'] = [3344]

    for key in extDeal:
        if extDeal[key] is None:
            extDeal[key] = ""

    extDeal = {"info": extDeal}
    # print_json(external)
    external.POST("request/create", extDeal)
    if external.result.status_code == 200:
        fio = f"{extDeal['info']['lastName']} {extDeal['info']['firstName']} {extDeal['info']['middleName']}"
        title = f"АЦ-{external.result.json()['requestId']} / " \
                f"{extDeal['info']['company'] if extDeal['info']['type'] > 1 else fio}"
        mes = f"[TABLE][TR][TD][B][COLOR=blue]Заявка успешно создана[/COLOR]: [/B][/TD][/TR]" \
              f"[TR][TD]Номер заявки {external.result.json()['result']}[/TD][/TR][/TABLE]"
        bitrix.GET(f"crm.deal.update?"
                   f"id={id}&"
                   f"fields[TITLE]={title}&"
                   f"fields[ORIGINATOR_ID]=АЦ&"
                   f"fields[ORIGIN_ID]={external.result.json()['requestId']}")
    else:
        mes = f"[TABLE][TR][TD][B][COLOR=blue]Ошибка создания[/COLOR]: [/B][/TD][/TR]" \
              f"[TR][TD]{external.result.text}[/TD][/TR][/TABLE]"
    # print("</body></html>")
except Exception as e:
    print(e.args[0])
    mes = f"[TABLE][TR][TD][B][COLOR=blue]Ошибка создания[/COLOR]: [/B][/TD][/TR]" \
          f"[TR][TD]Проверьте заполненность и корректность всех данных[/TD][/TR]" \
          f"[TR][TD][B][COLOR=blue]Обратите внимание! [/COLOR][/B]{e.args[0]}[/TD][/TR][/TABLE]"
finally:
    fields = dict()
    # Id автора комментария
    fields["AUTHOR_ID"] = 1
    # Комментарий
    fields["COMMENT"] = mes
    # Тип сущности куда пишется комментарий
    fields["ENTITY_TYPE"] = "deal"
    # Id сделки
    fields["ENTITY_ID"] = id
    # Запрос создания комментария
    bitrix.GET("crm.timeline.comment.add?" + "&".join([f"fields[{key}]={value}" for key, value in fields.items()]))
