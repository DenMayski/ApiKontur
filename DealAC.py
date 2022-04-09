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
ext = ApiExternal()
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
    id = 13471
    deal = bitrix.GET(f"crm.deal.get?id={id}").json()['result']
    external = dict()

    # for d in deal:
    #     print(f"{d} : {deal[d]}")
    # print("\n")

    if deal['COMPANY_ID'] != "0":
        company = bitrix.GET(f"crm.company.get?id={deal['COMPANY_ID']}").json()['result']
        # ИНН директора компании
        req = bitrix.GET(f"crm.requisite.list?"
                         f"filter[ENTITY_ID]={company['ID']}&"
                         f"filter[ENTITY_TYPE_ID]=4").json()['result'][0]
        res = bitrix.GetInfo(req['RQ_INN']).json()['suggestions'][0]
        # print_json(res)

        # Тип организации
        external['type'] = 3 if req['PRESET_ID'] == '1' else 2

        # Название организации
        external['company'] = res['value']
        if external['type'] == 2:
            # Фамилия руководителя ИП
            external['headLastName'] = res['data']['fio']['surname']
            # Имя руководителя ИП
            external['headFirstName'] = res['data']['fio']['name']
            # Отчество руководителя ИП
            external['headMiddleName'] = res['data']['fio']['patronymic']
            # Должность руководителя ИП
            external['headPosition'] = 'Индивидуальный предприниматель'
        else:
            fio = res['data']['management']['name']
            # Фамилия руководителя ЮЛ
            external['headLastName'] = fio.split()[0]
            # Имя руководителя ЮЛ
            external['headFirstName'] = fio.split()[1] if len(fio.split()) > 1 else ""
            # Отчество руководителя ЮЛ
            external['headMiddleName'] = " ".join(fio.split()[2:]) if len(fio.split()) > 2 else ""
            # Должность руководителя ЮЛ
            external['headPosition'] = res['data']['management']['post']

        # ИНН организации
        external['inn'] = req['RQ_INN']
        # ОГРН организации
        external['ogrn'] = req['RQ_OGRN'] if external['type'] == 3 else None
        external['ogrnip'] = req['RQ_OGRNIP'] if external['type'] == 2 else None
        # КПП организации
        external['kpp'] = req['RQ_KPP'] if external['type'] == 3 else None
        address = bitrix.GET(f"crm.address.list?"
                             f"filter[ENTITY_TYPE_ID]=8&"
                             f"filter[TYPE_ID]=6&"
                             f"filter[ENTITY_ID]={req['ID']}").json()['result'][0]

        if external['type'] == 2:
            # Регион компании
            external['region'] = external['ogrn'][3:5] if external['type'] == 3 else external['ogrnip'][3:5]
            # Город компании
            external['city'] = address['CITY'] if address['CITY'] else address['PROVINCE']
            # Адрес компании
            external['address'] = f"{address['ADDRESS_1']}" \
                                  f"{(', ' + address['ADDRESS_2']) if address['ADDRESS_2'] else ''}".strip()
        else:
            # Юридический регион компании
            external['regionLaw'] = external['ogrn'][3:5] if external['type'] == 3 else external['ogrnip'][3:5]
            # Юридический город компании
            external['cityLaw'] = address['CITY'] if address['CITY'] else address['PROVINCE']
            # Юридический адрес компании
            external['addressLaw'] = f"{address['ADDRESS_1']}" \
                                     f"{(', ' + address['ADDRESS_2']) if address['ADDRESS_2'] else ''}".strip()
    else:
        external['type'] = 1
        external['address'] = ""
        external['region'] = 29

    contacts = bitrix.GET(f"crm.deal.contact.items.get?id={id}").json()['result']

    if len(contacts) > 2:
        raise Exception("Количество контактов не может быть больше 2: Заявитель и контактное лицо")

    # Проверка контактов ИП
    if external['type'] == 2:
        # Получаем первый контакт
        c = bitrix.GET(f"crm.contact.get?id={contacts[0]['CONTACT_ID']}").json()['result']
        # Проверяем является ли он директором
        if c['NAME'] != external['headFirstName'] or c['LAST_NAME'] != external['headLastName'] or c['SECOND_NAME'] != \
                external['headMiddleName']:
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
        if contact['HONORIFIC']:
            req = bitrix.GET(f"crm.requisite.list?"
                             f"filter[ENTITY_ID]={contact['ID']}&"
                             f"filter[ENTITY_TYPE_ID]=3").json()['result'][0]
            if external['type'] == 2:
                if contact['NAME'] != external['headFirstName'] or contact['LAST_NAME'] != external['headLastName'] or \
                        contact['middleName'] != contact['SECOND_NAME']:
                    raise Exception("В сделке с ИП заявителем может быть только директор")

            # Первый контакт в сделке это заявитель
            if i == 0:
                # Имя заявителя
                external['firstName'] = contact['NAME']
                # Фамилия заявителя
                external['lastName'] = contact['LAST_NAME']
                # Отчество заявителя
                external['middleName'] = contact['SECOND_NAME']
                # ИНН заявителя
                external['personInn'] = req['RQ_INN']
                # Серия паспорта
                external['passportSerial'] = req['RQ_IDENT_DOC_SER']
                # Номер паспорта
                external['passportNumber'] = req['RQ_IDENT_DOC_NUM']
                # Дата выдачи паспорта
                external['passportDate'] = req['RQ_IDENT_DOC_DATE']
                # Код подразделения
                external['passportCode'] = req['RQ_IDENT_DOC_DEP_CODE']
                # Кто выдал паспорт
                external['passportDivision'] = ""
                # Пол
                external['gender'] = "M" if "1" in contact['HONORIFIC'] else "F"
                # Дата рождения
                external['birthDate'] = datetime.datetime.strftime(
                    datetime.datetime.strptime(contact['BIRTHDATE'][:contact['BIRTHDATE'].index("T")], "%Y-%m-%d"),
                    "%d.%m.%Y"
                )
                # СНИЛС заявителя
                external['snils'] = req['UF_CRM_SNILS'] if req['UF_CRM_SNILS'] else '11111111111'
                if external['type'] != 1:
                    # Если ФИО заявителя совпадает с ФИО директора
                    if external['headLastName'] == external['lastName'] and \
                            external['headFirstName'] == external['firstName'] and \
                            external['middleName'] == external['headMiddleName']:
                        external['position'] = external['headPosition']
                    else:
                        # Должность заявителя
                        external['position'] = contact['POST']

                # Есть присоединение к оферте
                external['offerJoining'] = True
                # Способ идентификации 0 - Лично, 1 - по ЭЦП
                external['identificationKind'] = 0

            i += 1
            # Проверка, что контакт последний
            if len(contacts) == i:
                if contact['EMAIL']:
                    # Email контакта
                    external['email'] = contact['EMAIL'][0]['VALUE']
                else:
                    raise Exception("Укажите email у контактного лица")

                if contact['PHONE']:
                    # Телефон контакта
                    external['phone'] = contact['PHONE'][0]['VALUE'][-10:]
                else:
                    raise Exception("Укажите номер телефона у контактного лица")
        else:
            raise Exception(f"Укажите обращение к контакту: "
                            f"{contact['LAST_NAME']} {contact['NAME']} {contact['SECOND_NAME']}")

    print_json(external)
    # ext.POST("request/create", external)
    # fio = f"{external['lastName']} {external['firstName']} {external['middleName']}"
    # title = f"АЦ-{ext.result.json()['result']} / {external['company'] if external['type'] > 1 else fio}"
    # mes = f"[TABLE][TR][TD][B][COLOR=blue]Заявка успешно создана[/COLOR]: [/B][/TD][/TR]" \
    #       f"[TR][TD]Номер заявки {ext.result.json()['result']}[/TD][/TR][/TABLE]"
    # bitrix.GET(f"crm.deal.update?id={id}&fields[TITLE]={title}")
    # print("</body></html>")
except Exception as e:
    print(e.args[0])
    mes = f"[TABLE][TR][TD][B][COLOR=blue]Ошибка создания[/COLOR]: [/B][/TD][/TR]" \
          f"[TR][TD]{e.args[0]}[/TD][/TR][/TABLE]"
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
