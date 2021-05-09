import csv

from pymongo import MongoClient
import time


def check_time(counter_for_insert,text,function, collection):
    start_time = time.monotonic()
    print("начинаем мерять время")
    a = function(counter_for_insert,text,collection)
    print("Закончили мерять время")
    print("Время выполнения", time.monotonic() - start_time)
    return a

counter_for_insert = 0
counter_zapyska = 0

def mongodb(counter_for_insert,user_text,collection):
    counter = 0
    print("Прогружаем файлы")
    file_name = user_text + ".csv"
    inserter_list = []
    counter_for_see = 0
    with open(file_name, encoding="utf-8") as csv_file:
        names_list = []
        for i in csv_file:  # можно добавить индекс ч
                            # то б точно знать сколько строчек мы загрузили в базу из конкретного года,
                            # а можно делать запрос в саму базу
            if counter == 0:
                i = i.replace('"', "")
                names_list = i.split(";")
                # names_list.index("engBall")
                names_list = names_list[:-10]
                names_list += ["Year"]
                counter += 1
            else:
                i = i.replace('"', "")
                value_list = i.split(";")[:-10]
                value_list += [int(user_text)]

                # names_list_id = ["_id"] + names_list
                # print(names_list)
                # print(value_list)
                a = dict(zip(names_list, value_list))
                a["_id"] = counter_for_insert + counter_for_see
                inserter_list += [a]
                # names_list_id = names_list
                counter_for_see += 1
                if counter_for_see % 100 == 0:
                    collection.insert_many(inserter_list)
                    inserter_list = []
                    # print("RABOTA")
                if counter_for_see == 1000:  # количество строчек для добавления
                    break


    # print(len(inserter_list))

    print("Готово")
    return counter_for_see




def select(collection):
    first_year_list = []
    second_year_list = []
    all_rows = collection.find()
    for b in all_rows:
        # print(b['Year'])
        if b['Year'] == 2019 and b['UkrPTRegName'] != "null":
            # print("nnn")
            first_year_list += [b['OUTID'], b['Birth'], b['engBall100'].replace(",", "."), b['UkrPTRegName'], b['Year']]
        elif b['Year'] == 2020 and b['UkrPTRegName'] != "null":
            second_year_list += [b['OUTID'], b['Birth'], b['engBall100'].replace(",", "."), b['UkrPTRegName'], b['Year']]
        # print(second_year_list)
    # print(first_year_list[3::5][:10])
    all_oblasti = set(first_year_list[3::5])
    slovar_1year = dict()
    slovar_2year = dict()
    # кладем минимальніые данные в 2 словаря область - бал
    for i in all_oblasti:
        bal_in_oblast_2019 = [float(ii) for j, ii in enumerate(first_year_list[2::5]) if first_year_list[j * 5 + 3] == i and ii != "null" and float(ii) > 100]
        bal_in_oblast_2020 = [float(ii) for j, ii in enumerate(second_year_list[2::5]) if second_year_list[j * 5 + 3] == i and ii != "null" and float(ii) > 100]

        # slovar_1year[i] = sum(bal_in_oblast_2019)/len(bal_in_oblast_2019) # средний бал
        # slovar_2year[i] = sum(bal_in_oblast_2020)/len(bal_in_oblast_2020) # средний бал
        slovar_1year[i] = min(bal_in_oblast_2019)
        slovar_2year[i] = min(bal_in_oblast_2020)

        # print(second_year_list)
    last_slovar = dict()
    for i in slovar_1year.keys():
        if slovar_1year[i] > slovar_2year[i]:
            # print(slovar_1year[i],slovar_2year[i])
            last_slovar[i + "2019"] = slovar_1year[i]
        else:
            # print(slovar_1year[i], slovar_2year[i])
            last_slovar[i + "2020"] = slovar_2year[i]
    print(last_slovar)
    with open("Answer.csv", 'w', newline='', encoding="UTF-8") as f:
        my_writer = csv.DictWriter(f, fieldnames=last_slovar.keys())
        my_writer.writeheader()
        # for i in slovar_hranenie:
        my_writer.writerow(last_slovar)


while True:
    a = input()

    print(counter_for_insert)
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['Vova']

        collection = db['Vova']
    except:
        print("Мы не можем в данный момент подключится к базе попробуйте позже")
        continue
    if a == "1":
        break
    elif a == "zapros":
        select(collection)
    else:
        try:
            if counter_zapyska == 1: # сценарий падения базы на втором запуске
                print("Мы падаем")
                client = MongoClient("mongodb://localhost:27017////5")
                db = client['Vova']
                collection = db['Vova']
            counter_for_insert2 = check_time(counter_for_insert, a, mongodb, collection)
            counter_zapyska += 1
        # mongodb(collection)
        except:
            counter_for_insert2 = None
            counter_zapyska += 1
            print("Что то пошло не так, приносим наши извинения")
        if counter_for_insert2 is not None:
            counter_for_insert += counter_for_insert2

