import os
from datetime import datetime

from logger import logger


current_dir = os.getcwd()
folder_path = os.path.join(current_dir, 'Синтетические данные (CSV)')
folder_path_prefix = os.path.join(current_dir, 'Префиксы телефонных номеров (CSV)')

if not os.path.exists(os.path.join(current_dir, 'Обработанные данные')):
    os.mkdir('Обработанные данные')
    logger.info('Создана папка Обработанные данные')
out_folder_path = os.path.join(current_dir, 'Обработанные данные')

prefix_dct = {}  # словарь с префиксами
total_duration_dct = {}  # словарь с общей длительностью разговоров для каждой пары префиксов
dct_len_prefix = {
        '1': [], '2': [], '3': [], '4': [],
        '5': [], '6': [], '7': [], '8': [], '9': [], '0': []}  # длины префиксов


def get_list_file(folder_path):
    """
    получаем список файлов из папки
    """
    file_list = os.listdir(folder_path)
    return file_list



def get_file_data(file_path: str):
    """
    возвращает содержимое файла
    """
    with open(file_path, 'r') as file:
        for line in file:
            yield line


def write_to_file(file_name: str, data: list):
    """
    записываем строки в файл
    """
    file_path = os.path.join(out_folder_path, file_name)
    with open(file_path, 'a') as file:
        for el in data:
            file.write(el)


def load_prefix(file_list: list):
    """
    загружаем префиксы в словарь
    """
    line = get_file_data(os.path.join(folder_path_prefix, file_list[0]))
    while True:
        try:
            result = next(line)
            lst = result.replace('\n','').split(',')
            prefix_dct[lst[1]] = lst[0]
        except StopIteration:
            logger.info('Префиксы загружены')
            break


def load_len_prefix():
    """
    загружаем длины префиксов в словарь
    """
    for key in prefix_dct:
        if len(key):
            lst = dct_len_prefix[key[0]]
            if len(key) not in lst:
                lst.append(len(key))
                dct_len_prefix[key[0]] = lst


def total_duration(msisdn_prfx: str, dialed_prfx: str, duration: int):
     """
     суммируем общую продолжительность разговоров по паре префиксов
     """
     total_duration_dct[f'{msisdn_prfx}-{dialed_prfx}'] = total_duration_dct.get(f'{msisdn_prfx}-{dialed_prfx}', 0) + duration


def parse_phone_connect(phn_conn: str):
    """
    обрабатываем каждую строку
    """
    out = phn_conn.replace('\n', '').split(',')

    msisdn = out[5]
    msisdn_prfx = find_prefix_in_phone(msisdn) #  префикс вызывающего абонента
    
    dialed = out[6]
    dialed_prfx = find_prefix_in_phone(dialed) #  префикс вызываемого абонента
    
    duration = int(out[8])
    total_duration(msisdn_prfx, dialed_prfx, duration)

    out[9] = msisdn_prfx
    out[10] = dialed_prfx

    return ','.join(out) + '\n'


def find_prefix_in_phone(phone: str):
    """
    подбираем префикс
    """
    lst = dct_len_prefix[phone[0]]
    for index in lst: # можно брать срезы только определенной длины
        slice_ = phone[:index]
        if slice_ in prefix_dct:
            return prefix_dct.get(slice_)
    return 'Unknown'


def main():
    # загружаем префиксы
    logger.info('Загружаем префиксы')
    file_list = get_list_file(folder_path_prefix)
    load_prefix(file_list)

    load_len_prefix()

    # получаем список файлов
    file_list = get_list_file(folder_path)

    # Проходим по каждому файлу и читаем его содержимое построчно
    logger.info('Обрабатываем файлы')
    for file_name in file_list:
        data = []
        file_path = os.path.join(folder_path, file_name)
        file_data = get_file_data(file_path)
        while True:
            try:
                line = next(file_data)
                result = parse_phone_connect(line)
                data.append(result)
            except StopIteration:
                write_to_file(file_name, data)
                logger.info(f'Даныне записаны в файл {file_name}')
                break

    logger.info('Записываем данные в файл VOLUMES.TXT')
    data = []
    for key, value in total_duration_dct.items():
        prefix_zone_msisdn, prefix_zone_dialed = key.split('-')
        data.append(f'{prefix_zone_msisdn},{prefix_zone_dialed},{value}\n')
    write_to_file('VOLUMES.TXT', data)


if __name__ == '__main__':
    t0 = datetime.now()
    main()
    t1 = datetime.now()
    logger.info(f'Время выпонения скрипта: {t1 - t0}')