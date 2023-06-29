import os

from logger import logger


current_dir = os.getcwd()
folder_path = os.path.join(current_dir, 'Синтетические данные (CSV)')
folder_path_prefix = os.path.join(current_dir, 'Префиксы телефонных номеров (CSV)')

if not os.path.exists(os.path.join(current_dir, 'Обработанные данные')):
    os.mkdir('Обработанные данные')
    logger.info('Создана папка Обработанные данные')
out_folder_path = os.path.join(current_dir, 'Обработанные данные')

prefix_dct = {}
total_duration_dct = {}


def get_list_file(folder_path):
    """
    получаем список файлов из папки
    """
    file_list = os.listdir(folder_path)
    return file_list



def get_file_data(file_path: str):
    # возвращает содержимое файла
    with open(file_path, 'r') as file:
        for line in file:
            yield line


def write_to_file(file_name: str, data: str):
    # записываем строки в файл
    file_path = os.path.join(out_folder_path, file_name)
    with open(file_path, 'a') as file:
        file.write(data)


def load_prefix(file_list: list):
    # загружаем префиксы в словарь
    line = get_file_data(os.path.join(folder_path_prefix, file_list[0]))
    while True:
        try:
            result = next(line)
            lst = result.replace('\n','').split(',')
            prefix_dct[lst[1]] = lst[0]
        except StopIteration:
            logger.info('Префиксы загружены')
            break


def total_duration(msisdn_prfx: str, dialed_prfx: str, duration: int):
     # суммируем общую продолжительность разговоров по паре префиксов
     total_duration_dct[f'{msisdn_prfx}-{dialed_prfx}'] = total_duration_dct.get(f'{msisdn_prfx}-{dialed_prfx}', 0) + duration


def parse_phone_connect(phn_conn: str):
    # обрабатываем каждую строку
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
    # подбираем префикс
    max_length = 11
    for index in range(max_length-1, -1, -1): # можно брать срезы только определенной длины
        slice_ = phone[:index]
        # if prefix_dct.get(slice_, None):
        if slice_ in prefix_dct:
            # print('Find prefix in phone:',phone,  slice_, prefix_dct.get(slice_))
            return prefix_dct.get(slice_)
    return 'Unknown'


# выделение префикса из телефона вызывающего и вызываемого абонента
#
#запись префиксов в строку
#
#запись продолжительности разговоров по каждой префиксной паре в файл
#

def main():
    # загружаем префиксы
    logger.info('Загружаем префиксы')
    file_list = get_list_file(folder_path_prefix)
    load_prefix(file_list)

    # получаем список файлов
    file_list = get_list_file(folder_path)

    # Проходим по каждому файлу и читаем его содержимое построчно
    logger.info('Обрабатываем файлы')
    for file_name in file_list:
        file_path = os.path.join(folder_path, file_name)
        file_data = get_file_data(file_path)
        while True:
            try:
                line = next(file_data)
                result = parse_phone_connect(line)
                write_to_file(file_name, result)
            except StopIteration:
                logger.info(f'Даныне записаны в файл {file_name}')
                break

    logger.info('Записываем данные в файл VOLUMES.TXT')
    for key, value in total_duration_dct.items():
        prefix_zone_msisdn, prefix_zone_dialed = key.split('-')
        write_to_file('VOLUMES.TXT', f'{prefix_zone_msisdn},{prefix_zone_dialed},{value}\n') # плохо переделать


if __name__ == '__main__':
    main()