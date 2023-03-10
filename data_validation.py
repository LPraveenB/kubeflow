import json
import os
from google.oauth2 import service_account
from google.cloud import storage
from calendar import monthrange
from datetime import datetime as dt
from datetime import date, timedelta
import arrow
import xlsxwriter
import logging
import re
import multiprocessing


def get_client():
    """
        The get_client function authenticates GCP cloud storage bucket using service account key.
        @:param self
        @:return: storage client for accessing extracted-bucket-dollar-tree
    """

    if cloud_enabled:
        service_account_file = env["SERVICE_ACCOUNT_FILE"]
        credentials = service_account.Credentials.from_service_account_file(
            service_account_file)

        storage_client = storage.Client(credentials=credentials)

        if storage_client is not None:
            logging.info("Authentication to env['BUCKET_NAME'] bucket successful..")
    else:
        storage_client = None
        logging.info("Running locally, skipping authentication to cloud storage.")

    return storage_client


def get_files_from_bucket():
    """
        The get_files_from_bucket function retrieves all filenames matching with the table name prefix in env[TABLES]
        list present in GCP bucket.
        @:param self
        @:return: list of files in the bucket
    """

    client = get_client()

    received_files = []

    logging.info("Reading filenames from bucket env['BUCKET_NAME']")

    for blob in client.list_blobs(env["BUCKET_NAME"], prefix=env["BLOB_PREFIX"]):
        each_file_from_path = ((str(blob.name)).split('/')[-1])
        each_file_with_date = each_file_from_path.split('-')[0]
        for i in range(len(env["TABLES"])):
            if each_file_with_date.__contains__(env["TABLES"][i]):
                received_files.append(each_file_with_date)
        if each_file_with_date.__contains__(env["store_inv_table"]):
            received_files.append(each_file_with_date)

    return received_files


def get_files_from_local():
    """
            The get_files_from_bucket function retrieves all filenames matching with the
            table name prefix in env[TABLES] list present in local system.
            @:param self
            @:return: list of files in the local
        """

    received_files = []

    logging.info("Reading filenames from local.")

    for path, subdir, files in os.walk(env["FOLDER"]):
        for blob in files:
            each_file_from_path = (blob.split('/')[-1])
            each_file_with_date = each_file_from_path.split('-')[0]
            for i in range(len(env["TABLES"])):
                if each_file_with_date.__contains__(env["TABLES"][i]):
                    received_files.append(each_file_with_date)
            if each_file_with_date.__contains__(env["store_inv_table"]):
                received_files.append(each_file_with_date)

    return received_files


def process_files():
    """
        This function extracts dates from filenames based on their formats and sorts them into different lists.
        Filenames containing date ranges are expanded to include all dates within the range.
        This function also utilizes multiprocessing to classify files in parallel.
        @:param self
        @:return: list of files with dates and list of files from STORE_INV

    """

    if cloud_enabled:
        files = get_files_from_bucket()
    else:
        files = get_files_from_local()

    global store_inv_files
    date_range_files = []
    date_files = []
    month_range_files = []

    date_patterns = env["date_patterns"]

    num_cpus = multiprocessing.cpu_count() - 1
    pool_input_list = [(each_file, date_patterns) for each_file in files]
    pool_map = multiprocessing.Pool(num_cpus)
    result = pool_map.starmap(classify_files, pool_input_list)
    pool_map.close()
    logging.info(f'Number of CPU cores: {num_cpus}')

    for each_result in result:
        store_inv_files += each_result[0]
        date_range_files += each_result[1]
        date_files += each_result[2]
        month_range_files += each_result[3]

    expanded_month_range_files = expand_month_range_files(month_range_files)
    expanded_date_range_files = expand_date_range_files(date_range_files)
    files_except_store_inv = expanded_month_range_files + expanded_date_range_files + date_files

    return store_inv_files, files_except_store_inv


def classify_files(each_file, date_patterns):
    """
        The classify_files function classifies a given file into one of four categories based on its date format.
        The function looks for matches between the file name and a list of date patterns, and then appends the file
        to the corresponding category list including store_inv_files.
        @:param each_file: the file being classified
        @:param: date_patterns: a list of regular expressions used to match date formats
        @:return: a tuple containing store_inv_files, date_range_files, date_files, and month_range_files

    """

    date_range_files = []
    date_files = []
    month_range_files = []

    for date_pattern in date_patterns:
        match1 = re.search(date_pattern, (each_file.split('_')[-1]))
        match2 = re.search(date_pattern, (each_file.split('_')[-2]))
        match3 = re.search(date_pattern, ('_'.join(each_file.split('_')[-2:])))

        if (match1 is not None) and (match2 is not None):
            date_range_files.append(each_file)
        elif (match1 is not None) and (match2 is None):
            date_files.append(each_file)
        elif match3 is not None:
            month_range_files.append(each_file)
        else:
            pass
    if each_file.__contains__(env["store_inv_table"]):
        store_inv_files.append(each_file)

    return store_inv_files, date_range_files, date_files, month_range_files


def get_table_from_each_file(filename_for_table):
    """
        The get_table_from_each_file function takes filename as input and returns table name matching in filename.
        @:param filename: str
        @:return: table name
    """

    table = ""
    for i in range(len(env["TABLES"])):
        if filename_for_table.__contains__(env["TABLES"][i]):
            table = env["TABLES"][i]

    return table


def get_index_from_table(table):
    """
        The get_index_from_table function takes table name as input and returns index of TABLES[]
        list for matching table name.
        @:param table name: str
        @:return: index
    """

    index = 0
    for i in range(len(env["TABLES"])):
        if table == (env["TABLES"][i]):
            index = i
    return index + 1


def get_date_from_file(filename_for_date):
    """
        The get_date_from_file function takes filename as input and returns date in its name.
        @:param filename: str
        @:return: date
    """

    date_from_file = filename_for_date.split('_')[-1]
    date_from_file = dt.strptime(date_from_file, '%Y%m%d').date()

    return date_from_file


def get_store_id_from_each_file(filename_for_store_id):
    """
        The get_store_id_from_each_file function takes filename as input and returns store id.
        @:param filename: str
        @:return: store id
    """

    store_id = '_'.join(filename_for_store_id.split('_')[-3:-1])

    return store_id


def create_store_id_list():
    """
        The create_store_id_list function creates a list of store ids from store_inv files.
        @:param self
        @:return: list of store ids
    """
    global store_id_list

    for i in range(len(store_inv_files) - 1):
        file1 = store_inv_files[i].split('.')[0]
        file2 = store_inv_files[i + 1].split('.')[0]
        store_id1 = '_'.join(file1.split('_')[-3:-1])
        store_id2 = '_'.join(file2.split('_')[-3:-1])
        if store_id1 != store_id2:
            store_id_list.append(store_id1)
        store_id_list.append(store_id2)

    store_id_list = list(dict.fromkeys(store_id_list))
    store_id_list.sort()

    return store_id_list


def get_index_from_store_id_list(store_id):
    """
        The get_index_from_store_id_list function takes store id as input and returns index of store_id_list
        @:param store id: str
        @:return: index
    """

    index = 0
    for i in range(len(store_id_list)):
        if store_id == (store_id_list[i]):
            index = i
    return index + 1


def get_date_from_store_inv_file(file_date_inv):
    """
        The get_date_from_store_inv_file function takes filename as input and returns date in its name.
        @:param filename: str
        @:return: date
    """
    file_date_inv = file_date_inv.split('.')[0]
    date_from_store_inv_file = file_date_inv.split('_')[-1]
    date_from_store_inv_file = dt.strptime(date_from_store_inv_file, '%Y%m%d').date()

    return date_from_store_inv_file


def expand_month_range_files(text_dated_files):
    """
        The expand_month_range_files function takes filename with date format JAN_20 as input and
        expand it between Jan 1 2020 to Jan 31 2020. Returns a list of all expanded files.
        @:param filename: str
        @:return: list of all expanded names of files
    """
    expanded_file_list = []

    for date_file in text_dated_files:
        old_file = date_file
        date_file = date_file.split('_')[-2] + ' ' + '20' + date_file.split('_')[-1]
        date_file = arrow.get(date_file, 'MMM YYYY').format('YYYY-MM')
        year = int(str(date_file).split('-')[0])
        month = int(str(date_file).split('-')[-1])
        initial_date = date(year, month, 1)
        weekday, days = (monthrange(year, month))
        last_date = date(year, month, 1 + (days - 1))
        date_range = [initial_date + timedelta(days=x) for x in range((last_date - initial_date).days + 1)]
        for each_date in date_range:
            each_date = ''.join(str(each_date).split('-'))
            formatted_file = '_'.join(old_file.split('_')[:-2]) + '_' + str(each_date)
            expanded_file_list.append(formatted_file)

    return expanded_file_list


def expand_date_range_files(date_range_files):
    """
        The expand_date_range_files function takes filename with date format 20201022_20201028 in its name
        as input and expand the filenames from initial date 20201022 to final date 20201028. Returns a list
        of all expanded files.
        @:param filename: str
        @:return: date
    """
    expanded_date_range_files = []

    for dr_file in date_range_files:
        start_date = dt.strptime(dr_file.split('_')[-2], '%Y%m%d').date()
        end_date = dt.strptime(dr_file.split('_')[-1], '%Y%m%d').date()
        date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
        for each_date in date_range:
            each_date = ''.join(str(each_date).split('-'))
            old_file = '_'.join(str(dr_file).split('_')[:-2]) + '_' + str(each_date)
            expanded_date_range_files.append(old_file)

    return expanded_date_range_files


def save_excel():
    """
        The save_excel function saves the filename received on a particular date under respective table
        name of the file. If the file is not received on a particular date value is left blank. Store inv
        files are saved in store inv sheet and filename is saved for a particular date under respective store id.
        @:param self
        @:return:
    """

    logging.info("Saving filenames in excel for all files received for a date except store inv files")
    workbook = xlsxwriter.Workbook(env["excel_file_path"])
    worksheet = workbook.add_worksheet()
    bold = workbook.add_format({'bold': True})
    worksheet.write('A1', 'Date', bold)

    col_count = 1

    for col_name in env["TABLES"]:
        worksheet.write(0, col_count, col_name, bold)
        col_count += 1

    row = 1

    date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

    for key in date_dict:
        worksheet.write(row, 0, key, date_format)
        each_files = date_dict[key]
        for ky in each_files:
            worksheet.write(row, get_index_from_table(ky), each_files[ky])
        row += 1

    worksheet = workbook.add_worksheet(env["store_inv_table"])

    col_count = 0

    global store_id_list

    for col_count in range(len(store_id_list)):
        worksheet.write(0, 0, 'Date', bold)
        worksheet.write(0, col_count + 1, store_id_list[col_count], bold)
    col_count += 1

    row = 1

    date_format = workbook.add_format({'num_format': 'yyyy-mm-dd'})

    logging.info("Saving store inv files received on a particular date for respective store ids")
    for key in date_dict_store_inv:
        worksheet.write(row, 0, key, date_format)
        filenames = date_dict_store_inv[key]
        for ky in filenames:
            worksheet.write(row, get_index_from_store_id_list(ky), filenames[ky])
        row += 1

    worksheet = workbook.add_worksheet('Error')
    worksheet.write('A1', "Duplicates", bold)
    row_count = 0
    for dup_file in duplicate_files:
        worksheet.write(row_count + 1, 0, dup_file)
        row_count += 1

    workbook.close()


if __name__ == "__main__":

    with open('data_validation.json', 'r') as f:
        env = json.load(f)

    cloud_enabled = env['cloud_enabled']

    d1 = dt.strptime(env["start_date"], "%Y-%m-%d").date()
    d2 = dt.strptime(env["end_date"], "%Y-%m-%d").date()
    dd = [d1 + timedelta(days=x) for x in range((d2 - d1).days + 1)]

    date_dict = {}
    for d in dd:
        date_dict[d] = {'': ''}

    date_dict_store_inv = {}
    for d in dd:
        date_dict_store_inv[d] = {'': ''}

    store_inv_files = []
    store_id_list = []
    duplicate_files = []

    logging.basicConfig(filename=env["log_file_path"], level=logging.INFO,
                        format="%(asctime)s:%(message)s", filemode="w")
    logging.info(">>>>>STARTING VALIDATION OF RECEIVED DATA<<<<<")
    inv_file_list, all_except_inv_files_list = process_files()

    create_store_id_list()

    logging.info("Creating dictionary with date as key and value as dict of table as key and value filename")

    for j in range(len(all_except_inv_files_list)):
        inner_dict = date_dict[get_date_from_file(all_except_inv_files_list[j])]
        table_name = get_table_from_each_file(all_except_inv_files_list[j])
        if table_name in inner_dict:
            existing_each_file = inner_dict[table_name]
            if existing_each_file == all_except_inv_files_list[j]:
                duplicate_files.append(existing_each_file)
        else:
            inner_dict[table_name] = all_except_inv_files_list[j]

    logging.info("Dictionary of all files created except store inv files")

    logging.info("Creating dict of store inv files")

    for k in range(len(inv_file_list) - 1):
        inner_dict_inv = date_dict_store_inv[get_date_from_store_inv_file(inv_file_list[k])]
        inner_dict_inv[get_store_id_from_each_file(inv_file_list[k])] = inv_file_list[k]
        existing_inv_file_name = inner_dict_inv[get_store_id_from_each_file(inv_file_list[k])]
        if existing_inv_file_name == inv_file_list[k+1]:
            duplicate_files.append(existing_inv_file_name)
        else:
            inner_dict_inv[get_store_id_from_each_file(inv_file_list[k+1])] = inv_file_list[k+1]

    # for k in range(len(inv_file_list)):
    #     inner_dict_inv = date_dict_store_inv[get_date_from_store_inv_file(inv_file_list[k])]
    #     inner_dict_inv[get_store_id_from_each_file(inv_file_list[k])] = inv_file_list[k]
    #     existing_inv_each_file = inner_dict_inv[get_store_id_from_each_file(inv_file_list[k])]
    #     if existing_inv_each_file == inv_file_list[k]:
    #         duplicate_files.append(existing_inv_each_file)
    #     else:
    #         inner_dict_inv[get_store_id_from_each_file(inv_file_list[k + 1])] = inv_file_list[k]

    logging.info("Dictionary of store inv files created")
    save_excel()

    logging.info("<<<<<DATA VALIDATION COMPLETED>>>>>")

    exit(0)
