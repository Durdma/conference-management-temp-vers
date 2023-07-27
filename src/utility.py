from typing import Union, List, Optional
import json
from datetime import datetime

from fastapi import HTTPException

import gspread
from gspread import Worksheet
from gspread.cell import Cell

from src.schema import Application, ApplicationCreate, ApplicationUpdate


def open_by_key(key: str):
    """
    Открывает таблицу google sheets по значению ключа таблицы
    """
    gs = gspread.service_account()
    return gs.open_by_key(key).sheet1


def parse_record(record: list, sheet: Worksheet) -> Application:
    """
    Приводит полученный список значений из таблицы к схеме Application
    """
    field_names = sheet.row_values(1)
    try:
        record[0] = int(record[0])
    except ValueError:
        raise ValueError("Error in cell id. id is not an integer.")
    tmp = {field_name: field for field_name, field in zip(field_names, record)}
    # Декодируем поле coauthors из JSON обратно в список словарей
    coauthors_json = tmp.get("coauthors")
    if coauthors_json:
        tmp["coauthors"] = json.loads(coauthors_json)
    
    return Application(**tmp)


def parse_records(records: List[list], sheet: Worksheet) -> List[Application]:
    """
    Обертка для списка значений, полученных из таблицы, для приведения к схеме Application
    """
    for key, record in enumerate(records):
        records[key] = parse_record(record, sheet)

    return records


def find_application_sheet(conference_id: int, sheet: Worksheet):
    """
    Осуществляет поиск по таблице конференций необходимой таблицы для заявок
    """
    coord = sheet.find(str(conference_id), in_column=1)
    field_names = sheet.row_values(1)
    if coord:
        data = sheet.row_values(coord.row)
        return {field_name: field for field_name, field in zip(field_names, data)}
    else:
        raise HTTPException(status_code=404, detail="Conference not found")


def find_record_by_id(record_id: int, sheet: Worksheet) -> Application:
    """
    Осуществляет поиск записей по ее id
    """
    record_coord = sheet.find(str(record_id), in_column=1)

    if record_coord:
        record = sheet.row_values(record_coord.row)
        record = parse_record(record, sheet)
        print(record)
        return record
    else:
        raise HTTPException(status_code=404, detail="Application not found")


def find_record_by_tg_ds_or_email(param: dict, sheet: Worksheet) -> List[Application]:
    """
    Осуществляет поиск записей по одному из значений telegram_id, discord_id or email
    """
    if param.get("telegram_id", None):
        records_coords = sheet.findall(query=param.get("telegram_id"), in_column=2)
    elif param.get("discord_id", None):
        records_coords = sheet.findall(query=param.get("discord_id"), in_column=3)
    elif param.get("email", None):
        records_coords = sheet.findall(query=param.get("email"), in_column=6)
    else:
        raise HTTPException(status_code=500, detail="Internal error in find_record_by_tg_ds_or_email")
    
    if records_coords:
        records = [sheet.row_values(coord.row) for coord in records_coords]
        records = parse_records(records, sheet)
        print(records)
        return records
    else:
        raise HTTPException(status_code=404, detail="Application not found")
    

def del_val(record_id: int, sheet: Worksheet):
    """
    Обнуляет значения в ячейках таблицы
    """
    range_start = f"B{record_id+1}"
    range_end = f"P{record_id+1}"
    cell_range = sheet.range(range_start + ":" + range_end)

    for cell in cell_range:
        cell.value = ""

    sheet.update_cells(cell_range)
    return


def del_record_by_id(record_id: int, param: dict, sheet: Worksheet) -> Application:
    """
    Осуществляет удаление записи из таблицы
    """
    record: Optional[Application] = find_record_by_id(record_id, sheet)
    
    if record:
        if param.get("telegram_id", None) == record.telegram_id:
            del_val(record_id, sheet)
            return record
        elif param.get("discord_id", None) == record.discord_id:
            del_val(record_id, sheet)
            return record
        elif param.get("email", None) == record.email:
            del_val(record_id, sheet)
            return record
        else:
            raise HTTPException(status_code=403, detail="Nor telegram_id nor discord_id nor email are not equal")
    
    raise HTTPException(status_code=500, detail="Something gone wrong white deleting record")       


def update_record_by_id(record_id: int, body: ApplicationUpdate, sheet: Worksheet) -> Application:
    """
    Осуществляет обновление записи в таблице
    """
    record: Optional[Application] = find_record_by_id(record_id, sheet)
    body.updated_at = datetime.now().astimezone().isoformat()
    coauthors_json = json.dumps(body.coauthors, ensure_ascii=False)
    data = list(body.model_dump().values())
    print(data)
    data[data.index(body.coauthors)] = coauthors_json
    if record:
        if record.telegram_id == body.telegram_id or record.discord_id == body.discord_id or record.email == body.email:
            record = list(record.model_dump().values())
            values_range = sheet.range(record_id+1, 2, record_id+1, 1 + len(data))

            for i, (update_value, value) in enumerate(zip(data, record)):
                if update_value == "null":
                    values_range[i].value = value
                else:
                    values_range[i].value = update_value

            sheet.update_cells(values_range)

            return find_record_by_id(record_id, sheet)
        else:
            raise HTTPException(status_code=403, detail="Nor telegram_id nor discord_id nor email are not equal")


def update_record_by_id_v2(record_id: int, body: ApplicationUpdate, sheet: Worksheet) -> Application:
    record: Optional[Application] = find_record_by_id(record_id, sheet)
    body.updated_at = datetime.now().astimezone().isoformat()
    coauthors_json = json.dumps(body.coauthors, ensure_ascii=False)
    
    if record:
        if record.telegram_id == body.telegram_id:
            pass
        elif record.discord_id == body.discord_id:
            pass
        elif record.email == body.email:
            pass
        else:
            raise HTTPException(status_code=403, detail="Nor telegram_id nor discord_id nor email are not equal")
        
        body.telegram_id = record.telegram_id
        body.discord_id = record.discord_id
        body.email = record.email

        data = list(body.model_dump().values())
        data[data.index(body.coauthors)] = coauthors_json

        record = list(record.model_dump().values())
        values_range = sheet.range(record_id+1, 2, record_id+1, 1 + len(data))

        for i, (update_value, value) in enumerate(zip(data, record)):
                if update_value == "null":
                    values_range[i].value = value
                else:
                    values_range[i].value = update_value

        sheet.update_cells(values_range)

        return find_record_by_id(record_id, sheet)


def next_available_row(sheet: Worksheet):
    """
    Осуществляет поиск следующей свободной строки
    """
    str_list = list(filter(None, sheet.col_values(2)))  # fastest
    return str(len(str_list)+1)


def check_date(timestamp: str, registration_start_date: str, registration_end_date: str):
    """
    Проверка, что запись/обновление записи в таблицу производится во время
    """
    timestamp = datetime.fromisoformat(timestamp)
    registration_start_date = datetime.strptime(registration_start_date, "%d.%m.%Y").date()
    registration_end_date = datetime.strptime(registration_end_date, "%d.%m.%Y").date()

    if registration_start_date <= timestamp.date() <= registration_end_date:
        return True
    else:
        raise HTTPException(status_code=403, detail=f"Not in a registration period. Registration period is from {registration_start_date} to {registration_end_date}")


def add_record_v4(record: ApplicationCreate, sheet: Worksheet) -> Application:
    """
    Осуществляет добавление записи в таблицу
    """
    record.submitted_at = datetime.now().astimezone().isoformat()
    record.updated_at = datetime.now().astimezone().isoformat()

    cell_coord = sheet.find("", in_column=1)
    record_fields = list(record.model_dump().values())

    if cell_coord:
        cell_row = cell_coord.row
    else:
        cell_row = int(next_available_row(sheet))

    coauthors_json = json.dumps(record.coauthors, ensure_ascii=False)
    record_fields[record_fields.index(record.coauthors)] = coauthors_json

    values_range = sheet.range(cell_row, 2, cell_row, 1 + len(record_fields))
    for i, field_value in enumerate(record_fields):
        if field_value == "null":
            values_range[i].value = None
        else:
            values_range[i].value = field_value

    sheet.update_cells(values_range)

    return find_record_by_id(cell_row - 1, sheet)
