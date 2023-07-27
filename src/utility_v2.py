from typing import Union, List, Optional, Dict
import json
from datetime import datetime

from fastapi import HTTPException

import gspread
from gspread import Worksheet
from gspread.cell import Cell

from src.schema import Application, ApplicationCreate, ApplicationUpdate


class TableConnector:
    def __init__(self) -> None:
        # Переписать на использование self.gs2 = gspread.service_account_from_dict
        self.__gs = gspread.service_account()
        self.__main_table: Worksheet = self.__open_table("1KZ6rndDzUMhqTY6eWvDD9khaLh65xnWHXh6hGmckQ1M")


    def __open_table(self, key: str) -> Worksheet:
        """
        Открытие google-таблицы по значению ключа таблицы
        """
        return self.__gs.open_by_key(key).sheet1
    

    def __check_date(self, timestamp: datetime, registration_start_date: str, registration_end_date: str) -> bool:
        """
        Проверка, что действие в таблице происходит в определенный период времени
        """
        timestamp = timestamp.date()
        registration_start_date = datetime.strptime(registration_start_date, "%d.%m.%Y").date()
        registration_end_date = datetime.strptime(registration_end_date, "%d.%m.%Y").date()

        if registration_start_date <= timestamp <= registration_end_date:
            return True
        else:
            raise HTTPException(status_code=403, detail=f"Not in a registration period. Registration period is from {registration_start_date} to {registration_end_date}")


    def __find_application_sheet(self, conference_id: int) -> Worksheet:
        """
        Осуществляет поиск по таблице конференций необходимой таблицы для заявок
        """
        coord: Optional[Cell] = self.__main_table.find(str(conference_id), in_column=1)
        field_names: Optional[List[str]] = self.__main_table.row_values(1)
        if coord:
            data: Optional[List[str]] = self.__main_table.row_values(coord.row)
            data: Optional[Dict[str, str]] = {field_name: field for field_name, field in zip(field_names, data)}

            if self.__check_date(datetime.now(), data.get("registration_start_date"), data.get("registration_end_date")):
                return self.__open_table(data.get("sheet_id"))
            
        raise HTTPException(status_code=404, detail="Conference not found")

    # TODO Переписать, чтобы использовать для любого типа записи
    def __parse_record(self, record: List[str], sheet: Worksheet) -> Application:
        """
        Приводит полученный список значений из таблицы к схеме Application
        """
        field_names: List[str] = sheet.row_values(1)
        try:
            record[0] = int(record[0])
        except ValueError:
            raise ValueError("Error in cell id. id is not an integer.")
        
        tmp = {field_name: field for field_name, field in zip(field_names, record)}
        coauthors_json = tmp.get("coauthors")
        if coauthors_json:
            tmp["coauthors"] = json.loads(coauthors_json)

        return Application(**tmp)


    def __parse_records(self, records: List[List[str]], sheet: Worksheet) -> List[Application]:
        """
        Обертка для списка значений, полученных из таблицы, для приведения к схеме Application
        """
        for key, record in enumerate(records):
            records[key] = self.__parse_record(record, sheet)

        return records


    def __find_record_by_id(self, record_id: int, sheet: Worksheet) -> Application:
        """
        Осуществляет поиск записей по ее id
        """
        record_coord: Optional[Cell] = sheet.find(str(record_id), in_column=1)

        if record_coord:
            record: List[str] = sheet.row_values(record_coord.row)
            record: Application = self.__parse_record(record, sheet)
            return record
        else:
            raise HTTPException(status_code=404, detail="Application not found")


    def __next_available_row(self, sheet: Worksheet) -> str:
        """
        Осуществляет поиск следующей свободной строки
        """
        str_list = list(filter(None, sheet.col_values(2)))
        return str(len(str_list)+1)


    def add_application(self, conference_id: int, record: ApplicationCreate) -> Application:
        """
        Осуществляет добавление записи в таблицу
        """
        record.submitted_at: str = datetime.now().astimezone().isoformat()
        record.updated_at: str = datetime.now().astimezone().isoformat()

        sheet: Worksheet = self.__find_application_sheet(conference_id)

        cell_coord: Optional[Cell] = sheet.find("", in_column=1)
        record_fields: List[str] = list(record.model_dump().values())

        if cell_coord:
            cell_row: int = cell_coord.row
        else:
            cell_row: int = int(self.__next_available_row(sheet))

        coauthors_json: str = json.dumps(record.coauthors, ensure_ascii=False)
        record_fields[record_fields.index(record.coauthors)] = coauthors_json

        values_range: List[Cell] = sheet.range(cell_row, 2, cell_row, 1 + len(record_fields))
        for i, field_value in enumerate(record_fields):
            if field_value == "null":
                values_range[i].value = None
            else:
                values_range[i].value = field_value
        
        sheet.update_cells(values_range)

        return self.__find_record_by_id(cell_row - 1, sheet)



    def update_application(self, record_id: int, conference_id: int, body: ApplicationUpdate) -> Application:
        """
        Осуществляет обновление записи в таблице
        """
        sheet: Worksheet = self.__find_application_sheet(conference_id)
        record: Optional[Application] = self.__find_record_by_id(record_id, sheet)

        if record:
            if (record.telegram_id == body.telegram_id) and body.telegram_id is not None:
                ...
            elif (record.discord_id == body.discord_id) and body.discord_id is not None:
                ...
            elif (record.email == body.email) and body.email is not None:
                ...
            else:
                raise HTTPException(status_code=403, detail="Nor telegram_id nor discord_id nor email are not equal")
            
            body.updated_at = datetime.now().astimezone().isoformat()
            body.telegram_id = record.telegram_id
            body.discord_id = record.discord_id
            body.email = record.email

            coauthors_json: str = json.dumps(body.coauthors, ensure_ascii=False)
            data: List[str] = list(body.model_dump().values())
            data[data.index(body.coauthors)] = coauthors_json

            record_fields: List[str] = list(record.model_dump().values())
            values_range: List[Cell] = sheet.range(record_id+1, 2, record_id+1, 1 + len(data))

            for i, (update_value, value) in enumerate(zip(data, record_fields)):
                if update_value == "null":
                    values_range[i].value = value
                else:
                    values_range[i].value = update_value

            sheet.update_cells(values_range)

            return self.__find_record_by_id(record_id, sheet)
        else:
            raise HTTPException(status_code=404, detail="Record not found")


    def __del_val(self, record_id: int, sheet: Worksheet) -> None:
        """
        Обнуляет значения в ячейках таблицы
        """
        range_start: str = f"B{record_id+1}"
        range_end: str = f"P{record_id+1}"
        cell_range: List[Cell] = sheet.range(range_start + ":" + range_end)

        for cell in cell_range:
            cell.value = ""

        sheet.update_cells(cell_range)
        return


    def del_application(self, record_id: int, conference_id: int, param: dict) -> Application:
        """
        Осуществляет удаление записи из таблицы
        """
        sheet: Worksheet = self.__find_application_sheet(conference_id)
        record: Optional[Application] = self.__find_record_by_id(record_id, sheet)

        if record:
            if (param.get("telegram_id", None) == record.telegram_id) and record.telegram_id is not None:
                ...
            elif (param.get("discord_id", None) == record.discord_id) and record.discord_id is not None:
                ...
            elif (param.get("email", None) == record.email) and record.email is not None:
                ...
            else:
                raise HTTPException(status_code=404, detail="Record not found")
            
            self.__del_val(record_id, sheet)
            return record
        else:
            raise HTTPException(status_code=404, detail="Record not found")


    def find_application_by_field(self, conference_id: int, param: dict) -> List[Application]:
        """
        Осуществляет обновление записи в таблице
        """
        sheet: Worksheet = self.__find_application_sheet(conference_id)

        if param.get("telegram_id", None) and param.get("discord_id", None) is None and param.get("email", None) is None:
            records_coords: List[Optional[Cell]] = sheet.findall(query=param.get("telegram_id"), in_column=2)
        elif param.get("discord_id", None) and param.get("telegram_id", None) is None and param.get("email", None) is None:
            records_coords: List[Optional[Cell]] = sheet.findall(query=param.get("discord_id"), in_column=3)
        elif param.get("email", None) and param.get("telegram_id", None) is None and param.get("discord_id", None) is None:
            records_coords: List[Optional[Cell]] = sheet.findall(query=param.get("email"), in_column=6)
        else:
            raise HTTPException(status_code=403, detail="Query parameters are incorrect")

        if records_coords:
            records: List[Optional[str]] = [sheet.row_values(coord.row) for coord in records_coords]
            records = self.__parse_records(records, sheet)
            return records
        else:
            raise HTTPException(status_code=404, detail="Applications not found")
