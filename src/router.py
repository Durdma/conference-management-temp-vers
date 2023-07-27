from datetime import datetime
from typing import Optional, Annotated
from fastapi import APIRouter, HTTPException, status, Depends

from src.schema import Application, ApplicationCreate, ApplicationDelete, ApplicationUpdate
from src.utility import add_record_v4, check_date, del_record_by_id, find_application_sheet, find_record_by_tg_ds_or_email, open_by_key, update_record_by_id, update_record_by_id_v2
from src.utility_v2 import TableConnector

router = APIRouter()


@router.get("/")
def read_root():
    return {"Hello": "World"}


@router.post("/conferences/{conference_id}/applications", response_model=Application, status_code=status.HTTP_201_CREATED)
def create_application(conference_id: int, application: ApplicationCreate, tbc: Annotated[TableConnector, Depends(TableConnector)]):
    return tbc.add_application(conference_id=conference_id, record=application)


# @router.post("/conferences/{conference_id}/applications", response_model=Application, status_code=status.HTTP_201_CREATED)
# def create_application(conference_id: int, application: ApplicationCreate):
#     conference_sheet = open_by_key("1KZ6rndDzUMhqTY6eWvDD9khaLh65xnWHXh6hGmckQ1M")
#     application_info = find_application_sheet(conference_id, conference_sheet)
#     application_sheet = open_by_key(application_info.get("sheet_id"))

#     if check_date(datetime.now().astimezone().isoformat(), application_info.get("registration_start_date"), application_info.get("registration_end_date")):
#         return add_record_v4(application, application_sheet)
    
#     raise HTTPException(status_code=500, detail="Internal error in create_application")


@router.patch("/conferences/{conference_id}/applications/{application_id}", response_model=Application, status_code=status.HTTP_200_OK)
def update_application(conference_id: int, application_id: int, data: ApplicationUpdate, tbc: Annotated[TableConnector, Depends(TableConnector)]):
    return tbc.update_application(record_id=application_id, conference_id=conference_id, body=data)


# @router.patch("/conferences/{conference_id}/applications/{application_id}", response_model=Application, status_code=status.HTTP_200_OK)
# def update_application(conference_id: int, application_id: int, data: ApplicationUpdate):
#     conference_sheet = open_by_key("1KZ6rndDzUMhqTY6eWvDD9khaLh65xnWHXh6hGmckQ1M")
#     application_info = find_application_sheet(conference_id, conference_sheet)
#     application_sheet = open_by_key(application_info.get("sheet_id"))

#     if check_date(datetime.now().astimezone().isoformat(), application_info.get("registration_start_date"), application_info.get("registration_end_date")):
#         return update_record_by_id_v2(application_id, data, application_sheet)
    
#     raise HTTPException(status_code=500, detail="Internal error in update_application")


@router.delete("/conferences/{conference_id}/applications/{application_id}", response_model=Application, status_code=status.HTTP_200_OK)
def delete_application(conference_id: int, application_id: int, data: ApplicationDelete, tbc: Annotated[TableConnector, Depends(TableConnector)]):
    return tbc.del_application(record_id=application_id, conference_id=conference_id, param=data.check_fields)


# @router.delete("/conferences/{conference_id}/applications/{application_id}", response_model=Application, status_code=status.HTTP_200_OK)
# def delete_application(conference_id: int, application_id: int, data: ApplicationDelete):
#     conference_sheet = open_by_key("1KZ6rndDzUMhqTY6eWvDD9khaLh65xnWHXh6hGmckQ1M")
#     application_info = find_application_sheet(conference_id, conference_sheet)
#     application_sheet = open_by_key(application_info.get("sheet_id"))

#     if check_date(datetime.now().astimezone().isoformat(), application_info.get("registration_start_date"), application_info.get("registration_end_date")):
#         return del_record_by_id(application_id, data.check_fields, application_sheet)
    
#     raise HTTPException(status_code=500, detail="Internal error in delete_application")
            

@router.get("/conferences/{conference_id}/applications", response_model=list[Application], status_code=status.HTTP_200_OK)
def get_applications(tbc: Annotated[TableConnector, Depends(TableConnector)], conference_id: int, telegram_id: Optional[str] = None, discord_id: Optional[str] = None, email: Optional[str] = None):
    return tbc.find_application_by_field(conference_id=conference_id, param={"telegram_id": telegram_id, "discord_id": discord_id, "email": email})


# @router.get("/conferences/{conference_id}/applications", response_model=list[Application], status_code=status.HTTP_200_OK)
# def get_applications(conference_id: int, telegram_id: Optional[str] = None, discord_id: Optional[str] = None, email: Optional[str] = None):
#     conference_sheet = open_by_key("1KZ6rndDzUMhqTY6eWvDD9khaLh65xnWHXh6hGmckQ1M")
#     application_info = find_application_sheet(conference_id, conference_sheet)
#     application_sheet = open_by_key(application_info.get("sheet_id"))

#     if check_date(datetime.now().astimezone().isoformat(), application_info.get("registration_start_date"), application_info.get("registration_end_date")):
#         if telegram_id and not discord_id and not email:
#             return find_record_by_tg_ds_or_email({"telegram_id": telegram_id}, application_sheet)
#         elif discord_id and not telegram_id and not email:
#             return find_record_by_tg_ds_or_email({"discord_id": discord_id}, application_sheet)
#         elif email and not discord_id and not telegram_id:
#             return find_record_by_tg_ds_or_email({"email": email}, application_sheet)
#         else:
#             raise HTTPException(status_code=403, detail="Query parameters are incorrect")
