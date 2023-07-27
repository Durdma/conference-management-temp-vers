from datetime import datetime
from typing import List, Dict, Optional, Union
import re

from fastapi import HTTPException

from pydantic_core.core_schema import FieldValidationInfo
from pydantic import BaseModel, EmailStr, Field, ValidationError, validator, field_validator, root_validator
from pydantic_extra_types.phone_numbers import PhoneNumber

from src.constants import Coauthors
from src.validators import adviser_validate, app_role_validate, coauthors_validate, fio_validate, id_validate, submitted_timemark, telephone_number_validate, updated_timemark


class ApplicationBase(BaseModel):
    telegram_id: Optional[str] = None
    discord_id: Optional[str] = None
    submitted_at: Optional[str] = datetime.now().astimezone().isoformat()
    updated_at: Optional[str] = datetime.now().astimezone().isoformat()
    email: EmailStr
    phone: Optional[PhoneNumber] = None
    name: str
    surname: str
    patronymic: str
    university: str
    student_group: Optional[str] = None
    application_role: str
    title: str
    adviser: str
    coauthors: Coauthors = None
    
    tg_id_validation = field_validator("telegram_id")(id_validate)
    ds_id_validation = field_validator("discord_id")(id_validate)
    phone_validation = field_validator("phone")(telephone_number_validate)
    name_validation = field_validator("name")(fio_validate)
    surname_validation = field_validator("surname")(fio_validate)
    patronymic_validation = field_validator("patronymic")(fio_validate)
    adviser_validation = field_validator("adviser")(adviser_validate)
    coauthors_validation = field_validator("coauthors")(coauthors_validate)
    application_role_validate = field_validator("application_role")(app_role_validate)
        

class ApplicationCreate(ApplicationBase):
    creation_time = field_validator("submitted_at")(updated_timemark)
    update_time = field_validator("updated_at")(updated_timemark)


class Application(ApplicationBase):
    id: int

    creation_time = field_validator("submitted_at")(submitted_timemark)
    update_time = field_validator("updated_at")(submitted_timemark)


class ApplicationUpdate(ApplicationBase):
    id: Optional[int] = None
    telegram_id: Optional[str] = None
    discord_id: Optional[str] = None
    submitted_at: Optional[str] = None
    updated_at: Optional[str] = datetime.now().astimezone().isoformat()
    email: Optional[EmailStr] = None
    phone: Optional[PhoneNumber] = None
    name: Optional[str] = None
    surname: Optional[str] = None
    patronymic: Optional[str] = None
    university: Optional[str] = None
    student_group: Optional[str] = None
    application_role: Optional[str] = None
    title: Optional[str] = None
    adviser: Optional[str] = None
    coauthors: Optional[Coauthors] = None

    creation_time = field_validator("submitted_at")(submitted_timemark)
    update_time = field_validator("updated_at")(updated_timemark)


class ApplicationDelete(BaseModel):
    telegram_id: Optional[str] = None
    discord_id: Optional[str] = None
    email: Optional[EmailStr] = None

    tg_id_validation = field_validator("telegram_id")(id_validate)
    ds_id_validation = field_validator("discord_id")(id_validate)

    @property
    def check_fields(self):
        if self.telegram_id and not self.discord_id and not self.email:
            return {"telegram_id": self.telegram_id}
        elif self.discord_id and not self.telegram_id and not self.email:
            return {"discord_id": self.discord_id}
        elif self.email and not self.telegram_id and not self.discord_id:
            return {"email": self.email}
        elif not self.email and not self.discord_id and not self.telegram_id:
            raise HTTPException(status_code=422, detail="There are no fields to check")
        else:
            raise HTTPException(status_code=500, detail="There are more than 1 field")
