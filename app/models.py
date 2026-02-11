from pydantic import BaseModel, EmailStr, Field
from datetime import datetime
from typing import Optional

class User(BaseModel):
    user_id: str = None
    full_name: str = Field(..., min_length=2)
    email: EmailStr
    age: int = Field(..., min=13, max=120)
    phone: Optional[str] = Field(..., min_length=9, max_length=15)
    city: Optional[str] = Field(..., min_length=2)
    create_at: datetime = None