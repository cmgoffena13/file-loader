import re

from pydantic import EmailStr, Field, field_validator
from pydantic_extra_types.pendulum_dt import Date

from src.sources.base import CSVSource, TableModel

# Cache compiled regex patterns for performance
_PHONE_CLEAN_PATTERN = re.compile(r"[^\d+]")


class Customer(TableModel):
    customer_id: str = Field(alias="Customer Id", max_length=50)
    first_name: str = Field(alias="First Name", max_length=100)
    last_name: str = Field(alias="Last Name", max_length=100)
    company_name: str = Field(alias="Company", max_length=100)
    city: str = Field(max_length=100)
    country: str = Field(max_length=100)
    phone_one: str = Field(alias="Phone 1", max_length=25)
    phone_two: str = Field(alias="Phone 2", max_length=25)
    email: EmailStr = Field(max_length=100)
    subscription_date: Date = Field(alias="Subscription Date")
    website: str = Field(max_length=100)

    @field_validator("phone_one", "phone_two", mode="before")
    @classmethod
    def clean_phone(cls, v):
        """Clean phone number by removing common formatting characters."""
        if not isinstance(v, str):
            return v
        # Use pre-compiled regex pattern (much faster than re.sub() with string pattern)
        cleaned = _PHONE_CLEAN_PATTERN.sub("", v.strip())
        return cleaned if cleaned else v

    @field_validator("email", mode="before")
    @classmethod
    def clean_email(cls, v):
        """Clean email by trimming whitespace and lowercasing."""
        if not isinstance(v, str):
            return v
        return v.strip().lower()


CUSTOMERS = CSVSource(
    file_pattern="customers-*.csv",
    source_model=Customer,
    table_name="customers",
    grain=["customer_id"],
    delimiter=",",
    encoding="utf-8",
    skip_rows=0,
)
