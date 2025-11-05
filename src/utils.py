import logging
from typing import Dict

from src.readers.base_reader import BaseReader
from src.sources.base import DataSource

logger = logging.getLogger(__name__)


def create_field_mapping(reader: BaseReader) -> Dict[str, str]:
    """Create a mapping from field aliases/lowercase names to actual field names."""
    field_mapping = {}
    for field_name, field_info in reader.source.source_model.model_fields.items():
        if field_info.alias:
            field_mapping[field_info.alias.lower()] = field_name
        else:
            field_mapping[field_name.lower()] = field_name
    return field_mapping


def create_reverse_field_mapping(reader: BaseReader) -> Dict[str, str]:
    reverse_mapping = {}
    for field_name, field_info in reader.source.source_model.model_fields.items():
        if field_info.alias:
            reverse_mapping[field_name] = field_info.alias
        else:
            reverse_mapping[field_name] = field_name
    return reverse_mapping


def get_field_alias(source: DataSource, field_name: str) -> str:
    """Get the file column name (alias) for a field name.

    Returns the alias if it exists, otherwise returns the field name.
    """
    field_info = source.source_model.model_fields.get(field_name)
    if field_info and field_info.alias:
        return field_info.alias
    return field_name


def extract_failed_field_names(validation_error: any, grain: list[str]) -> set[str]:
    failed_field_names = set()
    if isinstance(validation_error, list):
        for error in validation_error:
            if isinstance(error, dict) and error.get("loc"):
                # Get the last element of loc (field name)
                field_name = str(error["loc"][-1]) if len(error["loc"]) > 0 else None
                if field_name:
                    failed_field_names.add(field_name)
    elif isinstance(validation_error, dict):
        if validation_error.get("loc"):
            field_name = (
                str(validation_error["loc"][-1])
                if len(validation_error["loc"]) > 0
                else None
            )
            if field_name:
                failed_field_names.add(field_name)
    # Include grain fields for record identification
    failed_field_names.update(grain)
    return failed_field_names


def extract_validation_error_message(
    validation_error: any, reverse_field_mapping: Dict[str, str]
) -> str:
    """Extract all error messages from validation error (list, dict, or string).

    Returns a string representation of a list of error dictionaries:
    [{column_name: quantity, column_value: not_a_number, error_type: int_parsing, error_msg: ...}]
    """
    if isinstance(validation_error, list) and len(validation_error) > 0:
        error_dicts = []
        for error in validation_error:
            if isinstance(error, dict):
                error_dict = {}
                # column_name: last element of loc (field name) converted to file column name (alias)
                if error.get("loc"):
                    field_name = (
                        str(error["loc"][-1]) if len(error["loc"]) > 0 else "unknown"
                    )
                    # Convert field name to file column name (alias) using reverse mapping
                    column_name = reverse_field_mapping.get(field_name, field_name)
                    error_dict["column_name"] = column_name
                # column_value: input value
                if error.get("input") is not None:
                    error_dict["column_value"] = error["input"]
                # error_type: error type
                if error.get("type"):
                    error_dict["error_type"] = error["type"]
                # error_msg: error message (lowercased)
                if error.get("msg"):
                    error_dict["error_msg"] = error["msg"].lower()
                error_dicts.append(error_dict)
            else:
                error_dicts.append({"error_msg": str(error).lower()})

        # Format as string: [{key: value, key: value}]
        if error_dicts:
            formatted_errors = []
            for error_dict in error_dicts:
                parts = [f"{k}: {v}" for k, v in error_dict.items()]
                formatted_errors.append("{" + ", ".join(parts) + "}")
            return "[" + ", ".join(formatted_errors) + "]"
        return "[]"
    elif isinstance(validation_error, dict):
        error_dict = {}
        if validation_error.get("loc"):
            field_name = (
                str(validation_error["loc"][-1])
                if len(validation_error["loc"]) > 0
                else "unknown"
            )
            # Convert field name to file column name (alias) using reverse mapping
            column_name = reverse_field_mapping.get(field_name, field_name)
            error_dict["column_name"] = column_name
        if validation_error.get("input") is not None:
            error_dict["column_value"] = validation_error["input"]
        if validation_error.get("type"):
            error_dict["error_type"] = validation_error["type"]
        if validation_error.get("msg"):
            error_dict["error_msg"] = validation_error["msg"].lower()

        if error_dict:
            parts = [f"{k}: {v}" for k, v in error_dict.items()]
            return "[{" + ", ".join(parts) + "}]"
        return "[{}]"
    else:
        return f"[{{error_msg: {str(validation_error).lower()}}}]"
