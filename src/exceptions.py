class AuditFailedError(Exception):
    @property
    def error_type(self) -> str:
        return "Audit Failed"


class MissingHeaderError(Exception):
    @property
    def error_type(self) -> str:
        return "Missing Header"


class MissingColumnsError(Exception):
    @property
    def error_type(self) -> str:
        return "Missing Columns"


class ValidationThresholdExceededError(Exception):
    @property
    def error_type(self) -> str:
        return "Validation Threshold Exceeded"
