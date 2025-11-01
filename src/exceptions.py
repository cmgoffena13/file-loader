class AuditFailedError(Exception):
    error_type = "Audit Failed"


class MissingHeaderError(Exception):
    error_type = "Missing Header"


class MissingColumnsError(Exception):
    error_type = "Missing Columns"


class ValidationThresholdExceededError(Exception):
    error_type = "Validation Threshold Exceeded"
