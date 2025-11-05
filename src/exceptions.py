class GrainValidationError(Exception):
    error_type = "Grain Validation Error"


class AuditFailedError(Exception):
    error_type = "Audit Failed"


class MissingHeaderError(Exception):
    error_type = "Missing Header"


class MissingColumnsError(Exception):
    error_type = "Missing Columns"


class ValidationThresholdExceededError(Exception):
    error_type = "Validation Threshold Exceeded"


# File-specific errors that should not be retried and are handled via email notifications
FILE_ERROR_EXCEPTIONS = {
    MissingHeaderError,
    MissingColumnsError,
    ValidationThresholdExceededError,
    AuditFailedError,
    GrainValidationError,
}
