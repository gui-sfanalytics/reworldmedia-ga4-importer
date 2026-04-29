"""Input validation utilities for security and data integrity."""
import re
from datetime import datetime
from typing import Optional, List


class ValidationError(ValueError):
    """Raised when input validation fails."""
    pass


def validate_bucket_name(bucket_name: str) -> str:
    """
    Validate GCS bucket name format.

    Rules:
    - 3-63 characters
    - Lowercase letters, numbers, hyphens, underscores, dots
    - Must start and end with letter or number

    Args:
        bucket_name: Bucket name to validate

    Returns:
        str: Validated bucket name

    Raises:
        ValidationError: If bucket name is invalid
    """
    if not bucket_name:
        raise ValidationError("Bucket name cannot be empty")

    if not isinstance(bucket_name, str):
        raise ValidationError("Bucket name must be a string")

    if len(bucket_name) < 3 or len(bucket_name) > 63:
        raise ValidationError("Bucket name must be 3-63 characters")

    pattern = r'^[a-z0-9][a-z0-9_\-\.]*[a-z0-9]$'
    if not re.match(pattern, bucket_name):
        raise ValidationError(
            "Bucket name must contain only lowercase letters, numbers, "
            "hyphens, underscores, and dots, and start/end with letter or number"
        )

    return bucket_name


def validate_file_name(file_name: str, allowed_extensions: Optional[List[str]] = None) -> str:
    """
    Validate file name format and prevent path traversal attacks.

    Args:
        file_name: File name to validate
        allowed_extensions: List of allowed extensions (e.g., ['.csv', '.json'])

    Returns:
        str: Validated file name

    Raises:
        ValidationError: If file name is invalid
    """
    if not file_name:
        raise ValidationError("File name cannot be empty")

    if not isinstance(file_name, str):
        raise ValidationError("File name must be a string")

    # Check for path traversal attempts
    if '..' in file_name or file_name.startswith('/'):
        raise ValidationError("File name contains invalid path components")

    # Check for absolute paths (Windows and Unix)
    if file_name.startswith('\\') or (len(file_name) > 1 and file_name[1] == ':'):
        raise ValidationError("File name cannot be an absolute path")

    # Validate extension if specified
    if allowed_extensions:
        if not any(file_name.endswith(ext) for ext in allowed_extensions):
            raise ValidationError(
                f"File must have one of these extensions: {', '.join(allowed_extensions)}"
            )

    return file_name


def validate_date_format(date_str: str, format_str: str = '%Y-%m-%d') -> datetime:
    """
    Validate and parse date string.

    Args:
        date_str: Date string to validate
        format_str: Expected date format (default: YYYY-MM-DD)

    Returns:
        datetime: Parsed datetime object

    Raises:
        ValidationError: If date is invalid
    """
    if not date_str:
        raise ValidationError("Date cannot be empty")

    if not isinstance(date_str, str):
        raise ValidationError("Date must be a string")

    try:
        return datetime.strptime(date_str, format_str)
    except ValueError as e:
        raise ValidationError(f"Invalid date format. Expected {format_str}: {str(e)}")


def validate_table_name(table_name: str) -> str:
    """
    Validate BigQuery table name.

    Rules:
    - Up to 1,024 characters
    - Letters, numbers, underscores
    - Cannot start with number or underscore

    Args:
        table_name: Table name to validate

    Returns:
        str: Validated table name

    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name:
        raise ValidationError("Table name cannot be empty")

    if not isinstance(table_name, str):
        raise ValidationError("Table name must be a string")

    if len(table_name) > 1024:
        raise ValidationError("Table name cannot exceed 1024 characters")

    pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
    if not re.match(pattern, table_name):
        raise ValidationError(
            "Table name must start with a letter and contain only "
            "letters, numbers, and underscores"
        )

    return table_name


def validate_project_id(project_id: str) -> str:
    """
    Validate GCP project ID format.

    Rules:
    - 6-30 characters
    - Lowercase letters, digits, and hyphens
    - Must start with a letter
    - Cannot end with a hyphen

    Args:
        project_id: Project ID to validate

    Returns:
        str: Validated project ID

    Raises:
        ValidationError: If project ID is invalid
    """
    if not project_id:
        raise ValidationError("Project ID cannot be empty")

    if not isinstance(project_id, str):
        raise ValidationError("Project ID must be a string")

    if len(project_id) < 6 or len(project_id) > 30:
        raise ValidationError("Project ID must be 6-30 characters")

    pattern = r'^[a-z][a-z0-9\-]*[a-z0-9]$'
    if not re.match(pattern, project_id):
        raise ValidationError(
            "Project ID must start with a letter, end with a letter or digit, "
            "and contain only lowercase letters, digits, and hyphens"
        )

    return project_id


def validate_dataset_id(dataset_id: str) -> str:
    """
    Validate BigQuery dataset ID format.

    Rules:
    - Up to 1,024 characters
    - Letters, numbers, underscores
    - Can start with letter or underscore

    Args:
        dataset_id: Dataset ID to validate

    Returns:
        str: Validated dataset ID

    Raises:
        ValidationError: If dataset ID is invalid
    """
    if not dataset_id:
        raise ValidationError("Dataset ID cannot be empty")

    if not isinstance(dataset_id, str):
        raise ValidationError("Dataset ID must be a string")

    if len(dataset_id) > 1024:
        raise ValidationError("Dataset ID cannot exceed 1024 characters")

    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
    if not re.match(pattern, dataset_id):
        raise ValidationError(
            "Dataset ID must start with a letter or underscore and contain only "
            "letters, numbers, and underscores"
        )

    return dataset_id


def validate_property_id(property_id: str) -> str:
    """
    Validate GA4 property ID format.

    Property IDs are numeric strings.

    Args:
        property_id: GA4 property ID to validate

    Returns:
        str: Validated property ID

    Raises:
        ValidationError: If property ID is invalid
    """
    if not property_id:
        raise ValidationError("Property ID cannot be empty")

    if not isinstance(property_id, str):
        raise ValidationError("Property ID must be a string")

    if not property_id.isdigit():
        raise ValidationError("Property ID must contain only digits")

    if len(property_id) < 5 or len(property_id) > 15:
        raise ValidationError("Property ID must be 5-15 digits")

    return property_id


def sanitize_string(input_str: str, max_length: int = 1000) -> str:
    """
    Sanitize user input string by removing potentially dangerous characters.

    Args:
        input_str: String to sanitize
        max_length: Maximum allowed length

    Returns:
        str: Sanitized string

    Raises:
        ValidationError: If input is invalid
    """
    if not isinstance(input_str, str):
        raise ValidationError("Input must be a string")

    # Remove null bytes
    sanitized = input_str.replace('\x00', '')

    # Limit length
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]

    return sanitized.strip()


def validate_config(config_obj) -> None:
    """
    Perform a comprehensive validation of the configuration object.

    Args:
        config_obj: The configuration object to validate (usually appconfig.config)

    Raises:
        ValidationError: If any configuration value is invalid
    """
    try:
        validate_project_id(config_obj.CLIENT_PROJECT_ID)
        validate_project_id(config_obj.SERVICE_PROJECT_ID)
        validate_dataset_id(config_obj.CLIENT_DATASET_ID)
        validate_property_id(config_obj.PROPERTY_IDS)
    except ValidationError as e:
        raise ValidationError(f"Configuration validation failed: {str(e)}")

