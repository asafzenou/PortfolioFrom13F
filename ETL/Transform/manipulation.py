from typing import List, Dict, Any


def transform_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Apply standard transformations to extracted records.
    Normalize field names, types, and clean data.
    """
    transformed = []

    for record in records:
        # Normalize record
        normalized = {}

        for key, value in record.items():
            # Normalize key names
            norm_key = key.lower().replace(" ", "_").strip()

            # Clean value
            if isinstance(value, str):
                normalized[norm_key] = value.strip()
            else:
                normalized[norm_key] = value

        transformed.append(normalized)

    return transformed
