import uuid
from datetime import datetime

Key = str
"""Key is a type alias for a string that represents a unique identifier for a record."""


def generate_tsid() -> Key:
    """Generates a unique identifier for a record.

    Returns:
        Key: A unique identifier for a record.
    """
    timestamp = str(datetime.utcnow().timestamp() * 1000)[:13]
    timestamp += "0" * (13 - len(timestamp))
    uid = uuid.uuid4().hex
    tsid = f"{timestamp}{uid}"
    return tsid
