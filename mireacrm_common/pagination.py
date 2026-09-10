"""Курсорная пагинация.

Смещения не используем: списки пополняются во время листания, и offset
начинает пропускать записи. Курсор кодирует последний ключ сортировки.
"""

import base64
import uuid

from mireacrm_common.errors import InvalidArgumentError

_SEPARATOR = "\x00"


def encode_cursor(sort_key: str, ident: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(f"{sort_key}{_SEPARATOR}{ident}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, uuid.UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        sort_key, separator, ident = raw.partition(_SEPARATOR)
        if not separator:
            raise ValueError("нет разделителя")
        return sort_key, uuid.UUID(ident)
    except (ValueError, UnicodeDecodeError) as exc:
        raise InvalidArgumentError("некорректный курсор") from exc
