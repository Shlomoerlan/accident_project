def safe_int(value) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0