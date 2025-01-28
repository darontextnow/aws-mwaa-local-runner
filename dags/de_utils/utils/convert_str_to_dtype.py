from datetime import datetime


def convert_str_to_dtype(val: str) -> any:
    """Returns best guess conversion of a string to appropriate Python type.
    Useful for when a resource like Snowflake returns a string that should be a more specific dtype.
    """
    if val.lower() in ["null", "none"]:
        return None

    if val.lower() in ["true", "false"]:
        return val.lower() == "true"

    try:
        return int(val)
    except (ValueError, TypeError):
        pass

    try:
        return float(val)
    except (ValueError, TypeError):
        pass

    import decimal
    try:
        return decimal.Decimal(val)
    except decimal.InvalidOperation:
        pass

    try:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f %Z")
    except (ValueError, TypeError):
        try:
            return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, TypeError):
            try:
                return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                try:
                    return datetime.strptime(val, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    pass

    return val  # if all castings above fail, return orig value
