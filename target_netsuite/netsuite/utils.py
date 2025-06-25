import re

def clean_logs(log):
    return re.sub(r"\s+", " ",str(log)).replace("\n", "")

def stringify_number(num):
    if isinstance(num, float) and num.is_integer():
        return str(int(num))
    else:
        return str(num)

