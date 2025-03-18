import re

def clean_logs(log):
    return re.sub(r"\s+", " ",str(log)).replace("\n", "")

