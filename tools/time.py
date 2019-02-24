import time
from datetime import timedelta, datetime


def quantize_time(duration: timedelta, offset: timedelta=timedelta(), round_up=False):
    secs = datetime.utcnow().timestamp()

    o = offset.total_seconds()
    d = duration.total_seconds()

    secs = int((secs - o) / d) * d + o  # quantize

    if round_up:
        secs += d

    return datetime.fromtimestamp(secs)
