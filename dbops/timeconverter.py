from dateutil import tz
from pyrfc3339 import generate, parse
from datetime import datetime


def rfc3339_to_unix(rfc3339):
    """ Convert a RFC3339 format timestamp to a unix timestamp """
    return int(parse(rfc3339).astimezone(tz.tzlocal()).strftime('%s'))


def unix_to_rfc3339(unix):
    """ Convert a Unix timestamp to a RFC3339 timesatmp"""
    return generate(datetime.fromtimestamp(unix).replace(tzinfo=tz.tzlocal()))
