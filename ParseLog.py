import re
import datetime
import logging

from pyspark.sql import Row

month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May':5, 'Jun': 6, 'Jul': 7,
    'Aug': 8,  'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}

IIS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

# IIS log Example :
#10.16.14.8 - - [02/Jun/2016:00:00:02 -0700] "GET /f5up/default.aspx HTTP/0.9" 200 693

def parse_IIS_time(s):
    """
    Convert IIS time format into Python datetime object
    :param s: string date and time in IIS time format
    :return: datetime object
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))

def parseIISLogLine(logline):
    """
    Parse IIS log lines
    :param logline: log line in string format
    :return:
    """

    match = re.search(IIS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    #host = match.group(1)
    #client_identd = match.group(2)
    #user_id = match.group(3)
    #date_time = parse_IIS_time(match.group(4))
    #method = match.group(5)
    #endpoint = match.group(6)
    #protocol = match.group(7)
    #response_code = int(match.group(8))
    #content_size = size

    #return(host, client_identd, user_id, date_time, method, endpoint, protocol, response_code, content_size)

    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_IIS_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)