from datetime import datetime


def parse_time_ymdhis(line):
    """
    Parse time from a string.

    :param line: String to parse
    :return: DateTime
    """
    return datetime.strptime(line.strip(), '%Y-%m-%d %H:%M:%S')