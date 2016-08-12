class SizeFormatter:
    def __init__(self, unit=None, k=1024, prefix='', postfix=''):
        self.unit = unit
        self.prefix = prefix
        self.postfix = postfix
        self.k = k

    def format(self, x, p):
        return self.prefix + ''.join(self.format_size(x)) + self.postfix

    def format_size(self, value, unit=None):
        value = self.convert_to_unit(value, unit)
        if value < self.k:
            return str(int(value)), ''
        elif value < self.k ** 2:
            return str(int(float(value) / float(self.k ** 1))), 'K'
        elif value < self.k ** 3:
            return str(int(float(value) / float(self.k ** 2))), 'M'
        elif value < self.k ** 4:
            return str(int(float(value) / float(self.k ** 3))), 'G'
        elif value < self.k ** 5:
            return str(int(float(value) / float(self.k ** 4))), 'T'
        elif value < self.k ** 6:
            return str(int(float(value) / float(self.k ** 5))), 'P'

    def convert_to_unit(self, value, unit=None):
        if unit is None:
            return value
        if unit.lower() == 'k':
            return value * self.k ** 1
        if unit.lower() == 'm':
            return value * self.k ** 2
        if unit.lower() == 'g':
            return value * self.k ** 3
        if unit.lower() == 't':
            return value * self.k ** 4
        if unit.lower() == 'p':
            return value * self.k ** 5
        return value


class BytesFormatter(SizeFormatter):
    def __init__(self, unit=None, k=1024, prefix='', postfix=''):
        self.unit = unit
        self.prefix = prefix
        self.postfix = 'B' + postfix
        self.k = k


class BitsFormatter(SizeFormatter):
    def __init__(self, unit=None, k=1000, prefix='', postfix=''):
        self.unit = unit
        self.prefix = prefix
        self.postfix = 'b' + postfix
        self.k = k
