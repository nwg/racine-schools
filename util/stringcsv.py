from io import StringIO
import csv

class StringDictWriter:
    def __init__(self, *args, **kwargs):
        self.stringio = StringIO()
        self.dict_reader = csv.DictWriter(self.stringio, *args, **kwargs)
        print(f'created DictWriter with args={args} kwargs={kwargs}')
    def getrow(self, d):
        self.dict_reader.writerow(d)
        self.stringio.seek(0)
        s = self.stringio.readline()
        self.stringio.seek(0)
        self.stringio.truncate()
        return s
