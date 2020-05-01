from mrjob.job import MRJob

from mrjob.step import MRStep

import re

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        yield sum(line)



if __name__ == '__main__':
    MRWordFreqCount.run()