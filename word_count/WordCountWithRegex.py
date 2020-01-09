from mrjob.job import MRJob
import re

word_regex = re.compile(r"[\w']+")

class WordCountWithRegexMR(MRJob):
    
    def mapper(self, _, line):
        words = word_regex.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == '__main__':
    WordCountWithRegexMR.run()
