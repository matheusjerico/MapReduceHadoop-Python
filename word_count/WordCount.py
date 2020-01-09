from mrjob.job import MRJob

class WordCountMR(MRJob):
    
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1
    
    def reducer(self, word, occurences):
        yield word, sum(occurences)
        
if __name__ == '__main__':
    WordCountMR.run()
