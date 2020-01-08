from mrjob.job import MRJob

class MovieEvaluateMR(MRJob):
    def mapper(self, key, line):
        (ID, ID_MOVIE, rating, Timestamp) = line.split('\t')
        yield rating, 1
    
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)
        
if __name__ == '__main__':
    MovieEvaluateMR.run()
