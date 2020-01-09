from mrjob.job import MRJob, MRStep
import re

# two map and reduce

word_regex = re.compile(r"[\w']+")

class WordCountWithRegexOrderMR(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words),
            MRStep(mapper = self.mapper_make_counts_key, reducer = self.reducer_output_words)
        ]
    
    def mapper_get_words(self, _, line):
        words = word_regex.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer_count_words(self, word, occurences):
        yield word, sum(occurences)
        
    def mapper_make_counts_key(self, word, count):
        yield '%04d'%int(count), word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word
   
        
if __name__ == '__main__':
    WordCountWithRegexOrderMR.run()
