from mrjob.job import MRJob

class FriendsAgeMR(MRJob):
    def mapper(self, _, line):
        (ID, name, age, nFriends) = line.split(',')
        yield age, float(nFriends)
    
    def reducer(self, age, nFriends):
        count = 0
        total = 0
        for x in nFriends:
            count += 1
            total += x
        
        yield age, (total / count)
        
if __name__ == '__main__':
    FriendsAgeMR.run()
