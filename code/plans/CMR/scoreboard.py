'''
Created on May 8, 2020

@author: Mania Abdi
'''

class ScoreBoardEntry:
    def __init__(self, size):
        '''
        size: 
        '''
        self.maximum = size;
        self.occupied = 0;
        
    def inject(self, size):
        '''
        size: the amount that should be added to the score board
        return: the amount that couldn't be added to the score board.
        '''
        remain = self.maximum - self.occupied
        
        if remain >= size:
            self.occupied += size; 
            return 0;
        
        self.occupied = self.maximum;
        return size - remain;
        

class ScoreBoardStat:
    def __init__(self, size):
        self.maximum = size;
        self.ondemand = 0;
        self.prefetch = 0;
        
    def inject_or_assert(self, remote_read):
        if remote_read > (self.maximum - self.ondemand):
            raise NameError('The remote read requires more time than stage size')
        self.ondemand += remote_read;
        
    def inject(self, prefetch_read): 
        remain = self.maximum - (self.ondemand + self.prefetch)
        
        if remain >= prefetch_read:
            self.prefetch += prefetch_read; 
            return 0;
        
        self.prefetch = self.maximum - self.ondemand;
        return prefetch_read - self.prefetch;
        

class ScoreBoard:
    def __init__(self, exect, bandwidth):
        '''
        bandwidth = X gbps
        scoreboard units = 1sec x Xbyte
        '''
        self.initialize(exect, bandwidth)
        self.sb_stat_cp = self.sb_stat; # keep a check point 
    
    def initialize(self, exect, bandwidth):
        self.sb = [] # score board
        self.exect = exect;
        self.sb_entry_size = bandwidth/8 # number of bytes in bandwidth
                
        # build the score board array
        for tid in range(0, exect):
            self.sb.append(ScoreBoardEntry(self.sb_entry_size))
            
        sb_size = self.exect*self.sb_entry_size;
        self.sb_stat = ScoreBoardStat(sb_size)
        
    
    def transform(self, exect, bandwidth):
        self.sb_stat_cp = self.sb_stat; # check point the status
        self.initialize(exect, bandwidth)
        
    
    def inject_or_assert(self, remote_read):        
        self.sb_stat.inject_or_assert(remote_read)
        
        ''' try to fit the already scheduled prefetch in here '''
        return self.inject(self.sb_stat_cp.prefetch)
        
    def inject(self, prefetch_read):
        return self.sb_stat.inject(prefetch_read)
    