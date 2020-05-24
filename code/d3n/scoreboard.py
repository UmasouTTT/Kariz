'''
Created on May 8, 2020

@author: Mania Abdi
'''
import datetime


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
        return prefetch_read - remain;

    def reset(self):
        self.ondemand = 0;
        self.prefetch = 0;

    def get_scheduled_prefetch(self):
        return self.prefetch


class ScoreBoard:
    def __init__(self, exect, bandwidth):
        '''
        bandwidth = X gbps
        scoreboard units = 1sec x Xbyte
        '''
        self.bigbang = datetime.datetime.now()
        self.checkpoint = self.bigbang
        self.outstanding_prefetch = 0

        self.exect = exect;
        self.sb_entry_size = bandwidth / 1  # it was 8 for now it should be 11 number of bytes in bandwidth

        # build the score board array
        self.sb = []  # score board
        for tid in range(0, exect):
            self.sb.append(ScoreBoardStat(self.sb_entry_size))


    def check_availability(self, prefetch):
        return True;

    def inject_or_assert(self, remote_read):
        self.sb_stat.inject_or_assert(remote_read)

        ''' try to fit the already scheduled prefetch in here '''
        return self.inject(self.sb_stat_cp.prefetch)

    def inject(self, prefetch_read):  # should get preferred end time
        now = datetime.datetime.now()
        t = int((now - self.bigbang).total_seconds())
        remaining = prefetch_read
        while True:
            remaining = self.sb[t].inject(remaining)
            t += 1
            if t == self.exect:
                raise NameError("Dude! it is Apocalyptic")

            if not remaining:
                break
        scheduled_prefetch = self.get_outstanding_prefetch_till(now)
        if self.outstanding_prefetch < scheduled_prefetch:
            raise NameError("Outstanding prefetch is smaller than scheduled")

        self.outstanding_prefetch -= scheduled_prefetch
        self.outstanding_prefetch += prefetch_read
        self.checkpoint = now

    def get_outstanding_prefetch_till(self, now):
        t_chkp = int((self.checkpoint - self.bigbang).total_seconds())
        t_now = int((now - self.bigbang).total_seconds())
        scheduled_prefetch = 0;

        for t in range(t_chkp, t_now):
            scheduled_prefetch += self.sb[t].get_scheduled_prefetch()
            #self.sb[t].reset()
        return scheduled_prefetch

    def get_outstanding_prefetch(self):
        t_now = int((datetime.datetime.now() - self.bigbang).total_seconds())
        scheduled_prefetch = 0;

        for t in range(t_now, self.exect):
            scheduled_prefetch += self.sb[t].get_scheduled_prefetch()
        return scheduled_prefetch

    def reset(self):
        print("Reset score board")

