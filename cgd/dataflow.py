from itertools import product, chain

class DFScan():

    def __init__(self, df):
        self.df = df
        self.N,_ = df.shape

    def __iter__(self):
        self.i = -1
        return self

    def __next__(self):
        if self.i + 1 == self.N:
            raise StopIteration()
        else:
            self.i += 1
            return (self.i , self.df.iloc[self.i,:])


class LiftingOperator():

    def __init__(self, dfscan, degree):
        self.dfscan = dfscan
        self.degree = degree

    def __iter__(self):
        self.iter = product(self.dfscan, repeat=self.degree)
        return self

    def __next__(self):
        return self.iter.__next__()



class ThetaJoin():

    def __init__(self, df1, df2, cond):
        self.dfs1 = DFScan(df1)
        self.dfs2 = DFScan(df2)
        self.cond = cond

    def __iter__(self):
        self.iter = product(self.dfs1, self.dfs2)
        return self

    def __next__(self):
        nxt = self.iter.__next__()

        tup = [j[1] for j in nxt]
        idx = [j[0] for j in nxt]

        if self.cond(*tup):
            return idx

        return self.__next__() 



class EqJoin():

    def __init__(self, df1, df2, lattrs, rattrs):
        self.dfs1 = DFScan(df1)
        self.dfs2 = DFScan(df2)

        self.lattrs = lattrs
        self.rattrs = rattrs

    
    def __iter__(self):
        hash_table = {}
        for idx, tup in self.dfs1:
            key = tuple(tup[self.lattrs])
            if key not in hash_table:
                hash_table[key] = []

            hash_table[key].append(idx)

        self.hash_table = hash_table
        self.keycount = 0

        #probe
        self.iter = iter(self.dfs2)
        self.nxtid, self.nxttup = self.iter.__next__()
        return self
    

    def __next__(self):
        
        key = tuple(self.nxttup[self.rattrs])

        if key not in self.hash_table:
            self.nxtid, self.nxttup = self.iter.__next__()
            self.keycount = 0
            return self.__next__() 

        probe = self.hash_table[key]

        if self.keycount < len(probe):
           pid = probe[self.keycount]
           self.keycount += 1
           return (self.nxtid, pid)
        else:
            self.nxtid, self.nxttup = self.iter.__next__()
            self.keycount = 0

        return self.__next__() 



class XOR():

    def __init__(self, viol1, viol2):
        self.viol1 = viol1
        self.viol2 = viol2

    def __iter__(self):
        self.iter = chain(product(self.viol1,-self.viol2),product(-self.viol1,self.viol2))
        return self

    def __next__(self):
        return self.iter.__next__()