from itertools import product

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
