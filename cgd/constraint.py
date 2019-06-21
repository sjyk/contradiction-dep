from inspect import signature
from itertools import product
from .dataflow import *

class Constraint(object):

    def __init__(self):
        self.degree = 0
        pass

    def __getitem__(self, df):
        return self._get_violations(df)

    def _get_violations(self,df):
        raise NotImplemented("You must implement a _get_violations function")


class Violation():

    def __init__(self, viols, N):
        self.viols = viols
        self.N = N
        self.data = viols
        for v in viols:
            break
        self.degree = len(v)

    def __iter__(self):
        return iter(self.data)

    def __neg__(self):
        cons_set = set()
        for t in product(range(self.N), repeat=self.degree):
            if t not in self.viols:
                cons_set.add(t)
        return Violation(cons_set, self.N)



class EmbeddedDependency(Constraint):

    def __init__(self, pred, impl, suffix=""):
        sig = signature(pred)
        params = sig.parameters
        self.degree = len(params)
        self.suffix = suffix

        if len(signature(impl).parameters) != self.degree:
            raise ValueError("The predicate and the implication must have the same number of arguments")

        self.pred = pred
        self.impl = impl

    def _filter_suffix(self, tup):
        columns = [col for col in tup.index if col.endswith(self.suffix)]
        projection = tup[columns]
        rename = {col:col[:len(col)-len(self.suffix)] for col in columns}
        tup.rename(rename)
        return tup

    def _get_violations(self,df):

        viol_ids = set()

        for i in LiftingOperator(DFScan(df), self.degree):

            tup = [self._filter_suffix(j[1]) for j in i]
            idx = [j[0] for j in i]

            if self.pred(*tup) and not self.impl(*tup):
                viol_ids.add(tuple(idx))

        return Violation(viol_ids, df.shape[0])
