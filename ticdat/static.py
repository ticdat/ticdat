"""
Provides assistance for hard coded ticDat objects.
"""

import ticdat._private.utils as _utls
from ticdat._private.utils import verify, freezableFactory, FrozenDict, doIt

class _TicDat(_utls.freezableFactory(object, "_isFrozen")) :
    def _freeze(self):
        if getattr(self, "_isFrozen", False) :
            return
        for t in getattr(self, "_tables", {}) :
            for v in getattr(self, t).values() :
                v._dataFrozen =True
                v._attributesFrozen = True
            getattr(self, t)._dataFrozen  = True
            getattr(self, t)._attributesFrozen = True
        self._isFrozen = True

class TicDatFactory(freezableFactory(object, "_isFrozen")) :
    def __init__(self, primaryKeyFields = {}, dataFields = {}):
        primaryKeyFields, dataFields = _utls.checkSchema(primaryKeyFields, dataFields)

        assert set(dataFields).issubset(primaryKeyFields), "this code assumes all tables have primary keys"
        dataRowFactory = FrozenDict({t : _utls.ticDataRowFactory(t, primaryKeyFields[t], dataFields.get(t, ()))
                            for t in primaryKeyFields})

        class FrozenTicDat(_TicDat) :
            def __init__(self, **initTables):
                for t in initTables :
                    verify(t in set(primaryKeyFields).union(dataFields), "Unexpected table name %s"%t)
                for t,v in initTables.items():
                    badTicDatTable = []
                    if not (_utls.goodTicDatTable(v, lambda x : badTicDatTable.append(x))) :
                        raise _utls.TicDatError(t + " is an improper ticDat table : " + badTicDatTable[-1])
                    if v:
                        verify(len(v.keys()[0]) == len(primaryKeyFields.get(t, ())) or
                               (_utls.stringish(v.keys()[0]) and len(primaryKeyFields.get(t, ())) == 1),
                           "Unexpected number of primary key fields for %s"%t)
                        verify(set(v.values()[0]) == set(dataFields.get(t, {})),
                           "Unexpected data fields for %s"%t)
                    setattr(self, t, FrozenDict({_k : dataRowFactory[t](_v) for _k, _v in v.items()}))
                    for v in getattr(self, t).values() :
                        v._dataFrozen =True
                        v._attributesFrozen = True
                for t in set(primaryKeyFields).union(dataFields).difference(initTables) :
                    setattr(self, t, FrozenDict())
                self._isFrozen = True
            def __repr__(self):
                return "td:" + tuple(set(primaryKeyFields).union(dataFields).keys()).__repr__()
        self.FrozenTicDat = FrozenTicDat

        self._isFrozen = True


def datDictFactory(dataRowFactory) :
    class StaticTableDict (_utls.FreezeableDict) :
        def __setitem__(self, key, value):
            verify(_utls.dictish(value), "the values of a TableDict should all be parallel dictionaries")
            return super(StaticTableDict, self).__setitem__(key, dataRowFactory(value))




def freezeMe(x) :
    """
    Freezes a
    :param x: ticDat object
    :return: x, after it has been frozen
    """
    if not getattr(x, "_isFrozen", True) : #idempotent
        x._freeze()
    return x