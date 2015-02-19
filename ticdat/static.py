"""
Provides assistance for hard coded ticDat objects.
"""

import ticdat._private.utils as _utls

# I think we need a creator object that can be called with a schema

def datDictFactory(dataRowFactory) :
    class StaticTableDict (_utls.FreezeableDict) :
        def __setitem__(self, key, value):
            assert _utls.dictish(value), "the values of a TableDict should all be parallel dictionaries"
            return super(StaticTableDict, self).__setitem__(key, dataRowFactory(value))

class _TicDat(_utls.freezableFactory(object, "_isFrozen")) :
    def _freeze(self):
        pass


class FrozenTicDat(_TicDat) :
    def __init__(self, **initTables):
        assert not any(x.startswith("_") for x in initTables), "avoid starting table names with underscore"
        for t,v in initTables:
            badTicDatTable = []
            assert _utls.goodTicDatTable(v, lambda x : badTicDatTable.append(x)), \
                t + " is an improper ticDat table : " + badTicDatTable[-1]
            setattr()

def freezeMe(x) :
    """
    Freezes a
    :param x: ticDat object
    :return: x, after it has been frozen
    """
    if not getattr(x, "_isFrozen", True) : #idempotent
        x._freeze()
    return x