"""
Primary ticDat module. Client code can do the following.
-> Create a TicDatFactory from a schema (a listing of the primary key fields and data fields for each table)
->-> A TicDatFactory can then create frozen and editable ticDat data objects from a variety of data sources.
     These objects are functionally equivalent to the "attributes of dict of dicts" that are
     demonstrated in the simple example code.
-> Validate whether simple "dict of dicts" tables and "attribute collection of dict of dicts tables" objects are
   ticDat compliant. ticDat compliances simply refers common sense consistency. (I.e. the dictionary keys
   are consistent).
"""

# !!! KNOWN BUGS !!!!
# -> For xls file writing, None not being written out as NULL. Not sure how xlwd, xlwt is supposed to handle this?
# -> For csv file reading, all data is turned into floats if possible. So zip codes will be turned into floats
# !!! REMEMBER !!!! ticDat is supposed to facilitate rapid prototyping and port-ready solver engine
#                   development. The true fix for these cosmetic flaws is to use the Opalytics platform
#                   for industrial data and the ticDat library for cleaner, isolated development/testing.

import ticdat._private.utils as utils
from ticdat._private.utils import verify, freezableFactory, FrozenDict, FreezeableDict, doIt, dictish, containerish
from ticdat._private.utils import generatorish
import ticdat._private.xls as xls
import ticdat._private.csvtd as csv
import collections as clt
from itertools import izip_longest

def _keyLen(k) :
    if not utils.containerish(k) :
        return 1
    try:
        rtn = len(k)
    except :
        rtn = 0
    return rtn


class TicDatFactory(freezableFactory(object, "_isFrozen")) :
    """
    Primary class for ticDat library. This class is constructed with a schema (a listing of the primary key
    fields and data fields for each table). Client code can then read/write ticDat objects from a variety of data
    sources. Analytical code that that reads/writes from ticDat objects can then be used, without change,
    on different data sources, or on the Opalytics platform.
    """
    def __init__(self, primaryKeyFields = {}, dataFields = {}, generatorTables = ()):
        primaryKeyFields, dataFields = utils.checkSchema(primaryKeyFields, dataFields)
        self.primaryKeyFields, self.dataFields = primaryKeyFields, dataFields
        self.allTables = frozenset(set(self.primaryKeyFields).union(self.dataFields))
        verify(containerish(generatorTables) and set(generatorTables).issubset(self.allTables),
               "generatorTables should be a container of table names")
        verify(not any(primaryKeyFields.get(t) for t in generatorTables),
               "Can not make generators from tables with primary keys")
        dataRowFactory = FrozenDict({t : utils.ticDataRowFactory(t, primaryKeyFields.get(t, ()), dataFields.get(t, ()))
                            for t in self.allTables})
        self.generatorTables = frozenset(generatorTables)
        goodTicDatTable = self.goodTicDatTable
        superSelf = self
        class _TicDat(utils.freezableFactory(object, "_isFrozen")) :
            def _freeze(self):
                if getattr(self, "_isFrozen", False) :
                    return
                for t in superSelf.allTables :
                    _t = getattr(self, t)
                    if utils.dictish(_t) or utils.containerish(_t) :
                        for v in getattr(_t, "values", lambda : _t)() :
                            if not getattr(v, "_dataFrozen", False) :
                                v._dataFrozen =True
                                v._attributesFrozen = True
                            else : # we freeze the data-less ones off the bat as empties, easiest way
                                assert (len(v) == 0) and v._attributesFrozen
                        if utils.dictish(_t) :
                            _t._dataFrozen  = True
                            _t._attributesFrozen = True
                        elif utils.containerish(_t) :
                            setattr(self, t, tuple(_t))
                    else :
                        assert callable(_t) and t in superSelf.generatorTables
                self._isFrozen = True
            def __repr__(self):
                return "td:" + tuple(superSelf.allTables).__repr__()
        def ticDatTableFactory(tableName) :
            assert tableName not in self.generatorTables
            keyLen = len(self.primaryKeyFields.get(tableName, ()))
            if keyLen > 0 :
                class TicDatDict (FreezeableDict) :
                    def __setitem__(self, key, value):
                        verify(containerish(key) ==  (keyLen > 1) and (keyLen == 1 or keyLen == len(key)),
                               "inconsistent key length for %s"%tableName)
                        return super(TicDatDict, self).__setitem__(key,dataRowFactory[tableName](value))
                    def __getitem__(self, item):
                        if item not in self:
                            self[item] = dataRowFactory[tableName]({})
                        return super(TicDatDict, self).__getitem__(item)
                assert dictish(TicDatDict)
                return TicDatDict
            class TicDatDataList(clt.MutableSequence):
                def __init__(self, *_args):
                    self._list = list()
                    self.extend(list(_args))
                def __len__(self): return len(self._list)
                def __getitem__(self, i): return self._list[i]
                def __delitem__(self, i): del self._list[i]
                def __setitem__(self, i, v):
                    self._list[i] = dataRowFactory[tableName](v)
                def insert(self, i, v):
                    self._list.insert(i, dataRowFactory[tableName](v))
                def __repr__(self):
                    return "td:" + self._list.__repr__()
            assert containerish(TicDatDataList) and not dictish(TicDatDataList)
            return TicDatDataList
        def generatorFactory(data, tableName) :
            assert tableName in self.generatorTables
            def generatorFunction() :
                for row in (data if containerish(data) else data()):
                    yield dataRowFactory[tableName](row)
            return generatorFunction
        class TicDat(_TicDat) :
            def __init__(self, **initTables):
                for t in initTables :
                    verify(t in superSelf.allTables, "Unexpected table name %s"%t)
                for t,v in initTables.items():
                    badTicDatTable = []
                    if not (goodTicDatTable(v, t, lambda x : badTicDatTable.append(x))) :
                        raise utils.TicDatError(t + " cannot be treated as a ticDat table : " + badTicDatTable[-1])
                    if superSelf.primaryKeyFields.get(t) :
                     for _k in v :
                        verify((hasattr(_k, "__len__") and (len(_k) == len(primaryKeyFields.get(t, ())) > 1) or
                               len(primaryKeyFields.get(t, ())) == 1),
                           "Unexpected number of primary key fields for %s"%t)
                     # lots of verification inside the dataRowFactory
                     setattr(self, t, ticDatTableFactory(t)({_k : dataRowFactory[t](v[_k]
                                                            if utils.dictish(v) else ()) for _k in v}))
                    elif t in superSelf.generatorTables :
                        setattr(self, t, generatorFactory(v, t))
                    else :
                        setattr(self, t, ticDatTableFactory(t)(*v))
                for t in set(superSelf.allTables).difference(initTables) :
                    setattr(self, t, ticDatTableFactory(t)())
        self.TicDat = TicDat
        class FrozenTicDat(TicDat) :
            def __init__(self, **initTables):
                super(FrozenTicDat, self).__init__(**initTables)
                self._freeze()
        self.FrozenTicDat = FrozenTicDat
        if xls.importWorked :
            self.xls = xls.XlsTicFactory(self)
        if csv.importWorked :
            self.csv = csv.CsvTicFactory(self)

        self._isFrozen = True


    def goodTicDatObject(self, dataObj,  badMessageHandler = lambda x : None):
        """
        determines if an object can be can be converted to a TicDat data object.
        :param dataObj: the object to verify
        :param badMessageHandler: a call back function to receive description of any failure message
        :return: True if the dataObj can be converted to a TicDat data object. False otherwise.
        """
        rtn = True
        for t in self.allTables:
            if not hasattr(dataObj, t) :
                badMessageHandler(t + " not an attribute.")
                return False
            rtn = rtn and  self.goodTicDatTable(getattr(dataObj, t), t,
                    lambda x : badMessageHandler(t + " : " + x))
        return rtn

    def goodTicDatTable(self, dataTable, tableName, badMessageHandler = lambda x : None) :
        """
        determines if an object can be can be converted to a TicDat data table.
        :param dataObj: the object to verify
        :param tableName: the name of the table
        :param badMessageHandler: a call back function to receive description of any failure message
        :return: True if the dataObj can be converted to a TicDat data table. False otherwise.
        """
        if tableName not in self.allTables:
            badMessageHandler("%s is not a valid table name for this schema"%tableName)
            return False
        if tableName in self.generatorTables :
            assert not self.primaryKeyFields.get(tableName)
            verify((containerish(dataTable) or callable(dataTable)) and not dictish(dataTable),
                   "Expecting a container of rows or a generator function of rows for %s"%tableName)
            return self._goodDataRows(dataTable if containerish(dataTable) else dataTable(),
                                      tableName, badMessageHandler)
        if self.primaryKeyFields.get(tableName) :
            if utils.dictish(dataTable) :
                return self._goodTicDatDictTable(dataTable, tableName, badMessageHandler)
            if utils.containerish(dataTable):
                return  self._goodTicDatKeyContainer(dataTable, tableName, badMessageHandler)
        else :
            verify(utils.containerish(dataTable), "Unexpected ticDat table type for %s."%tableName)
            return self._goodDataRows(dataTable, tableName, badMessageHandler)
        badMessageHandler("Unexpected ticDat table type for %s."%tableName)
        return False


    def _goodTicDatKeyContainer(self, ticDatTable, tableName, badMessageHandler = lambda x : None) :
        assert containerish(ticDatTable) and not dictish(ticDatTable)
        if tableName in self.dataFields :
            badMessageHandler("%s contains data fields, and thus must be represented by a dict"%tableName)
            return False
        if not len(ticDatTable) :
            return True
        if not all(_keyLen(k) == len(self.primaryKeyFields[tableName])  for k in ticDatTable) :
            badMessageHandler("Inconsistent key lengths")
            return False
        return True
    def _goodTicDatDictTable(self, ticDatTable, tableName, badMessageHandler = lambda x : None):
        assert dictish(ticDatTable)
        if not len(ticDatTable) :
            return True
        if not all(_keyLen(k) == len(self.primaryKeyFields[tableName]) for k in ticDatTable.keys()) :
            badMessageHandler("Inconsistent key lengths")
            return False
        return self._goodDataRows(ticDatTable.values(), tableName, badMessageHandler)
    def _goodDataRows(self, dataRows, tableName, badMessageHandler = lambda x : None):
        dictishRows = tuple(x for x in dataRows if utils.dictish(x))
        if not all(set(x.keys()) == set(self.dataFields.get(tableName,())) for x in dictishRows) :
            badMessageHandler("Inconsistent data field name keys.")
            return False
        containerishRows = tuple(x for x in dataRows if utils.containerish(x) and not  utils.dictish(x))
        if not all(len(x) == len(self.dataFields.get(tableName,())) for x in containerishRows) :
            badMessageHandler("Inconsistent data row lengths.")
            return False
        singletonishRows = tuple(x for x in dataRows if not (utils.containerish(x) or utils.dictish(x)))
        if singletonishRows and (len(self.dataFields.get(tableName,())) != 1)  :
            badMessageHandler("Non-container data rows supported only for single-data-field tables")
            return False
        return True

    def _sameData(self, obj1, obj2):
        assert self.goodTicDatObject(obj1) and self.goodTicDatObject(obj2)
        def sameRow(r1, r2) :
            if bool(r1) != bool(r2) or set(r1) != set(r2) :
                return False
            for _k in r1:
                if r1[_k] != r2[_k] :
                    return False
            return True
        for t in self.allTables :
            t1 = getattr(obj1, t)
            t2 = getattr(obj2, t)
            if dictish(t1) != dictish(t2) :
                return False
            if dictish(t1) :
                if set(t1) != set(t2) :
                    return False
                for k in t1 :
                    if not sameRow(t1[k], t2[k]) :
                        return False
            else :
                _iter = lambda x : x if containerish(x) else x()
                for r1, r2 in izip_longest(_iter(t1), _iter(t2)) :
                    if not sameRow(r1, r2):
                        return False
        return True

def goodTicDatObject(ticDatObject, tableList = None, badMessageHandler = lambda x : None):
    """
    determines if an object qualifies as attribute collection of valid dict-of-dicts tibDat tables
    :param ticDatObject: the object to verify
    :param tableList: an optional list of attributes to verify. if missing, then all non calleable, non private,
                      attributes will be checked
    :param badMessageHandler: a call back function to receive description of any failure message
    :return: True if the ticDatObject is an attribute collection of valid dict-of-dicts. False otherwise.
    """
    if tableList is None :
        tableList = tuple(x for x in dir(ticDatObject) if not x.startswith("_") and
                          not callable(getattr(ticDatObject, x)))
    def _hasAttr(t) :
        if not hasattr(ticDatObject, t) :
            badMessageHandler(t + " not an attribute.")
            return False
        return True
    return all([_hasAttr(t) and goodTicDatTable(getattr(ticDatObject, t),
                lambda x : badMessageHandler(t + " : " + x)) for t in tableList])

def goodTicDatTable(ticDatTable, badMessageHandler = lambda x : None):
    """
    determines if a simple, dict-of-dicts qualifies as a valid ticDat table object
    :param ticDatTable: the object to verify
    :param badMessageHandler: a call back function to receive description of any failure message
    :return: True if the ticDatTable is a valid dict-of-dicts. False otherwise
    """
    if not (dictish(ticDatTable) or containerish(ticDatTable) or callable(ticDatTable)) :
        badMessageHandler("Unexpected object.")
        return False
    rows = ticDatTable.values() if dictish(ticDatTable) else (
           ticDatTable if containerish(ticDatTable) else tuple(ticDatTable()))
    if not rows :
        return True
    if dictish(ticDatTable) :
        def keyLen(k) :
            if not containerish(k) :
                return "singleton"
            try:
                rtn = len(k)
            except :
                rtn = 0
            return rtn
        if not all(keyLen(k) == keyLen(ticDatTable.keys()[0]) for k in ticDatTable.keys()) :
            badMessageHandler("Inconsistent key lengths")
            return False
    if not all(dictish(x) for x in rows) :
        badMessageHandler("At least one value is not a dict-like object")
        return False
    if not all(set(x.keys()) == set(rows[0].keys()) for x in rows) :
        badMessageHandler("Inconsistent field name keys.")
        return False
    return True

def freezeMe(x) :
    """
    Freezes a
    :param x: ticDat object
    :return: x, after it has been frozen
    """
    if not getattr(x, "_isFrozen", True) : #idempotent
        x._freeze()
    return x