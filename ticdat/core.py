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

import ticdat._private.utils as utils
from ticdat._private.utils import verify, freezableFactory, FrozenDict, FreezeableDict, doIt, dictish, containerish
import ticdat._private.xls as xls

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
    def __init__(self, primaryKeyFields = {}, dataFields = {}):
        primaryKeyFields, dataFields = utils.checkSchema(primaryKeyFields, dataFields)
        self.primaryKeyFields, self.dataFields = primaryKeyFields, dataFields
        assert set(dataFields).issubset(primaryKeyFields), "this code assumes all tables have primary keys"
        dataRowFactory = FrozenDict({t : utils.ticDataRowFactory(t, primaryKeyFields[t], dataFields.get(t, ()))
                            for t in primaryKeyFields})
        goodTicDatTable = self.goodTicDatTable
        class _TicDat(utils.freezableFactory(object, "_isFrozen")) :
            def _freeze(self):
                if getattr(self, "_isFrozen", False) :
                    return
                for t in set(primaryKeyFields).union(dataFields) :
                    for v in getattr(self, t).values() :
                        if not getattr(v, "_dataFrozen", False) :
                            v._dataFrozen =True
                            v._attributesFrozen = True
                        else : # we freeze the data-less ones off the bat as empties, easiest way
                            assert (len(v) == 0) and v._attributesFrozen
                    getattr(self, t)._dataFrozen  = True
                    getattr(self, t)._attributesFrozen = True
                self._isFrozen = True
            def __repr__(self):
                return "td:" + tuple(set(primaryKeyFields).union(dataFields)).__repr__()

        def ticDatDictFactory(tableName) :
            keyLen = len(self.primaryKeyFields[tableName])
            class TicDatDict (FreezeableDict) :
                def __setitem__(self, key, value):
                    verify(containerish(key) ==  (keyLen > 1) and keyLen == 1 or keyLen == len(key),
                           "inconsistent key length for %s"%tableName)
                    return super(TicDatDict, self).__setitem__(key,value)
            assert dictish(TicDatDict)
            return TicDatDict

        class TicDat(_TicDat) :
            def __init__(self, **initTables):
                for t in initTables :
                    verify(t in set(primaryKeyFields).union(dataFields), "Unexpected table name %s"%t)
                for t,v in initTables.items():
                    badTicDatTable = []
                    if not (goodTicDatTable(v, t, lambda x : badTicDatTable.append(x))) :
                        raise utils.TicDatError(t + " cannot be treated as a ticDat table : " + badTicDatTable[-1])
                    for _k in v :
                        verify((hasattr(_k, "__len__") and (len(_k) == len(primaryKeyFields.get(t, ())) > 1) or
                               len(primaryKeyFields.get(t, ())) == 1),
                           "Unexpected number of primary key fields for %s"%t)
                    # lots of verification inside the dataRowFactory
                    setattr(self, t, ticDatDictFactory(t)({_k : dataRowFactory[t](v[_k]
                                                            if utils.dictish(v) else ()) for _k in v}))
                for t in set(primaryKeyFields).union(dataFields).difference(initTables) :
                    setattr(self, t, ticDatDictFactory(t)())

        self.TicDat = TicDat
        class FrozenTicDat(TicDat) :
            def __init__(self, **initTables):
                super(FrozenTicDat, self).__init__(**initTables)
                self._freeze()
        self.FrozenTicDat = FrozenTicDat
        if xls.importWorked :
            self.xls = xls.XlsTicFactory(self)
        self._isFrozen = True


    def goodTicDatObject(self, dataObj,  badMessageHandler = lambda x : None):
        """
        determines if an object can be can be converted to a TicDat data object.
        :param dataObj: the object to verify
        :param badMessageHandler: a call back function to receive description of any failure message
        :return: True if the dataObj can be converted to a TicDat data object. False otherwise.
        """
        def _hasAttr(t) :
            if not hasattr(dataObj, t) :
                badMessageHandler(t + " not an attribute.")
                return False
            return True
        rtn = True
        for t in set(self.primaryKeyFields).union(self.dataFields):
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
        if tableName not in set(self.primaryKeyFields).union(self.dataFields):
            badMessageHandler("%s is not a valid table name for this schema"%tableName)
            return False
        if utils.dictish(dataTable) :
            return self._goodTicDatDictTable(dataTable, tableName, badMessageHandler)
        if utils.containerish(dataTable):
            return  self._goodTicDatKeyContainer(dataTable, tableName, badMessageHandler)
        badMessageHandler("Unexpected ticDat table type.")
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
        dictishRows = tuple(x for x in ticDatTable.values() if utils.dictish(x))
        if not all(set(x.keys()) == set(self.dataFields.get(tableName,())) for x in dictishRows) :
            badMessageHandler("Inconsistent data field name keys.")
            return False
        containerishRows = tuple(x for x in ticDatTable.values() if utils.containerish(x) and not  utils.dictish(x))
        if not all(len(x) == len(self.dataFields.get(tableName,())) for x in containerishRows) :
            badMessageHandler("Inconsistent data row lengths.")
            return False
        singletonishRows = tuple(x for x in ticDatTable.values() if not (utils.containerish(x) or utils.dictish(x)))
        if singletonishRows and (len(self.dataFields[tableName]) != 1)  :
            badMessageHandler("Non-container data rows supported only for single-data-field tables")
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
    if not dictish(ticDatTable) :
        badMessageHandler("Not a dict-like object.")
        return False
    if not len(ticDatTable) :
        return True
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
    if not all(dictish(x) for x in ticDatTable.values()) :
        badMessageHandler("At least one value is not a dict-like object")
        return False
    if not all(set(x.keys()) == set(ticDatTable.values()[0].keys()) for x in ticDatTable.values()) :
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