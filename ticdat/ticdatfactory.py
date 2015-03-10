
# !!! KNOWN BUGS !!!!
# -> For xls file writing, None not being written out as NULL. Not sure how xlwd, xlwt is supposed to handle this?
# -> For csv file reading, all data is turned into floats if possible. So zip codes will be turned into floats
# !!! REMEMBER !!!! ticDat is supposed to facilitate rapid prototyping and port-ready solver engine
#                   development. The true fix for these cosmetic flaws is to use the Opalytics platform
#                   for industrial data and the ticDat library for cleaner, isolated development/testing.

import utils as utils
from utils import verify, freezableFactory, FrozenDict, FreezeableDict,  dictish, containerish
import collections as clt
import xls
import csvtd as csv

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
    def __init__(self, primary_key_fields = {}, data_fields = {}, generator_tables = (), foreign_keys = {},
                 default_values = {}):
        primary_key_fields, data_fields = utils.checkSchema(primary_key_fields, data_fields)
        self.primary_key_fields, self.data_fields = primary_key_fields, data_fields
        self.all_tables = frozenset(set(self.primary_key_fields).union(self.data_fields))
        verify(containerish(generator_tables) and set(generator_tables).issubset(self.all_tables),
               "generator_tables should be a container of table names")
        verify(not any(primary_key_fields.get(t) for t in generator_tables),
               "Can not make generators from tables with primary keys")
        verify(dictish(foreign_keys) and set(foreign_keys).issubset(self.all_tables) and
               all (containerish(_) for _ in foreign_keys.values()),
               "foreign_keys needs to a dictionary mapping table names to containers of foreign key definitions")
        for t,l in foreign_keys.items() :
            msg = []
            if not all(self._goodForeignKey(t, fk, msg.append) for fk in l) :
                raise utils.TicDatError("Bad foreign key for %s : %s"%(t, ",".join(msg)))
        self.foreign_keys = utils.deepFreezeContainer(foreign_keys)

        verify(dictish(default_values) and set(default_values).issubset(self.all_tables),
               "default values needs to be a dictionary keyed by table names")
        for t,d in default_values.items():
            verify(dictish(d) and set(d).issubset(self._allFields(t)),
                   "The default values for table %s is not a dictionary keyed by field names"%t)
        self.default_values = utils.deepFreezeContainer(default_values)
        dataRowFactory = FrozenDict({t : utils.ticDataRowFactory(t,
                self.primary_key_fields.get(t, ()), self.data_fields.get(t, ()),
                defaultValues=  {k:v for k,v in self.default_values.get(t, {}).items()
                    if k in self.data_fields.get(t, ())}) for t in self.all_tables})
        self.generator_tables = frozenset(generator_tables)
        goodTicDatTable = self.good_tic_dat_table
        superSelf = self
        def ticDatTableFactory(allDataDicts, tableName, primaryKey = (), rowFactory = None) :
            assert containerish(primaryKey)
            primaryKey = primaryKey or  self.primary_key_fields.get(tableName, ())
            keyLen = len(primaryKey)
            rowFactory = rowFactory or dataRowFactory[tableName]
            if keyLen > 0 :
                class TicDatDict (FreezeableDict) :
                    def __init__(self, *_args, **_kwargs):
                        super(TicDatDict, self).__init__(*_args, **_kwargs)
                        allDataDicts.append(self)
                    def __setitem__(self, key, value):
                        verify(containerish(key) ==  (keyLen > 1) and (keyLen == 1 or keyLen == len(key)),
                               "inconsistent key length for %s"%tableName)
                        return super(TicDatDict, self).__setitem__(key, rowFactory(value))
                    def __getitem__(self, item):
                        if item not in self and rowFactory is dataRowFactory.get(tableName):
                            self[item] = rowFactory({})
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
                    self._list[i] = rowFactory(v)
                def insert(self, i, v):
                    self._list.insert(i, rowFactory(v))
                def __repr__(self):
                    return "td:" + self._list.__repr__()
            assert containerish(TicDatDataList) and not dictish(TicDatDataList)
            return TicDatDataList
        def generatorFactory(data, tableName) :
            assert tableName in self.generator_tables
            def generatorFunction() :
                for row in (data if containerish(data) else data()):
                    yield dataRowFactory[tableName](row)
            return generatorFunction
        class _TicDat(utils.freezableFactory(object, "_isFrozen")) :
            def _freeze(self):
                if getattr(self, "_isFrozen", False) :
                    return
                for t in superSelf.all_tables :
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
                        assert callable(_t) and t in superSelf.generator_tables
                for _t in getattr(self, "_allDataDicts", ()) :
                    if utils.dictish(_t) and not getattr(_t, "_attributesFrozen", False) :
                        _t._dataFrozen  = True
                        _t._attributesFrozen = True
                self._isFrozen = True
            def __repr__(self):
                return "td:" + tuple(superSelf.all_tables).__repr__()
        class TicDat(_TicDat) :
            def __init__(self, **initTables):
                self._allDataDicts = []
                self._madeForeignLinks = False
                for t in initTables :
                    verify(t in superSelf.all_tables, "Unexpected table name %s"%t)
                for t,v in initTables.items():
                    badTicDatTable = []
                    if not (goodTicDatTable(v, t, lambda x : badTicDatTable.append(x))) :
                        raise utils.TicDatError(t + " cannot be treated as a ticDat table : " + badTicDatTable[-1])
                    if superSelf.primary_key_fields.get(t) :
                     for _k in v :
                        verify((hasattr(_k, "__len__") and (len(_k) == len(primary_key_fields.get(t, ())) > 1) or
                               len(primary_key_fields.get(t, ())) == 1),
                           "Unexpected number of primary key fields for %s"%t)
                     # lots of verification inside the dataRowFactory
                     setattr(self, t, ticDatTableFactory(self._allDataDicts, t)({_k : dataRowFactory[t](v[_k]
                                                            if utils.dictish(v) else ()) for _k in v}))
                    elif t in superSelf.generator_tables :
                        setattr(self, t, generatorFactory(v, t))
                    else :
                        setattr(self, t, ticDatTableFactory(self._allDataDicts, t)(*v))
                for t in set(superSelf.all_tables).difference(initTables) :
                    if t in superSelf.generator_tables :
                        setattr(self, t, generatorFactory((), t)) # a calleable that returns an empty generator
                    else :
                        setattr(self, t, ticDatTableFactory(self._allDataDicts, t)())
                if initTables :
                    self._tryMakeForeignLinks()
            def _tryMakeForeignLinks(self):
                assert not self._madeForeignLinks, "call once"
                self._madeForeignLinks = True
                canLinkWithMe = lambda t : t not in generator_tables and superSelf.primary_key_fields.get(t)
                for t, fks in superSelf.foreign_keys.items() :
                  if canLinkWithMe(t):
                    lens = {z:len([x for x in fks if x["foreignTable"] == z]) for z in [y["foreignTable"] for y in fks]}
                    for fk in fks:
                      if canLinkWithMe(fk["foreignTable"])  :
                        linkName = t if lens[fk["foreignTable"]] ==1 else (t + "_" + "_".join(fk["mappings"].keys()))
                        if linkName not in ("keys", "items", "values") :
                            ft = getattr(self, fk["foreignTable"])
                            foreignPrimaryKey = superSelf.primary_key_fields[fk["foreignTable"]]
                            localPrimaryKey = superSelf.primary_key_fields[t]
                            assert all(pk for pk in (foreignPrimaryKey, localPrimaryKey))
                            assert set(foreignPrimaryKey) == set(fk["mappings"].values())
                            appendageForeignKey = (
                                            set(foreignPrimaryKey) == set(fk["mappings"].values()) and
                                            set(localPrimaryKey) == set(fk["mappings"].keys()))
                            reverseMapping  = {v:k for k,v in fk["mappings"].items()}
                            tableFields = superSelf.primary_key_fields.get(t, ()) + superSelf.data_fields.get(t, ())
                            localPosition = {x:tableFields.index(reverseMapping[x]) for x in foreignPrimaryKey}
                            unusedLocalPositions = {i for i,_ in enumerate(tableFields) if i not in
                                                    localPosition.values()}
                            if not appendageForeignKey :
                                newPrimaryKey = tuple(x for x in localPrimaryKey if x not in fk["mappings"].keys())
                                newDataDict = ticDatTableFactory(self._allDataDicts, linkName,
                                                newPrimaryKey, lambda x : x)
                                for row in ft.values() :
                                    setattr(row, linkName, newDataDict())
                            for key,row in getattr(self, t).items() :
                                keyRow = ((key,) if not containerish(key) else key) + \
                                         tuple(row[x] for x in data_fields[t])
                                lookUp = tuple(keyRow[localPosition[x]] for x in foreignPrimaryKey)
                                linkRow = ft.get(lookUp[0] if len(lookUp) ==1 else lookUp, None)
                                if linkRow is not None :
                                    if  appendageForeignKey :
                                        # the attribute is simply a reference to the mapping table if such a reference exists
                                        assert not hasattr(linkRow, linkName)
                                        setattr(linkRow, linkName,row)
                                    else :
                                        _key = tuple(x for i,x in enumerate(keyRow[:-len(row)]) if i in unusedLocalPositions)
                                        getattr(linkRow, linkName)[_key[0] if len(_key) == 1 else _key] = row

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

    def _allFields(self, table):
        assert table in self.all_tables
        return set(self.primary_key_fields.get(table, ())).union(self.data_fields.get(table, ()))
    def _goodForeignKey(self, table, fk, badMessageHandler = lambda x : None):
        assert table in self.all_tables
        if not self.primary_key_fields.get(table) :
            badMessageHandler("%s has no primary keys and can't participate in a foreign key"%table)
            return True
        if not dictish(fk) :
            badMessageHandler("Not a dict")
            return False
        if set(fk.keys()) != {"foreignTable", "mappings"} :
            badMessageHandler("Unexpected keys")
            return False
        if fk["foreignTable"] not in self.all_tables:
            badMessageHandler("Bad foreignTable value")
            return False
        if not self.primary_key_fields.get(fk["foreignTable"]) :
            badMessageHandler("%s has no primary keys and can't participate in a foreign key"%fk["foreignTable"])
            return True
        if not dictish(fk["mappings"]) :
            badMessageHandler("mappings should refer to a dictionary")
            return False
        if not self._allFields(table).issuperset(fk["mappings"].keys()):
            badMessageHandler("the mappings dictionary should have keys in %s"%str(self._allFields(table)))
            return False
        if not set(self.primary_key_fields[fk["foreignTable"]]) == set(fk["mappings"].values()):
            badMessageHandler("the mappings dictionary should have values that match the primary key of %s"%
                        fk["foreignTable"])
            return False
        
        return True

    def good_tic_dat_object(self, data_obj, bad_message_handler = lambda x : None):
        """
        determines if an object can be can be converted to a TicDat data object.
        :param data_obj: the object to verify
        :param bad_message_handler: a call back function to receive description of any failure message
        :return: True if the dataObj can be converted to a TicDat data object. False otherwise.
        """
        rtn = True
        for t in self.all_tables:
            if not hasattr(data_obj, t) :
                bad_message_handler(t + " not an attribute.")
                return False
            rtn = rtn and  self.good_tic_dat_table(getattr(data_obj, t), t,
                    lambda x : bad_message_handler(t + " : " + x))
        return rtn

    def good_tic_dat_table(self, data_table, table_name, bad_message_handler = lambda x : None) :
        """
        determines if an object can be can be converted to a TicDat data table.
        :param dataObj: the object to verify
        :param table_name: the name of the table
        :param bad_message_handler: a call back function to receive description of any failure message
        :return: True if the dataObj can be converted to a TicDat data table. False otherwise.
        """
        if table_name not in self.all_tables:
            bad_message_handler("%s is not a valid table name for this schema"%table_name)
            return False
        if table_name in self.generator_tables :
            assert not self.primary_key_fields.get(table_name)
            verify((containerish(data_table) or callable(data_table)) and not dictish(data_table),
                   "Expecting a container of rows or a generator function of rows for %s"%table_name)
            return self._goodDataRows(data_table if containerish(data_table) else data_table(),
                                      table_name, bad_message_handler)
        if self.primary_key_fields.get(table_name) :
            if utils.dictish(data_table) :
                return self._goodTicDatDictTable(data_table, table_name, bad_message_handler)
            if utils.containerish(data_table):
                return  self._goodTicDatKeyContainer(data_table, table_name, bad_message_handler)
        else :
            verify(utils.containerish(data_table), "Unexpected ticDat table type for %s."%table_name)
            return self._goodDataRows(data_table, table_name, bad_message_handler)
        bad_message_handler("Unexpected ticDat table type for %s."%table_name)
        return False


    def _goodTicDatKeyContainer(self, ticDatTable, tableName, badMessageHandler = lambda x : None) :
        assert containerish(ticDatTable) and not dictish(ticDatTable)
        if tableName in self.data_fields :
            badMessageHandler("%s contains data fields, and thus must be represented by a dict"%tableName)
            return False
        if not len(ticDatTable) :
            return True
        if not all(_keyLen(k) == len(self.primary_key_fields[tableName])  for k in ticDatTable) :
            badMessageHandler("Inconsistent key lengths")
            return False
        return True
    def _goodTicDatDictTable(self, ticDatTable, tableName, badMessageHandler = lambda x : None):
        assert dictish(ticDatTable)
        if not len(ticDatTable) :
            return True
        if not all(_keyLen(k) == len(self.primary_key_fields[tableName]) for k in ticDatTable.keys()) :
            badMessageHandler("Inconsistent key lengths")
            return False
        return self._goodDataRows(ticDatTable.values(), tableName, badMessageHandler)
    def _goodDataRows(self, dataRows, tableName, badMessageHandler = lambda x : None):
        dictishRows = tuple(x for x in dataRows if utils.dictish(x))
        if not all(set(x.keys()) == set(self.data_fields.get(tableName,())) for x in dictishRows) :
            badMessageHandler("Inconsistent data field name keys.")
            return False
        containerishRows = tuple(x for x in dataRows if utils.containerish(x) and not  utils.dictish(x))
        if not all(len(x) == len(self.data_fields.get(tableName,())) for x in containerishRows) :
            badMessageHandler("Inconsistent data row lengths.")
            return False
        singletonishRows = tuple(x for x in dataRows if not (utils.containerish(x) or utils.dictish(x)))
        if singletonishRows and (len(self.data_fields.get(tableName,())) != 1)  :
            badMessageHandler("Non-container data rows supported only for single-data-field tables")
            return False
        return True

    def _keyless(self, obj):
        assert self.good_tic_dat_object(obj)
        class _ (object) :
            pass
        rtn = _()
        for t in self.all_tables :
            _rtn = []
            _t = getattr(obj, t)
            if dictish(_t) :
                for pk, dr in _t.items() :
                    _rtn.append(dict(dr, **{_f: _pk for _f,_pk in
                                zip(self.primary_key_fields[t], pk if containerish(pk) else (pk,))}))
            else :
                for dr in (_t if containerish(_t) else _t()) :
                    _rtn.append(dict(dr))
            setattr(rtn, t, _rtn)
        return rtn
    def _sameData(self, obj1, obj2):
        assert self.good_tic_dat_object(obj1) and self.good_tic_dat_object(obj2)
        def sameRow(r1, r2) :
            assert dictish(r1) and dictish(r2)
            if bool(r1) != bool(r2) or set(r1) != set(r2) :
                return False
            for _k in r1:
                if r1[_k] != r2[_k] :
                    return False
            return True
        for t in self.all_tables :
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
                if not len(list(_iter(t1))) == len(list(_iter(t2))) :
                    return False
                for r1 in _iter(t1):
                    if not any (sameRow(r1, r2) for r2 in _iter(t2)) :
                        return False
        return True

def freeze_me(x) :
    """
    Freezes a
    :param x: ticDat object
    :return: x, after it has been frozen
    """
    verify(hasattr(x, "_freeze"), "x not a freezeable object")
    if not getattr(x, "_isFrozen", True) : #idempotent
        x._freeze()
    return x