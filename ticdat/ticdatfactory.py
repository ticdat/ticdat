"""
Create TicDatFactory. Main entry point for ticdat library.
PEP8
"""
import collections as clt
import utils as utils
from utils import verify, freezable_factory, FrozenDict, FreezeableDict
from utils import dictish, containerish, deep_freeze, lupish
from string import uppercase
from collections import namedtuple
import xls
import csvtd as csv
import sqlitetd as sql
import mdb

def _keylen(k) :
    if not utils.containerish(k) :
        return 1
    try:
        rtn = len(k)
    except :
        rtn = 0
    return rtn

_ForeignKey = namedtuple("ForeignKey", ("native_table", "foreign_table", "mapping", "cardinality"))
_ForeignKeyMapping = namedtuple("FKMapping", ("native_field", "foreign_field"))
def _nativefields(fk):
    return (fk.mapping.native_field,) if type(fk.mapping) is _ForeignKeyMapping \
                                       else tuple(_.native_field for _ in fk.mapping)
def _foreigntonativemapping(fk):
    if type(fk.mapping) is _ForeignKeyMapping :
        return {fk.mapping.foreign_field:fk.mapping.native_field}
    else :
        return {_.foreign_field:_.native_field for _ in fk.mapping}

class TicDatFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for ticdat library. This class is constructed with a schema,
    and can be used to generate TicDat objects, or to write TicDat objects to
    different file types. Analytical code that uses TicDat objects can be used,
    without change, on different data sources.
    """
    @property
    def primary_key_fields(self):
        return self._primary_key_fields
    @property
    def data_fields(self):
        return self._data_fields
    @property
    def generator_tables(self):
        return deep_freeze(self._generator_tables)
    @property
    def default_values(self):
        return deep_freeze(self._default_values)
    def set_default_values(self, **tableDefaults):
        verify(not self._has_been_used,
               "The default values can't be changed after a TicDatFactory has been used.")
        for k,v in tableDefaults.items():
            verify(k in self.all_tables, "Unrecognized table name %s"%k)
            verify(dictish(v) and set(v).issubset(self.data_fields[k]),
                "Default values for %s should be a dictionary mapping data field names to values"
                %k)
            self._default_values[k] = dict(self._default_values[k], **v)
    def set_generator_tables(self, g):
        verify(not self._has_been_used,
               "The generator tables can't be changed after a TicDatFactory has been used.")
        verify(containerish(g) and set(g).issubset(self.all_tables),
               "Generator_tables should be a container of table names")
        verify(not any(self.primary_key_fields.get(t) for t in g),
               "Can not make generators from tables with primary keys")
        self._generator_tables[:] = [_ for _ in g]
    def clear_foreign_keys(self, native_table = None):
        """
        create a TicDatFactory
        :param native_table: optional. The table whose foreign keys should be cleared.
                             If omitted, all foreign keys are cleared.
        """
        verify(not self._has_been_used,
               "The foreign keys can't be changed after a TicDatFactory has been used.")
        verify(native_table is None or native_table in self.all_tables,
               "If provided, native_table should specify a table.")
        deleteme = []
        for nt,ft in self._foreign_keys:
            if nt == (native_table or nt) :
                deleteme.append((nt,ft))
        for nt, ft in deleteme:
            del(self._foreign_keys[nt,ft])
    @property
    def foreign_keys(self):
        rtn = []
        for (native,foreign), nativefieldtuples in self._foreign_keys.items():
            for nativefields in nativefieldtuples :
                mappings = tuple(_ForeignKeyMapping(nf,ff) for nf,ff in
                                 zip(nativefields, self.primary_key_fields[foreign]))
                mappings = mappings[0] if len(mappings)==1 else mappings
                if set(nativefields) == set(self.primary_key_fields.get(native, ())) :
                    cardinality = "one-to-one"
                else:
                   cardinality = "many-to-one"
                rtn.append(_ForeignKey(native, foreign, mappings, cardinality))
        assert len(rtn) == len(set(rtn))
        return tuple(rtn)
    def _foreign_keys_by_native(self):
        rtn = clt.defaultdict(list)
        for fk in self.foreign_keys:
            rtn[fk.native_table].append(fk)
        return utils.FrozenDict({k:frozenset(v) for k,v in rtn.items()})
    def add_foreign_key(self, native_table, foreign_table, mappings):
        verify(not self._has_been_used,
                "The foreign keys can't be changed after a TicDatFactory has been used.")
        for t in (native_table, foreign_table):
            verify(t in self.all_tables, "%s is not a table name"%t)
        verify(lupish(mappings) and mappings, "mappings needs to be a non empty list or a tuple")
        if lupish(mappings[0]):
            verify(all(len(_) == 2 for _ in mappings),
"""when making a compound foreign key, mappings should contain sublists of format
[native field, foreign field]""")
            _mappings = {k:v for k,v in mappings}
        else :
            verify(len(mappings) == 2,
"""when making a simple foreign key, mappings should be a list of the form [native field, foreign field]""")
            _mappings = {mappings[0]:mappings[1]}
        for k,v in _mappings.items() :
            verify(k in self._allFields(native_table),
                   "%s does not refer to one of %s 's fields"%(k, native_table))
            verify(v in self._allFields(foreign_table),
                   "%s does not refer to one of %s 's fields"%(k, foreign_table))
        verify(set(self.primary_key_fields.get(foreign_table, ())) == set(_mappings.values()),
            """%s is not the primary key for %s.
This exception is being thrown because ticDat doesn't currently support many-to-many
foreign key relationships. The ticDat API is forward compatible with re: to many-to-many
relationships. When a future version of ticDat is released that supports many-to-many
foreign keys, the code throwing this exception will be removed.
            """%(",".join(_mappings.values()), foreign_table))
        reverseMapping = {v:k for k,v in _mappings.items()}
        self._foreign_keys[native_table, foreign_table].add(tuple(reverseMapping[pkf]
                            for pkf in self.primary_key_fields[foreign_table]))
    def _trigger_has_been_used(self):
        if self._has_been_used :
            return # idempotent
        def findderivedforeignkey() :
            curFKs = self._foreign_keys_by_native()
            for (nativetable, bridgetable), nativefieldstuples in self._foreign_keys.items():
                for nativefieldtuple in nativefieldstuples :
                    for bfk in curFKs.get(bridgetable,()):
                        nativefields = _nativefields(bfk)
                        if set(nativefields)\
                                .issubset(self.primary_key_fields[bridgetable]):
                            bridgetonative = {pkf:nf for pkf,nf in
                                    zip(self.primary_key_fields[bridgetable], nativefieldtuple)}
                            foreigntobridge = _foreigntonativemapping(bfk)
                            newnativeft = tuple(bridgetonative[foreigntobridge[pkf]] for pkf in
                                            self.primary_key_fields[bfk.foreign_table])
                            fkSet = self._foreign_keys[nativetable, bfk.foreign_table]
                            if newnativeft not in fkSet :
                                return fkSet.add(newnativeft) or True
        while findderivedforeignkey():
            pass
        for (nativetable, foreigntable), nativeFieldsTuples in self._foreign_keys.items():
            nativeFieldsSet = frozenset(frozenset(_) for _ in nativeFieldsTuples)
            if len(nativeFieldsSet)==1:
                self._linkName[nativetable, foreigntable, next(_ for _ in nativeFieldsSet)] = \
                    nativetable
            else :
                for nativeFields in nativeFieldsSet :
                    trialLinkName = nativeFields
                    for _ in nativeFieldsSet.difference({nativeFields}) :
                        trialLinkName = trialLinkName.difference(_)
                    self._linkName[nativetable, foreigntable, nativeFields] = \
                        "_".join([nativetable] + [x for x in trialLinkName or nativeFields])
        self._has_been_used[:] = [True]
    def pickle_this(self, ticdat):
        '''
        As a nested class, TicDat and FrozenTicDat objects cannot be pickled
        directly. Instead, the dictionary returned by this function can be pickled.
        For unpickling, first unpickle the pickled 'pickle_this' dictionary, and then pass it,
        unpacked, to the TicDat/FrozenTicDat constructor.
        :param ticdat: a TicDat or FrozenTicDat object whose data is to be returned as a pickleable dict
        :return: A dictionary that can either be pickled, or unpacked to a
                TicDat/FrozenTicDat constructor
        '''
        verify(not self.generator_tables, "Can't pickle generator tables.")
        rtn = {}
        dict_tables = {t for t,pk in self.primary_key_fields.items() if pk}
        for t in dict_tables:
            rtn[t] = {pk : {k:v for k,v in row.items()} for pk,row in getattr(ticdat,t).items()}
        for t in set(self.all_tables).difference(dict_tables):
            rtn[t] = [{k:v for k,v in row.items()} for row in getattr(ticdat, t)]
        return rtn
    def __init__(self, **init_fields):
        """
        create a TicDatFactory
        :param init_fields: a mapping of tables to primary key fields
                            and data fields. Each field listing consists
                            of two sub lists ... first primary keys fields,
                            than data fields.
        ex: TicDatFactory (categories =  [["name"],["minNutrition", "maxNutrition"]],
                           foods  =  [["name"],["cost"]]
                           nutritionQuantities = [["food", "category"],["qty"]])
        :return: a TicDatFactory
        """
        self._has_been_used = [] # append to this to make it truthy
        self._linkName = {}
        verify(not any(x.startswith("_") for x in init_fields),
               "table names shouldn't start with underscore")
        for k,v in init_fields.items():
            verify(containerish(v) and len(v) == 2 and all(containerish(_) for _ in v),
                   ("Table %s needs to specify two sublists, " +
                    "one for primary key fields and one for data fields")%k)
            verify(all(utils.stringish(s) for _ in v for s in _),
                   "The field names for %s need to be strings"%k)
            verify(v[0] or v[1], "No field names specified for table %s"%k)
            verify(not set(v[0]).intersection(v[1]),
                   "The same field name is both a data field and primary key field for table %s"%k)
        self._primary_key_fields = FrozenDict({k : tuple(v[0])for k,v in init_fields.items()})
        self._data_fields = FrozenDict({k : tuple(v[1]) for k,v in init_fields.items()})
        self._default_values = clt.defaultdict(dict)
        self._generator_tables = []
        self._foreign_keys = clt.defaultdict(set)
        self.all_tables = frozenset(init_fields)

        datarowfactory = lambda t :  utils.td_row_factory(t, self.primary_key_fields.get(t, ()),
                        self.data_fields.get(t, ()), self.default_values.get(t, {}))

        goodticdattable = self.good_tic_dat_table
        superself = self
        def ticdattablefactory(alldatadicts, tablename, primarykey = (), rowfactory_ = None) :
            assert containerish(primarykey)
            primarykey = primarykey or  self.primary_key_fields.get(tablename, ())
            keylen = len(primarykey)
            rowfactory = rowfactory_ or datarowfactory(tablename)
            if keylen > 0 :
                class TicDatDict (FreezeableDict) :
                    def __init__(self, *_args, **_kwargs):
                        super(TicDatDict, self).__init__(*_args, **_kwargs)
                        alldatadicts.append(self)
                    def __setitem__(self, key, value):
                        verify(containerish(key) ==  (keylen > 1) and
                               (keylen == 1 or keylen == len(key)),
                               "inconsistent key length for %s"%tablename)
                        return super(TicDatDict, self).__setitem__(key, rowfactory(value))
                    def __getitem__(self, item):
                        if (item not in self) and (not getattr(self, "_dataFrozen", False)):
                            self[item] = rowfactory({})
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
                    self._list[i] = rowfactory(v)
                def insert(self, i, v):
                    self._list.insert(i, rowfactory(v))
                def __repr__(self):
                    return "td:" + self._list.__repr__()
            assert containerish(TicDatDataList) and not dictish(TicDatDataList)
            return TicDatDataList
        def generatorfactory(data, tablename) :
            assert tablename in self.generator_tables
            drf = datarowfactory(tablename)
            def generatorFunction() :
                for row in (data if containerish(data) else data()):
                    yield drf(row)
            return generatorFunction
        class _TicDat(utils.freezable_factory(object, "_isFrozen")) :
            def _freeze(self):
                if getattr(self, "_isFrozen", False) :
                    return
                for t in superself.all_tables :
                    _t = getattr(self, t)
                    if utils.dictish(_t) or utils.containerish(_t) :
                        for v in getattr(_t, "values", lambda : _t)() :
                            if not getattr(v, "_dataFrozen", False) :
                                v._dataFrozen =True
                                v._attributesFrozen = True
                            else : # we freeze the data-less ones off the bat as empties
                                assert (len(v) == 0) and v._attributesFrozen
                        if utils.dictish(_t) :
                            _t._dataFrozen  = True
                            _t._attributesFrozen = True
                        elif utils.containerish(_t) :
                            setattr(self, t, tuple(_t))
                    else :
                        assert callable(_t) and t in superself.generator_tables
                for _t in getattr(self, "_allDataDicts", ()) :
                    if utils.dictish(_t) and not getattr(_t, "_attributesFrozen", False) :
                        _t._dataFrozen  = True
                        _t._attributesFrozen = True
                self._isFrozen = True
            def __repr__(self):
                return "td:" + tuple(superself.all_tables).__repr__()
        class TicDat(_TicDat) :
            def _generatorfactory(self, data, tableName):
                return generatorfactory(data, tableName)
            def __init__(self, **init_tables):
                superself._trigger_has_been_used()
                self._all_data_dicts = []
                self._made_foreign_links = False
                for t in init_tables :
                    verify(t in superself.all_tables, "Unexpected table name %s"%t)
                for t,v in init_tables.items():
                    badticdattable = []
                    if not (goodticdattable(v, t, lambda x : badticdattable.append(x))) :
                        raise utils.TicDatError(t + " cannot be treated as a ticDat table : " +
                                                badticdattable[-1])
                    if superself.primary_key_fields.get(t) :
                     for _k in v :
                        verify((hasattr(_k, "__len__") and
                                (len(_k) == len(superself.primary_key_fields.get(t, ())) > 1)
                                or len(superself.primary_key_fields.get(t, ())) == 1),
                           "Unexpected number of primary key fields for %s"%t)
                     drf = datarowfactory(t) # lots of verification inside the datarowfactory
                     setattr(self, t, ticdattablefactory(self._all_data_dicts, t)(
                                    {_k : drf(v[_k] if utils.dictish(v) else ()) for _k in v}))
                    elif t in superself.generator_tables :
                        setattr(self, t, generatorfactory(v, t))
                    else :
                        setattr(self, t, ticdattablefactory(self._all_data_dicts, t)(*v))
                for t in set(superself.all_tables).difference(init_tables) :
                    if t in superself.generator_tables :
                        # a calleable that returns an empty generator
                        setattr(self, t, generatorfactory((), t))
                    else :
                        setattr(self, t, ticdattablefactory(self._all_data_dicts, t)())
                if init_tables :
                    self._try_make_foreign_links()
            def _try_make_foreign_links(self):
                assert not self._made_foreign_links, "call once"
                self._made_foreign_links = True
                can_link_w_me = lambda t : t not in superself.generator_tables and \
                                           superself.primary_key_fields.get(t)
                for fk in superself.foreign_keys :
                    t = fk.native_table
                    if can_link_w_me(t):
                      if can_link_w_me(fk.foreign_table)  :
                        nativefields = _nativefields(fk)
                        linkname = superself._linkName[t, fk.foreign_table, frozenset(nativefields)]
                        if linkname not in ("keys", "items", "values") :
                            ft = getattr(self, fk.foreign_table)
                            foreign_pk = superself.primary_key_fields[fk.foreign_table]
                            local_pk = superself.primary_key_fields[t]
                            assert all(pk for pk in (foreign_pk, local_pk))
                            reversemapping  = _foreigntonativemapping(fk)
                            if len(nativefields) == 1:
                                assert set(foreign_pk) =={fk.mapping.foreign_field}
                            else:
                                assert set(foreign_pk) == {_.foreign_field for _ in fk.mapping}
                            appendage_fk = fk.cardinality == "one-to-one"
                            tablefields = superself.primary_key_fields.get(t, ()) + \
                                          superself.data_fields.get(t, ())
                            local_posn = {x:tablefields.index(reversemapping[x])
                                             for x in foreign_pk}
                            unused_local_posn = {i for i,_ in enumerate(tablefields) if i not in
                                                    local_posn.values()}
                            if not appendage_fk :
                                new_pk = tuple(x for x in local_pk if x not in nativefields)
                                new_data_dct = ticdattablefactory(self._all_data_dicts, linkname,
                                                new_pk, lambda x : x)
                                for row in ft.values() :
                                    setattr(row, linkname, new_data_dct())
                            for key,row in getattr(self, t).items() :
                                keyrow = ((key,) if not containerish(key) else key) + \
                                         tuple(row[x] for x in superself.data_fields[t])
                                lookup = tuple(keyrow[local_posn[x]] for x in foreign_pk)
                                linkrow = ft.get(lookup[0] if len(lookup) ==1 else lookup, None)
                                if linkrow is not None :
                                    if appendage_fk :
                                        # the attribute is simply a reference to the mapping table
                                        assert not hasattr(linkrow, linkname)
                                        setattr(linkrow, linkname,row)
                                    else :
                                        _key = keyrow[:-len(row)] if row else keyrow
                                        _key = tuple(x for i,x in enumerate(_key)
                                                     if i in unused_local_posn)
                                        getattr(linkrow, linkname)\
                                            [_key[0] if len(_key) == 1 else _key] = row

        self.TicDat = TicDat
        class FrozenTicDat(TicDat) :
            def __init__(self, **init_tables):
                super(FrozenTicDat, self).__init__(**init_tables)
                self._freeze()
        self.FrozenTicDat = FrozenTicDat
        if xls.import_worked :
            self.xls = xls.XlsTicFactory(self)
        if csv.import_worked :
            self.csv = csv.CsvTicFactory(self)
        if sql.import_worked :
            self.sql = sql.SQLiteTicFactory(self)
        if mdb.import_worked:
            self.mdb = mdb.MdbTicFactory(self)
        self._isFrozen=True

    def _allFields(self, table):
        assert table in self.all_tables
        return set(self.primary_key_fields.get(table, ())).union(self.data_fields.get(table, ()))

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
        :param bad_message_handler: a call back function to receive
               description of any failure message
        :return: True if the dataObj can be converted to a TicDat
                 data table. False otherwise.
        """
        if table_name not in self.all_tables:
            bad_message_handler("%s is not a valid table name for this schema"%table_name)
            return False
        if table_name in self.generator_tables :
            assert not self.primary_key_fields.get(table_name), "this should be verified in __init__"
            verify((containerish(data_table) or callable(data_table)) and not dictish(data_table),
                   "Expecting a container of rows or a generator function of rows for %s"%table_name)
            return self._good_data_rows(data_table if containerish(data_table) else data_table(),
                                      table_name, bad_message_handler)
        if self.primary_key_fields.get(table_name) :
            if utils.dictish(data_table) :
                return self._good_ticdat_dict_table(data_table, table_name, bad_message_handler)
            if utils.containerish(data_table):
                return  self._good_ticdat_key_container(data_table, table_name, bad_message_handler)
        else :
            verify(utils.containerish(data_table),
                   "Unexpected ticDat table type for %s."%table_name)
            return self._good_data_rows(data_table, table_name, bad_message_handler)
        bad_message_handler("Unexpected ticDat table type for %s."%table_name)
        return False
    def _good_ticdat_key_container(self, ticdat_table, tablename,
                                  bad_msg_handler = lambda x : None) :
        assert containerish(ticdat_table) and not dictish(ticdat_table)
        if self.data_fields.get(tablename) :
            bad_msg_handler("%s contains data fields, and thus must be represented by a dict"%
                              tablename)
            return False
        if not len(ticdat_table) :
            return True
        if not all(_keylen(k) == len(self.primary_key_fields[tablename])  for k in ticdat_table):
            bad_msg_handler("Inconsistent key lengths")
            return False
        return True
    def _good_ticdat_dict_table(self, ticdat_table, table_name, bad_msg_handler = lambda x : None):
        assert dictish(ticdat_table)
        if not len(ticdat_table) :
            return True
        if not all(_keylen(k) == len(self.primary_key_fields[table_name])
                   for k in ticdat_table.keys()) :
            bad_msg_handler("Inconsistent key lengths")
            return False
        return self._good_data_rows(ticdat_table.values(), table_name, bad_msg_handler)
    def _good_data_rows(self, data_rows, table_name, bad_message_handler = lambda x : None):
        dictishrows = tuple(x for x in data_rows if utils.dictish(x))
        if not all(set(x.keys()).issubset(self.data_fields.get(table_name,())) for x in dictishrows):
            bad_message_handler("Inconsistent data field name keys.")
            return False
        containerishrows = tuple(x for x in data_rows
                                 if utils.containerish(x) and not  utils.dictish(x))
        if not all(len(x) == len(self.data_fields.get(table_name,())) for x in containerishrows) :
            bad_message_handler("Inconsistent data row lengths.")
            return False
        singletonishrows = tuple(x for x in data_rows if not
                                 (utils.containerish(x) or utils.dictish(x)))
        if singletonishrows and (len(self.data_fields.get(table_name,())) != 1)  :
            bad_message_handler(
                "Non-container data rows supported only for single-data-field tables")
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
                    _rtn.append(dict(dr, **{_f: _pk for _f,_pk in zip(self.primary_key_fields[t], pk
                                                                if containerish(pk) else (pk,))}))
            else :
                for dr in (_t if containerish(_t) else _t()) :
                    _rtn.append(dict(dr))
            setattr(rtn, t, _rtn)
        return rtn
    def _same_data(self, obj1, obj2):
        assert self.good_tic_dat_object(obj1) and self.good_tic_dat_object(obj2)
        def samerow(r1, r2) :
            if dictish(r1) and dictish(r2):
                if bool(r1) != bool(r2) or set(r1) != set(r2) :
                    return False
                for _k in r1:
                    if r1[_k] != r2[_k] :
                        return False
                return True
            if dictish(r2) and not dictish(r1) :
                return samerow(r2, r1)
            def containerize(_r) :
                if utils.containerish(_r) :
                    return list(_r)
                return [_r,]
            if dictish(r1) :
                return list(r1.values()) == containerize(r2)
            return containerize(r1) == containerize(r2)
        for t in self.all_tables :
            t1 = getattr(obj1, t)
            t2 = getattr(obj2, t)
            if dictish(t1) != dictish(t2) :
                return False
            if dictish(t1) :
                if set(t1) != set(t2) :
                    return False
                for k in t1 :
                    if not samerow(t1[k], t2[k]) :
                        return False
            else :
                _iter = lambda x : x if containerish(x) else x()
                if not len(list(_iter(t1))) == len(list(_iter(t2))) :
                    return False
                for r1 in _iter(t1):
                    if not any (samerow(r1, r2) for r2 in _iter(t2)) :
                        return False
        return True
    def copy_tic_dat(self, tic_dat, freeze_it = False):
        """
        copies the tic_dat object into a new tic_dat object
        performs a deep copy
        :param tic_dat: a ticdat object
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a deep copy of the tic_dat argument
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = self.TicDat(**{t:getattr(tic_dat, t) for t in self.all_tables})
        return self.freeze_me(rtn) if freeze_it else rtn
    def freeze_me(self, tic_dat):
        """
        Freezes a ticdat object
        :param tic_dat: ticdat object
        :return: tic_dat, after it has been frozen
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        return freeze_me(tic_dat)
    def find_foreign_key_failures(self, tic_dat):
        """
        Finds the foreign key failures for a ticdat object
        :param tic_dat: ticdat object
        :return: A dictionary constructed as follow:
                 The keys are namedTuples with members "native_table", "foreign_table", "mapping"
                 The key data matches the arguments to add_foreign_key that constructed the foreign key.
                 The values are namedTuples with the following members.
                 --> native_values - the values of the native fields that failed to match
                 --> native_pks - the primary key entries of the native table rows
                                  corresponding to the native_values.
                 That is to say, native_values tells you which values in the native table
                 can't find a foreign key match, and thus generate a foreign key failure.
                 native_pks tells you which native table rows will be removed if you call remove_foreign_keys_failures.
                 contained a child-to-parent foreign key failure.
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn_values, rtn_pks = clt.defaultdict(set), clt.defaultdict(set)
        for native, fks in self._foreign_keys_by_native().items():
            def getcell(native_pk, native_data_row, field_name):
                 assert field_name in self.primary_key_fields.get(native, ()) + \
                                      self.data_fields.get(native, ())
                 if [field_name] == list(self.primary_key_fields.get(native, ())):
                     return native_pk
                 if field_name in native_data_row:
                     return native_data_row[field_name]
                 return native_pk[self.primary_key_fields[native].index(field_name)]
            for fk in fks:
                foreign_to_native = {v:k for k,v in ((fk.mapping,)
                                     if type(fk.mapping) is _ForeignKeyMapping else fk.mapping)}
                for native_pk, native_data_row in (getattr(tic_dat, native).items()
                            if dictish(getattr(tic_dat, native))
                            else enumerate(getattr(tic_dat, native))):
                    foreign_pk = tuple(getcell(native_pk, native_data_row, foreign_to_native[_fpk])
                                       for _fpk in self.primary_key_fields[fk.foreign_table])
                    foreign_pk = foreign_pk[0] if len(foreign_pk) == 1 else foreign_pk
                    if foreign_pk not in getattr(tic_dat, fk.foreign_table):
                        rtn_pks[fk].add(native_pk)
                        if type(fk.mapping) is _ForeignKeyMapping :
                            rtn_values[fk].add(getcell(native_pk, native_data_row,
                                                       fk.mapping.native_field))
                        else:
                            rtn_values[fk].add(tuple(getcell(native_pk,
                                    native_data_row, _.native_field) for _ in fk.mapping))
        assert set(rtn_pks) == set(rtn_values)
        RtnType = namedtuple("ForeignKeyFailures", ("native_values", "native_pks"))

        return {k:RtnType(tuple(rtn_values[k]), tuple(rtn_pks[k])) for k in rtn_pks}

    def remove_foreign_keys_failures(self, tic_dat, propagate=True):
        """
        Removes foreign key failures (i.e. child records with no parent table record)
        :param tic_dat: ticdat object
        :param propagate boolean: remove cascading failures? (if removing the child record
                                  results in new failures, should those be removed as well?)
        :return: tic_dat, with the foreign key failures removed
        """
        fk_failures = self.find_foreign_key_failures(tic_dat)
        for fk, (_, failed_pks) in fk_failures.items():
            for failed_pk in failed_pks:
                if failed_pk in getattr(tic_dat, fk.native_table) :
                    del(getattr(tic_dat, fk.native_table)[failed_pk])
        if fk_failures and propagate:
            return self.remove_foreign_keys_failures(tic_dat)
        return tic_dat

    def obfusimplify(self, tic_dat, table_prepends = {}, skip_tables = (), freeze_it = False) :
        """
        copies the tic_dat object into a new, obfuscated, simplified tic_dat object
        :param tic_dat: a ticdat object
        :param table_prepends: a dictionary with mapping each table to the prepend it should apply
                               when its entries are renamed.  A valid table prepend must be all caps and
                               not end with I. Should be restricted to entity tables (single field primary
                               that is not a foreign key child)
        :param skip_tables: a listing of entity tables whose single field primary key shouldn't be renamed
        :param freeze_it: boolean. should the returned copy be frozen?
        :return: A named tuple with the following components.
                 copy : a deep copy of the tic_dat argument, with the single field primary key values
                        renamed to simple "short capital letters followed by numbers" strings.
                 renamings : a dictionary matching the new entries to their original (table, primary key value)
                             this entry can be used to cross reference any diagnostic information gleaned from the
                             obfusimplified copy to the original names. For example, "P5 has no production"
                             can easily be recognized as "Product KX12212 has no production".
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not self.find_foreign_key_failures(tic_dat),
               "Cannot obfusimplify an object with foreign key failures")
        verify(not self.generator_tables, "Cannot obfusimplify a tic_dat that uses generators")
        verify(not set(table_prepends).intersection(skip_tables),
               "Can't specify a table prepend for an entity that you're skipping")
        verify(self._has_been_used,
               "The cascading foreign keys won't necessarily be present until the factory is used")

        entity_tables = {t for t,v in self.primary_key_fields.items() if len(v) == 1}
        foreign_keys_by_native = self._foreign_keys_by_native()
        # if a native table is one-to-one with a foreign table, it isn't an entity table
        for nt in entity_tables.intersection(foreign_keys_by_native):
            for fk in foreign_keys_by_native[nt]:
                if (((type(fk.mapping) is _ForeignKeyMapping) and
                     (fk.mapping.native_field == self.primary_key_fields[nt])) or
                    ((type(fk.mapping) is not _ForeignKeyMapping) and
                     ({_.native_field for _ in fk.mapping} == set(self.primary_key_fields[nt])))) :
                    entity_tables = entity_tables.difference({nt})
        verify(entity_tables.issuperset(skip_tables), "should only specify entity tables to skip")
        entity_tables = entity_tables.difference(skip_tables)

        for k,v in table_prepends.items():
            verify(k in self.all_tables, "%s is not a table name")
            verify(len(self.primary_key_fields.get(k, ())) ==1, "%s does not have a single primary key field"%k)
            verify(k in entity_tables, "%s is not an entity table due to child foreign key relationship"%k)
            verify(utils.stringish(v) and  set(v).issubset(uppercase) and not v.endswith("I"),
                   "Your table_prepend string %s is not an all uppercase string ending in a letter other than I")
        verify(len(set(table_prepends.values())) == len(table_prepends.values()),
               "You provided duplicate table prepends")
        table_prepends = dict(table_prepends)
        for t in entity_tables.difference(table_prepends):
            def getname():
                chars = tuple(_ for _ in uppercase if _ != "I")
                from_table = "".join(_ for _ in t.upper() if _ in chars)
                rtnnum = 1
                while True:
                    if rtnnum <= len(from_table):
                        yield from_table[:rtnnum]
                    else :
                        yield from_table + "".join(chars[_] for _ in
                                  utils.baseConverter(rtnnum-len(from_table)-1, len(chars)))
                    rtnnum += 1
            namegetter = getname()
            name = namegetter.next()
            while name in table_prepends.values():
                name = namegetter.next()
            table_prepends[t]=name
        assert set(entity_tables) == set(table_prepends)

        reverse_renamings = {}
        for t in table_prepends :
            for i,k in enumerate(sorted(getattr(tic_dat, t))) :
                reverse_renamings[t, k] = "%s%s"%(table_prepends[t],i+1)
        foreign_keys = {}
        for fk in self.foreign_keys:
            nt = fk.native_table
            if fk.foreign_table in table_prepends:
                if type(fk.mapping) is _ForeignKeyMapping:
                    foreign_keys = dict(foreign_keys, **{(nt,fk.mapping.native_field) : fk.foreign_table})
                else:
                    foreign_keys = dict(foreign_keys, **{(nt,nf) : fk.foreign_table
                                    for nf, ff in fk.mappings})
        # remember -- we've used this factory so any cascading foreign keys are present
        rtn_dict  = clt.defaultdict(dict)
        for t in self.all_tables:
            read_table = getattr(tic_dat, t)
            def fix_all_row(all_row):
                return {k: reverse_renamings[foreign_keys[t, k], v] if (t,k) in foreign_keys else v
                           for k,v in all_row.items()}
            if dictish(read_table):
                for pk, data_row in read_table.items():
                    if len(self.primary_key_fields.get(t, ())) == 1:
                        pkf = self.primary_key_fields[t][0]
                        if (t,pkf) in foreign_keys:
                            new_pk = reverse_renamings[foreign_keys[t,pkf], pk]
                        else:
                            new_pk = reverse_renamings.get((t, pk), pk)
                        rtn_dict[t][new_pk] = fix_all_row(data_row)
                    else :
                        assert containerish(pk) and len(pk) == len(self.primary_key_fields[t])
                        new_row = fix_all_row(dict(data_row, **{pkf: pkv for pkf, pkv in
                                                    zip(self.primary_key_fields[t], pk)}))
                        new_pk = tuple(new_row[_] for _ in self.primary_key_fields[t])
                        rtn_dict[t][new_pk] = {k:new_row[k] for k in data_row}
            else :
                rtn_dict[t] = []
                for data_row in read_table:
                    rtn_dict[t].append(fix_all_row(data_row))

        RtnType = namedtuple("ObfusimplifyResults", "copy renamings")

        rtn = RtnType(self.freeze_me(self.TicDat(**rtn_dict)) if freeze_it else self.TicDat(**rtn_dict),
                      {v:k for k,v in reverse_renamings.items()})
        assert not self.find_foreign_key_failures(rtn.copy)
        assert len(rtn.renamings) == len(reverse_renamings)
        return rtn
def freeze_me(x) :
    """
    Freezes a ticdat object
    :param x: ticdat object
    :return: x, after it has been frozen
    """
    verify(hasattr(x, "_freeze"), "x not a freezeable object")
    if not getattr(x, "_isFrozen", False) : #idempotent
        x._freeze()
    assert x._isFrozen
    return x