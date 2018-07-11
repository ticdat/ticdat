"""
Create PanDatFactory. Along with ticdatfactory.py, one of two main entry points for ticdat library.
PEP8
"""

import ticdat.utils as utils
from ticdat.utils import ForeignKey, ForeignKeyMapping, TypeDictionary, verify, dictish
from ticdat.utils import lupish, deep_freeze, containerish, FrozenDict, safe_apply
from itertools import count
from math import isnan
import collections as clt
pd, DataFrame = utils.pd, utils.DataFrame # if pandas not installed will be falsey



class PanDatFactory(object):
    """
    Creates simple schema shin functionality for defining schemas with pandas.DataFrame objects.
    This class is constructed with a schema, and can be used to generate PanDat objects,
    or to write PanDat objects to different file types. Analytical code that uses PanDat objects can be used,
    without change, on different data sources.
    """
    @property
    def primary_key_fields(self):
        return self._primary_key_fields
    @property
    def data_fields(self):
        return self._data_fields
    def schema(self, include_ancillary_info = False):
        """
        :param include_ancillary_info: if True, include all the foreign key, default, and data type information
                                       as well. Otherwise, just return table-fields dictionary
        :return: a dictionary with table name mapping to a list of lists
                 defining primary key fields and data fields
                 If include_ancillary_info, this table-fields dictionary is just one entry in a more comprehensive
                 dictionary.
        """
        tables_fields = {t: [list(self.primary_key_fields.get(t, [])),
                             list(self.data_fields.get(t, []))]
                          for t in set(self.primary_key_fields).union(self.data_fields)}
        for t in self.generic_tables:
            tables_fields[t]='*'
        if not include_ancillary_info:
            return tables_fields
        return {"tables_fields" : tables_fields,
                "foreign_keys" : self.foreign_keys,
                "default_values" : self.default_values,
                "data_types" : self.data_types}
    @staticmethod
    def create_from_full_schema(full_schema):
        """
        create a PanDatFactory complete with default values, data types, and foreign keys
        :param full_schema: a dictionary consistent with the data returned by a call to schema()
                            with include_ancillary_info = True
        :return: a TicDatFactory reflecting the tables, fields, default values, data types,
                 and foreign keys consistent with the full_schema argument
        """
        verify(dictish(full_schema) and set(full_schema) == {"tables_fields", "foreign_keys",
                                                             "default_values", "data_types"},
               "full_schema should be the result of calling schema(True) for some TicDatFactory")
        fks = full_schema["foreign_keys"]
        verify( (not fks) or (lupish(fks) and all(lupish(_) and len(_) >= 3 for _ in fks)),
                "foreign_keys entry poorly formed")
        dts = full_schema["data_types"]
        verify( (not dts) or (dictish(dts) and all(map(dictish, dts.values())) and
                              all(all(map(lupish, _.values())) for _ in dts.values())),
                "data_types entry poorly formatted")
        dvs = full_schema["default_values"]
        verify( (not dvs) or (dictish(dvs) and all(map(dictish, dvs.values()))),
                "default_values entry poorly formatted")

        rtn = PanDatFactory(**full_schema["tables_fields"])
        for fk in (fks or []):
            rtn.add_foreign_key(*fk[:3])
        for t,fds in (dts or {}).items():
            for f,dt in fds.items():
                rtn.set_data_type(t, f, *dt)
        for t,fdvs in (dvs or {}).items():
            for f, dv in fdvs.items():
                rtn.set_default_value(t,f,dv)
        return rtn
    def clone(self):
        """
        clones the TicDatFactory
        :return: a clone of the TicDatFactory
        """
        rtn = PanDatFactory.create_from_full_schema(self.schema(include_ancillary_info=True))
        for tbl, row_predicates in self._data_row_predicates.items():
            for pn, p in row_predicates.items():
                rtn.add_data_row_predicate(tbl, predicate=p, predicate_name=pn)
        return rtn
    @property
    def default_values(self):
        return deep_freeze(self._default_values)
    @property
    def data_types(self):
        return utils.FrozenDict({t : utils.FrozenDict({k :v for k,v in vd.items()})
                                for t,vd in self._data_types.items()})
    def set_data_type(self, table, field, number_allowed = True,
                      inclusive_min = True, inclusive_max = False, min = 0, max = float("inf"),
                      must_be_int = False, strings_allowed= (), nullable = False):
        """
        sets the data type for a field. By default, fields don't have types. Adding a data type doesn't block
        data of the wrong type from being entered. Data types are useful for recognizing errant data entries
        with find_data_type_failures(). Errant data entries can be replaced with replace_data_type_failures().
        :param table: a table in the schema
        :param field: a data field for this table
        :param number_allowed: boolean does this field allow numbers?
        :param inclusive_min: boolean : if number allowed, is the min inclusive?
        :param inclusive_max: boolean : if number allowed, is the max inclusive?
        :param min: if number allowed, the minimum value
        :param max: if number allowed, the maximum value
        :param must_be_int: boolean : if number allowed, must the number be integral?
        :param strings_allowed: if a collection - then a list of the strings allowed.
                                The empty collection prohibits strings.
                                If a "*", then any string is accepted.
        :param nullable : boolean : can this value contain null (aka None aka nan (since pandas treats null as nan))
        :return:
        """
        verify(not self._has_been_used,
               "The data types can't be changed after a PanDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(table not in self.generic_tables, "Cannot set data type for generic table")
        verify(field in self.data_fields[table] + self.primary_key_fields[table],
               "%s does not refer to a field for %s"%(field, table))

        verify((strings_allowed == '*') or
               (containerish(strings_allowed) and all(utils.stringish(x) for x in strings_allowed)),
"""The strings_allowed argument should be a container of strings, or the single '*' character.""")
        if utils.containerish(strings_allowed):
            strings_allowed = tuple(strings_allowed) # defensive copy
        if number_allowed:
            verify(utils.numericish(max), "max should be numeric")
            verify(utils.numericish(min), "min should be numeric")
            verify(max >= min, "max cannot be smaller than min")
            self._data_types[table][field] = TypeDictionary(number_allowed=True,
                strings_allowed=strings_allowed,  nullable = bool(nullable),
                min = min, max = max, inclusive_min= bool(inclusive_min), inclusive_max = bool(inclusive_max),
                must_be_int = bool(must_be_int))
        else :
            self._data_types[table][field] = TypeDictionary(number_allowed=False,
                strings_allowed=strings_allowed,  nullable = bool(nullable),
                min = 0, max = float("inf"), inclusive_min= True, inclusive_max = True,
                must_be_int = False)

    def clear_data_type(self, table, field):
        """
        clears the data type for a field. By default, fields don't have types.  Adding a data type doesn't block
        data of the wrong type from being entered. Data types are useful for recognizing errant data entries.
        If no data type is specified (the default) then no errant data will be recognized.
        :param table: table in the schema
        :param field:
        :return:
        """
        if field not in self._data_types.get(table, ()):
            return
        verify(not self._has_been_used,
               "The data types can't be changed after a PanDatFactory has been used.")
        del(self._data_types[table][field])

    def add_data_row_predicate(self, table, predicate, predicate_name = None):
        """
        Adds a data row predicate for a table. Row predicates can be used to check for
        sophisticated data integrity problems of the sort that can't be easily handled with
        a data type rule. For example, a min_supply column can be verified to be no larger than
        a max_supply column.
        !!! NB!!!!
                   pandas will render None as nan.
                   Don't check for None in your predicate functions, use math.isnan instead
        !!!!!!!!!!
        :param table: table in the schema
        :param predicate: A one argument function that accepts a table row as an argument and returns
                          Truthy if the row is valid and Falsey otherwise. The argument passed to
                          predicate will be a dict that maps field name to data value for all fields
                          (both primary key and data field) in the table.
                          Note - if None is passed as a predicate, then any previously added
                          predicate matching (table, predicate_name) will be removed.
        :param predicate_name: The name of the predicate. If omitted, the smallest non-colliding
                               number will be used.
        :return:
        """
        verify(not self._has_been_used,
               "The data row predicates can't be changed after a TicDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)

        if predicate is None:
            if table in self._data_row_predicates:
                self._data_row_predicates[table].pop(predicate_name, None)
            return

        verify(callable(predicate), "predicate should be a one argument function")
        if predicate_name is None:
            predicate_name = next(i for i in count() if i not in self._data_row_predicates[table])
        self._data_row_predicates[table][predicate_name] = predicate

    def set_default_value(self, table, field, default_value):
        """
        sets the default value for a specific field
        :param table: a table in the schema
        :param field: a field in the table
        :param default_value: the default value to apply
        :return:
        """
        verify(not self._has_been_used,
               "The default values can't be changed after a TicDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(field in self.data_fields[table] + self.primary_key_fields[table],
               "%s does not refer to a field for %s"%(field, table))
        verify(utils.acceptable_default(default_value), "%s can not be used as a default value"%default_value)
        self._default_values[table][field] = default_value

    def set_default_values(self, **tableDefaults):
        """
        sets the default values for the fields
        :param tableDefaults:
             A dictionary of named arguments. Each argument name (i.e. each key) should be a table name
             Each value should itself be a dictionary mapping data field names to default values
             Ex: tdf.set_default_values(categories = {"minNutrition":0, "maxNutrition":float("inf")},
                         foods = {"cost":0}, nutritionQuantities = {"qty":0})
        :return:
        """
        verify(not self._has_been_used,
               "The default values can't be changed after a TicDatFactory has been used.")
        for k,v in tableDefaults.items():
            verify(k in self.all_tables, "Unrecognized table name %s"%k)
            verify(dictish(v) and set(v).issubset(self.data_fields[k] + self.primary_key_fields[k]),
                "Default values for %s should be a dictionary mapping field names to values"
                %k)
            verify(all(utils.acceptable_default(_v) for _v in v.values()), "some default values are unacceptable")
            self._default_values[k] = dict(self._default_values[k], **v)

    def clear_foreign_keys(self, native_table = None):
        """
        create a PanDatFactory
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
        for (native,foreign), nativeforeignmappings in self._foreign_keys.items():
            for n_f_mapping in nativeforeignmappings :
                mappings = tuple(ForeignKeyMapping(nf,ff) for nf,ff in n_f_mapping)
                mappings = mappings[0] if len(mappings)==1 else mappings
                def half_card(tbl, fields):
                    assert fields.issubset(self._all_fields(tbl))
                    pkfs = self.primary_key_fields.get(tbl, ())
                    if pkfs and fields.issuperset(pkfs):
                        return "one"
                    return "many"
                cardinality = "%s-to-%s"%(half_card(native, {_[0] for _ in n_f_mapping}),
                                          half_card(foreign, {_[1] for _ in n_f_mapping}))
                rtn.append(ForeignKey(native, foreign, mappings, cardinality))
        assert len(rtn) == len(set(rtn))
        return tuple(rtn)
    def _foreign_keys_by_native(self):
        rtn = clt.defaultdict(list)
        for fk in self.foreign_keys:
            rtn[fk.native_table].append(fk)
        return utils.FrozenDict({k:frozenset(v) for k,v in rtn.items()})
    def _all_fields(self, table):
        assert table in self.all_tables
        return set(self.primary_key_fields.get(table, ())).union(self.data_fields.get(table, ()))
    def add_foreign_key(self, native_table, foreign_table, mappings):
        """
        Adds a foreign key relationship to the schema.  Adding a foreign key doesn't block
        the entry of child records that fail to find a parent match. It does make it easy
        to recognize such records (with find_foreign_key_failures()) and to remove such records
        (with remove_foreign_keys_failures())
        :param native_table: (aka child table). The table with fields that must match some other table.
        :param foreign_table: (aka parent table). The table providing the matching entries.
        :param mappings: For simple foreign keys, a [native_field, foreign_field] pair.
                         For compound foreign keys an iterable of [native_field, foreign_field]
                         pairs.
        :return:
        """
        verify(not self._has_been_used,
                "The foreign keys can't be changed after a PanDatFactory has been used.")
        for t in (native_table, foreign_table):
            verify(t in self.all_tables, "%s is not a table name"%t)
            verify(t not in self.generic_tables, "%s is a generic table"%t)
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
            verify(k in self._all_fields(native_table),
                   "%s does not refer to one of %s 's fields"%(k, native_table))
            verify(v in self._all_fields(foreign_table),
                   "%s does not refer to one of %s 's fields"%(v, foreign_table))
        self._foreign_keys[native_table, foreign_table].add(tuple(_mappings.items()))
    def _simple_fk(self, ftbl, fk):
        assert ftbl in self.all_tables
        ftbl_pks = set(self.primary_key_fields.get(ftbl,()))
        assert lupish(fk) and all(lupish(_) and len(_) == 2 for _ in fk)
        ffs = {_[1] for _ in fk}
        assert ffs.issubset(ftbl_pks.union(self.data_fields.get(ftbl,())))
        return ftbl_pks == ffs
    def _complex_fks(self):
        return tuple((native, foreign, fk) for (native, foreign), fks in self._foreign_keys.items()
                    for fk in fks if not self._simple_fk(foreign, fk))
    def _trigger_has_been_used(self):
        self._has_been_used = True
    def __init__(self, **init_fields):
        """
        create a PanDatFactory
        :param init_fields: a mapping of tables to primary key fields
                            and data fields. Each field listing consists
                            of two sub lists ... first primary keys fields,
                            than data fields.
        ex: PanDatFactory (categories =  [["name"],["Min Nutrition", "Max Nutrition"]],
                           foods  =  [["Name"],["Cost"]]
                           nutritionQuantities = [["Food", "Category"],["Qty"]])
                           Use '*' instead of a pair of lists for generic tables
        ex: PanDatFactory (typical_table = [["Primary Key Field"],["Data Field"]],
                           generic_table = '*')
        :return: a PanDatFactory
        """
        verify(DataFrame and pd, "Need to install pandas in order to create a PanDatFactory")
        self._has_been_used = False
        verify(not any(x.startswith("_") for x in init_fields),
               "table names shouldn't start with underscore")
        verify(not any(" " in x for x in init_fields), "table names shouldn't have white space")
        verify(len(init_fields) == len({_.lower() for _ in init_fields}),
               "there are case insensitive duplicate table names")
        for k,v in init_fields.items():
            verify(v == '*' or
                   (containerish(v) and len(v) == 2 and all(containerish(_) for _ in v)),
                   ("Table %s needs to indicate it is a generic table by using '*'\n" +
                    "or specify two sublists, one for primary key fields and one for data fields")
                   %k)
            if v != '*':
                verify(all(utils.stringish(s) for _ in v for s in _),
                       "The field names for %s need to be strings"%k)
                verify(v[0] or v[1], "No field names specified for table %s"%k)
                verify(len(set(v[0]).union(v[1])) == len(v[0])+len(v[1]),
                       "There are duplicate field names for table %s"%k)
                verify(len({_.lower() for _ in list(v[0]) + list(v[1])}) == len(v[0])+len(v[1]),
                       "There are case insensitive duplicate field names for %s"%k)
        self.generic_tables = frozenset(k for k,v in init_fields.items() if v == '*')
        self._primary_key_fields = FrozenDict({k : tuple(v[0])for k,v in init_fields.items()
                                               if v != '*'})
        self._data_fields = FrozenDict({k : tuple(v[1]) for k,v in init_fields.items() if v != '*'})
        self._default_values = clt.defaultdict(dict)
        for tbl,flds in self._data_fields.items():
            for fld in flds:
                self._default_values[tbl][fld] = 0
        self._data_types = clt.defaultdict(dict)
        self._data_row_predicates = clt.defaultdict(dict)
        self._foreign_keys = clt.defaultdict(set)
        self.all_tables = frozenset(init_fields)
        superself = self
        class PanDat(object):
            def __repr__(self):
                tlen = lambda t: len(getattr(self, t)) if isinstance(getattr(self, t), DataFrame) else None
                return "pd: {" + ", ".join("%s: %s"%(t, tlen(t)) for t in superself.all_tables) + "}"
            def __init__(self, **init_tables):
                superself._trigger_has_been_used()
                for t in init_tables :
                    verify(t in superself.all_tables, "Unexpected table name %s"%t)
                    tbl = safe_apply(DataFrame)(init_tables[t])
                    verify(isinstance(tbl, DataFrame), "Failed to provide a valid DataFrame for %s"%t)
                    setattr(self, t, tbl.copy())
                missing_fields = {(t, f) for t in superself.all_tables for f in
                                  superself.primary_key_fields.get(t, ()) + superself.data_fields.get(t, ())
                                  if f not in getattr(self, t).columns}
                verify(not missing_fields,
                       "The following are (table, field) pairs missing from the data.\n%s"%missing_fields)
        self.PanDat = PanDat

    def good_pan_dat_object(self, data_obj, bad_message_handler = lambda x : None):
        """
        determines if an object is a valid PanDat object for this schema
        :param data_obj: the object to verify
        :param bad_message_handler: a call back function to receive description of any failure message
        :return: True if the dataObj can be recognized as a PanDat data object. False otherwise.
        """
        verify(DataFrame and pd, "Need to install pandas")
        for t in self.all_tables:
            if not hasattr(data_obj, t) :
                bad_message_handler(t + " not an attribute.")
                return False
            if not isinstance(getattr(data_obj, t), DataFrame):
                bad_message_handler(t + " is not a DataFrame")
                return False
        missing_fields = {(t, f) for t in self.all_tables for f in
                          self.primary_key_fields.get(t, ()) + self.data_fields.get(t, ())
                          if f not in getattr(data_obj, t).columns}
        if missing_fields:
            bad_message_handler("The following are (table, field) pairs missing from the data.\n%s"%missing_fields)
            return False
        return True
    def copy_pan_dat(self, pan_dat):
        """
        copies the tic_dat object into a new tic_dat object
        performs a deep copy
        :param pan_dat: a pandat object
        :return: a deep copy of the pan_dat argument
        """
        msg  = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        return self.PanDat(**{t:getattr(pan_dat, t) for t in self.all_tables})
    def copy_to_tic_dat(self, pan_dat, freeze_it = False):
        """
        copies the pan_dat object into a new tic_dat object
        performs a deep copy
        :param pan_dat: a pandat object
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a deep copy of the pan_dat argument in tic_dat format
        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        from ticdat import TicDatFactory
        tdf = TicDatFactory(**self.schema())
        def df(t):
            rtn = getattr(pan_dat, t)
            if self.primary_key_fields.get(t, ()):
                return rtn.set_index(list(self.primary_key_fields[t]), drop=False)
            return rtn
        rtn = tdf.TicDat(**{t:df(t) for t in self.all_tables})
        return tdf.freeze_me(rtn) if freeze_it else rtn
    def _same_data(self, obj1, obj2, epsilon = 0):
        from ticdat import TicDatFactory
        tdf = TicDatFactory(**self.schema())
        return tdf._same_data(self.copy_to_tic_dat(obj1), self.copy_to_tic_dat(obj2), epsilon=epsilon)
    def find_data_type_failures(self, pan_dat):
        """
        Finds the data type failures for a pandat object
        :param pan_dat: pandat object
        :return: A dictionary constructed as follow:
                 The keys are namedTuples with members "table", "field". Each (table,field) pair
                 has data values that are inconsistent with its data type. (table, field) pairs
                 with no data type at all are never part of the returned dictionary.
                 The values are DataFrames that contain the subset of rows that exhibit data failures
                 for this specific table, field pair.
        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))

        rtn = {}
        TableField = clt.namedtuple("TableField", ["table", "field"])
        for table, type_row in self._data_types.items():
            _table = getattr(pan_dat, table)
            for field, data_type in type_row.items():
                def bad_row(row):
                    data = row[field]
                    # pandas turns None into nan
                    return not data_type.valid_data(None if safe_apply(isnan)(data) else data)
                bad_table = _table[_table.apply(bad_row, axis=1)]
                if len(bad_table):
                    rtn[TableField(table, field)] = bad_table.copy()
        return rtn
    def find_data_row_failures(self, pan_dat):
        """
        Finds the data row failures for a ticdat object
        :param pan_dat: a pandat object
        :return: A dictionary constructed as follow:

                 The keys are namedTuples with members "table", "predicate_name".

                 The values are DataFrames that contain the subset of rows that exhibit data failures
                 for this specific table, predicate pair.
        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = {}
        TPN = clt.namedtuple("TablePredicateName", ["table", "predicate_name"])
        for tbl, row_predicates in self._data_row_predicates.items():
            for pn, p in row_predicates.items():
                _table = getattr(pan_dat, tbl)
                bad_row = lambda row: not p(row)
                bad_table = _table[_table.apply(bad_row, axis=1)]
                if len(bad_table):
                    rtn[TPN(tbl, pn)] = bad_table.copy()
        return rtn
    def find_foreign_key_failures(self, pan_dat):
        """
        Finds the foreign key failures for a pandat object
        :param pan_dat: pandat object
        :return: A dictionary constructed as follow:
                 The keys are namedTuples with members "native_table", "foreign_table",
                 "mapping", "cardinality".
                 The key data matches the arguments to add_foreign_key that constructed the
                 foreign key (with "cardinality" being deduced from the overall schema).

                 The values are DataFrames that contain the subset of native table rows that fail to find
                 foreign table matching defined by the associated returned key.
        """
        rtn = {}
        for fk, rows in self._find_foreign_key_failure_rows(pan_dat).items():
            native, foreign, mappings, card = fk
            rtn[fk] = getattr(pan_dat, native)[rows]
        return rtn
    def _find_foreign_key_failure_rows(self, pan_dat):
        msg  = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = {}
        for fk in self.foreign_keys:
            native, foreign, mappings, card = fk
            child = getattr(pan_dat, native).copy(deep=True)
            # makes sense to deep copy the possibly smaller drop_duplicates slice of the parent table
            parent = getattr(pan_dat, foreign)
            _ = 0
            while any("_%s_"%_ in c for c in set(parent.columns).union(child.columns)):
                _ += 1
            magic_field = "_%s_"%_
            if all(hasattr(mappings, _) for _ in ["native_field", "foreign_field"]):
                parent = parent.drop_duplicates(mappings.foreign_field, inplace=False).copy(deep=True)
                parent[mappings.native_field] = parent[mappings.foreign_field]
                new_index = mappings.native_field
            else:
                parent = parent.drop_duplicates([_.foreign_field for _ in mappings], inplace=False).copy(deep=True)
                for _ in mappings:
                    parent[_.native_field] = parent[_.foreign_field]
                new_index = [_.native_field for _ in mappings]
            # sadly a join might knacker the row order, hence the ugliness with magic_field*2 for child
            parent[magic_field] = True
            child.insert(0, magic_field*2, range(0, len(child)))
            parent.set_index(new_index, drop=True, inplace=True)
            child.set_index(new_index, drop=True, inplace=True)
            joined = child.join(parent, rsuffix=magic_field)
            bad_rows = set(joined[joined[magic_field] != True][magic_field*2])
            if bad_rows:
                rtn[fk] = list(child.apply(lambda row: row[magic_field*2] in bad_rows, axis=1))
        return rtn
    def remove_foreign_keys_failures(self, pan_dat):
        """
        Removes foreign key failures (i.e. child records with no parent table record)
        :param pan_dat: pandat object (will be side-effected)
        :return: pan_dat, with the foreign key failures removed
                 Note that all foreign key removals are cascading. When a child removal results in
                 new foreign key failures, those failures are removed as well.
        """
        remove_rows = self._find_foreign_key_failure_rows(pan_dat)
        while remove_rows:
            # just performing one bulk removal per iteration, since fks can intermingle in complicated ways
            fk = next(iter(remove_rows))
            native, foreign, mappings, card = fk
            rows = remove_rows[fk]
            setattr(pan_dat, native, getattr(pan_dat, native)[[not _ for _ in rows]].copy(deep=True))
            remove_rows = self._find_foreign_key_failure_rows(pan_dat)

        return pan_dat
    # NEED fk failure testing
    # NEED find_duplicates since that we don't step on duplicates during reading