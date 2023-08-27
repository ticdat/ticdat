"""
Create TicDatFactory. Along with pandatfactory.py, one of two main entry points for ticdat library.
PEP8
"""
import collections as clt
from collections import namedtuple, defaultdict
import ticdat.utils as utils
from ticdat.utils import verify, freezable_factory, FrozenDict, FreezeableDict
from ticdat.utils import dictish, containerish, deep_freeze, lupish, safe_apply
from ticdat.utils import ForeignKey, ForeignKeyMapping, TypeDictionary, RowPredicateInfo
from string import ascii_uppercase as uppercase
from itertools import count
import ticdat.xls as xls
import ticdat.csvtd as csv
import ticdat.sqlitetd as sql
import ticdat.mdb as mdb
import ticdat.jsontd as json
from ticdat.pgtd import PostgresTicFactory
import sys
import math
try:
    import amplpy
except:
    amplpy = None

pd, DataFrame = utils.pd, utils.DataFrame # if pandas not installed will be falsey

def _keylen(k) :
    if not utils.containerish(k) :
        return 1
    try:
        rtn = len(k)
    except :
        rtn = 0
    return rtn

class TicDatFactory(freezable_factory(object, "_isFrozen", {"opl_prepend", "ampl_prepend"})) :
    """
    Primary class for ticdat library. This class is constructed with a schema.
    It can be used to generate TicDat objects, to write TicDat objects to
    different file types, or to perform bulk query operations to diagnose
    common data integrity failures.

    Analytical code that uses TicDat objects can be used, without change, on different data
    sources, thus facilitating the "separate model from data" design goal.

    :param init_fields: a mapping of tables to primary key fields and data fields. Each field listing consists
                        of two sub lists ... first primary keys fields, than data fields.

    ex:
    ```TicDatFactory (categories =  [["name"],["minNutrition", "maxNutrition"]],
                       foods  =  [["name"],["cost"]]
                       nutritionQuantities = [["food", "category"],["qty"]])```

    Use '*' instead of a pair of lists for generic tables,
                       which will render as pandas.DataFrame objects.

    ex:
    ```TicDatFactory (typical_table = [["primary key field"],["data field"]],
                       generic_table = '*')```
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
                "data_types" : self.data_types,
                "parameters": self.parameters,
                "infinity_io_flag": self.infinity_io_flag,
                "xlsx_trailing_empty_rows": self.xlsx_trailing_empty_rows,
                "duplicates_ticdat_init": self.duplicates_ticdat_init,
                "tooltips": utils.make_tooltips_dict_json_friendly(self.tooltips)}
    @staticmethod
    def create_from_full_schema(full_schema):
        """

        create a TicDatFactory complete with default values, data types, and foreign keys

        :param full_schema: a dictionary consistent with the data returned by a call to schema()
                            with include_ancillary_info = True

        :return: a TicDatFactory reflecting the tables, fields, default values, data types,
                 and foreign keys consistent with the full_schema argument
        """
        old_schema = {"tables_fields", "foreign_keys", "default_values", "data_types"}
        verify(dictish(full_schema) and set(full_schema).issuperset(old_schema) and  set(full_schema) in
               utils.all_subsets(old_schema.union({"parameters", "infinity_io_flag", "xlsx_trailing_empty_rows",
                                                   "duplicates_ticdat_init", "tooltips"})),
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
        params = full_schema.get("parameters", {})
        if params:
            verify(dictish(params) and all(map(utils.stringish, params)), "parameters not well formatted")
            verify(all(len(v) == 2 and (v[0] is None or len(v[0]) in [8, 9])
                       and not containerish(v[1]) for v in params.values()),
                   "parameters improperly formatted")
        rtn = TicDatFactory(**full_schema["tables_fields"])
        for fk in (fks or []):
            rtn.add_foreign_key(*fk[:3])
        for t,fds in (dts or {}).items():
            for f,dt in fds.items():
                rtn.set_data_type(t, f, *dt)
        for t,fdvs in (dvs or {}).items():
            for f, dv in fdvs.items():
                rtn.set_default_value(t,f,dv)
        for p, (dt, df) in (params or {}).items():
            if dt is None:
                rtn.add_parameter(p, df, enforce_type_rules=False)
            else:
                rtn.add_parameter(p, *((df,) + tuple(dt)), enforce_type_rules=True)
        if "infinity_io_flag" in full_schema:
            rtn.set_infinity_io_flag(full_schema["infinity_io_flag"])
        if "xlsx_trailing_empty_rows" in full_schema:
            rtn.set_xlsx_trailing_empty_rows(full_schema["xlsx_trailing_empty_rows"])
        if "duplicates_ticdat_init" in full_schema:
            rtn.set_duplicates_ticdat_init(full_schema["duplicates_ticdat_init"])
        if "tooltips" in full_schema:
            for t, tip_dict in full_schema["tooltips"].items():
                for f, tip in tip_dict.items():
                    rtn.set_tooltip(t, f, tip)
        return rtn
    @property
    def generator_tables(self):
        return deep_freeze(self._generator_tables)
    @property
    def default_values(self):
        return deep_freeze(self._default_values)
    @property
    def parameters(self):
        return FrozenDict(self._parameters)
    @property
    def data_types(self):
        return utils.FrozenDict({t : utils.FrozenDict({k :v for k,v in vd.items()})
                                for t,vd in self._data_types.items()})
    @property
    def tooltips(self):
        return utils.FrozenDict(self._tooltips)

    def set_tooltip(self, table, field, tooltip):
        """
        Set the tooltip for a table, or for a (table, field) pair.

        :param table: a table in the schema

        :param field: an empty string (if you want to set the tooltip for a table)
                      or a field for this table

        :param tooltip: an empty string (if you want to delete a previously set tooltip) or the
                        tooltip you want to set

        :return:

        After calling this function, the tooltips property for this TicDatFactory will be appropriately adjusted.
        """
        utils.set_tooltip(self, table, field, tooltip, self._tooltips)

    def set_data_type(self, table, field, number_allowed = True,
                      inclusive_min = True, inclusive_max = False, min = 0, max = float("inf"),
                      must_be_int = False, strings_allowed= (), nullable = False, datetime = False):
        """
        sets the data type for a field. By default, fields don't have types. Adding a data type doesn't block
        data of the wrong type from being entered. Data types are useful for recognizing errant data entries
        with find_data_type_failures(). Errant data entries can be replaced with replace_data_type_failures().

        :param table: a table in the schema

        :param field: a field for this table

        :param number_allowed: boolean does this field allow numbers?

        :param inclusive_min: boolean : if number allowed, is the min inclusive?

        :param inclusive_max: boolean : if number allowed, is the max inclusive?

        :param min: if number allowed, the minimum value

        :param max: if number allowed, the maximum value

        :param must_be_int: boolean : if number allowed, must the number be integral?

        :param strings_allowed: if a collection - then a list of the strings allowed.
                                The empty collection prohibits strings.
                                If a "*", then any string is accepted.

        :param nullable : boolean : can this value contain null (aka None)


        :param datetime: If truthy, then number_allowed through strings_allowed are ignored. Should the data either
                         be a datetime.datetime object or a string that can be parsed into a datetime.datetime object?
                         Note that the various readers will try to coerce strings into datetime.datetime objects
                         on read for fields with datetime data types. pandas.Timestamp is itself a datetime.datetime,
                         and the bias will be to create such an object.
        :return:
        """
        verify(not self._has_been_used,
               "The data types can't be changed after a TicDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(table not in self.generic_tables, "Cannot set data type for generic table")
        verify(field in self.data_fields[table] + self.primary_key_fields[table],
               "%s does not refer to a field for %s"%(field, table))
        verify(not (table == "parameters" and field in self.data_fields[table] and self.parameters),
               "Don't set the data type for the parameters data field if you are using add_parameters.")

        self._data_types[table][field] = TypeDictionary.safe_creator(number_allowed, inclusive_min, inclusive_max,
                                                                     min, max, must_be_int, strings_allowed, nullable,
                                                                     datetime)
        self._none_as_infinity_bias_cache.clear()

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
               "The data types can't be changed after a TicDatFactory has been used.")
        del(self._data_types[table][field])
        self._none_as_infinity_bias_cache.clear()

    def add_data_row_predicate(self, table, predicate, predicate_name=None,
                               predicate_kwargs_maker=None,
                               predicate_failure_response="Boolean"):
        """
        The purpose of calling add_data_row_predicate is to prepare for a future call to find_data_row_failures.
        See https://bit.ly/3e9pdCP for more details on these two functions.

        Adds a data row predicate for a table. Row predicates can be used to check for
        sophisticated data integrity problems of the sort that can't be easily handled with
        a data type rule. For example, a min_supply column can be verified to be no larger than
        a max_supply column.

        :param table: table in the schema

        :param predicate: A one argument function that accepts a table row as an argument and returns
                          Truthy if the row is valid and Falsey otherwise. (See below, there are other arguments that
                          can refine how predicate works). The row argument passed to predicate will be a dict that
                          maps field name to data value for all fields (both primary key and data field) in the table.
                          Note - if None is passed as a predicate, then any previously added
                          predicate matching (table, predicate_name) will be removed.

        :param predicate_name: The name of the predicate. If omitted, the smallest non-colliding
                               number will be used.

        :param predicate_kwargs_maker: A function used to support predicate if predicate accepts more than just
                                       the row argument. This function accepts a single dat argument and is called
                                       exactly once per find_data_row_failures call. If predicate_kwargs_maker returns a
                                       dict, then this dict is unpacked for each call to predicate. An error (or a bulk
                                       row failure) results if predicate_kwargs_maker fails to return a dict.

        :param predicate_failure_response: Either "Boolean" or "Error Message". If the latter then predicate indicates
                                           a clean row by returning True (the one and only literal True in Python)
                                           and a dirty row by returning a non-empty string (which is an error message).

        See find_data_row_failures for details on handling exceptions thrown by predicate or predicate_kwargs_maker.
        :return:
        """
        verify(not self._has_been_used,
               "The data row predicates can't be changed after a TicDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(table not in self.generic_tables, "Cannot add row predicate for generic table")
        verify(table not in self.generator_tables, "Cannot add row predicate for generator table")

        if predicate is None:
            if table in self._data_row_predicates:
                self._data_row_predicates[table].pop(predicate_name, None)
            return

        verify(callable(predicate), "predicate should be a one argument function")
        verify(not predicate_kwargs_maker or callable(predicate_kwargs_maker),
               "predicate_kwargs_maker should be a one argument function")
        verify(predicate_failure_response in ["Boolean", "Error Message"],
               "predicate_failure_response should be Boolean or Error Message")
        if predicate_name is None:
            predicate_name = next(i for i in count() if i not in self._data_row_predicates[table])
        self._data_row_predicates[table][predicate_name] = RowPredicateInfo(predicate, predicate_kwargs_maker,
                                                                            predicate_failure_response)


    def get_row_predicates(self, table):
        '''
        return all the row predicates for a given table

        :param table: a table in the schema

        :return: a dictionary mapping predicate_name to RowPredicateInfo named tuple (the entries of which
                 are based on the prior call to add_data_row_predicate).
        '''
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        return {k: v for k, v in self._data_row_predicates.get(table, {}).items()}

    def add_parameter(self, name, default_value, number_allowed = True,
                      inclusive_min = True, inclusive_max = False, min = 0, max = float("inf"),
                      must_be_int = False, strings_allowed= (), nullable = False,
                      datetime = False, enforce_type_rules = True):
        """
        Add (or reset) a parameters option. Requires that a parameters table with one primary key field and one
        data field already be present. The legal parameters options will be enforced as part of find_data_row_failures
        Note that if you are using this function, then you would typically read from the parameters table indirectly,
        by using the dictionary returned by create_full_parameters_dict.

        :param name: name of the parameter to add or reset

        :param default_value: default value for the parameter (used for create_full_parameters_dict)

        :param number_allowed: boolean does this parameter allow numbers?

        :param inclusive_min: if number allowed, is the min inclusive?

        :param inclusive_max: if number allowed, is the max inclusive?

        :param min: if number allowed, the minimum value

        :param max: if number allowed, the maximum value

        :param must_be_int: boolean : if number allowed, must the number be integral?

        :param strings_allowed: if a collection - then a list of the strings allowed.
                                The empty collection prohibits strings.
                                If a "*", then any string is accepted.

        :param nullable:  boolean : can this parameter be set to null (aka None)

        :param datetime: If truthy, then number_allowed through strings_allowed are ignored.
                         Should the data either be a datetime.datetime object or a string that can be parsed into a
                         datetime.datetime object?

        :param enforce_type_rules: boolean: ignore all of number_allowed through nullable, and only
                                   enforce the parameter names and default values
        :return:
        """
        verify("parameters" in self.all_tables, "No parameters table")
        verify(len(self.primary_key_fields.get("parameters", [])) ==
               len(self.data_fields.get("parameters", [])) == 1, "parameters table is badly formatted")
        verify(self.data_fields["parameters"][0] not in self._data_types.get("parameters", {}),
                "Don't set the data type for the parameters data field if you are going to use add_parameters.")
        verify(not self._has_been_used,
               "The parameters can't be changed after a TicDatFactory has been used.")
        td = None
        if enforce_type_rules:
            td = TypeDictionary.safe_creator(number_allowed, inclusive_min, inclusive_max,
                                             min, max, must_be_int, strings_allowed, nullable, datetime)
            verify(td.valid_data(default_value), f"{default_value} is not a legal default value for parameter {name}")
        ParameterInfo = namedtuple("ParameterInfo", ["type_dictionary", "default_value"])
        self._parameters[name] = ParameterInfo(td, default_value)

    def remove_parameter(self, name):
        '''
        Undo a previous call to add_parameter.

        :param name: name of the parameter to remove

        :return:
        '''
        verify(name in self._parameters, f"{name} is not a valid parameter")
        self._parameters.pop(name)

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

        Ex:
        ```tdf.set_default_values(categories = {"minNutrition":0, "maxNutrition":float("inf")},
                                  foods = {"cost":0}, nutritionQuantities = {"qty":0})```

        :return:
        """
        verify(not self._has_been_used,
               "The default values can't be changed after a TicDatFactory has been used.")
        for k,v in tableDefaults.items():
            verify(k in self.all_tables, "Unrecognized table name %s"%k)
            verify(dictish(v) and set(v).issubset(set(self.data_fields[k]).union(self.primary_key_fields[k])),
                "Default values for %s should be a dictionary mapping field names to values"
                %k)
            verify(all(utils.acceptable_default(_v) for _v in v.values()), "some default values are unacceptable")
            self._default_values[k] = dict(self._default_values[k], **v)
    def set_generator_tables(self, g):
        """
        sets which tables are to be generator tables. Generator tables are represented as generators
        pulled from the actual data store. This prevents them from being fulled loaded into memory.
        Generator tables are only appropriate for truly massive data tables with no primary key.

        :param g: An iterable of table name.

        :return:
        """
        verify(not self._has_been_used,
               "The generator tables can't be changed after a TicDatFactory has been used.")
        verify(containerish(g) and set(g).issubset(self.all_tables),
               "Generator_tables should be a container of table names")
        verify(not set(g).intersection(self.generic_tables),
               "Generator tables cannot refer to generic tables.\n" +
               "I.e. generic tables cannot be generator tables.")
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
        for (native,foreign), nativeforeignmappings in self._foreign_keys.items():
            for n_f_mapping in nativeforeignmappings :
                mappings = tuple(ForeignKeyMapping(nf,ff) for nf,ff in n_f_mapping)
                mappings = mappings[0] if len(mappings)==1 else mappings
                def half_card(tbl, fields):
                    assert fields.issubset(self._allFields(tbl))
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
    def enable_foreign_key_links(self):
        """
        call to enable foreign key links. For ex. a TicDat object made from
        a factory with foreign key enabled will pass the following assert
        assert (dat.foods["chicken"].nutritionQuantities["protein"] is
                dat.categories["protein"].nutritionQuantities["chicken"] is
                dat.nutritionQuantities["chicken", "protein"])
        Note that by default, TicDatFactories don't create foreign key links since doing so
        can slow down TicDat creation.

        :return:
        """
        self._foreign_key_links_enabled[:] = [True]
    def add_implied_foreign_keys(self):
        '''
        Cascades foreign keys downward (i.e. computes implied foreign key relationships)
        Calling this routine won't change the boolean result of subsequent calls to find_foreign_key_failures,
        but it might change the size of the find_foreign_key_failures returned dictionary.
        :return:
        '''
        verify(not self._has_been_used,
                "The foreign keys can't be changed after a TicDatFactory has been used.")
        def findderivedforeignkey():
            curFKs = self._foreign_keys_by_native()
            if not self._complex_fks():
                for (nativetable, bridgetable), nativebridgemappings in list(self._foreign_keys.items()):
                    for nb_map in nativebridgemappings :
                      for bfk in curFKs.get(bridgetable,()):
                        nativefields = bfk.nativefields()
                        assert set(nativefields).issubset(self._allFields(bridgetable))
                        bridgetonative = {bf:nf for nf,bf in nb_map}
                        foreigntobridge = bfk.foreigntonativemapping()
                        newnativeft = tuple((bridgetonative[bf], ff) for ff,bf in
                                             foreigntobridge.items() if bf in bridgetonative)
                        fkSet = self._foreign_keys[nativetable, bfk.foreign_table]
                        if newnativeft not in fkSet and self._simple_fk(bfk.foreign_table, newnativeft):
                            return fkSet.add(newnativeft) or True
        while findderivedforeignkey():
            pass
    def add_foreign_key(self, native_table, foreign_table, mappings):
        """
        Adds a foreign key relationship to the schema.  Adding a foreign key doesn't block
        the entry of child records that fail to find a parent match. It does make it easy
        to recognize such records (with find_foreign_key_failures()) and to remove such records
        (with remove_foreign_key_failures())

        :param native_table: (aka child table). The table with fields that must match some other table.

        :param foreign_table: (aka parent table). The table providing the matching entries.

        :param mappings: For simple foreign keys, a [native_field, foreign_field] pair.
                         For compound foreign keys an iterable of [native_field, foreign_field]
                         pairs.

        :return:
        """
        verify(not self._has_been_used,
                "The foreign keys can't be changed after a TicDatFactory has been used.")
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
            verify(k in self._allFields(native_table),
                   "%s does not refer to one of %s 's fields"%(k, native_table))
            verify(v in self._allFields(foreign_table),
                   "%s does not refer to one of %s 's fields"%(v, foreign_table))
        if utils.does_new_fk_complete_circle(native_table, foreign_table, self):
            print(f"*** A circular foreign key relationship will be creating by adding the {native_table} to " +
                  f"{foreign_table} connection")
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
        if self._has_been_used :
            return # idempotent
        for (nativetable, foreigntable), nativeforeignmappings in self._foreign_keys.items():
            nativeFieldsSet = frozenset(frozenset(_[0] for _ in nfm) for nfm in nativeforeignmappings
                                        if self._simple_fk(foreigntable, nfm)) # ignore for complex
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
    def as_dict(self, ticdat):
        '''
        Returns the ticdat object as a dictionary.
        Note that, as a nested class, TicDat  objects cannot be pickled
        directly. Instead, the dictionary returned by this function can be pickled.
        For unpickling, first unpickle the pickled dictionary, and then pass it,
        unpacked, to the TicDat constructor.

        (Note that if you want to pickle a TicDatFactory, you can use a similar approach with schema)

        :param ticdat: a TicDat object whose data is to be returned as a dict

        :return: A dictionary that can either be pickled, or unpacked to a
                TicDat constructor
        '''
        verify(not self.generator_tables, "as_dict doesn't work with generator tables.")
        rtn = {}
        dict_tables = {t for t,pk in self.primary_key_fields.items() if pk}
        for t in dict_tables:
            rtn[t] = {pk : {k:v for k,v in row.items()} for pk,row in getattr(ticdat,t).items()}
        for t in set(self.all_tables).difference(dict_tables, self.generic_tables):
            rtn[t] = [{k:v for k,v in row.items()} for row in getattr(ticdat, t)]
        for t in self.generic_tables:
            rtn[t] = getattr(ticdat, t).to_dict()
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
                           Use '*' instead of a pair of lists for generic tables,
                           which will render as pandas.DataFrame objects.
        ex: TicDatFactory (typical_table = [["primary key field"],["data field"]],
                           generic_table = '*')
        :return: a TicDatFactory
        """
        self._has_been_used = [] # append to this to make it truthy
        self._linkName = {}
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
        verify(not (self.generic_tables and not DataFrame),
               "Need to install pandas in order to specify variable schema tables")
        self._primary_key_fields = FrozenDict({k : tuple(v[0])for k,v in init_fields.items()
                                               if v != '*'})
        self._data_fields = FrozenDict({k : tuple(v[1]) for k,v in init_fields.items() if v != '*'})
        self._default_values = clt.defaultdict(dict)
        for tbl,flds in self._data_fields.items():
            for fld in flds:
                self._default_values[tbl][fld] = 0
        self._data_types = clt.defaultdict(dict)
        self._data_row_predicates = clt.defaultdict(dict)
        self._tooltips = {}
        self._generator_tables = []
        self._foreign_keys = clt.defaultdict(set)
        self.all_tables = frozenset(init_fields)
        # using list for truthiness to work around freezing headaches
        self._foreign_key_links_enabled = []

        datarowfactory = lambda t :  utils.td_row_factory(t, self.primary_key_fields.get(t, ()),
                        self.data_fields.get(t, ()), self.default_values.get(t, {}))

        goodticdattable = self._good_tic_dat_table_for_init
        superself = self
        def ticdattablefactory(alldatadicts, tablename, primarykey = (), rowfactory_ = None) :
            assert tablename not in self.generic_tables
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
            class TicDatDataList(clt.abc.MutableSequence):
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
                for t in set(superself.all_tables).difference(superself.generic_tables):
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
                tlen = lambda t: utils.safe_apply(len)(getattr(self, t))
                return "td: {" + ", ".join("%s: %s"%(t, tlen(t)) for t in sorted(superself.all_tables)) + "}"
        class TicDat(_TicDat) :
            def _len_dict(self):
                '''
                :return: a dictionary summarizing table lengths. Zero length tables omitted. Safe to use, I won't change
                '''
                return {t: l for t in superself.all_tables for l in [len(getattr(self, t))] if l}
            def _generatorfactory(self, data, tableName):
                return generatorfactory(data, tableName)
            def __init__(self, **init_tables):
                superself._trigger_has_been_used()
                self._all_data_dicts = []
                self._made_foreign_links = False
                lens = {t: l for t, v in init_tables.items() for l in [utils.safe_apply(len)(v)] if l is not None}
                for t in init_tables :
                    verify(t in superself.all_tables, "Unexpected table name %s"%t)
                    if t in superself.generic_tables:
                        setattr(self, t, DataFrame(init_tables[t]))
                for t,v in init_tables.items():
                  if t not in superself.generic_tables:
                    badticdattable = []
                    if DataFrame and isinstance(v, DataFrame) and \
                       set(superself.default_values.get(t, {})).difference(v.columns):
                        v = v.copy(deep=True)
                        for f, d in superself.default_values.get(t, {}).items():
                            if f not in v.columns:
                                v[f] = d
                    if not (goodticdattable(v, t, lambda x : badticdattable.append(x))) :
                        raise utils.TicDatError(t + " cannot be treated as a ticDat table : " +
                                                badticdattable[-1])
                    if pd and isinstance(v, pd.Series):
                        v = DataFrame(v)
                        v.rename(columns = {v.columns[0] : superself.data_fields[t][0]}, inplace=True)
                    if DataFrame and isinstance(v, DataFrame):
                      apply = utils.faster_df_apply
                      setattr(self, t, ticdattablefactory(self._all_data_dicts, t)())
                      _rd_ = lambda row_dict: {k: row_dict[k] for k in superself.data_fields.get(t, [])}
                      if superself.primary_key_fields.get(t):
                          if not set(superself.primary_key_fields[t]).issubset(v.columns):
                              v = v.reset_index(drop=False)
                          key_maker = (lambda rd: rd[superself.primary_key_fields[t][0]]) \
                                      if len(superself.primary_key_fields[t]) == 1 else  \
                                      (lambda rd: tuple(rd[_] for _ in superself.primary_key_fields[t]))
                          def add_row(row_dict):
                              getattr(self, t)[key_maker(row_dict)] = _rd_(row_dict)
                          apply(v, add_row)
                      else :
                          apply(v, lambda row_dict: getattr(self, t).append(_rd_(row_dict)))
                    elif superself.primary_key_fields.get(t) and not utils.dictish(v):
                         pklen = len(superself.primary_key_fields[t])
                         def handle_row_dict(r):
                             if not utils.dictish(r):
                                 return r
                             return [r.get(k, 0) for k in superself.primary_key_fields[t] +
                                      superself.data_fields.get(t,[])]
                         drf = datarowfactory(t) # lots of verification inside the datarowfactory
                         setattr(self, t, ticdattablefactory(self._all_data_dicts, t)(
                             {r if not utils.containerish(r) else
                              (r[0] if pklen == 1 else tuple(r[:pklen])):
                              drf([] if not utils.containerish(r) else r[pklen:])
                              for _r in v for r in [handle_row_dict(_r)]}
                         ))
                    elif superself.primary_key_fields.get(t) :
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
                    elif t in superself.generic_tables:
                        setattr(self, t, DataFrame())
                    else :
                        setattr(self, t, ticdattablefactory(self._all_data_dicts, t)())
                if init_tables :
                    self._try_make_foreign_links()
                if superself.duplicates_ticdat_init != "ignore":
                    dups = {k for k, v in lens.items() for l in [utils.safe_apply(len)(getattr(self, k))]
                            if v > (l or 0)}
                    assert superself.duplicates_ticdat_init == "warn" or not dups, \
                        f"Duplicate rows found in initialization of following tables: {dups}"
                    if dups and superself.duplicates_ticdat_init == "warn":
                        print("--> Warning: Some rows have been lost due to duplicate rows passed to TicDat.__init__")
                        print(f"--> {dups}")
            def _try_make_foreign_links(self):
                if not superself._foreign_key_links_enabled:
                    return
                verify(not superself._complex_fks(), ("complex foreign key between %s and %s " +
                       "prevents foreign_key_links")%((superself._complex_fks() or [(None,)*3])[0][:2]))
                assert not self._made_foreign_links, "call once"
                self._made_foreign_links = True
                can_link_w_me = lambda t : t not in superself.generator_tables and \
                                           superself.primary_key_fields.get(t)
                for fk in superself.foreign_keys :
                    t = fk.native_table
                    if can_link_w_me(t):
                      if can_link_w_me(fk.foreign_table)  :
                        nativefields = fk.nativefields()
                        linkname = superself._linkName[t, fk.foreign_table, frozenset(nativefields)]
                        if linkname not in ("keys", "items", "values") :
                            ft = getattr(self, fk.foreign_table)
                            foreign_pk = superself.primary_key_fields[fk.foreign_table]
                            local_pk = superself.primary_key_fields[t]
                            assert all(pk for pk in (foreign_pk, local_pk))
                            reversemapping  = fk.foreigntonativemapping()
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
        self.xls = xls.XlsTicFactory(self)
        self.csv = csv.CsvTicFactory(self)
        self.sql = sql.SQLiteTicFactory(self)
        self.mdb = mdb.MdbTicFactory(self)
        self.json = json.JsonTicFactory(self)
        self.pgsql = PostgresTicFactory(self)
        self._prepends = {}
        self._parameters = {}
        self._infinity_io_flag = ["N/A"]
        self._xlsx_trailing_empty_rows = ["prune"]
        self._duplicates_ticdat_init = ["assert"]
        self._none_as_infinity_bias_cache = {}
        self._isFrozen=True

    @property
    def duplicates_ticdat_init(self):
        """
        see __doc__ for set_duplicates_ticdat_init
        """
        return self._duplicates_ticdat_init[0]
    def set_duplicates_ticdat_init(self, value):
        """
        Set the duplicates_ticdat_init for the TicDatFactory. Choices are:
        --> 'assert' : an assert is raised if duplicate rows are passed to TicDat.__init__
        --> 'warn'   : emit a warning if duplicate rows are passed to TicDat.__init__
        --> 'ignore' : don't do anything if duplicate rows are passed to TicDat.__init__
        :param value: either 'assert', 'warn' or 'ignore'
        :return:
        """
        verify(value in ["assert", "warn", "ignore"], f"bad value {value}")
        self._duplicates_ticdat_init[0] = value

    @property
    def xlsx_trailing_empty_rows(self):
        """
        see __doc__ for set_xlsx_trailing_empty_rows
        """
        return self._xlsx_trailing_empty_rows[0]
    def set_xlsx_trailing_empty_rows(self, value):
        """
        Set the xlsx_trailing_empty_rows for the TicDatFactory. Choices are:
        --> 'prune' : (the default) when reading an xlsx/xlsm file, look for trailing all None rows in each table, and
                      prune them
        --> 'ignore': retain such rows
        With the move to openpyxl for xlsx/xlsm file reading, its more likely that Excel users accidentally creating
        trailing all none rows.
        :param value: either 'prune' or 'ignore'
        :return:
        """
        verify(value in ["prune", "ignore"], f"bad value {value}")
        self._xlsx_trailing_empty_rows[0] = value

    @property
    def infinity_io_flag(self):
        """
        see __doc__ for set_infinity_io_flag
        """
        return self._infinity_io_flag[0]
    def set_infinity_io_flag(self, value):
        """
        Set the infinity_io_flag for the TicDatFactory.
        'N/A' (the default) is recognized as a flag to disable infinity I/O buffering.

        If numeric, when writing data to the file system (or a database), float("inf") will be replaced by the
        infinity_io_flag and float("-inf") will be replaced by -infinity_io_flag, prior to writing.
        Similarly, the read data will replace any number >= the infinity_io_flag with float("inf") and any
        number smaller than float("-inf") with -infinity_io_flag.

        If None, then +/- infinity will be replaced by None prior to writing.
        Similarly, subsequent to reading, None will be replaced either by float("inf") or float("-inf"), depending
        on field data types.
        Note that None flagging will only perform replacements on fields whose data types allow infinity and not None.

        For all cases, these replacements will be done on a temporary copy of the data that is created prior to writing.

        Also note that none of the these replacements will be done on the parameters table. The assumption is the
        parameters table will be serialized to a string/string database table. Infinity can thus be represented by
        "inf"/"-inf" in such serializations. File readers will attempt to cast strings to floats on a row-by-row
        basis, as determined by add_parameter settings. File writers will cast parameters table entries to strings
        (assuming the add_parameters functionality is being used).

        :param value: a valid infinity_io_flag

        :return:
        """
        verify(value == "N/A" or (utils.numericish(value) and (0 < value < float("inf"))) or (value is None),
           "infinity_io_flag needs to be 'N/A' (to indicate it isn't being used), or None, or a positive finite number")
        self._infinity_io_flag[0] = value
        self._none_as_infinity_bias_cache.clear()

    def _general_read_cell(self, t, f, x):
        '''
        we expect other routines inside ticdat to access this routine, even though it starts with _
        :param t: table name
        :param f: field name
        :param x: cell value which might need to be adjusted
        :return: x, adjusted as required
        '''
        assert t in self.all_tables
        if t == "parameters": # infinity flagging doesn't apply to parameters table, see set_infinity_flag __doc__
            return x
        # SPEED IS IMPORTANT HERE!!!!
        if self._data_types.get(t, {}).get(f) and self._data_types[t][f].datetime and \
                (not (x is None or (utils.pd and utils.pd.isnull(x)))) and \
                utils.dateutil_adjuster(x) is not None:
            return utils.dateutil_adjuster(x)
        if utils.numericish(self.infinity_io_flag) and utils.numericish(x):
            if x >= self.infinity_io_flag:
                return float("inf")
            if x <= -self.infinity_io_flag:
                return float("-inf")
        if x is None and self.infinity_io_flag is None and utils.numericish(self._none_as_infinity_bias(t, f)):
            return float("inf") * self._none_as_infinity_bias(t, f)
        return x
    def _infinity_flag_write_cell(self, t, f, x):
        """
        we expect other routines inside ticdat to access this routine, even though it starts with _
        :param t: table name
        :param f: field name
        :param x: cell value which might need to be adjusted
        :return: x, adjusted as required
        """
        if t == "parameters" and self._parameters: # SPEED IS IMPORTANT
            # I will assume a parameters table without parameters specification is just a naive developer
            return None if x is None or (utils.pd and utils.pd.isnull(x)) else x
        if self.infinity_io_flag is None and (self._none_as_infinity_bias(t, f) or float("nan"))*float("inf") == x:
            return None
        if utils.numericish(self.infinity_io_flag) and utils.numericish(x):
            return max(min(x, self.infinity_io_flag), -self.infinity_io_flag)
        return x
    def _none_as_infinity_bias(self, t, f):
        if self.infinity_io_flag is not None:
            return None
        assert t in self.all_tables
        if (t,f) not in self._none_as_infinity_bias_cache:
            def _f():
                fld_type = self.data_types.get(t, {}).get(f)
                if fld_type and fld_type.number_allowed and not fld_type.valid_data(None):
                    verify(not (fld_type.valid_data(float("inf")) and fld_type.valid_data(-float("inf"))),
                           f"None cannot be used as an infinity IO flag for {t}.{f}")
                    for rtn in [1, -1]:
                        if fld_type.valid_data(rtn * float("inf")):
                            return rtn
            self._none_as_infinity_bias_cache[t, f] = _f()
        return self._none_as_infinity_bias_cache[t, f]
    def _parameter_table_post_read_adjustment(self, dat):
        """
        we expect other routines inside ticdat to access this routine, even though it starts with _
        this is the routine that is used in lieu of infinity_io_flag logic for the parameters table
        it is predicated on the assumption that the parameters table will be serialized to a string/string table
        :param dat: TicDat object
        :return: the same dat object, with parameter table entries adjusted to handle typing of strings-to-floats as
                 appropriate
        """
        if not self.parameters:
            return dat
        data_field = self.data_fields["parameters"][0]
        for k, v in list(dat.parameters.items()):
            td = getattr(self.parameters.get(k, None), "type_dictionary", None)
            if td and td.nullable and v[data_field] == "" and not td.valid_data(v[data_field]):
                dat.parameters[k] = None
            elif td and td.datetime:
                datetime_v = utils.dateutil_adjuster(v[data_field])
                dat.parameters[k] = datetime_v if datetime_v is not None else v[data_field]
            else:
                number_allowed = True
                if td and not td.number_allowed:
                    number_allowed = False
                if number_allowed:
                    number_v = utils.safe_apply(float)(v[data_field])
                    if number_v is not None and utils.safe_apply(int)(number_v) == number_v:
                        number_v = int(number_v)
                    dat.parameters[k] = number_v if number_v is not None else v[data_field]
        return dat

    def _allFields(self, table):
        assert table in self.all_tables
        return set(self.primary_key_fields.get(table, ())).union(self.data_fields.get(table, ()))

    def good_tic_dat_object(self, data_obj, bad_message_handler = lambda x : None,
                            row_checking="strict"):
        """
        determines if an object can be can be converted to a TicDat data object.

        :param data_obj: the object to verify

        :param bad_message_handler: a call back function to receive description of any failure message

        :param row_checking: either "generous" or "strict". If the latter, then we expect all the rows to be dicts
                             with the correct columns (except for things like generic tables)
                             defaults to strict since this is the protector for the solve functions

        :return: True if the dataObj can be converted to a TicDat data object. False otherwise.
        """
        rtn = True
        for t in self.all_tables:
            if not hasattr(data_obj, t) :
                bad_message_handler(t + " not an attribute.")
                return False
            if DataFrame:
                if isinstance(getattr(data_obj, t), DataFrame) and t not in self.generic_tables:
                    bad_message_handler(t + " is a DataFrame but not a generic table.\n" +
                                        "DataFrames can only be used to construct a TicDat " +
                                        "or as attributes for generic_tables")
                    return False
                if not isinstance(getattr(data_obj, t), DataFrame) and t in self.generic_tables:
                    bad_message_handler(t + " is a generic table, but not a DataFrame")
                    return False
            elif t in self.generic_tables:
                    bad_message_handler("Strangely, you have generic tables but not pandas")
                    return False
            rtn = rtn and  self.good_tic_dat_table(getattr(data_obj, t), t,
                    lambda x : bad_message_handler(t + " : " + x), row_checking)
        return rtn

    def _good_tic_dat_table_for_init(self, data_table, table_name,
                                     bad_message_handler = lambda x : None):
         if self.primary_key_fields.get(table_name, None) and containerish(data_table) \
                 and not dictish(data_table) and not utils.stringish(data_table) \
                 and not (utils.DataFrame and isinstance(data_table, utils.DataFrame)) \
                 and not (pd and isinstance(data_table, pd.Series)):
             tdf = TicDatFactory(**{table_name:[[], list(self.primary_key_fields[table_name]) +
                                                    list(self.data_fields.get(table_name, []))]})
             return tdf.good_tic_dat_table(data_table, table_name, bad_message_handler)
         return self.good_tic_dat_table(data_table, table_name, bad_message_handler)

    def good_tic_dat_table(self, data_table, table_name, bad_message_handler = lambda x : None,
                           row_checking="generous") :
        """
        determines if an object can be can be converted to a TicDat data table.

        :param dataObj: the object to verify

        :param table_name: the name of the table

        :param bad_message_handler: a call back function to receive
               description of any failure message

        :param row_checking: either "generous" or "strict". If the latter, then we expect all the rows to be dicts
                             with the correct columns (except for things like generic tables)
                             defaults to generous since this gets used a lot internally

        :return: True if the dataObj can be converted to a TicDat
                 data table. False otherwise.
        """
        assert row_checking in ["generous", "strict"]
        if table_name not in self.all_tables:
            bad_message_handler("%s is not a valid table name for this schema"%table_name)
            return False
        if table_name in self.generator_tables :
            assert not self.primary_key_fields.get(table_name), "this should be verified in __init__"
            verify((containerish(data_table) or callable(data_table)) and not dictish(data_table),
                   "Expecting a container of rows or a generator function of rows for %s"%table_name)
            return self._good_data_rows(data_table if containerish(data_table) else data_table(),
                                      table_name, bad_message_handler, row_checking)
        if pd and isinstance(data_table, pd.Series) and len(self.data_fields.get(table_name, ())) == 1:
            data_table = DataFrame(data_table)
            data_table.rename(columns = {data_table.columns[0] : self.data_fields[table_name][0]},
                              inplace=True)
        if DataFrame and isinstance(data_table, DataFrame):
            if row_checking == "strict" and table_name not in self.generic_tables:
                bad_message_handler(f"{table_name} is a pandas object, not a TicDat table object")
                return False
            if table_name in self.generator_tables:
                bad_message_handler("%s is a generator table and can not be populated with a DataFrame"
                                    %table_name)
                return False
            if "name" in self.data_fields.get(table_name, ()):
                bad_message_handler("%s has the string 'name' as a data field "%table_name +
                                    "and thus cannot be populated with a DataFrame")
                return False
            pks = self.primary_key_fields.get(table_name, ())
            if "name" in pks and len(pks) > 1:
                bad_message_handler("%s has the string 'name' as part of a multi-field "%table_name +
                                    "primary key and thus cannot be populated with a DataFrame")
                return False
            if not set(data_table.columns).issuperset(self.data_fields.get(table_name, ())):
                bad_message_handler("Could not find pandas columns for all the data fields for %s"%table_name)
                return False
            if pks and (pks != utils.safe_apply(lambda : tuple(data_table.index.names))()) and \
                not set(data_table.columns).issuperset(pks):
                bad_message_handler("Unable to find pandas column nor index matching the primary key for %s"%table_name)
                return False
            return True
        if self.primary_key_fields.get(table_name):
            if row_checking == "strict" and  not utils.dictish(data_table):
                bad_message_handler(f"{table_name} is not a dict")
                return False
            if utils.dictish(data_table) :
                return self._good_ticdat_dict_table(data_table, table_name, bad_message_handler, row_checking)
            if utils.containerish(data_table):
                return self._good_ticdat_key_container(data_table, table_name, bad_message_handler)
        else :
            verify(utils.containerish(data_table),
                   "Unexpected ticDat table type for %s."%table_name)
            return self._good_data_rows(data_table, table_name, bad_message_handler, row_checking)
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
    def _good_ticdat_dict_table(self, ticdat_table, table_name, bad_msg_handler = lambda x : None,
                                row_checking="generous"):
        assert dictish(ticdat_table)
        if not len(ticdat_table) :
            return True
        if not all(_keylen(k) == len(self.primary_key_fields[table_name])
                   for k in ticdat_table.keys()) :
            bad_msg_handler("Inconsistent key lengths")
            return False
        return self._good_data_rows(ticdat_table.values(), table_name, bad_msg_handler, row_checking)
    def _good_data_rows(self, data_rows, table_name, bad_message_handler = lambda x : None, row_checking="generous"):
        assert row_checking in ["generous", "strict"]
        dictishrows, containerishrows, singletonishrows = [], [], []
        for x in data_rows:
            if utils.dictish(x):
                dictishrows.append(x)
            elif utils.containerish(x):
                containerishrows.append(x)
            else:
                singletonishrows.append(x)
        if not all(set(x.keys()).issubset(self.data_fields.get(table_name,())) for x in dictishrows):
            bad_message_handler("Inconsistent data field name keys.")
            return False
        if not all(len(x) == len(self.data_fields.get(table_name,())) for x in containerishrows) :
            bad_message_handler("Inconsistent data row lengths.")
            return False
        if singletonishrows and (len(self.data_fields.get(table_name,())) != 1)  :
            bad_message_handler(
                "Non-container data rows supported only for single-data-field tables")
            return False
        if row_checking == "strict":
            if containerishrows or singletonishrows:
                bad_message_handler("Non dict data rows")
                return False
            if not all(set(x.keys()) == set(self.data_fields.get(table_name, ())) for x in dictishrows):
                bad_message_handler("Mismatched data field name keys.")
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
    def _same_data(self, obj1, obj2, epsilon = 0, nans_are_same_for_data_rows = False,
                   empty_strings_can_be_nan = False):
        is_nan = lambda x : (x == "" and empty_strings_can_be_nan) or safe_apply(math.isnan)(x) or (pd and pd.isnull(x))
        assert self.good_tic_dat_object(obj1, row_checking="generous") and \
               self.good_tic_dat_object(obj2, row_checking="generous")
        assert epsilon >= 0
        _n_s = lambda x, y: False
        if epsilon > 0:
            _n_s = lambda x, y: utils.safe_apply(utils.nearly_same)(x, y, epsilon)
        def samerow(r1, r2):
            if dictish(r1) and dictish(r2):
                if bool(r1) != bool(r2) or set(r1) != set(r2) :
                    return False
                for _k in r1:
                    if r1[_k] != r2[_k] and not _n_s(r1[_k], r2[_k]) and \
                        not (nans_are_same_for_data_rows and
                             all(map(safe_apply(is_nan), [r1[_k], r2[_k]]))):
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
    def clone(self, table_restrictions=None, clone_factory=None):
        """
        clones the TicDatFactory

        :param table_restrictions : if None, then argument is ignored. Otherwise, a container listing the
                                    tables to keep in the clone. Tables outside table_restrictions are removed from
                                    the clone.

        :param clone_factory : optional. Defaults to TicDatFactory. Can also be PanDatFactory. Can also be a function,
                               in which case it should behave similarly to create_from_full_schema.
                               If clone_factory=PanDatFactory, the row predicates that use predicate_kwargs_maker
                               won't be copied over.

        :return: a clone of the TicDatFactory. Returned object will based on clone_factory, if provided.

        Note - If you want to remove tables via a clone, then call like this
               tdf_new = tdf.clone(table_restrictions=set(tdf.all_tables).difference(tables_to_remove))
               Other schema editing operations are available with clone_add_a_table, clone_add_a_column,
               clone_remove_a_column and clone_rename_a_column.
        """
        clone_factory = clone_factory or TicDatFactory
        from ticdat import PanDatFactory
        no_copy_predicate_kwargs_maker = clone_factory == PanDatFactory
        if hasattr(clone_factory, "create_from_full_schema"):
            clone_factory = clone_factory.create_from_full_schema
        full_schema = utils.clone_a_anchillary_info_schema(self.schema(include_ancillary_info=True),
                                                           table_restrictions)
        rtn = clone_factory(full_schema)
        if hasattr(rtn, "set_generator_tables"):
            rtn.set_generator_tables(self.generator_tables)
        for tbl, row_predicates in self._data_row_predicates.items():
            if table_restrictions is None or tbl in table_restrictions:
                for pn, rpi in row_predicates.items():
                    if not (rpi.predicate_kwargs_maker and no_copy_predicate_kwargs_maker):
                        rtn.add_data_row_predicate(tbl, predicate=rpi.predicate, predicate_name=pn,
                                                   predicate_kwargs_maker=rpi.predicate_kwargs_maker,
                                                   predicate_failure_response=rpi.predicate_failure_response)
        rtn.enable_foreign_key_links() if self._foreign_key_links_enabled else None
        return rtn
    def clone_add_a_table(self, table, pk_fields, df_fields):
        '''

        add a table to the TicDatFactory

        :param table: table not in the schema

        :param pk_fields: container of the primary key fields

        :param df_fields: container of the data fields

        :return: a clone of the TicDatFactory, with the new table added
        '''
        return utils.clone_add_a_table(self, table, pk_fields, df_fields)
    def clone_add_a_column(self, table, field, field_type, field_position="append"):
        '''

        add a column to the TicDatFactory

        :param table: table in the schema

        :param field: name of the new field to be added

        :param field_type: either "primary key" or "data"

        :param field_position: integer between 0 and the length of self.primary_key_fields[table] (if "primary key")
                               or self.data_fields[table] (if "data"), inclsuive.
                               Alternately, can be "append", which will just insert the column at the end of the
                               appropriate list.

        :return: a clone of the TicDatFactory, with field inserted into location field_position for field_type
        '''
        return utils.clone_add_a_column(self, table, field, field_type, field_position)
    def clone_rename_a_column(self, table, field, new_field):
        '''
        rename a column in the TicDatFactory

        :param table: table in the schema

        :param field: name of the field to be removed

        :param new_field: new name for the field

        :return: a clone of the TicDatFactory, with field renamed to new_field. Data types, default values,
                 foreign keys and tooltips will reflect the new field name, but row predicates will be copied over as-is
                 (and thus you will need to re-create them as needed).
        '''
        return utils.clone_rename_a_column(self, table, field, new_field)
    def clone_remove_a_column(self, table, field):
        '''
        remove a column from the TicDatFactory

        :param table: table in the schema

        :param field: name of the field to be removed

        :return: a clone of the TicDatFactory, with field removed
        '''
        return utils.clone_remove_a_column(self, table, field)
    def copy_tic_dat(self, tic_dat, freeze_it = False):
        """
        copies the tic_dat object into a new tic_dat object
        performs a deep copy

        :param tic_dat: a ticdat object

        :param freeze_it: boolean. should the returned object be frozen?

        :return: a deep copy of the tic_dat argument
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append, row_checking="generous"),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = self.TicDat(**{t:getattr(tic_dat, t) for t in self.all_tables})
        return self.freeze_me(rtn) if freeze_it else rtn
    def copy_from_ampl_variables(self, ampl_variables):
        """
        copies the solution results from ampl_variables into a new ticdat object

        :param ampl_variables: a dict mapping from (table_name, field_name) -> amplpy.variable.Variable
                               (amplpy.variable.Variable is the type object returned by
                                AMPL.getVariable)
                                table_name should refer to a table in the schema that has
                                primary key fields.
                                field_name can refer to a data field for table_name, or it
                                can be falsey. If the latter, then AMPL variables that
                                pass the filter (see below) will simply populate the primary key
                                of the table_name.
                                Note that by default, only non-zero data is copied over.
                                If you want to override this filter, then instead of mapping to
                                amplpy.variable.Variable you should map to a
                                (amplpy.variable.Variable, filter) where filter accepts a data value
                                and returns a boolean.

        :return: a deep copy of the ampl_variables into a ticdat object
        """
        def good_map_onto(v):
            if containerish(v):
                return len(v) == 2 and good_map_onto(v[0]) and callable(v[1])
            return not containerish(v) and hasattr(v, "getValues")
        # not that if amplpy changes so that amplpy.variable.Variable is containerish then this
        # verify will always fail
        verify(dictish(ampl_variables) and
               all(containerish(k) and len(k) == 2 and self.primary_key_fields.get(k[0]) and
                   (k[1] in self.data_fields[k[0]] or not k[1]) and good_map_onto(v)
                   for k,v in ampl_variables.items()), "invalid ampl_variables argument")
        rtn = self.TicDat()
        for (t,f), av in ampl_variables.items():
          filter_ = lambda x : x != 0
          if containerish(av):
            av, filter_ = av
          if av.getValues().toDict(): # if there are data rows
            df = av.getValues().toPandas()
            verify(len(df.columns) == 1, "unexpected number of data columns found for ampl_variable" +
                                         "object " + str((t,f)))
            df.rename(columns={next(iter(df.columns)):f or "ticdat_dummy"}, inplace=True)
            df = df[df.apply(lambda row: filter_(row[f or "ticdat_dummy"]), axis=1)]
            if len(self.primary_key_fields[t]) == 1:
                df.index.rename(self.primary_key_fields[t][0], inplace=True)
            else:
                verify(pd, "pandas needs to installed to help process table %s"%t)
                df.index = pd.MultiIndex.from_tuples(df.index, names=self.primary_key_fields[t])
            if f:
                tic_dat = self.TicDat(**{t:df})
                for k,r in getattr(tic_dat, t).items():
                    getattr(rtn, t)[k][f] = r[f]
            else:
                tic_dat = TicDatFactory(**{t:[self.primary_key_fields[t],
                                              ["ticdat_dummy"]]}).TicDat(**{t:df})
                for k,r in getattr(tic_dat, t).items():
                    if k not in getattr(rtn, t):
                        getattr(rtn, t)[k] = {}
        return rtn
    def copy_to_ampl(self, tic_dat, field_renamings = None, excluded_tables = None):
        """
        copies the tic_dat object into a new tic_dat object populated with amplpy.DataFrame objects
        performs a deep copy

        :param tic_dat: a ticdat object

        :param field_renamings: dict or None. If fields are to be renamed in the copy, then
                                a mapping from (table_name, field_name) -> new_field_name
                                If a data field is to be omitted, then new_field can be falsey
                                table_name cannot refer to an excluded table. (see below)

        :param excluded_tables: If truthy, a list of tables to be excluded from the copy.
                                Tables without primary key fields are always excluded.

        :return: a deep copy of the tic_dat argument into amplpy.DataFrames
        """
        verify(amplpy, "amplpy needs to be installed in order to enable AMPL functionality")
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not excluded_tables or (containerish(excluded_tables) and
                                       set(excluded_tables).issubset(self.all_tables)),
               "bad excluded_tables argument")
        copy_tables = {t for t in self.all_tables if self.primary_key_fields[t]}.\
                      difference(excluded_tables or [])
        field_renamings = field_renamings or {}
        verify(dictish(field_renamings) and
               all(containerish(k) and len(k) == 2 and k[0] in copy_tables and
                   k[1] in self.primary_key_fields[k[0]] + self.data_fields[k[0]] and
                   ((not bool(v) and k[1] in self.data_fields[k[0]]) or (v and utils.stringish(v)))
                   for k,v in field_renamings.items()), "invalid field_renamings argument")
        class AmplTicDat(object):
            def __repr__(self):
                return "td:" + tuple(copy_tables).__repr__()
        rtn = AmplTicDat()
        copy_from = self.copy_to_pandas(tic_dat, copy_tables, drop_pk_columns=False)
        for t in copy_tables:
            rename = lambda f : field_renamings.get((t, f), f)
            df_ampl = amplpy.DataFrame(index=tuple(map(rename, self.primary_key_fields[t])))
            for f in self.primary_key_fields[t]:
                df_ampl.setColumn(rename(f), list(getattr(copy_from, t)[f]))
            for f in self.data_fields[t]:
                if rename(f):
                    df_ampl.addColumn(rename(f), list(getattr(copy_from, t)[f]))
            setattr(rtn, t, df_ampl)
        return rtn
    def set_ampl_data(self, tic_dat, ampl, table_to_set_name = None):
        """
        performs bulk setData on the AMPL first argument.

        :param tic_dat: an AmplTicDat object created by calling copy_to_ampl

        :param ampl: an amplpy.AMPL object

        :param table_to_set_name: a mapping of table_name to ampl set name
        :return:
        """
        verify(all(a.startswith("_") or a in self.all_tables for a in dir(tic_dat)),
               "bad ticdat argument")
        verify(hasattr(ampl, "setData"), "bad ampl argument")
        table_to_set_name = table_to_set_name or {}
        verify(dictish(table_to_set_name) and all(hasattr(tic_dat, k) and
                   utils.stringish(v) for k,v in table_to_set_name.items()),
               "bad table_to_set_name argument")
        for t in set(self.all_tables).intersection(dir(tic_dat)):
            try:
                ampl.setData(getattr(tic_dat, t), *([table_to_set_name[t]]
                                                if t in table_to_set_name else []))
            except:
                raise utils.TicDatError(t + " cannot be passed as an argument to AMPL.setData()")
    def copy_to_pandas(self, tic_dat, table_restrictions = None, drop_pk_columns = None,
                       reset_index=False):
        """
        copies the tic_dat object into a new object populated with pandas.DataFrame objects
        performs a deep copy

        :param tic_dat: a ticdat object

        :param table_restrictions: If truthy, a list of tables to turn into
                                   data frames. Defaults to all tables.

        :param drop_pk_columns: boolean or None. should the primary key columns be dropped
                                from the data frames after they have been incorporated
                                into the index.
                                If None, then pk fields will be dropped only for tables with data fields
        :param reset_index: boolean. If true, then drop_pk_columns is ignored and the returned DataFrames have
                                     a simple integer index with both primary key and data fields as columns.

        :return: a deep copy of the tic_dat argument into DataFrames
                 To get a valid pan_object object, either set drop_pk_columns to False or set reset_index to True.
                 I.e.
                    copy_1 = tdf.copy_to_pandas(dat, drop_pk_columns=False)
                    copy_2 = tdf.copy_to_pandas(dat, reset_index=True)
                    assert all(PanDatFactory(**tdf.schema()).good_pan_dat_object(_) for _ in [copy_1, copy_2])

                Note that None will be converted to nan in the returned object (as is the norm for pandas.DataFrame)

        """
        verify(DataFrame, "pandas needs to be installed in order to enable pandas functionality")
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        normal_tables = set(self.all_tables).difference(self.generator_tables)
        table_restrictions = table_restrictions or normal_tables
        verify(containerish(table_restrictions) and normal_tables.issuperset(table_restrictions),
           "if provided, table_restrictions should be a subset of the table names")
        superself = self
        class PandasTicDat(object):
            def __repr__(self):
                tlen = lambda t: utils.safe_apply(len)(getattr(self, t))
                return "pd: {" + ", ".join("%s: %s"%(t, tlen(t)) for t in sorted(superself.all_tables)) + "}"
        rtn = PandasTicDat()

        # this is the only behavior change we exhibit from between 2 and 3. can
        # clean it up at some point.
        _sorted = lambda x : sorted(x) if sys.version_info[0] == 2 else x

        for tname in table_restrictions:
            tdtable = getattr(tic_dat, tname)
            if tname in self.generic_tables:
                df = tname
            elif len(tdtable) == 0 :
                df = DataFrame([], columns = self.primary_key_fields.get(tname,tuple()) +
                                                self.data_fields.get(tname, tuple()))
            elif dictish(tdtable):
                pks = self.primary_key_fields[tname]
                dfs = self.data_fields.get(tname, tuple())
                cols = pks + dfs
                df = DataFrame([ (list(k) if containerish(k) else [k]) + [v[_] for _ in dfs]
                              for k,v in _sorted(getattr(tic_dat, tname).items())],
                              columns =cols)
                if not reset_index:
                    df.set_index(list(pks), inplace=True,
                             drop= bool(dfs if drop_pk_columns == None else drop_pk_columns))
                utils.Sloc.add_sloc(df)
            else :
                df = DataFrame([[v[_] for _ in self.data_fields[tname]]
                                  for v in _sorted(getattr(tic_dat, tname))],
                                  columns = self.data_fields[tname])
            setattr(rtn, tname, df)
        return rtn
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
    def find_foreign_key_failures(self, tic_dat, verbosity="High", max_failures=float("inf")):
        """
        Finds the foreign key failures for a ticdat object

        :param tic_dat: ticdat object

        :param max_failures: number. An upper limit on the number of failures to find. Will short circuit and return
                                     ASAP with a partial failure enumeration when this number is reached.

        :param verbosity: either "High" or "Low"

        :return: A dictionary constructed as follow (for verbosity = 'High'):

         The keys are namedtuples with members "native_table", "foreign_table",
         "mapping", "cardinality".

         The key data matches the arguments to add_foreign_key that constructed the
         foreign key (with "cardinality" being deduced from the overall schema).

         The values are namedtuples with the following members.

         --> native_values - the values of the native fields that failed to match

         --> native_pks - the primary key entries of the native table rows
                          corresponding to the native_values.

         That is to say, native_values tells you which values in the native table
         can't find a foreign key match, and thus generate a foreign key failure.
         native_pks tells you which native table rows will be removed if you call
         remove_foreign_key_failures().

         For verbosity = 'Low' a simpler return object is created that doesn't use namedtuples
         and omits the foreign key cardinality.
        """
        verify(verbosity in ["High", "Low"], "verbosity needs to be either 'High' or 'Low'")
        assert self.good_tic_dat_object(tic_dat), "tic_dat not a good object for this factory"
        assert max_failures > 0, "max_failures should be a positive number"
        rtn_values, rtn_pks = clt.defaultdict(set), clt.defaultdict(set)

        def getcell(tblname, native_pk, native_data_row, field_name):
             assert field_name in self.primary_key_fields.get(tblname, ()) + \
                                  self.data_fields.get(tblname, ())
             if [field_name] == list(self.primary_key_fields.get(tblname, ())):
                 return native_pk
             if field_name in native_data_row:
                 return native_data_row[field_name]
             return native_pk[self.primary_key_fields[tblname].index(field_name)]

        table_data = defaultdict(set)
        def get_table_data(tblname, fields):
            if fields == self.primary_key_fields.get(tblname, ()):
                return getattr(tic_dat, tblname)
            if (tblname, fields) not in table_data:
                add_here = table_data[tblname, fields]
                tbl = getattr(tic_dat, tblname)
                for k,v in (tbl.items() if dictish(tbl) else enumerate(tbl)):
                    add_here.add(tuple(getcell(tblname, k, v, f) for f in fields))
            return table_data[tblname, fields]
        number_failures = [0] if max_failures < float("inf") else None
        def populate_rtn():
            def inc_failures_trips_end():
                if number_failures:
                    number_failures[0] += 1
                    return number_failures[0] >= max_failures
            for native, fks in self._foreign_keys_by_native().items():
                def getcell_(native_pk, native_data_row, field_name):
                     return getcell(native, native_pk, native_data_row, field_name)
                for fk in fks:
                    foreign_to_native = fk.foreigntonativemapping()
                    for native_pk, native_data_row in (getattr(tic_dat, native).items()
                                if dictish(getattr(tic_dat, native))
                                else enumerate(getattr(tic_dat, native))):
                        ffs = tuple(_ff for _ff in self.primary_key_fields.get(fk.foreign_table, ()) +
                                    self.data_fields.get(fk.foreign_table, ())
                                    if _ff in foreign_to_native)
                        foreign_look_up = tuple(getcell_(native_pk, native_data_row, foreign_to_native[_ff])
                                            for _ff in ffs)
                        if ffs == self.primary_key_fields.get(fk.foreign_table) and len(ffs)==1:
                            foreign_look_up = foreign_look_up[0]
                        foreign_look_into = get_table_data(fk.foreign_table, ffs)
                        if foreign_look_up not in foreign_look_into:
                            rtn_pks[fk].add(native_pk)
                            if type(fk.mapping) is ForeignKeyMapping :
                                rtn_values[fk].add(getcell_(native_pk, native_data_row,
                                                            fk.mapping.native_field))
                            else:
                                rtn_values[fk].add(tuple(getcell_(native_pk,
                                                    native_data_row, _.native_field) for _ in fk.mapping))
                            if inc_failures_trips_end():
                                return
        populate_rtn()
        assert set(rtn_pks) == set(rtn_values)
        RtnType = namedtuple("ForeignKeyFailures", ("native_values", "native_pks"))

        rtn = {k:RtnType(tuple(rtn_values[k]), tuple(rtn_pks[k])) for k in rtn_pks}
        if verbosity == "Low":
            rtn = {tuple(k[:2]) + (tuple(k[2]),): tuple(v) for k,v in rtn.items()}
        return rtn
    def create_full_parameters_dict(self, dat):
        """
        create a fully populated dictionary of all the parameters

        :param dat: a TicDat object that has a parameters table

        :return: a dictionary that maps parameter option to actual dat.parameters value.
                 if the specific option isn't part of dat.parameters, then the default value is used.
                 Note that for datetime parameters, the default will be coerced into a datetime object, if possible.
        """
        assert self.good_tic_dat_object(dat)
        verify(self.parameters, "no parameters options have been specified")
        defaults = {k: dt if dt is not None and v.type_dictionary and v.type_dictionary.datetime else df
                    for k,v in self._parameters.items() for df in [v.default_value]
                    for dt in [utils.dateutil_adjuster(df)]}
        return dict(defaults, **{k: v[self.data_fields["parameters"][0]] for k,v in dat.parameters.items()})

    def remove_foreign_key_failures(self, tic_dat, propagate=True):
        """
        Removes foreign key failures (i.e. child records with no parent table record)

        :param tic_dat: ticdat object

        :param propagate boolean: remove cascading failures? (if removing the child record
                                  results in new failures, should those be removed as well?)

        :return: tic_dat, with the foreign key failures removed
        """
        fk_failures = self.find_foreign_key_failures(tic_dat)
        needs_removal = set()
        for fk, (_, failed_pks) in fk_failures.items():
            for failed_pk in failed_pks:
                if self.primary_key_fields.get(fk.native_table):
                    assert dictish(getattr(tic_dat, fk.native_table))
                    if failed_pk in getattr(tic_dat, fk.native_table) :
                        del(getattr(tic_dat, fk.native_table)[failed_pk])
                else:
                    needs_removal.add((fk.native_table, failed_pk))
        for t,row_index in sorted(needs_removal, reverse=True):
            getattr(tic_dat, t).pop(row_index)
        if fk_failures and propagate:
            return self.remove_foreign_key_failures(tic_dat)
        return tic_dat

    def _get_full_row(self, ticdat, table, pk):
        full_row = dict(getattr(ticdat, table)[pk])
        if len(self.primary_key_fields[table]) == 1:
            full_row[self.primary_key_fields[table][0]] = pk
        else:
            full_row = dict(full_row, **{f:d for f,d in
                                         zip(self.primary_key_fields[table], pk)})
        return full_row
    def find_data_type_failures(self, tic_dat, max_failures=float("inf")):
        """
        Finds the data type failures for a ticdat object

        :param tic_dat: ticdat object

        :param max_failures: number. An upper limit on the number of failures to find. Will short circuit and return
                                     ASAP with a partial failure enumeration when this number is reached.

        :return: A dictionary constructed as follow:

         The keys are namedtuples with members "table", "field". Each (table,field) pair
         has data values that are inconsistent with its data type. (table, field) pairs
         with no data type at all are never part of the returned dictionary.

         The values of the returned dictionary are namedtuples with the following attributes.

         --> bad_values - the distinct values for the (table, field) pair that are inconsistent
                          with the data type for (table, field).

         --> pks - the distinct primary key entries of the table containing the bad_values
                   data. (will be row index for tables with no primary key)

         That is to say, bad_values tells you which values in field are failing the data type check,
         and pks tells you which table rows will have their field entry changed if you call
         replace_data_type_failures().

         Note that for primary key fields (but not data fields) with no explicit data type, a temporary filter
         that excludes only Null will be applied. If you want primary key fields to allow Null, you must explicitly
         opt-in by calling set_data_type appropriately.
         See issue https://github.com/ticdat/ticdat/issues/46 for more info.
        """
        assert self.good_tic_dat_object(tic_dat), "tic_dat not a good object for this factory"
        assert max_failures > 0, "max_failures should be a positive number"

        rtn_values, rtn_pks = clt.defaultdict(set), clt.defaultdict(set)
        tmp_tdf = TicDatFactory.create_from_full_schema(self.schema(include_ancillary_info=True))
        for t, pks in self.primary_key_fields.items():
            for pk in pks:
                if pk not in self._data_types.get(t, ()):
                    tmp_tdf.set_data_type(t, pk, number_allowed=True,
                      inclusive_min=True, inclusive_max=True, min=-float("inf"), max=float("inf"),
                      must_be_int=False, strings_allowed='*', nullable=False, datetime=False)
        number_failures = [0] if max_failures < float("inf") else None
        def populate_rtn():
            def inc_failures_trips_end():
                if number_failures:
                    number_failures[0] += 1
                    return number_failures[0] >= max_failures
            for table, type_row in tmp_tdf._data_types.items():
                _table = getattr(tic_dat, table)
                if dictish(_table):
                    for pk  in _table:
                        full_row = self._get_full_row(tic_dat, table, pk)
                        for field, data_type in type_row.items():
                            if not data_type.valid_data(full_row[field]) :
                                rtn_values[(table, field)].add(full_row[field])
                                rtn_pks[(table, field)].add(pk)
                                if inc_failures_trips_end():
                                    return
                elif containerish(_table):
                    for pk, data_row in enumerate(_table):
                        for field, data_type in type_row.items():
                            if not data_type.valid_data(data_row[field]) :
                                rtn_values[(table, field)].add(data_row[field])
                                rtn_pks[(table, field)].add(pk)
                                if inc_failures_trips_end():
                                    return
        populate_rtn()
        assert set(rtn_values).issuperset(set(rtn_pks))
        TableField = clt.namedtuple("TableField", ["table", "field"])
        ValuesPks = clt.namedtuple("ValuesPks", ["bad_values", "pks"])
        return {TableField(*tf):ValuesPks(tuple(rtn_values[tf]),
                                          tuple(rtn_pks[tf]) if tf in rtn_pks else None)
                for tf in rtn_values}

    def replace_data_type_failures(self, tic_dat, replacement_values = FrozenDict()):
        """
        Replace the data cells with data type failures with the default value for the appropriate field.

        :param tic_dat: a TicDat object appropriate for this schema

        :param replacement_values: a dictionary mapping (table, field) to replacement value.
               the default value will be used for (table, field) pairs not in replacement_values

        :return: the tic_dat object with replacements made. The tic_dat object itself will be edited in place.

        Replaces any of the data failures found in find_data_type_failures() with the appropriate
        replacement_value.

        Note - won't perform primary key replacements.
        """
        msg  = []
        verify(self.good_tic_dat_object(tic_dat, msg.append),
               "tic_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(dictish(replacement_values) and all(len(k)==2 for k in replacement_values),
               "replacement_values should be a dictionary mapping (table, field) to valid replacement value")
        for (table,field), v in replacement_values.items():
            verify(table in self.all_tables, "%s is not a table for this schema"%table)
            verify(field in self.data_fields.get(table, ()), "%s is not a data field for %s"%(field, table))

        replacements_needed = self.find_data_type_failures(tic_dat)
        if not replacements_needed:
            return tic_dat

        real_replacements = {}
        for table, type_row in self._data_types.items():
            for field in type_row:
                if field not in self.primary_key_fields[table]:
                    real_replacements[table, field] = replacement_values.get((table, field),
                        self._default_values.get(table, {}).get(field, 0))
        for (table, field), value in real_replacements.items():
            if (table, field) in replacements_needed:
                verify(self._data_types[table][field].valid_data(value),
                       "The replacement value %s is not itself valid for %s : %s"%(value, table, field))

        for (table, field), (vals, pks) in replacements_needed.items() :
            if (table, field) in real_replacements:
                for pk in pks:
                    getattr(tic_dat, table)[pk][field] = real_replacements[table, field]
        assert not set(self.find_data_type_failures(tic_dat)).intersection(real_replacements)
        return tic_dat

    def find_data_row_failures(self, tic_dat, exception_handling="__debug__", max_failures=float("inf")):
        """
        Finds the data row failures for a ticdat object

        :param tic_dat: ticdat object

        :param exception_handling: One of "Handled as Failure",  "Unhandled" or "__debug__"
              "Handled as Failure": Any exception generated by calling a row predicate function will indicate a data
                                    failure for that row. (Similarly, predicate_kwargs_maker exceptions create an entry
                                    in the returned failure dictionary).
              "Unhandled": Exceptions resulting from calling a row predicate (or a predicate_kwargs_maker) will not be
                           handled by data_row_failures.
              "__debug__": Since "Handled as Failure" makes more sense for production runs and "Unhandled" makes more
                           sense for debugging, this option will use the latter if __debug__ is True and the former
                           otherwise. See -o and __debug__ in Python documentation for more details.

        :param max_failures: number. An upper limit on the number of failures to find. Will short circuit and return
                                     ASAP with a partial failure enumeration when this number is reached.

        :return: A dictionary constructed as follow:

         The keys are namedtuples with members "table", "predicate_name".

         The values of the returned dictionary are tuples indicating which rows
         failed the predicate test. For tables with a primary key this tuple will
         contain the primary key value of each failed row. Otherwise, this tuple
         will list the positions of the failed rows.

         If the predicate_failure_response for the predicate is "Error Message" (instead of "Boolean") then
         the values of the returned dict will themselves be namedtuples with members "primary_key" and "error_message".

         If a predicate_kwargs_maker is provided and it fails (either by failing to return a dictionary or by
         throwing a handled exception) then a similar namedtuple is entered as the value, with primary_key='*'
         and error_message as a string.
        """
        assert self.good_tic_dat_object(tic_dat), "tic_dat not a good object for this factory"
        assert max_failures > 0, "max_failures should be a positive number"
        verify(exception_handling in ["Handled as Failure", "Unhandled", "__debug__"],
               "bad exception_handling argument")
        if exception_handling == "__debug__":
            exception_handling = "Unhandled" if __debug__ else "Handled as Failure"
        data_row_predicates = {k: dict(v) for k,v in self._data_row_predicates.items()}
        if self._parameters:
            def good_parameter(row):
                k = row[self.primary_key_fields["parameters"][0]]
                v = row[self.data_fields["parameters"][0]]
                chk = self._parameters.get(k)
                return chk and (chk.type_dictionary is None or chk.type_dictionary.valid_data(v))
            _ = "Good Name/Value Check"
            make_name = lambda i: _ if _ not in self._data_row_predicates.get("parameters", {}) else f"{_}_{i}"
            predicate_name = next(make_name(i) for i in count() if make_name(i) not in
                                  self._data_row_predicates.get("parameters", {}))
            data_row_predicates["parameters"] = data_row_predicates.get("parameters", {})
            data_row_predicates["parameters"][predicate_name] = RowPredicateInfo(good_parameter, None, "Boolean")

        predicate_kwargs_maker_results = {}
        rtn = clt.defaultdict(set)
        PKEM = clt.namedtuple("PrimaryKeyErrorMessage", ["primary_key", "error_message"])
        number_failures = [0] if max_failures < float("inf") else None
        def populate_rtn():
            def inc_failures_trips_end():
                if number_failures:
                    number_failures[0] += 1
                    return number_failures[0] >= max_failures
            for tbl, row_predicates in data_row_predicates.items():
                for pn, rpi in row_predicates.items():
                    predicate_kwargs = {}
                    if rpi.predicate_kwargs_maker:
                        if rpi.predicate_kwargs_maker not in predicate_kwargs_maker_results:
                            if exception_handling == "Handled as Failure":
                                try:
                                    _predicate_kwargs = rpi.predicate_kwargs_maker(tic_dat)
                                except Exception as e:
                                    _predicate_kwargs = f"Exception<{e}>"
                            else:
                                _predicate_kwargs = rpi.predicate_kwargs_maker(tic_dat)
                            predicate_kwargs_maker_results[rpi.predicate_kwargs_maker] = _predicate_kwargs
                        predicate_kwargs = predicate_kwargs_maker_results[rpi.predicate_kwargs_maker]
                    if not isinstance(predicate_kwargs, dict):
                        rtn[tbl, pn] = PKEM('*', predicate_kwargs
                                            if (isinstance(predicate_kwargs, str) and "Exception<" in predicate_kwargs)
                                            else f"predicate_kwargs_maker failed to return a dict")
                        if inc_failures_trips_end():
                            return
                    else:
                        if rpi.predicate_failure_response == "Boolean":
                            def _p(row):
                                try:
                                    return rpi.predicate(row, **predicate_kwargs)
                                except:
                                    return False
                        else:
                            def _p(row):
                                try:
                                    return rpi.predicate(row, **predicate_kwargs)
                                except Exception as e:
                                    return f"Exception<{e}>"
                        if exception_handling == "Unhandled":
                            _p = lambda row: rpi.predicate(row, **predicate_kwargs)
                        _table = getattr(tic_dat, tbl)
                        def handle_full_row_trips_end(pk, full_row):
                            if rpi.predicate_failure_response == "Boolean" and not _p(full_row):
                                rtn[tbl, pn].add(pk)
                                return inc_failures_trips_end()
                            if rpi.predicate_failure_response == "Error Message":
                                _ = _p(full_row)
                                if not _ is True:
                                    rtn[tbl, pn].add(PKEM(pk, str(_)))
                                    return inc_failures_trips_end()
                        if dictish(_table):
                            for pk  in _table:
                                full_row = self._get_full_row(tic_dat, tbl, pk)
                                if handle_full_row_trips_end(pk, full_row):
                                    return
                        else:
                            for i, data_row in enumerate(_table):
                                if handle_full_row_trips_end(i, data_row):
                                    return
        populate_rtn()
        TPN = clt.namedtuple("TablePredicateName", ["table", "predicate_name"])

        return {TPN(*k):(v if isinstance(v, PKEM) else tuple(v)) for k,v in rtn.items()}

    def obfusimplify(self, tic_dat, table_prepends = utils.FrozenDict(), skip_tables = (),
                     freeze_it = False) :
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
               "tic_dat not a good object for this factory : %s" %"\n".join(msg))
        verify(not self._complex_fks(), ("complex foreign key between %s and %s prevents " +
                                         "obfusimplify") % ((self._complex_fks() or [(None,) * 3])[0][:2]))
        verify({fk.cardinality for fk in self.foreign_keys}.issubset({"many-to-one", "one-to-one"}),
               "many-to-many and one-to-many foreign keys are not currently supported for obfusimplify")
        verify(not self.find_foreign_key_failures(tic_dat),
               "Cannot obfusimplify an object with foreign key failures")
        verify(not self.generator_tables, "Cannot obfusimplify a tic_dat that uses generators")
        verify(not set(table_prepends).intersection(skip_tables),
               "Can't specify a table prepend for an entity that you're skipping")
        better_self = self.clone()
        better_self.add_implied_foreign_keys()

        entity_tables = {t for t,v in better_self.primary_key_fields.items() if len(v) == 1}
        foreign_keys_by_native = better_self._foreign_keys_by_native()
        # if a native table is one-to-one with a foreign table, it isn't an entity table
        for nt in entity_tables.intersection(foreign_keys_by_native):
            if any(ft.cardinality == "one-to-one" for ft in foreign_keys_by_native[nt]):
                entity_tables.discard(nt)

        verify(entity_tables.issuperset(skip_tables), "should only specify entity tables to skip")
        entity_tables = entity_tables.difference(skip_tables)

        for k,v in table_prepends.items():
            verify(k in better_self.all_tables, "%s is not a table name")
            verify(len(better_self.primary_key_fields.get(k, ())) == 1, f"{k} does not have a single primary key field")
            verify(k in entity_tables, "%s is not an entity table due to child foreign key relationship"%k)
            verify(utils.stringish(v) and set(v).issubset(uppercase) and not v.endswith("I"),
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
            name = next(namegetter)
            while name in table_prepends.values():
                name = next(namegetter)
            table_prepends[t]=name
        assert set(entity_tables) == set(table_prepends)

        reverse_renamings = {}
        for t in table_prepends :
            for i,k in enumerate(sorted(getattr(tic_dat, t))) :
                reverse_renamings[t, k] = "%s%s"%(table_prepends[t],i+1)
        foreign_keys = {}
        for fk in better_self.foreign_keys:
            nt = fk.native_table
            if fk.foreign_table in table_prepends:
                foreign_keys = utils.dict_overlay(foreign_keys,
                                {(nt,nf) : fk.foreign_table for nf, ff in
                                  fk.nativetoforeignmapping().items()})
        # remember -- we've used this factory so any cascading foreign keys are present
        rtn_dict  = clt.defaultdict(dict)
        for t in better_self.all_tables:
            read_table = getattr(tic_dat, t)
            def fix_all_row(all_row):
                return {k: reverse_renamings[foreign_keys[t, k], v] if (t,k) in foreign_keys else v
                           for k,v in all_row.items()}
            if dictish(read_table):
                for pk, data_row in read_table.items():
                    if len(better_self.primary_key_fields.get(t, ())) == 1:
                        pkf = better_self.primary_key_fields[t][0]
                        if (t,pkf) in foreign_keys:
                            new_pk = reverse_renamings[foreign_keys[t,pkf], pk]
                        else:
                            new_pk = reverse_renamings.get((t, pk), pk)
                        rtn_dict[t][new_pk] = fix_all_row(data_row)
                    else :
                        assert containerish(pk) and len(pk) == len(better_self.primary_key_fields[t])
                        new_row = fix_all_row(dict(data_row, **{pkf: pkv for pkf, pkv in
                                                                zip(better_self.primary_key_fields[t], pk)}))
                        new_pk = tuple(new_row[_] for _ in better_self.primary_key_fields[t])
                        rtn_dict[t][new_pk] = {k:new_row[k] for k in data_row}
            else :
                rtn_dict[t] = []
                for data_row in read_table:
                    rtn_dict[t].append(fix_all_row(data_row))

        RtnType = namedtuple("ObfusimplifyResults", "copy renamings")

        rtn = RtnType(better_self.freeze_me(better_self.TicDat(**rtn_dict)) if freeze_it else better_self.TicDat(**rtn_dict),
                      {v:k for k,v in reverse_renamings.items()})
        assert not better_self.find_foreign_key_failures(rtn.copy)
        assert len(rtn.renamings) == len(reverse_renamings)
        return rtn

    @property
    def opl_prepend(self):
        return self._prepends.get("opl", "")

    @property
    def ampl_prepend(self):
        return self._prepends.get("ampl","")

    @opl_prepend.setter
    def opl_prepend(self, value):
        verify(utils.stringish(value), "opl_prepend should be a string")
        self._prepends["opl"] = value

    @ampl_prepend.setter
    def ampl_prepend(self,value):
        verify(utils.stringish(value), "ampl_prepend should be a string")
        self._prepends["ampl"] = value

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

