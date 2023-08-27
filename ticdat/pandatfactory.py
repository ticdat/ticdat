"""
Create PanDatFactory. Along with ticdatfactory.py, one of two main entry points for ticdat library.
PEP8
"""

import ticdat.utils as utils
from ticdat.utils import ForeignKey, ForeignKeyMapping, TypeDictionary, verify, dictish, RowPredicateInfo
from ticdat.utils import lupish, deep_freeze, containerish, FrozenDict, safe_apply, stringish
import ticdat.pandatio as pandatio
from itertools import count
import math
try:
    from pandas import isnull
    import numpy
except:
    isnull = numpy = None
import collections as clt
from ticdat.pgtd import PostgresPanFactory
try:
    import amplpy
except:
    amplpy = None

pd, DataFrame = utils.pd, utils.DataFrame # if pandas not installed will be falsey

def _is_last_rows_all_nan(df, last_rows):
    assert last_rows > 0
    # quick last row check to make faster
    all_nan_row =  lambda row: all(map(pd.isnull, row.values()))
    return utils.faster_df_apply(df.tail(1), all_nan_row).all() and \
           utils.faster_df_apply(df.tail(last_rows), all_nan_row,
                            trip_wire_check=lambda x: all_nan_row if x else lambda row: False).all()

def _find_number_all_nan_last_rows(df, min_rtn, max_rtn):
    assert max_rtn >= min_rtn
    if min_rtn == max_rtn:
        assert min_rtn == 0 or _is_last_rows_all_nan(df, min_rtn)
        return min_rtn
    med_rtn =  math.ceil((min_rtn+max_rtn)/2)
    if _is_last_rows_all_nan(df, med_rtn):
        return _find_number_all_nan_last_rows(df, med_rtn, max_rtn)
    return _find_number_all_nan_last_rows(df, min_rtn, med_rtn-1)

def remove_trailing_all_nan(df):
    all_nan_last_rows = _find_number_all_nan_last_rows(df, 0, len(df))
    if all_nan_last_rows:
        return df.head(len(df)-all_nan_last_rows).copy(deep=True)
    return df

class PanDatFactory(object):
    """
     Defines a schema for a collection of pandas.DataFrame objects.
     This class is constructed with a schema. It can be used to generate PanDat objects,
     to write PanDat objects to different file types, or to perform bulk query operations
     to diagnose common data integrity failures.

     Analytical code that uses PanDat objects can be used, without change, on different data
     sources, thus facilitating the "separate model from data" design goal.

     A PanDat object is itself a collection of DataFrames that conform to a predefined schema.

    :param init_fields: a mapping of tables to primary key fields and data fields. Each field listing consists
                        of two sub lists ... first primary keys fields, then data fields.

        ex:
        ```PanDatFactory (categories =  [["name"],["Min Nutrition", "Max Nutrition"]],
                           foods  =  [["Name"],["Cost"]]
                           nutritionQuantities = [["Food", "Category"],["Qty"]])```

        Use '*' instead of a pair of lists for generic tables

        ex:
        ```PanDatFactory (typical_table = [["Primary Key Field"],["Data Field"]],
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
        Return a dictionary that summarizes the schema.

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

        create a PanDatFactory complete with default values, data types, and foreign keys

        :param full_schema: a dictionary consistent with the data returned by a call to schema()
                            with include_ancillary_info = True

        :return: a PanDatFactory reflecting the tables, fields, default values, data types,
                 and foreign keys consistent with the full_schema argument
        """
        old_schema = {"tables_fields", "foreign_keys", "default_values", "data_types"}
        verify(dictish(full_schema) and set(full_schema).issuperset(old_schema) and set(full_schema) in
               utils.all_subsets(old_schema.union({"parameters", "infinity_io_flag", "xlsx_trailing_empty_rows",
                                                   "duplicates_ticdat_init", "tooltips"})),
               "full_schema should be the result of calling schema(True) for some PanDatFactory")
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

        rtn = PanDatFactory(**full_schema["tables_fields"])
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
    def clone(self, table_restrictions=None, clone_factory=None):
        """

        clones the PanDatFactory

        :param table_restrictions : if None, then argument is ignored. Otherwise, a container listing the
                                    tables to keep in the clone. Tables outside table_restrictions are removed from
                                    the clone.

        :param clone_factory : optional. Defaults to PanDatFactory. Can also be TicDatFactory.  Can also be a function,
                               in which case it should behave similarly to create_from_full_schema.
                               If clone_factory=TicDatFactory, the row predicates that use predicate_kwargs_maker
                               won't be copied over.

        :return: a clone of the PanDatFactory. Returned object will be based on clone_factory, if provided.

        Note - If you want to remove tables via a clone, then call like this
               pdf_new = pdf.clone(table_restrictions=set(pdf.all_tables).difference(tables_to_remove))
               Other schema editing operations are available with clone_add_a_table, clone_add_a_column,
               clone_remove_a_column and clone_rename_a_column.
        """
        clone_factory = clone_factory or PanDatFactory
        from ticdat import TicDatFactory
        no_copy_predicate_kwargs_maker = clone_factory == TicDatFactory
        if hasattr(clone_factory, "create_from_full_schema"):
            clone_factory = clone_factory.create_from_full_schema
        full_schema = utils.clone_a_anchillary_info_schema(self.schema(include_ancillary_info=True),
                                                           table_restrictions)
        rtn = clone_factory(full_schema)
        for tbl, row_predicates in self._data_row_predicates.items():
            if table_restrictions is None or tbl in table_restrictions:
                for pn, rpi in row_predicates.items():
                    if not (rpi.predicate_kwargs_maker and no_copy_predicate_kwargs_maker):
                        rtn.add_data_row_predicate(tbl, predicate=rpi.predicate, predicate_name=pn,
                                                   predicate_kwargs_maker=rpi.predicate_kwargs_maker,
                                                   predicate_failure_response=rpi.predicate_failure_response)
        return rtn
    def clone_add_a_table(self, table, pk_fields, df_fields):
        '''

        add a table to the PanDatFactory

        :param table: table not in the schema

        :param pk_fields: container of the primary key fields

        :param df_fields: container of the data fields

        :return: a clone of the PanDatFactory, with the new table added
        '''
        return utils.clone_add_a_table(self, table, pk_fields, df_fields)
    def clone_add_a_column(self, table, field, field_type, field_position="append"):
        '''

        add a column to the PanDatFactory

        :param table: table in the schema

        :param field: name of the new field to be added

        :param field_type: either "primary key" or "data"

        :param field_position: integer between 0 and the length of self.primary_key_fields[table] (if "primary key")
                               or self.data_fields[table] (if "data"), inclsuive.
                               Alternately, can be "append", which will just insert the column at the end of the
                               appropriate list.

        :return: a clone of the PanDatFactory, with field inserted into location field_position for field_type
        '''
        return utils.clone_add_a_column(self, table, field, field_type, field_position)
    def clone_remove_a_column(self, table, field):
        '''
        remove a column from the PanDatFactory

        :param table: table in the schema

        :param field: name of the field to be removed

        :return: a clone of the PanDatFactory, with field removed
        '''
        return utils.clone_remove_a_column(self, table, field)
    def clone_rename_a_column(self, table, field, new_field):
        '''
        rename a column in the PanDatFactory

        :param table: table in the schema

        :param field: name of the field to be removed

        :param new_field: new name for the field

        :return: a clone of the PanDatFactory, with field renamed to new_field. Data types, default values and
                 foreign keys will reflect the new field name, but row predicates will be copied over as-is (and thus you will need
                 to re-create them as needed).
        '''
        return utils.clone_rename_a_column(self, table, field, new_field)
    @property
    def default_values(self):
        return deep_freeze(self._default_values)
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

        After calling this function, the tooltips property for this PanDatFactory will be appropriately adjusted.
        """
        utils.set_tooltip(self, table, field, tooltip, self._tooltips)

    @property
    def parameters(self):
        return FrozenDict(self._parameters)
    @property
    def duplicates_ticdat_init(self):
        """
        see __doc__ for set_duplicates_ticdat_init
        """
        return self._duplicates_ticdat_init[0]
    def set_duplicates_ticdat_init(self, value):
        """
        Set the duplicates_ticdat_init for the PanDatFactory. Choices are:
        --> 'assert' : an assert is raised if duplicate rows are passed to TicDat.__init__
        --> 'warn'   : emit a warning if duplicate rows are passed to TicDat.__init__
        --> 'ignore' : don't do anything if duplicate rows are passed to TicDat.__init__
        This is only relevant when using copy_to_tic_dat
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
        Set the xlsx_trailing_empty_rows for the PanDatFactory. Choices are:
        --> 'prune' : (the default) when reading an xlsx/xlsm file, look for trailing all pd.isnull rows in each table,
                      and prune them
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
        return  self._infinity_io_flag[0]
    def set_infinity_io_flag(self, value):
        """
        Set the infinity_io_flag for the PanDatFactory.
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
        "inf"/"-inf" in such serializations.

        :param value: a valid infinity_io_flag

        :return:
        """
        verify(value == "N/A" or (utils.numericish(value) and (0 < value < float("inf"))) or (value is None),
           "infinity_io_flag needs to be 'N/A' (to indicate it isn't being used), or None, or a positive finite number")
        self._infinity_io_flag[0] = value
        self._none_as_infinity_bias_cache.clear()
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
    def _dtypes_for_pandas_read(self, table):
        '''
        we expect other routines inside ticdat to access this routine, even though it starts with _
        :param table: table in the schema
        :return valid argument for dtype argument for pandas.read_ routine
        '''
        rtn = {}
        assert table in self.all_tables
        for f, dt in self.data_types.get(table, {}).items():
            if not dt.datetime and not dt.number_allowed and dt.strings_allowed:
                rtn[f] = str
        if self.parameters and table == "parameters":
            for fld_singleton in self.schema()["parameters"]:
                if fld_singleton[0] not in rtn:
                    rtn[fld_singleton[0]] = str
        return rtn
    def _general_post_read_adjustment(self, dat, push_parameters_to_be_valid=False, json_read=False):
        '''
        we expect other routines inside ticdat to access this routine, even though it starts with _
        :param dat: PanDat object that was just read from an external data source. dat will be side-effected
        :param push_parameters_to_be_valid : needed for certain file formats, where pandas makes pushy assumptions
                                             about type that might need to be undone
        :param json_read: special 'None'->None override needed for pandas json reader
        '''
        assert push_parameters_to_be_valid or not json_read, "json_read should always push_parameters_to_be_valid"
        apply = utils.faster_df_apply
        for t in set(self.all_tables).difference(["parameters"]): # parameters table is handled differently
            df = getattr(dat, t)
            all_fields = tuple(self.primary_key_fields.get(t, ()) + self.data_fields.get(t, ()))
            if utils.numericish(self.infinity_io_flag):
                fields_w_issues = set()
                def find_fields_w_issues(row):
                    for f in all_fields:
                        if utils.numericish(row[f]) and abs(row[f]) >= self.infinity_io_flag:
                            fields_w_issues.add(f)
                apply(df, find_fields_w_issues)
                for f in fields_w_issues:
                    fixme = apply(df, lambda row: utils.numericish(row[f]) and row[f] >= self.infinity_io_flag)
                    if fixme.any():
                        df.loc[fixme, f] = float("inf")
                    fixme = apply(df, lambda row: utils.numericish(row[f]) and row[f] <= -self.infinity_io_flag)
                    if fixme.any():
                        df.loc[fixme, f] = -float("inf")
            for f in all_fields:
                if not utils.numericish(self.infinity_io_flag) and utils.numericish(self._none_as_infinity_bias(t, f)):
                    assert self.infinity_io_flag is None
                    df[f].fillna(value=self._none_as_infinity_bias(t, f) * float("inf"), inplace=True)
                dt = self.data_types.get(t, {}).get(f, None)
                if dt and dt.datetime:
                    def fixed_row(row):
                        new_row_f = utils.dateutil_adjuster(row[f])
                        return new_row_f if new_row_f is not None else row[f]
                    df[f] = apply(df, fixed_row)
                if json_read and self._dtypes_for_pandas_read(t).get(f) == str:
                    assert dt, "assumed because _dtypes_for_pandas_read result"
                    if dt.nullable:
                        def fixed_row(row):
                            if utils.stringish(row[f]) and row[f].lower() == "none":
                                return None
                            return row[f]
                        df[f] = apply(df, fixed_row)

        # this is the logic that is used in lieu of infinity_io_flag logic for the parameters table
        # it is predicated on the assumption that the parameters table will be serialized to a string/string table
        if self.parameters:
            [key_fld], [val_fld] = self.schema()["parameters"]
            td = lambda k : getattr(self.parameters.get(k, None), "type_dictionary", None)
            _can_parameter_have_number = lambda k : False if td(k) and not td(k).number_allowed else True
            _can_parameter_have_data = lambda k, data: False if td(k) and not td(k).valid_data(data) else True
            def fix_value(row):
                key, value = [row[_] for _ in [key_fld, val_fld]]
                if td(key) and td(key).datetime and stringish(value) and \
                    (utils.dateutil_adjuster(value) is not None) and \
                    _can_parameter_have_data(key, utils.dateutil_adjuster(value)) and \
                    push_parameters_to_be_valid:
                    return utils.dateutil_adjuster(value)
                if json_read and utils.stringish(value) and value.lower() == "none" and \
                    _can_parameter_have_data(key, None):
                    return None
                if not _can_parameter_have_number(key):
                    if push_parameters_to_be_valid and not _can_parameter_have_data(key, value) and \
                       _can_parameter_have_data(key, str(value)):
                        return str(value) # this is a bit of extreme defensive programming, not covered by unittests
                    return value
                number_v = safe_apply(float)(value)
                if number_v is not None and safe_apply(int)(number_v) == number_v:
                    number_v = int(number_v)
                return value if number_v is None else number_v
            dat.parameters[val_fld] = utils.faster_df_apply(dat.parameters, lambda row: fix_value(row))
        return dat
    def _pre_write_adjustment(self, dat):
        '''
        we expect other routines inside ticdat to access this routine, even though it starts with _
        :param dat: PanDat object that is just now going to be written to an external data source.
                    dat will NOT be side affected by this routine
        :return if infinity adjustment is needed, a deep copy of dat that has the appropriate adjustments
                if no adjustment is needed, an object that has unneeded columns removed
        '''

        rtn = self.PanDat()
        for t in self.all_tables:
            setattr(rtn, t, getattr(dat, t)[list(self._all_fields(t) or getattr(dat, t).columns)])
        if self.infinity_io_flag == "N/A" and not self.parameters:
            return rtn
        rtn = self.copy_pan_dat(rtn) # deep copy so data changes don't side effect
        if self.parameters: # Assuming a parameters table without parameters specification is just a naive developer
            fld = self.data_fields["parameters"][0]
            rtn.parameters[fld] = utils.faster_df_apply(rtn.parameters,
                                                   lambda row: None if isnull(row[fld]) else row[fld])
        if self.infinity_io_flag == "N/A":
            return rtn
        apply = utils.faster_df_apply
        for t in set(self.all_tables).difference(["parameters"]): # parameters table is handled differently
            df = getattr(rtn, t)
            all_fields = tuple(self.primary_key_fields.get(t, ()) + self.data_fields.get(t, ()))
            fields_w_issues = set()
            if utils.numericish(self.infinity_io_flag):
                def find_fields_w_issues(row):
                    for f in all_fields:
                        if utils.numericish(row[f]) and abs(row[f]) >= self.infinity_io_flag:
                            fields_w_issues.add(f)
                apply(df, find_fields_w_issues)
                for f in fields_w_issues:
                    fixme = apply(df, lambda row: utils.numericish(row[f]) and row[f] >= self.infinity_io_flag)
                    if fixme.any():
                        df.loc[fixme, f] = self.infinity_io_flag
                    fixme = apply(df, lambda row: utils.numericish(row[f]) and row[f] <= -self.infinity_io_flag)
                    if fixme.any():
                        df.loc[fixme, f] = -self.infinity_io_flag
            else:
                all_fields = tuple(f for f in all_fields if utils.numericish(self._none_as_infinity_bias(t, f)))
                def find_fields_w_issues(row):
                    for f in all_fields:
                        if row[f] == float("inf") * self._none_as_infinity_bias(t, f):
                            fields_w_issues.add(f)
                apply(df, find_fields_w_issues)
                for f in fields_w_issues:
                    assert self.infinity_io_flag is None
                    fixme = apply(df, lambda row: row[f] == float("inf") * self._none_as_infinity_bias(t, f))
                    if fixme.any():
                        df.loc[fixme, f] = None
        return rtn
    def set_data_type(self, table, field, number_allowed = True,
                      inclusive_min = True, inclusive_max = False, min = 0, max = float("inf"),
                      must_be_int = False, strings_allowed= (), nullable = False, datetime = False):
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

        :param datetime: If truthy, then number_allowed through strings_allowed are ignored. Should the data either
                         be a datetime.datetime object or a string that can be parsed into a datetime.datetime object?
                         Note that the various readers will try to coerce strings into datetime.datetime objects
                         on read for fields with datetime data types. pandas.Timestamp is itself a datetime.datetime,
                         and the bias will be to create such an object.

        :return:
        """
        verify(not self._has_been_used,
               "The data types can't be changed after a PanDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(table not in self.generic_tables, "Cannot set data type for generic table")
        verify(field in self.data_fields[table] + self.primary_key_fields[table],
               "%s does not refer to a field for %s"%(field, table))
        verify(not (table == "parameters" and field in self.data_fields[table] and self.parameters),
               "Don't set the data type for the parameters data field if you are using add_parameters.")

        self._data_types[table][field] = TypeDictionary.safe_creator(number_allowed, inclusive_min, inclusive_max,
                                            min, max, must_be_int, strings_allowed, nullable, datetime)
        self._none_as_infinity_bias_cache.clear()

    def clear_data_type(self, table, field):
        """
        clears the data type for a field. By default, fields don't have types.  Adding a data type doesn't block
        data of the wrong type from being entered. Data types are useful for recognizing errant data entries.
        If no data type is specified (the default) then no errant data will be recognized.

        :param table: table in the schema

        :param field: one of table's fields.

        :return:
        """
        if field not in self._data_types.get(table, ()):
            return
        verify(not self._has_been_used,
               "The data types can't be changed after a PanDatFactory has been used.")
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

        !!! NB!!!!

        **pandas will typically render NULL as nan. In rare cases, it uses None.**

        **Don't check for None (or nan) in your predicate functions. Use pandas.isnull**

        !!!!!!!!!!

        :param table: table in the schema

        :param predicate: A one argument function that accepts a table row as an argument and returns
                          Truthy if the row is valid and Falsey otherwise. (See below, there are other arguments that
                          can refine how predicate works). The row argument passed to predicate will be a dict that
                          maps field name to data value for all fields (both primary key and data field) in the table.
                          Note - if None is passed as a predicate, then any previously added
                          predicate matching (table, predicate_name) will be removed.
.

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
               "The data row predicates can't be changed after a PanDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)

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
                      must_be_int = False, strings_allowed= (), nullable = False, datetime = False,
                      enforce_type_rules = True):
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

        :param datetime: If truthy, then number_allowed through strings_allowed are ignored. Should the data either
                         be a datetime.datetime object or a string that can be parsed into a datetime.datetime object?
                         Note that the various readers will try to coerce strings into datetime.datetime objects
                         on read for parameters with datetime data types. pandas.Timestamp is itself a datetime.datetime,
                         and the bias will be to create such an object.

        :param enforce_type_rules: boolean: ignore all of number_allowed through datetime, and only
                                   enforce the parameter names and default values
        :return:
        """
        verify("parameters" in self.all_tables, "No parameters table")
        verify(len(self.primary_key_fields.get("parameters", [])) ==
               len(self.data_fields.get("parameters", [])) == 1, "parameters table is badly formatted")
        verify(self.data_fields["parameters"][0] not in self._data_types.get("parameters", {}),
                "Don't set the data type for the parameters data field if you are going to use add_parameters.")
        verify(not self._has_been_used,
               "The parameters can't be changed after a PanDatFactory has been used.")
        td = None
        if enforce_type_rules:
            td = TypeDictionary.safe_creator(number_allowed, inclusive_min, inclusive_max,
                                             min, max, must_be_int, strings_allowed, nullable, datetime)
            verify(td.valid_data(default_value), f"{default_value} is not a legal default value for parameter {name}")
        ParameterInfo = clt.namedtuple("ParameterInfo", ["type_dictionary", "default_value"])
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

        Note - the data fields of a schema will have the default default of zero. The primary key fields will
        have no default at all (NOT None, but rather, no default). replace_data_type_failures will only perform
        replacements on fields for which there is a default, unless there is some explicit override provided.
        (see replace_data_type_failures for details).
        This is deliberate, since a bulk replacement in a primary key field is likely to create a duplication failure.

        :return:
        """
        verify(not self._has_been_used,
               "The default values can't be changed after a PanDatFactory has been used.")
        verify(table in self.all_tables, "Unrecognized table name %s"%table)
        verify(field in self.data_fields[table] + self.primary_key_fields[table],
               "%s does not refer to a field for %s"%(field, table))
        verify(utils.acceptable_default(default_value), "%s can not be used as a default value"%default_value)
        self._default_values[table][field] = default_value

    def set_default_values(self, **table_defaults):
        """
        sets the default values for the fields

        :param table_defaults:
             A dictionary of named arguments. Each argument name (i.e. each key) should be a table name
             Each value should itself be a dictionary mapping data field names to default values

        Ex:

        ```pdf.set_default_values(categories = {"minNutrition":0, "maxNutrition":float("inf")},
                         foods = {"cost":0}, nutritionQuantities = {"qty":0})```

        :return:
        """
        verify(not self._has_been_used,
               "The default values can't be changed after a PanDatFactory has been used.")
        for k,v in table_defaults.items():
            verify(k in self.all_tables, "Unrecognized table name %s"%k)
            verify(dictish(v) and set(v).issubset(self.data_fields[k] + self.primary_key_fields[k]),
                "Default values for %s should be a dictionary mapping field names to values"
                %k)
            for f, dv in v.items():
                self.set_default_value(k, f, dv)

    def clear_foreign_keys(self, native_table = None):
        """
        create a PanDatFactory

        :param native_table: optional. The table whose foreign keys should be cleared.
                             If omitted, all foreign keys are cleared.
        """
        verify(not self._has_been_used,
               "The foreign keys can't be changed after a PanDatFactory has been used.")
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
    def _all_fields(self, table):
        assert table in self.all_tables
        return tuple(_ for _ in self.primary_key_fields.get(table, ()) + self.data_fields.get(table, ()))
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
        if utils.does_new_fk_complete_circle(native_table, foreign_table, self):
            print(f"*** A circular foreign key relationship will be creating by adding the {native_table} to " +
                  f"{foreign_table} connection")
        self._foreign_keys[native_table, foreign_table].add(tuple(_mappings.items()))
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
        self._tooltips = {}
        self._foreign_keys = clt.defaultdict(set)
        self._parameters = {}
        self._infinity_io_flag = ["N/A"]
        self._xlsx_trailing_empty_rows = ["prune"]
        self._duplicates_ticdat_init = ["assert"]
        self._none_as_infinity_bias_cache = {}


        self.all_tables = frozenset(init_fields)
        superself = self
        class PanDat(object):
            def _len_dict(self):
                '''
                :return: a dictionary summarizing table lengths. Zero length tables omitted. Safe to use, I won't change
                '''
                return {t: l for t in superself.all_tables for l in [len(getattr(self, t))] if l}
            def __repr__(self):
                tlen = lambda t: len(getattr(self, t)) if isinstance(getattr(self, t), DataFrame) else None
                return "pd: {" + ", ".join("%s: %s"%(t, tlen(t)) for t in sorted(superself.all_tables)) + "}"
            def __init__(self, **init_tables):
                superself._trigger_has_been_used()
                init_tables = {k: v for k,v in init_tables.items() if isinstance(v, (pd.DataFrame, pd.Series)) or
                               utils.safe_apply(bool)(v)}
                for t in init_tables:
                    verify(t in superself.all_tables, "Unexpected table name %s"%t)
                    tbl = safe_apply(DataFrame)(init_tables[t])
                    if tbl is None and dictish(init_tables[t]) and all(map(stringish, init_tables[t])):
                        tbl = safe_apply(DataFrame)(**init_tables[t])
                    verify(isinstance(tbl, DataFrame),
                           "Failed to provide a valid DataFrame or DataFrame construction argument for %s"%t)
                    setattr(self, t, tbl.copy())
                    df = getattr(self, t)
                    pks = superself.primary_key_fields.get(t, ())
                    if pks and not set(pks).intersection(df.columns) and \
                       set(pks) == utils.safe_apply(lambda: set(df.index.names))():
                        df.reset_index(drop=False, inplace=True)
                    if list(df.columns) == list(range(len(df.columns))) and \
                       len(df.columns) >= len(superself._all_fields(t)):
                        df.rename(columns={f1:f2 for f1, f2 in zip(df.columns, superself._all_fields(t))},
                                  inplace=True)
                    if list(df.columns) != list(range(len(df.columns))):
                        for f, d in superself.default_values.get(t, {}).items():
                            if f not in df.columns:
                                df[f] = d
                for t in set(superself.all_tables).difference(init_tables):
                    setattr(self, t, DataFrame({f:[] for f in utils.all_fields(superself, t)}))
                missing_fields = {(t, f) for t in superself.all_tables for f in superself._all_fields(t)
                                  if f not in getattr(self, t).columns}
                verify(not missing_fields,
                       "The following are (table, field) pairs missing from the data.\n%s"%missing_fields)
                for t in superself.all_tables:
                    af = list(superself._all_fields(t))
                    df = getattr(self, t)
                    if list(df.columns)[:len(af)] != af:
                        extra_cols = [_ for _ in list(df.columns) if _ not in af]
                        setattr(self, t, df[af + extra_cols])
                        assert list(getattr(self, t)) == af + extra_cols

        self.PanDat = PanDat
        self.xls = pandatio.XlsPanFactory(self)
        self.sql = pandatio.SqlPanFactory(self)
        self.csv = pandatio.CsvPanFactory(self)
        self.json = pandatio.JsonPanFactory(self)
        self.pgsql = PostgresPanFactory(self)

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
    def copy_to_tic_dat(self, pan_dat, freeze_it=False):
        """
        copies the pan_dat object into a new tic_dat object
        performs a deep copy

        :param pan_dat: a pandat object

        :param freeze_it: boolean. should the returned object be frozen?

        :return: a deep copy of the pan_dat argument in tic_dat format
                I.e.
                   assert TicDatFactory(**self.schema()).good_tic_dat_object(rtn)
                 works.

                 Note that nan will NOT be converted to None in the returned object.

        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = self._copy_to_tic_dat(pan_dat)
        from ticdat import TicDatFactory
        # note - this has a minor problem in that it might downcast pandas.Timestamp to numpy.datetime64
        # in the primary key entries.
        tdf = TicDatFactory(**self.schema())
        return tdf.freeze_me(rtn) if freeze_it else rtn
    def _copy_to_tic_dat(self, pan_dat, keep_generics_as_df=True):
        sch = self.schema()
        if not keep_generics_as_df:
            for t in self.generic_tables:
                sch[t] = [[], list(getattr(pan_dat, t).columns)]
        from ticdat import TicDatFactory
        tdf = TicDatFactory(**sch)
        tdf.set_duplicates_ticdat_init(self.duplicates_ticdat_init)
        def df(t):
            rtn = getattr(pan_dat, t)
            if t in self.generic_tables and not keep_generics_as_df:
                return list(map(list, rtn.itertuples(index=False)))
            return rtn
        return tdf.TicDat(**{t: df(t) for t in self.all_tables})
    def _same_data(self, obj1, obj2, epsilon = 0, nans_are_same_for_data_rows = False):
        from ticdat import TicDatFactory
        sch = self.schema()
        if not all(len(getattr(obj1, t)) == len(getattr(obj2, t)) for t in self.all_tables):
            return False
        for t in self.generic_tables:
            if set(getattr(obj1, t).columns) != set(getattr(obj2, t).columns):
                return False
            sch[t] = [[], list(getattr(obj1, t).columns)]
        tdf = TicDatFactory(**sch)
        return tdf._same_data(self._copy_to_tic_dat(obj1, keep_generics_as_df=False),
                              self._copy_to_tic_dat(obj2, keep_generics_as_df=False), epsilon=epsilon,
                              nans_are_same_for_data_rows=nans_are_same_for_data_rows)
    def _true_data_types(self):
        '''
        See issue https://github.com/ticdat/ticdat/issues/46  and the doc string for find_data_type_failures
        for more info
        :return:
        '''
        tmp_pdf = PanDatFactory.create_from_full_schema(self.schema(include_ancillary_info=True))
        for t, pks in self.primary_key_fields.items():
            for pk in pks:
                if pk not in self._data_types.get(t, ()):
                    tmp_pdf.set_data_type(t, pk, number_allowed=True,
                      inclusive_min=True, inclusive_max=True, min=-float("inf"), max=float("inf"),
                      must_be_int=False, strings_allowed='*', nullable=False, datetime=False)
        return tmp_pdf.data_types
    def find_data_type_failures(self, pan_dat, as_table=True, max_failures=float("inf")):
        """
        Finds the data type failures for a pandat object

        :param pan_dat: pandat object

        :param as_table: boolean - if truthy then the values of the return dictionary will be the
               data type failure rows themselves. Otherwise will return the boolean Series that indicates
               which rows have data type failures.

        :param max_failures: number. An upper limit on the number of failures to find. Will short circuit and return
                                     ASAP with a partial failure enumeration when this number is reached.

        :return: A dictionary constructed as follow:
                 The keys are namedtuples with members "table", "field". Each (table,field) pair
                 has data values that are inconsistent with its data type. (table, field) pairs
                 with no data type at all are never part of the returned dictionary.
                 The values are DataFrames that contain the subset of rows that exhibit data failures
                 for this specific table, field pair (or the boolean Series that identifies these rows).
                 
         Note that for primary key fields (but not data fields) with no explicit data type, a temporary filter
         that excludes only Null will be applied. If you want primary key fields to allow Null, you must explicitly
         opt-in by calling set_data_type appropriately.
         See issue https://github.com/ticdat/ticdat/issues/46 for more info.
        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        assert max_failures > 0, "max_failures should be a positive number"

        rtn = {}
        TableField = clt.namedtuple("TableField", ["table", "field"])
        number_failures = [0]
        check_too_many = None
        if max_failures < float("inf"):
            def check_too_many(is_bad_row):
                if is_bad_row:
                    number_failures[0] += 1
                    if number_failures[0] >= max_failures:
                        return lambda row: False  # all future rows will be good (i.e. not bad, i.e. False)
        for table, type_row in self._true_data_types().items():
            _table = getattr(pan_dat, table)
            for field, data_type in type_row.items():
                def bad_row(row):
                    data = row[field]
                    return not data_type.valid_data(None if isnull(data) else data)
                where_bad_rows = utils.faster_df_apply(_table, bad_row, trip_wire_check=check_too_many)
                if where_bad_rows.any():
                    rtn[TableField(table, field)] = _table[where_bad_rows].copy() if as_table else where_bad_rows
                if number_failures[0] >= max_failures:
                    return rtn
        return rtn
    def replace_data_type_failures(self, pan_dat, replacement_values=None):
        """
        Replace the data cells with data type failures with the default value for the appropriate field.

        :param pan_dat: a pandat object

        :param replacement_values: if provided, a dictionary mapping (table, field) to replacement value.
               the default value will be used for (table, field) pairs not in replacement_values

        :return: the pan_dat object with replacements made. The pan_dat object itself will be edited in place.

        Replaces any of the data failures found in find_data_type_failures() with the appropriate
        replacement_value.

        Will perform both primary key and data field replacements. However, for a replacement to be performed,
        the (table, field) pair must either have an entry in replacement_values, or there must be a default value
        that can be referenced via self.default_values.get(table, {})[field]. Note that a default default of zero is
        present for data fields but not for primary key fields. As a result, you need to explicitly opt-in (either
        with a prior call to set_data_type or via the replacement_values argument) to use this routine to replace
        primary key fields entries. This is deliberate, since a bulk replacement in a primary key
        field is likely to create a duplication failure.
        """
        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        replacement_values = replacement_values or {}
        verify(dictish(replacement_values) and all(len(k)==2 for k in replacement_values),
               "replacement_values should be a dictionary mapping (table, field) to valid replacement value")
        for (table,field), v in replacement_values.items():
            verify(table in self.all_tables, "%s is not a table for this schema"%table)
            verify(field in self._all_fields(table), "%s is not a field for %s"%(field, table))

        replacements_needed = self.find_data_type_failures(pan_dat, as_table=False)
        if not replacements_needed:
            return pan_dat

        real_replacements = {}
        for table, type_row in self._true_data_types().items():
            for field in type_row:
                if ((table, field) in replacement_values) or (field in self.default_values.get(table, {})):
                    real_replacements[table, field] = replacement_values.get((table, field),
                        self.default_values[table][field])
        for (table, field), value in real_replacements.items():
            if (table, field) in replacements_needed:
                verify(self._true_data_types()[table][field].valid_data(value),
                       "The replacement value %s is not itself valid for %s : %s"%(value, table, field))

        for (table, field), rows in replacements_needed.items() :
            if (table, field) in real_replacements:
                getattr(pan_dat, table).loc[rows, field] = real_replacements[table, field]
        assert not set(self.find_data_type_failures(pan_dat)).intersection(real_replacements)
        return pan_dat
    def find_data_row_failures(self, pan_dat, as_table=True, exception_handling="__debug__",
                               max_failures=float("inf")):
        """
        Finds the data row failures for a ticdat object

        :param pan_dat: a pandat object

        :param as_table: boolean - if truthy then the values of the return dictionary will be the
               predicate failure rows themselves. Otherwise will return the boolean Series that indicates
               which rows have predicate failures.

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

        :return: A dictionary constructed as follows:

        The keys are namedtuples with members "table", "predicate_name".

        The values are DataFrames that contain the subset of rows that exhibit data failures
        for this specific table, predicate pair (or the Series that identifies these rows).

        If the predicate_failure_response for the predicate is "Error Message" (instead of "Boolean")
        and as_table is truthy, then an "Error Message" column will be added to the appropriate DataFrame in the
        returned dict.

        If a predicate_kwargs_maker is provided and it fails (either by failing to return a dictionary or by
        throwing a handled exception) then appropriate value of the dictionary will be a namedtuple
        with members "primary_key" and "error message". The former will be populated with '*' (indicating all the rows)
        and the latter will be a string describing the failure.
        """
        assert max_failures > 0, "max_failures should be a positive number"
        number_failures = [0]
        check_too_many_bool = check_too_many_msg = None
        if max_failures < float("inf"):
            def check_too_many_bool(is_bad_row):  # here faster_df_apply is applying a function that
                if is_bad_row:  # returns True when the row is bad and False o/wise (the boolean reverse of the
                    number_failures[0] += 1 # of the row predicate)
                    if number_failures[0] >= max_failures:  # all future rows will be good
                        return lambda row: False  # which in this context is not bad (i.e. False)

            def check_too_many_msg(true_or_msg):  # here faster_df_apply is applying a function that
                if true_or_msg is not True:  # returns True when the row is good and a string o/wise
                    number_failures[0] += 1  # (i.e. faster_df_applyu is using the actual row predicate)
                    if number_failures[0] >= max_failures:  # all future rows will be good
                        return lambda row: True  # which in this context is True

        msg = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(exception_handling in ["Handled as Failure", "Unhandled", "__debug__"],
               "bad exception_handling argument")
        if exception_handling == "__debug__":
            exception_handling = "Unhandled" if __debug__ else "Handled as Failure"
        data_row_predicates = {k: dict(v) for k,v in self._data_row_predicates.items()}
        if self._parameters:
            def good_parameter(row):
                k = row[self.primary_key_fields["parameters"][0]]
                v = row[self.data_fields["parameters"][0]]
                v = None if isnull(v) else v
                chk = self._parameters.get(k)
                return chk and (chk.type_dictionary is None or chk.type_dictionary.valid_data(v))
            _ = "Good Name/Value Check"
            make_name = lambda i: _ if _ not in self._data_row_predicates.get("parameters", {}) else f"{_}_{i}"
            predicate_name = next(make_name(i) for i in count() if make_name(i) not in
                                  self._data_row_predicates.get("parameters", {}))
            data_row_predicates["parameters"] = data_row_predicates.get("parameters", {})
            data_row_predicates["parameters"][predicate_name] = RowPredicateInfo(good_parameter, None, "Boolean")

        rtn = {}
        predicate_kwargs_maker_results = {}
        TPN = clt.namedtuple("TablePredicateName", ["table", "predicate_name"])
        PKEM = clt.namedtuple("PrimaryKeyErrorMessage", ["primary_key", "error_message"])
        for tbl, row_predicates in data_row_predicates.items():
            _table = getattr(pan_dat, tbl)
            for pn, rpi in row_predicates.items():
                predicate_kwargs = {}
                if rpi.predicate_kwargs_maker:
                    if rpi.predicate_kwargs_maker not in predicate_kwargs_maker_results:
                        if exception_handling == "Handled as Failure":
                            try:
                                _predicate_kwargs = rpi.predicate_kwargs_maker(pan_dat)
                            except Exception as e:
                                _predicate_kwargs = f"Exception<{e}>"
                        else:
                            _predicate_kwargs = rpi.predicate_kwargs_maker(pan_dat)
                        predicate_kwargs_maker_results[rpi.predicate_kwargs_maker] = _predicate_kwargs
                    predicate_kwargs = predicate_kwargs_maker_results[rpi.predicate_kwargs_maker]
                if not isinstance(predicate_kwargs, dict):
                    rtn[TPN(tbl, pn)] = PKEM('*', predicate_kwargs
                                        if (isinstance(predicate_kwargs, str) and "Exception<" in predicate_kwargs)
                                        else f"predicate_kwargs_maker failed to return a dict")
                    number_failures[0] += 1
                else:
                    if rpi.predicate_failure_response == "Boolean":
                        def _p(row):
                            try:
                                return rpi.predicate(row, **predicate_kwargs)
                            except:
                                return False
                        bad_row = (lambda row: not rpi.predicate(row, **predicate_kwargs)) \
                                  if exception_handling == "Unhandled" else (lambda row: not _p(row))

                        where_bad_rows = utils.faster_df_apply(_table, bad_row, trip_wire_check=check_too_many_bool)
                        if where_bad_rows.any():
                            rtn[TPN(tbl, pn)] = _table[where_bad_rows].copy() if as_table else where_bad_rows
                    else:
                        def _p(row):
                            try:
                                return rpi.predicate(row, **predicate_kwargs)
                            except Exception as e:
                                return f"Exception<{e}>"
                        predicate = (lambda row: rpi.predicate(row, **predicate_kwargs)) \
                                    if exception_handling == "Unhandled" else (lambda row: _p(row))
                        predicate_result = utils.faster_df_apply(_table, predicate, trip_wire_check=check_too_many_msg)
                        where_bad_rows = predicate_result.apply(lambda x: x is not True)
                        if where_bad_rows.any():
                            if as_table:
                                rtn[TPN(tbl, pn)] = _df = _table[where_bad_rows].copy()
                                err_column = "Error Message"
                                _ = count(1)
                                while err_column in _df.columns:
                                    err_column = f"Error Message ({next(_)})"
                                _df[err_column] = predicate_result[where_bad_rows].copy()
                            else:
                                rtn[TPN(tbl, pn)] = where_bad_rows
                if number_failures[0] >= max_failures:
                    return rtn
        return rtn
    def find_foreign_key_failures(self, pan_dat, verbosity="High", as_table=True, max_failures=float("inf")):
        """
        Finds the foreign key failures for a pandat object

        :param pan_dat: pandat object

        :param verbosity: either "High" or "Low"

        :param as_table: as_table boolean : if truthy then the values of the return dictionary will be the
               failed rows themselves. Otherwise will return the a boolean list that indicates which rows
               have failures. (For technical reasons, not returning a boolean Series like the
               other find functions)

        :param max_failures: number. An upper limit on the number of failures to find. Will short circuit and return
                                     ASAP with a partial failure enumeration when this number is reached.

        :return: A dictionary constructed as follows:

         The keys are namedtuples with members "native_table", "foreign_table",
         "mapping", "cardinality".

         The key data matches the arguments to add_foreign_key that constructed the
         foreign key (with "cardinality" being deduced from the overall schema).

         The values are DataFrames that contain the subset of native table rows that fail to find
         the foreign table matching defined by the associated returned key (or the
         list that identifies these rows).

         For verbosity = 'Low' a simpler return object is created that doesn't use namedtuples
         and omits the foreign key cardinality.
        """
        # note - the as_table argument is messy here because I'm applying an index to a copy of the table
        # as a result, we provide the remove_foreign_key_failures companion function
        assert max_failures > 0, "max_failures should be a positive number"
        verify(verbosity in ["High", "Low"], "verbosity needs to be either 'High' or 'Low'")
        rtn = {}
        for fk, rows in self._find_foreign_key_failure_rows(pan_dat, max_failures=max_failures).items():
            native, foreign, mappings, card = fk
            rtn[fk] = getattr(pan_dat, native)[rows] if as_table else rows
        if verbosity == "Low":
            rtn = {tuple(k[:2]) + (tuple(k[2]),): v for k,v in rtn.items()}
        return rtn
    def _find_foreign_key_failure_rows(self, pan_dat, max_failures=float("inf")):
        msg  = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        number_failures = [0]
        check_too_many = None
        if max_failures < float("inf"):
            def check_too_many(is_bad_row):
                if is_bad_row:
                    number_failures[0] += 1
                    if number_failures[0] >= max_failures:
                        return lambda row: False  # all future rows will be good (i.e. not bad, i.e. False)

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
            try:
                joined = child.join(parent, rsuffix=magic_field)
            except TypeError:
                joined = None # see ticdat issue 173
            except:
                raise
            bad_rows = set(child[magic_field*2]) if joined is None else \
                       set(joined[joined[magic_field] != True][magic_field*2])
            if bad_rows:
                # I'm casting to list here because child is a copy that doesn't have the original index
                # This is fixable, (i.e. we could return a Series with an index matching the original child table)
                # but the fix isn't high priority.
                rtn[fk] = list(utils.faster_df_apply(child, lambda row: row[magic_field*2] in bad_rows,
                                                     trip_wire_check=check_too_many))
                if number_failures[0] >= max_failures:
                    return rtn
        return rtn
    def create_full_parameters_dict(self, dat):
        """
        create a fully populated dictionary of all the parameters

        :param dat: a PanDat object that has a parameters table

        :return: a dictionary that maps parameter option to actual dat.parameters value.
                 if the specific option isn't part of dat.parameters, then the default value is used.
                 Note that for datetime parameters, the default will be coerced into a datetime object, if possible.
        """
        msg  = []
        verify(self.good_pan_dat_object(dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(self.parameters, "no parameters options have been specified")
        defaults = {k: dt if dt is not None and v.type_dictionary and v.type_dictionary.datetime else df
                    for k,v in self._parameters.items() for df in [v.default_value]
                    for dt in [utils.dateutil_adjuster(df)]}
        df = dat.parameters[list(self.primary_key_fields["parameters"]) + list(self.data_fields["parameters"])]
        return dict(defaults, **{k: v for k,v in df.itertuples(index=False)})

    def remove_foreign_key_failures(self, pan_dat):
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
    def find_duplicates(self, pan_dat, keep="first", as_table=True):
        """
        Find the duplicated rows based on the primary key fields.

        :param pan_dat: pandat object

        :param keep: 'first': Treat all duplicated rows as duplicates except for the first occurrence.
                     'last': Treat all duplicated rows as duplicates except for the last occurrence.
                     False: Treat all duplicated rows as duplicates

        :param as_table: as_table boolean : if truthy then the values of the return dictionary will be the
               duplicated rows themselves. Otherwise will return the boolean Series that indicates which rows
               are duplicated rows.

        :return: A dictionary whose keys are the table names and whose values are duplicated rows (or the
                 Series that identifies these rows)
        """
        msg  = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        rtn = {}
        for t in self.all_tables:
            if self.primary_key_fields.get(t):
                dups = getattr(pan_dat, t).duplicated(list(self.primary_key_fields[t]), keep=keep)
                if dups.any():
                    rtn[t] = getattr(pan_dat, t)[list(dups)] if as_table else dups
        return rtn
    def copy_to_ampl(self, pan_dat, field_renamings = None, excluded_tables = None):
        """
        copies the pan_dat object into a new pan_dat object populated with amplpy.DataFrame objects
        performs a deep copy

        :param pan_dat: a PanDat object

        :param field_renamings: dict or None. If fields are to be renamed in the copy, then
                                a mapping from (table_name, field_name) -> new_field_name
                                If a data field is to be omitted, then new_field can be falsey
                                table_name cannot refer to an excluded table. (see below)
                                field_name doesn't have to refer to a field to an element of
                                self.data_fields[t], but it doesn't have to refer to a column in
                                the pan_dat.table_name DataFrame

        :param excluded_tables: If truthy, a list of tables to be excluded from the copy.
                                Tables without primary key fields are always excluded.

        :return: a deep copy of the tic_dat argument into amplpy.DataFrames
        """
        verify(amplpy, "amplpy needs to be installed in order to enable AMPL functionality")
        msg  = []
        verify(self.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not excluded_tables or (containerish(excluded_tables) and
                                       set(excluded_tables).issubset(self.all_tables)),
               "bad excluded_tables argument")
        copy_tables = {t for t in self.all_tables if self.primary_key_fields[t]}.\
                      difference(excluded_tables or [])
        field_renamings = field_renamings or {}
        verify(dictish(field_renamings), "invalid field_renamings argument")
        for k,v in field_renamings.items():
            verify(containerish(k) and len(k) == 2 and k[0] in copy_tables and
                   k[1] in getattr(pan_dat, k[0]).columns and
                   ((v and utils.stringish(v)) or (not bool(v) and k[1] not in self.primary_key_fields[k[0]])),
                   "invalid field_renamings argument %s:%s"%(k,v))
        class AmplPanDat(object):
            def __repr__(self):
                return "td:" + tuple(copy_tables).__repr__()
        rtn = AmplPanDat()
        for t in copy_tables:
            rename = lambda f : field_renamings.get((t, f), f)
            df_ampl = amplpy.DataFrame(index=tuple(map(rename, self.primary_key_fields[t])))
            for f in self.primary_key_fields[t]:
                df_ampl.setColumn(rename(f), list(getattr(pan_dat, t)[f]))
            for f in {f for _t,f in field_renamings if _t == t}.union(self.data_fields[t]):
                if rename(f):
                    df_ampl.addColumn(rename(f), list(getattr(pan_dat, t)[f]))
            setattr(rtn, t, df_ampl)
        return rtn
    def set_ampl_data(self, ampl_dat, ampl, table_to_set_name = None):
        """
        performs bulk setData on the AMPL-esque first argument.

        :param ampl_dat: an AmplTicDat object created by calling copy_to_ampl

        :param ampl: an amplpy.AMPL object

        :param table_to_set_name: a mapping of table_name to ampl set name

        :return:
        """
        verify(all(a.startswith("_") or a in self.all_tables for a in dir(ampl_dat)),
               "bad ampl_dat argument")
        verify(hasattr(ampl, "setData"), "bad ampl argument")
        table_to_set_name = table_to_set_name or {}
        verify(dictish(table_to_set_name) and all(hasattr(ampl_dat, k) and
                   utils.stringish(v) for k,v in table_to_set_name.items()),
               "bad table_to_set_name argument")
        for t in set(self.all_tables).intersection(dir(ampl_dat)):
            try:
                ampl.setData(getattr(ampl_dat, t), *([table_to_set_name[t]]
                                                if t in table_to_set_name else []))
            except:
                raise utils.TicDatError(t + " cannot be passed as an argument to AMPL.setData()")
    def copy_from_ampl_variables(self, ampl_variables):
        """
        copies the solution results from ampl_variables into a new PanDat object

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

        :return: a deep copy of the ampl_variables into a PanDat object
        """
        # note that the possibility for multiple table_names with different field_names can make
        # for a messy join problem. The easiest solution here is to just use the TicDatFactory logic
        from ticdat import TicDatFactory
        tdf = TicDatFactory.create_from_full_schema(self.schema(include_ancillary_info=True))
        _rtn = tdf.copy_from_ampl_variables(ampl_variables)
        _rtn = tdf.copy_to_pandas(_rtn, drop_pk_columns=False)
        for t in self.all_tables:
            getattr(_rtn, t).reset_index(drop=True, inplace=True)
        rtn = self.PanDat(**{t:getattr(_rtn, t) for t in self.all_tables})
        return rtn
