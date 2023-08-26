"""
general utility module
PEP8
"""
from numbers import Number
from itertools import chain, combinations
from collections import defaultdict
import ticdat
import getopt
import sys
import os
from collections import namedtuple
import time
import datetime as datetime_
try:
    import dateutil, dateutil.parser
except:
    dateutil = None
import json
try:
    import dspotconnect
except:
    dspotconnect = None
try:
    import pandas as pd
    from pandas import DataFrame
    import numpy
except:
    pd = DataFrame = numpy = None

try:
    import ocp_ticdat_drm as drm
except:
    drm = None
import inspect

def faster_df_apply(df, func, trip_wire_check=None):
    """
    pandas.DataFrame.apply is rarely used because it is slow. It is slow because it creates a Series for each row
    of the DataFrame, and passes this Series to the function. faster_df_apply creates a dict for each row of the
    DataFrame instead, and as a result is **much** faster.

    See https://bit.ly/3xnLFld.

    It's certainly possible newer versions of pandas will implement a more performant DataFrame.apply. The broader
    point is, row-wise apply should not be discarded wholesale for performance reasons, as DataFrame.itertuples
    is reasonably fast

    :param df: a DataFrame

    :param func: a function to apply to each row of the DataFrame. The function should accept a fieldname->data
                 dictionary as argument. func will be applied to each row of the DataFrame

    :param trip_wire_check: optional. If provided, a function that will be passed each result returned by func.
                                      trip_wire_check can either return falsey, or a replacement to func to be applied
                                      to the remainder of the DataFrame

    :return: a pandas Series with the same index as df and the values of calling func on each row dict.
    """
    verify(DataFrame and isinstance(df, DataFrame), "df argument needs to be a DataFrame")
    verify(callable(func), "func needs to be a function")
    verify(not trip_wire_check or callable(trip_wire_check), "trip_wire_check needs to None, or a function")
    cols = list(df.columns)
    data, index = [], []
    for row in df.itertuples(index=True):
        row_dict = {f:v for f,v in zip(cols, row[1:])}
        data.append(func(row_dict))
        index.append(row[0])
        if trip_wire_check:
            new_func = trip_wire_check(data[-1])
            if new_func:
                func = new_func
                trip_wire_check = None
    # will default to float for empty Series, like original pandas
    return pd.Series(data, index=index, **({"dtype": numpy.float64} if not data else {}))

def set_tooltip(tdf_pdf, table, field, tooltip, tooltips_dict):
    verify(table in tdf_pdf.all_tables, f"Unrecognized table name {table}")
    verify(field == "" or field in tdf_pdf.data_fields[table] + tdf_pdf.primary_key_fields[table],
           f"{field} is neither the empty string, nor does it refer to a field for {table}")
    verify(isinstance(tooltip, str), "tooltip argument needs to be a string")
    dict_key = (table, field) if field else table
    if not tooltip:
        tooltips_dict.pop(dict_key, "")
    else:
        tooltips_dict[dict_key] = tooltip

def make_tooltips_dict_json_friendly(tooltips_dict):
    rtn = defaultdict(dict)
    for k, v in tooltips_dict.items():
        if isinstance(k, str):
            rtn[k][""] = v
        else:
            rtn[k[0]][k[1]] = v
    return dict(rtn)

def clone_add_a_column(tdf_pdf, table, field, field_type, field_position="append"):
    verify(table in tdf_pdf.all_tables, "Unrecognized table name %s" % table)
    verify(isinstance(field, str), "field needs to be a string")
    verify(field_type in ['primary key', 'data'], "field_type needs to be 'primary key' or 'data'")
    current_fields = getattr(tdf_pdf, {"primary key": "primary_key_fields", "data": "data_fields"}[field_type])[table]
    verify(field not in current_fields, f"{field} already present in {table}")
    if field_position == "append":
        field_position = len(current_fields)
    verify(0 <= field_position <= len(current_fields),
           f"field_positon needs to be between 0 and {len(current_fields)}, inclusive")
    def clone_factory(full_schema):
        full_schema = clone_a_anchillary_info_schema(full_schema, table_restrictions=set(tdf_pdf.all_tables))
        full_schema["tables_fields"][table][{"primary key": 0, "data": 1}[field_type]].insert(field_position, field)
        return tdf_pdf.create_from_full_schema(full_schema)
    return tdf_pdf.clone(clone_factory=clone_factory)

def clone_remove_a_column(tdf_pdf, table, field):
    verify(table in tdf_pdf.all_tables, "Unrecognized table name %s" % table)
    verify(field in tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table],
           f"field needs to be one of {tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table]}")
    def clone_factory(full_schema):
        full_schema = clone_a_anchillary_info_schema(full_schema, table_restrictions=set(tdf_pdf.all_tables),
                                                     fields_to_remove=[[table, field]])
        return tdf_pdf.create_from_full_schema(full_schema)
    return tdf_pdf.clone(clone_factory=clone_factory)

def clone_add_a_table(tdf_pdf, table, pk_fields, df_fields):
    verify(table not in tdf_pdf.all_tables, f"{table} isn't a new table")
    verify(containerish(pk_fields) and all(isinstance(_, str) for _ in pk_fields),
           "pk_fields needs to be a container of strings")
    verify(containerish(df_fields) and all(isinstance(_, str) for _ in df_fields),
           "df_fields needs to be a container of strings")
    verify(pk_fields or df_fields, "Need to specify at least one field.")
    def clone_factory(full_schema):
        full_schema = clone_a_anchillary_info_schema(full_schema, table_restrictions=set(tdf_pdf.all_tables))
        full_schema["tables_fields"][table] = [list(pk_fields), list(df_fields)]
        return tdf_pdf.create_from_full_schema(full_schema)
    return tdf_pdf.clone(clone_factory=clone_factory)

def clone_rename_a_column(tdf_pdf, table, field, new_field):
    verify(table in tdf_pdf.all_tables, "Unrecognized table name %s" % table)
    verify(field in tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table],
           f"field needs to be one of {tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table]}")
    if field in tdf_pdf.primary_key_fields[table]:
        args = ("primary key", tdf_pdf.primary_key_fields[table].index(field))
    else:
        args = ("data", tdf_pdf.data_fields[table].index(field))
    verify(new_field not in tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table],
           f"new_field cannot be one of {tdf_pdf.primary_key_fields[table] + tdf_pdf.data_fields[table]}")
    rtn = clone_add_a_column(clone_remove_a_column(tdf_pdf, table, field), table, new_field, *args)
    if field in tdf_pdf.data_types.get(table, {}):
        rtn.set_data_type(table, new_field, *tdf_pdf.data_types[table][field])
    if field in tdf_pdf.default_values.get(table, {}):
        rtn.set_default_value(table, new_field, tdf_pdf.default_values[table][field])
    for fk in tdf_pdf.foreign_keys:
        def needs_renaming(mp):
            if hasattr(mp, "native_field"):
                return (table, field) in {(fk.native_table, mp.native_field), (fk.foreign_table, mp.foreign_field)}
            return any(needs_renaming(_) for _ in mp) if containerish(mp) else False
        def do_renaming(mp):
            field_renaming = lambda _t, _f: new_field if (_t, _f) == (table, field) else _f
            if hasattr(mp, "native_field"):
                return (field_renaming(fk.native_table, mp.native_field),
                        field_renaming(fk.foreign_table, mp.foreign_field))
            return tuple(do_renaming(_) for _ in mp)if containerish(mp) else mp
        if needs_renaming(fk.mapping):
            rtn.add_foreign_key(fk.native_table, fk.foreign_table, do_renaming(fk.mapping))
    if (table, field) in tdf_pdf.tooltips:
        rtn.set_tooltip(table, new_field, tdf_pdf.tooltips[table, field])
    return rtn

def clone_a_anchillary_info_schema(schema, table_restrictions, fields_to_remove=None):
    '''
    :param schema: the result of calling _.schema(include_ancillary_info=True) when _ is a
    TicDatFactory or PanDatFactory
    :param table_restrictions: None or a partial list of the tables in schema. If the latter, then this is a white-list
                               of tables to keep, and all the other tables to be removed.
    :param fields_to_remove: None or a list of (table, fields) pairs specifying which fields need to be removed
    :return: a clone of schema, except with the tables outside of table_restrictions removed, and the (table, field)
             pairs inside of field restrictions removed
             (if all those arguments are None, then schema is returned).
    Note - See also clone_add_a_table, clone_add_a_column, clone_remove_a_column and clone_rename_a_column.
    '''
    if all(_ is None for _ in [table_restrictions, fields_to_remove]):
        return schema
    verify(dictish(schema) and schema.get("tables_fields") and isinstance(schema["tables_fields"], dict),
           "schema has missing or invalid tables_fields entry")
    table_restrictions = table_restrictions or set(schema["tables_fields"])
    verify(containerish(table_restrictions) and table_restrictions and
           all(isinstance(_, str) for _ in table_restrictions), "table_restrictions needs to be a container of strings")
    def clean_up_set_of_str_pairs(x, name):
        x = x or []
        verify(containerish(x) and all(containerish(_) and len(_) == 2 and
                                       all(isinstance(__, str) for __ in _) for _ in x),
               f"{name} needs to be a container whose entries are string pairs")
        return {tuple(_) for _ in x}
    fields_to_remove = clean_up_set_of_str_pairs(fields_to_remove, "fields_to_remove")
    verify(set(table_restrictions).issubset(schema["tables_fields"]),
           "table_restrictions needs to be a subset of schema['tables_fields']")
    all_fields = {(t, f) for t, (pks, dfs) in schema["tables_fields"].items() for f in pks + dfs}
    verify(fields_to_remove.issubset(all_fields),
           "fields_to_remove needs to be a subset of the aggregate field set in schema['tables_fields']")
    rtn = {}
    for k, v in schema.items():
        if k == "tables_fields":
            rtn[k] = {}
            for t, (pks, dfs) in schema[k].items():
                if t in table_restrictions:
                    pks = [f for f in pks if (t, f) not in fields_to_remove]
                    dfs = [f for f in dfs if (t, f) not in fields_to_remove]
                    rtn[k][t] = [pks, dfs]
        elif k in ["default_values", "data_types", "tooltips"]:
            rtn[k] = {_k:dict(_v) for _k, _v in v.items() if _k in table_restrictions}
            for t, f in fields_to_remove:
                if f in rtn[k].get(t, {}):
                    rtn[k][t].pop(f)
        elif k == "foreign_keys":
            def good_fk(fk):
                if set(fk[:2]).issubset(table_restrictions):
                    def good_mapping(mp):
                        return (fk.native_table, mp.native_field) not in fields_to_remove and \
                               (fk.foreign_table, mp.foreign_field) not in fields_to_remove
                    if hasattr(fk.mapping, "native_field") and hasattr(fk.mapping, "foreign_field"):
                        return good_mapping(fk.mapping)
                    return all(good_mapping(_) for _ in fk.mapping)
            rtn[k] = tuple(fk for fk in v if good_fk(fk))
        elif k == "parameters":
            rtn[k] = v if k in table_restrictions else {}
        else:
            assert k in {"infinity_io_flag", "xlsx_trailing_empty_rows", "duplicates_ticdat_init"}, \
                f"{k} is unexpected part of schema"
            rtn[k] = v
    return rtn

def dateutil_adjuster(x):
    if isinstance(x, datetime_.datetime):
        return x
    # note that pd.Timestamp tends to create NaT from Falsey, this is ok so long as you check for null using pd.isnull
    # also, pd.Timestampp can do weird things making Timestamps from numbers, so not enabling that.
    def _try_to_timestamp(y):
        if pd and not numericish(y):
            rtn = safe_apply(pd.Timestamp)(y)
            if not pd.isnull(rtn):
                return rtn
        if dateutil:
            return safe_apply(dateutil.parser.parse)(y)
    rtn = _try_to_timestamp(x)
    if rtn is not None:
        return rtn
    if not numericish(x):
        return _try_to_timestamp(str(x))

def acceptable_default(v) :
    return numericish(v) or stringish(v) or (v is None)

def all_fields(tpdf, tbl):
    assert tbl in tpdf.all_tables
    return tpdf.primary_key_fields.get(tbl, ()) + tpdf.data_fields.get(tbl, ())

# can I get away with ordering this consistently with the function? hopefully I can!
class TypeDictionary(namedtuple("TypeDictionary",
                    ("number_allowed", "inclusive_min", "inclusive_max", "min",
                      "max", "must_be_int", "strings_allowed", "nullable", "datetime"))):
    def valid_data(self, data):
        if (pd and pd.isnull(data)) or (data is None):
            return bool(self.nullable)
        if self.datetime:
            # data is not None and dateutil_adjuster(data) is None will always be invalid, re:less of self.nullable
            # self.nullable only refers to whether the true None can be passed.
            return isinstance(data, datetime_.datetime) or dateutil_adjuster(data) is not None
        if numericish(data):
            if not self.number_allowed:
                return False
            if (data < self.min) or (data > self.max):
                return False
            if (not self.inclusive_min) and (data == self.min):
                return False
            if (not self.inclusive_max) and (data  == self.max):
                return False
            if (self.must_be_int) and (safe_apply(int)(data) != data) and \
               not (data == self.max == float("inf") and self.inclusive_max):
                return False
            return True
        if stringish(data):
            if self.strings_allowed == "*":
                return True
            assert containerish(self.strings_allowed)
            return data in self.strings_allowed
        return False
    @staticmethod
    def safe_creator(number_allowed, inclusive_min, inclusive_max, min, max,
                      must_be_int, strings_allowed, nullable, datetime=False):
        verify(dateutil or pd or not datetime,
               "dateutil or pandas needs to be installed in order to use datetime data type")
        if datetime:
            return TypeDictionary(number_allowed=False, strings_allowed=(), nullable=bool(nullable),
                                  min=0, max=float("inf"), inclusive_min=True, inclusive_max=True, must_be_int=False,
                                  datetime=True)
        verify((strings_allowed == '*') or
               (containerish(strings_allowed) and all(stringish(x) for x in strings_allowed)),
               """The strings_allowed argument should be a container of strings, or the single '*' character.""")
        if containerish(strings_allowed):
            strings_allowed = tuple(strings_allowed)  # defensive copy
        if number_allowed:
            verify(numericish(max), "max should be numeric")
            verify(numericish(min), "min should be numeric")
            verify(max >= min, "max cannot be smaller than min")
            return TypeDictionary(number_allowed=True, strings_allowed=strings_allowed, nullable=bool(nullable),
                                  min=min, max=max, inclusive_min=bool(inclusive_min),inclusive_max=bool(inclusive_max),
                                  must_be_int=bool(must_be_int), datetime=False)
        return TypeDictionary(number_allowed=False, strings_allowed=strings_allowed, nullable=bool(nullable),
                              min=0, max=float("inf"), inclusive_min=True, inclusive_max=True, must_be_int=False,
                              datetime=False)

class ForeignKey(namedtuple("ForeignKey", ("native_table", "foreign_table", "mapping", "cardinality"))) :
    def nativefields(self):
        return (self.mapping.native_field,) if type(self.mapping) is ForeignKeyMapping \
                                           else tuple(_.native_field for _ in self.mapping)
    def foreigntonativemapping(self):
        if type(self.mapping) is ForeignKeyMapping : # simple field fk
            return {self.mapping.foreign_field:self.mapping.native_field}
        else: # compound foreign key
            return {_.foreign_field:_.native_field for _ in self.mapping}
    def nativetoforeignmapping(self):
        return {v:k for k,v in self.foreigntonativemapping().items()}

ForeignKeyMapping = namedtuple("FKMapping", ("native_field", "foreign_field"))

# likely replace this with some sort of sys.platform call that makes a good guess
development_deployed_environment = False

def _integrity_solve(input_schema, dat):
    verify(pd, "pandas must be installed for this functionality to work")
    pdf = input_schema.clone(clone_factory=ticdat.PanDatFactory)
    # need to make sure the advanced row predicates copy over to pdf as well
    memo_dat_version_of_func = {}
    def dat_version_of_func(func_):  # closures and lambdas are tricky, this is needed
        if func_ not in memo_dat_version_of_func:
            memo_dat_version_of_func[func_] = lambda *args, **kwargs: func_(dat)
        return memo_dat_version_of_func[func_]
    if isinstance(input_schema, ticdat.TicDatFactory):
        for t in input_schema.all_tables:
            for predicate_name, predicate_tuple in input_schema.get_row_predicates(t).items():
                if predicate_tuple.predicate_kwargs_maker:
                    pdf.add_data_row_predicate(t, predicate_tuple.predicate, predicate_name,
                                               dat_version_of_func(predicate_tuple.predicate_kwargs_maker),
                                               predicate_tuple.predicate_failure_response)

    _id_flds = {t: (pks or dfs) for t, (pks, dfs) in input_schema.schema().items()}
    longest_id_flds =  max(len(id_fld) for id_fld in _id_flds.values())
    _fld_names = [f"Field {_ + 1}" for _ in range(longest_id_flds)]
    solution_schema = ticdat.PanDatFactory(
        duplicate_rows=[["Table Name"] + _fld_names, []],
        data_type_failures=[["Table Name", "Field Name"] + _fld_names, []],
        data_row_failures=[["Table Name", "Predicate Name", "Error Message"] + _fld_names, []],
        foreign_key_failures =[["Native Table", "Foreign Table", "Mapping"] + _fld_names, []])

    if isinstance(input_schema, ticdat.PanDatFactory):
        pan_dat = dat
    else:
        pan_dat = input_schema.copy_to_pandas(dat, reset_index=True)

    def add_error_row(default_dict_object, table_, row_):
        for f, c in zip(_fld_names, row_[:len(_id_flds[table_])]):
            default_dict_object[f].append(c)
        for i in range(len(_id_flds[table_]), longest_id_flds):
            default_dict_object[_fld_names[i]].append(None)

    dups = pdf.find_duplicates(pan_dat)
    duplicate_rows = defaultdict(list)
    for table, dup_df in dups.items():
        for row in dup_df.itertuples(index=False):
            duplicate_rows["Table Name"].append(table)
            add_error_row(duplicate_rows, table, row)

    dt_fails = pdf.find_data_type_failures(pan_dat)
    data_type_failures = defaultdict(list)
    for (table, field), dt_fail_df in dt_fails.items():
        for row in dt_fail_df.itertuples(index=False):
            data_type_failures["Table Name"].append(table)
            data_type_failures["Field Name"].append(field)
            add_error_row(data_type_failures, table, row)

    fk_fails = pdf.find_foreign_key_failures(pan_dat, verbosity="Low")
    foreign_key_failures = defaultdict(list)
    for (native_table, foreign_table, mapping), fk_fail_df in fk_fails.items():
        for row in fk_fail_df.itertuples(index=False):
            foreign_key_failures["Native Table"].append(native_table)
            foreign_key_failures["Foreign Table"].append(foreign_table)
            foreign_key_failures["Mapping"].append(str(mapping))
            add_error_row(foreign_key_failures, native_table, row)

    dr_fails = pdf.find_data_row_failures(pan_dat)
    data_row_failures = defaultdict(list)
    for (table, predicate), bad_rows in dr_fails.items():
        if hasattr(bad_rows, "primary_key") and hasattr(bad_rows, "error_message"):
            data_row_failures["Table Name"].append(table)
            data_row_failures["Predicate Name"].append(predicate)
            data_row_failures["Error Message"].append(bad_rows.error_message)
            for f in _fld_names:
                data_row_failures[f].append(None)
        else:
            for row in bad_rows.itertuples(index=False):
                data_row_failures["Table Name"].append(table)
                data_row_failures["Predicate Name"].append(predicate)
                error_message = None
                if len(row) > len(input_schema.primary_key_fields[table] + input_schema.data_fields[table]):
                    error_message = row[-1]
                data_row_failures["Error Message"].append(error_message)
                add_error_row(data_row_failures, table, row)

    return (solution_schema,
            solution_schema.PanDat(duplicate_rows=duplicate_rows, data_type_failures=data_type_failures,
                                      foreign_key_failures=foreign_key_failures, data_row_failures=data_row_failures))

def standard_main(input_schema, solution_schema, solve, case_space_table_names=False):
    """
     provides standardized command line functionality for a ticdat solve engine

    :param input_schema: a TicDatFactory or PanDatFactory defining the input schema

    :param solution_schema: a TicDatFactory or PanDatFactory defining the output schema

    :param solve: a function that takes a input_schema.TicDat object and
                  returns a solution_schema.TicDat object

    :param case_space_table_names - passed through to any TicDatFactory/PanDatFactory write functions that have
                                    case_space_table_names as an argument. Will also pass through to
                                    case_space_sheet_names for Excel writers.
                                    boolean - make best guesses how to add spaces and upper case characters to
                                    table names when writing to the file system.

    :return: None

    Implements a command line signature of

    "python engine_file.py --input <input_file_or_dir> --output <output_file_or_dir> --foresta <Foresta config file>
                           --errors <error_file_or_dir>"

    For the input/output command line arguments.

    --> endings in ".xls" or ".xlsx" imply reading/writing Excel files

    --> endings in ".mdb" or ".accdb" imply reading/writing Access files (TicDatFactory only)

    --> ending in ".db" imply reading/writing SQLite database files

    --> ending in ".sql" imply reading/writing SQLite text files rendered in
        schema-less SQL statements (TicDatFactory only)

    --> ending in ".json" imply reading/writing .json files

    --> otherwise, the assumption is that an input/output directory is being specified,
        which will be used for reading/writing .csv files.
        (Recall that .csv format is implemented as one-csv-file-per-table, so an entire
        model will be stored in a directory containing a series of .csv files)

    Defaults are input.xlsx, output.xlsx

    The Foresta config file is optional. See ticdat wiki for a description of the Foresta config file.
    """
    verify(all(isinstance(_, ticdat.TicDatFactory) for _ in (input_schema, solution_schema)) or
           all(isinstance(_, ticdat.PanDatFactory) for _ in (input_schema, solution_schema)),
               "input_schema and solution_schema both need to be TicDatFactory (or PanDatFactory) objects")
    verify(callable(solve), "solve needs to be a function")

    _args = inspect.getfullargspec(solve).args
    verify(_args, "solve needs at least one argument")
    create_routine = "create_pan_dat"
    if all(isinstance(_, ticdat.TicDatFactory) for _ in (input_schema, solution_schema)):
        create_routine = "create_tic_dat"
    file_name = sys.argv[0]
    def usage():
        print (f"python {file_name} --help --input <input file or dir> --output <output file or dir> " +
               "--foresta <Foresta config file> --errors <errors file or dir>")
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:f:e:", ["help", "input=", "output=", "foresta=", "errors="])
    except getopt.GetoptError as err:
        print (str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    input_file, output_file, foresta_file, error_file = "input.xlsx", "output.xlsx", None, None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--input"):
            input_file = a
        elif o in ("-o", "--output"):
            output_file = a
        elif o in ("-f", "--foresta"):
            foresta_file = a
        elif o in ("-e", "--errors"):
            error_file = a
        else:
            verify(False, "unhandled option")

    if foresta_file and error_file:
        print("The -f and -e command line arguments are incompatible. Use one, or the other, or neither, but not both.")

    recognized_extensions = (".json", ".xls", ".xlsx", ".db")
    if create_routine == "create_tic_dat":
        recognized_extensions += (".sql", ".mdb", ".accdb")

    foresta_based_solve = None
    if foresta_file:
        verify(dspotconnect, "dspotconnect not installed")
        if not os.path.isfile(foresta_file):
            print("%s is not a valid Foresta file"%foresta_file)
            return
        with open(foresta_file, "r") as f:
            foresta_dict = json.load(f)
        verify(isinstance(foresta_dict, dict) and set(foresta_dict).issuperset(["app_id", "server", "token"]),
               f"{foresta_file} failed to resolve to a proper json dict")
        valid_modes = ["upload and solve", "upload only", "download from scenario"]
        foresta_mode = foresta_dict.get("mode", valid_modes[0])
        verify(foresta_mode in valid_modes, f"mode entry from {foresta_file} needs to be one of {valid_modes}")
        con = dspotconnect.AppConnect(*(foresta_dict[_] for _ in ["server", "token", "app_id"]))
        print(f"Connection established with Foresta app {con.app_name} on server {foresta_dict['server']}")
        EngineProxy = namedtuple("EngineProxy", ["input_schema", "solution_schema", "solve"])
        dummy_engine = EngineProxy(input_schema, solution_schema, solve)
        engine_on_foresta = dspotconnect.TicDatConnector(con, dummy_engine)
        def foresta_upload(dat):
            upload_kwargs = {"scenario_name": foresta_dict["scenario"]} if "scenario" in foresta_dict else {}
            new_scenario_id = engine_on_foresta.upload_input_dat(dat, **upload_kwargs)
            print(f"Loaded data into Foresta scenario {con.current_scenarios()[new_scenario_id]}")
            return new_scenario_id
        if foresta_mode == "upload and solve":
            def foresta_based_solve(dat):
                new_scenario_id = foresta_upload(dat)
                con.launch_solve(new_scenario_id)
                while con.is_solving_underway(new_scenario_id):
                    time.sleep(1)
                    print("Solving on Foresta...")
                print(f"Downloading solution from Foresta scenario {con.current_scenarios()[new_scenario_id]}")
                return engine_on_foresta.download_solution(new_scenario_id)
        elif foresta_mode == "upload only":
            foresta_based_solve = lambda dat: foresta_upload(dat) and None
            output_file = None
        else:
            def foresta_based_solve():
                verify("scenario" in foresta_dict, "'download from scenario' mode requires a 'scenario' entry")
                print(f"Downloading solution from Foresta scenario {foresta_dict['scenario']}")
                return engine_on_foresta.download_solution(foresta_dict["scenario"])
            input_file = None

    file_or_dir = lambda f: "file" if any(f.endswith(_) for _ in recognized_extensions) else "directory"
    dat = None
    if input_file:
        if not (os.path.exists(input_file)):
            print("%s is not a valid input file or directory" % input_file)
            return
        print("input data from %s %s"%(file_or_dir(input_file), input_file))
        dat = _get_dat_object(tdf=input_schema, create_routine=create_routine, file_path=input_file,
                              file_or_directory=file_or_dir(input_file),
                              check_for_dups=create_routine == "create_tic_dat")

    write_sln_func, write_sln_kwargs = (None, None)
    if output_file:
        print("output %s %s"%(file_or_dir(output_file), output_file))
        write_sln_func, write_sln_kwargs = _get_write_function_and_kwargs(tdf=solution_schema, file_path=output_file,
                                                                  file_or_directory=file_or_dir(output_file),
                                                                  case_space_table_names=case_space_table_names)

    if error_file:
        print("checking for data integrity errors")
        err_pdf, err_sln = _integrity_solve(input_schema, dat)
        write_err_func, write_err_kwargs = _get_write_function_and_kwargs(tdf=err_pdf,
            file_path=error_file, file_or_directory=file_or_dir(error_file),
            case_space_table_names=case_space_table_names)
        err_cnt = sum(len(getattr(err_sln, t)) for t in err_pdf.all_tables)
        print(f"{err_cnt} data integrity error{'s' if err_cnt != 1 else ''} found")
        print("%s integrity errors %s %s" % ("Overwriting" if os.path.exists(error_file) else "Creating",
                                   file_or_dir(error_file), error_file))
        write_err_func(err_sln, error_file, **write_err_kwargs)
        if err_cnt == 0:
            print("will run solve next")
        else:
            return


    if foresta_based_solve is None:
        sln = solve(dat)
    elif output_file:
        sln = foresta_based_solve(*([dat] if input_file else []))
    else:
        foresta_based_solve(dat)
        print(f"Returning without solving, as requested by the 'upload only' mode from {foresta_file}")
        return

    verify(not (sln is not None and safe_apply(bool)(sln) is None),
           "The solve (or action) function should return either a TicDat/PanDat object (for success), " +
           "or something falsey (to indicate failure)")
    if sln:
        print("%s output %s %s"%("Overwriting" if os.path.exists(output_file) else "Creating",
                                 file_or_dir(output_file), output_file))
        write_sln_func(sln, output_file, **write_sln_kwargs)
    else:
        print("No solution was created!")

def _get_dat_object(tdf, create_routine, file_path, file_or_directory, check_for_dups):
    def inner_f():
        if os.path.isfile(file_path) and file_or_directory == "file":
            if file_path.endswith(".json"):
                assert not (check_for_dups and tdf.json.find_duplicates(file_path)), "duplicate rows found"
                return getattr(tdf.json, create_routine)(file_path)
            if file_path.endswith(".xls") or file_path.endswith(".xlsx"):
                assert not (check_for_dups and tdf.xls.find_duplicates(file_path)), "duplicate rows found"
                return getattr(tdf.xls, create_routine)(file_path)
            if file_path.endswith(".db"):
                assert not (check_for_dups and tdf.sql.find_duplicates(file_path)), "duplicate rows found"
                return getattr(tdf.sql, create_routine)(file_path)
            if file_path.endswith(".sql"):
                # no way to check a .sql file for duplications
                return tdf.sql.create_tic_dat_from_sql(file_path) # only TicDat objects handle .sql files
            if file_path.endswith(".mdb") or file_path.endswith(".accdb"):
                assert not (check_for_dups and tdf.mdb.find_duplicates(file_path)), "duplicate rows found"
                return tdf.mdb.create_tic_dat(file_path)
        elif os.path.isdir(file_path) and file_or_directory == "directory":
            assert not (check_for_dups and tdf.csv.find_duplicates(file_path)), "duplicate rows found"
            return getattr(tdf.csv, create_routine)(file_path)
    dat = inner_f()
    verify(dat, f"Failed to read from and/or recognize {file_path}{_extra_input_file_check_str(file_path)}")
    return dat

def _get_write_function_and_kwargs(tdf, file_path, file_or_directory, case_space_table_names):
    write_func = None
    if file_or_directory == "file":
        if file_path.endswith(".json"):
            write_func = tdf.json.write_file
        if file_path.endswith(".xls") or file_path.endswith(".xlsx"):
            write_func = tdf.xls.write_file
        if file_path.endswith(".db"):
            write_func = getattr(tdf.sql, "write_db_data", getattr(tdf.sql, "write_file", None))
        if file_path.endswith(".sql"):
            write_func = tdf.sql.write_sql_file
        if file_path.endswith(".mdb") or file_path.endswith(".accdb"):
            write_func = tdf.mdb.write_file
    else:
        write_func = tdf.csv.write_directory
    verify(write_func, f"Unable to resolve write function for {file_path}")
    kwargs = {"case_space_table_names": case_space_table_names, "case_space_sheet_names": case_space_table_names,
              "allow_overwrite": True}
    kwargs = {k: v for k, v in kwargs.items() if k in inspect.getfullargspec(write_func).args}
    return write_func, kwargs

def _extra_input_file_check_str(input_file):
    if os.path.isfile(input_file) and input_file.endswith(".csv"):
        return "\nTo load data from .csv files, pass the directory containing the .csv files as the " +\
               "command line argument."
    return ""

def verify(b, msg) :
    """
    raise a TicDatError exception if the boolean condition is False

    :param b: boolean condition.

    :param msg: string argument to the TicDatError construction

    :return:
    """
    if not b :
        raise TicDatError(msg)

try:
    import gurobipy as gu
    verify(set(gu.tuplelist(((1,2), (2,3),(3,2))).select("*", 2))
               == {(1, 2), (3, 2)}, "")
except:
    gu = None

# Our experience was that for a production license the following needed to be truthy, but when running unit tests
# with a development license, it needed to be disabled. See test_kehaar for example.
gurobi_env_explicit_creation_enabled = True

def gurobi_env(*args, **kwargs):
    """
    Return an object that can be passed to gurobipy.Model() as the env argument.
    On an ordinary Python installation, just returns None
    Useful for Gurobi licensing/DRM issues.

    :return: An object that can be passed to gurobipy.Model as the env argument
    """
    verify(gu, "gurobipy is not installed")
    if drm:
        return drm.gurobi_env()
    if gurobi_env_explicit_creation_enabled:
        return gu.Env()

try:
    import docplex.mp.progress as cplexprogress
except:
    cplexprogress = None

def ampl_format(mod_str, **kwargs):
    """
    Return a formatted version of mod_str, using substitutions from kwargs.
    The substitutions are identified by doubled-braces ('{{' and '}}').
    Very similar to str.format, except single braces are left unmolested and double-braces
    are used to identify substitutions. This allows AMPL mod code to be more readable
    to AMPL developers.

    :param mod_str: the string that has doubled-braced substitutions entries.

    :param kwargs: Named arguments map from substitution-entry label to value.

    :return: A copy of mod_str with the substitutions performed.
    """
    verify(stringish(mod_str), "mod_str argument should be a string")
    left, right = ["_ticdat_ampl_format_%s_"%_ for _ in ["[", "]"]]
    for _ in [left, right]:
        verify(_ not in mod_str, "The %s string cannot be a sub-string of mod_str"%_)
    rtn = mod_str.replace("{{", left).replace("}}", right)
    rtn = rtn.replace("{", "{{").replace("}", "}}")
    rtn = rtn.replace(left, "{").replace(right, "}")
    return rtn.format(**kwargs)

def dict_overlay(d1, d2):
    rtn = dict(d1)
    for k,v in d2.items():
        rtn[k] = v
    return rtn

def create_duplicate_focused_tdf(tdf):
    primary_key_fields = {k:v for k,v in tdf.primary_key_fields.items() if v}
    if primary_key_fields:
        return ticdat.TicDatFactory(**{k:[[],v] for k,v in primary_key_fields.items()})

def find_duplicates(td, tdf_for_dups):
    assert tdf_for_dups.good_tic_dat_object(td)
    assert not any(tdf_for_dups.primary_key_fields.values())
    assert not tdf_for_dups.generator_tables
    rtn = {t:defaultdict(int) for t in tdf_for_dups.primary_key_fields}
    for t,flds in list(tdf_for_dups.data_fields.items()):
        tbl = getattr(td, t)
        for row in tbl:
            k = tuple(row[f] for f in flds)
            k = k[0] if len(k)==1 else k
            rtn[t][k] += 1
        rtn[t] = {k:v for k,v in rtn[t].items() if v > 1}
        if not rtn[t]:
            del(rtn[t])
    return rtn

def find_duplicates_from_dict_ticdat(tdf, dict_ticdat):
     assert isinstance(tdf, ticdat.TicDatFactory)
     assert dictish(dict_ticdat) and all(map(stringish, dict_ticdat)) and \
            all(map(containerish, dict_ticdat.values()))
     primary_key_fields = {k:v for k,v in tdf.primary_key_fields.items() if v}
     if primary_key_fields:
         old_schema = {k:v for k,v in tdf.schema().items() if k in primary_key_fields}
         all_data_tdf = ticdat.TicDatFactory(**{t:[[], pks+dfs]
                                                for t,(pks,dfs) in old_schema.items()})
         td = all_data_tdf.TicDat(**{k:v for k,v in dict_ticdat.items()
                                     if k in primary_key_fields})
         rtn = {t:defaultdict(int) for t in primary_key_fields}
         for t,flds in list(primary_key_fields.items()):
             tbl = getattr(td, t)
             for row in tbl:
                 k = tuple(row[f] for f in flds)
                 k = k[0] if len(k)==1 else k
                 rtn[t][k] += 1
             rtn[t] = {k:v for k,v in rtn[t].items() if v > 1}
             if not rtn[t]:
                 del(rtn[t])
         return rtn

def find_case_space_duplicates(tdf):
    """
    Finds fields that are case space duplicates
    :param tdf: A TicDatFactory defining the schema
    :return: A dictionary with the keys being tables that have case space duplicates
    """
    schema = tdf.schema()
    tables_with_case_insensitive_dups = {}
    for table in schema:
        fields = set(schema[table][0]).union(schema[table][1])
        case_insensitive_fields = set(map(lambda k: k.lower().replace(" ", "_"), fields))
        if len(fields) != len(case_insensitive_fields):
            tables_with_case_insensitive_dups[table] = fields
    return tables_with_case_insensitive_dups

def case_space_to_pretty(str_):
    if not str_:
        return str_
    str_ = list(str_[0].upper() + str_[1:])
    for i in range(len(str_)):
        if str_[i] == "_":
            str_[i] = " "
            if i + 1 < len(str_):
                str_[i + 1] = str_[i + 1].upper()
    return "".join(str_)

def change_fields_with_reserved_keywords(tdf, reserved_keywords, undo=False):
    tdf_schema = tdf.schema()
    mapping = {}
    for table, fields in tdf_schema.items():
        for fields_list in [fields[0], fields[1]]:
            for findex in range(len(fields_list)):
                original_field = fields_list[findex]
                if not undo:
                    verify(not fields_list[findex].startswith('_'),
                           ("Field names cannot start with '_', in table %s : " +
                            "field is %s") % (table, fields_list[findex]))
                    if fields_list[findex].lower() in reserved_keywords:
                        fields_list[findex] = '_' + fields_list[findex]
                else:
                    if fields_list[findex].startswith('_'):
                        fields_list[findex] = fields_list[findex][1:]
                mapping[table,original_field] = fields_list[findex]
    rtn = ticdat.TicDatFactory(**tdf_schema)
    for (table, original_field),new_field in mapping.items():
        if original_field in tdf.default_values.get(table, ()):
            rtn.set_default_value(table, new_field,
                                  tdf.default_values[table][original_field])
        if original_field in tdf.data_types.get(table, ()):
            rtn.set_data_type(table, new_field,
                              *(tdf.data_types[table][original_field]))
    if hasattr(tdf,'opl_prepend'):
        rtn.opl_prepend = tdf.opl_prepend
    if hasattr(tdf,'ampl_prepend'):
        rtn.ampl_prepend = tdf.ampl_prepend
    return rtn

def create_generic_free(td, tdf):
    assert tdf.good_tic_dat_object(td)
    if not tdf.generic_tables:
        return td, tdf
    sch = {k:v for k,v in tdf.schema().items() if k not in tdf.generic_tables}
    for t in tdf.generic_tables:
        if len(getattr(td, t)):
            sch[t] = [[],list(getattr(td, t).columns)]
    rtn_tdf = ticdat.TicDatFactory(**sch)
    return rtn_tdf.TicDat(**{t:getattr(td, t) for t in rtn_tdf.all_tables}), rtn_tdf

class Slicer(object):
    """
    Object to perform multi-index slicing over an index sequence
    """
    def __init__(self, iter_of_iters):
        """
        Construct a multi-index Slicer object
        :param iter_of_iters An iterable of iterables. Usually a list of lists, or a list
        of tuples. Each inner iterable must be the same size. The "*" string has a special
        flag meaning and cannot be a member of any of the inner iterables.
        Slicer is fairly similar to gurobipy.tuplelist, and will try to use tuplelist for improved performance
        whenever possible. One key difference is Slicer can accommodate tuples that themselves contain tuples (or
        really any hashable) wherease tuplelist should only be used with tuples that themselves contain only primitives.
        """
        verify(hasattr(iter_of_iters, "__iter__"), "need an iterator of iterators")
        copied = tuple(iter_of_iters)
        verify(all(hasattr(_, "__iter__") for _ in copied), "need iterator of iterators")
        self._indicies = tuple(map(tuple, copied))
        if self._indicies:
            verify(min(map(len, self._indicies)) == max(map(len, self._indicies)),
                   "each inner iterator needs to have the same number of elements")
            verify(not any("*" in _ for _ in self._indicies),
                   "The '*' character cannot itself be used as an index")
        self._gu = None
        if gu and not any(any(map(containerish, _)) for _ in self._indicies):
            self._gu = gu.tuplelist(self._indicies)
            self._indicies = None
        self.clear()

    def slice(self, *args):
        """
        Perform a multi-index slice. (Not to be confused with the native Python slice)
        :param *args a series of index values or '*'. The latter means 'match every value'
        :return: a list of tuples which match  args.
        :caveat will run faster if gurobipy is available and tuplelist can accommodate the interior iterables
        """
        if not (self._indicies or self._gu):
            return []
        verify(len(args) == len((self._indicies or self._gu)[0]), "inconsistent number of elements")
        if self._gu:
            return self._gu.select(*args)
        wildcards = tuple(i for i,x in enumerate(args) if x == "*")
        fixedposns = tuple(i for i in range(len(args)) if i not in wildcards)
        def fa(t):
            return tuple(t[i] for i in fixedposns)
        if wildcards not in self._archived_slicings:
            for indx in self._indicies:
                self._archived_slicings[wildcards][fa(indx)].append(indx)
        return list(self._archived_slicings[wildcards][fa(args)])
    def clear(self):
        """
        reduce memory overheard by clearing out any archived slicing.
        this is a no-op if gurobipy is available
        :return:
        """
        self._archived_slicings = defaultdict(lambda : defaultdict(list))
    def _forceguout(self):
        if self._gu:
            self._indicies = tuple(map(tuple, self._gu))
            self._gu = None

def do_it(g): # just walks through everything in a gen - I like the syntax this enables
    for x in g :
        pass

def all_underscore_replacements(s):
    rtn = []
    underscore_positions = [i for i,c in enumerate(s) if c == "_"]
    for indexsets in chain.from_iterable(
            combinations(list(underscore_positions), r)
            for r in range(len(list(underscore_positions))+1)):
        s_ = str(s)
        for i in indexsets:
            s_ = s_[:i] + " " + s_[i+1:]
        rtn.append(s_)
    return rtn

def all_subsets(my_set):
    return [set(subset) for l in range(len(my_set)+1) for subset in combinations(my_set, l)]

class TicDatError(Exception) :
    pass

def debug_break():
    import ipdb; ipdb.set_trace()

def safe_apply(f) :
    def _rtn (*args, **kwargs) :
        try :
            return f(*args, **kwargs)
        except :
            return None
    return _rtn

def dictish(x):
    '''
    !WATCH OUT! a pandas.DataFrame qualifies as dictish. Probably a dumb subroutine.
    '''
    return all(hasattr(x, _) for _ in
                           ("__getitem__", "keys", "values", "items", "__contains__", "__len__"))
def stringish(x): return all(hasattr(x, _) for _ in ("lower", "upper", "strip"))
def containerish(x): return all(hasattr(x, _) for _ in ("__iter__", "__len__", "__contains__")) \
                                and not stringish(x)
def generatorish(x): return all(hasattr(x, _) for _ in ("__iter__", "next")) \
                            and not (containerish(x) or dictish(x))
def numericish(x) : return isinstance(x, Number) and not isinstance(x, bool)
def lupish(x) : return containerish(x) and hasattr(x, "__getitem__") and not dictish(x)

def baseConverter(number, base):
    if number < base:
        return [number]
    rtn = []
    power = base
    while power * base <= number:
        power *= base
    while power >= base :
        rtn.append(number / power)
        number -= power * (number/power)
        power /= base
    rtn.append(number%base)
    return rtn

def freezable_factory(baseClass, freezeAttr, alwaysEditable = None) :
    alwaysEditable = alwaysEditable or set()
    class _Freezeable(baseClass) :
        def __setattr__(self, key, value):
            if key in alwaysEditable or not getattr(self, freezeAttr, False):
                return super(_Freezeable, self).__setattr__(key, value)
            raise TicDatError("can't set attributes to a frozen " + self.__class__.__name__)
        def __delattr__(self, item):
            if not getattr(self, freezeAttr, False):
                return super(_Freezeable, self).__delattr__(item)
            raise TicDatError("can't del attributes to a frozen " + self.__class__.__name__)

    return _Freezeable


_FreezableDictBase = freezable_factory(dict, "_attributesFrozen")
class FreezeableDict(_FreezableDictBase) :
    def __setattr__(self, key, value):
        if key == "_dataFrozen" and value :
            return super(_FreezableDictBase, self).__setattr__(key, value)
        return super(FreezeableDict, self).__setattr__(key, value)
    def __setitem__(self, key, value):
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).__setitem__(key, value)
        raise TicDatError("Can't edit a frozen " + self.__class__.__name__)
    def __delitem__(self, key):
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).__delitem__(key)
        raise TicDatError("Can't edit a frozen " + self.__class__.__name__)
    def update(self, *args, **kwargs) :
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).update(*args, **kwargs)
        raise TicDatError("Can't edit a frozen " + self.__class__.__name__)
    def pop(self, *args, **kwargs) :
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).pop(*args, **kwargs)
        raise TicDatError("Can't edit a frozen " + self.__class__.__name__)

class FrozenDict(FreezeableDict) :
    def __init__(self, *args, **kwargs):
        super(FrozenDict, self).__init__(*args, **kwargs)
        self._dataFrozen = True # need to do first, obviously
        self._attributesFrozen  = True

def deep_freeze(x) :
    if stringish(x) or not hasattr(x, "__contains__") :
        return x
    if hasattr(x, "keys") and hasattr(x, "values") :
        return FrozenDict({deep_freeze(k) : deep_freeze(v) for k,v in x.items()})
    if hasattr(x, "__getitem__") :
        return tuple(map(deep_freeze, x))
    return frozenset(map(deep_freeze,x))

def deep_copy(x):
    '''
    useful utility function for copying the the sort of nested dictionary and list objects that I end up using with
    schema manipulations. Can also function as an unfreeze.
    :param x: the object to deep copy. should be nested dicts, tuples, lists or TicDatFactory/PanDatFactory
    :return: a deep (and unfrozen, other than tuples) copy of x
    '''
    if isinstance(x, (str, bool)) or ticdat.utils.numericish(x) or x is None:
        return x
    if isinstance(x, tuple):
        return tuple(deep_copy(y) for y in x)
    if isinstance(x, frozenset):
        return frozenset({deep_copy(y) for y in x})
    if isinstance(x, set):
        return {deep_copy(y) for y in x}
    if isinstance(x, list):
        return [deep_copy(y) for y in x]
    if pd and isinstance(x, pd.DataFrame):
       return x.copy(deep=True)
    if isinstance(x, dict) or dictish(x):
        return {deep_copy(k): deep_copy(v) for k, v in x.items()}
    if isinstance(x, (ticdat.TicDatFactory, ticdat.PanDatFactory)):
        return x.clone()
    if callable(x):
        return x
    verify(False, f"Unexpected object {x}")

def td_row_factory(table, key_field_names, data_field_names, default_values=None):
    default_values = default_values or {}
    assert dictish(default_values) and set(default_values).issubset(set(key_field_names).union(data_field_names))
    assert not set(key_field_names).intersection(data_field_names)
    if not data_field_names:
         # need a freezeable dict not a frozen dict here so can still link foreign keys
        def makefreezeabledict(x=()) :
            verify(containerish(x) and len(x) == 0, "Attempting to add non-empty data to %s"%table)
            return FreezeableDict()
        return makefreezeabledict
    fieldtoindex = {x:data_field_names.index(x) for x in data_field_names}
    indextofield = {v:k for k,v in fieldtoindex.items()}
    class TicDatDataRow(freezable_factory(object, "_attributesFrozen")) :
        def __init__(self, x):
            # since ticDat targeting numerical analysis, 0 is good default default
            self._data = [0] * len(fieldtoindex)
            if dictish(x) :
                verify(set(x.keys()).issubset(fieldtoindex),
                       "Applying inappropriate data field names to %s"%table)
                for f,i in fieldtoindex.items():
                    if f in default_values :
                        self._data[i] = default_values[f]
                for f,_d in x.items():
                    self[f] = _d
            elif containerish(x) :
                verify(len(x) == len(self), "%s requires each row to have %s data values"%
                       (table, len(self)))
                for i in range(len(self)):
                    self._data[i] = x[i]
            else:
                verify(len(self) ==1, "%s requires each row to have %s data values"%
                       (table, len(self)))
                self._data[0] = x
        def __getitem__(self, item):
            try :
                return self._data[fieldtoindex[item]]
            except :
                raise TicDatError("Key error : %s not data field name for table %s"% (item, table))
        def __setitem__(self, key, value):
            verify(key in fieldtoindex, "Key error : %s not data field name for table %s"%
                   (key, table))
            if getattr(self, "_dataFrozen", False) :
                raise TicDatError("Can't edit a frozen TicDatDataRow")
            self._data[fieldtoindex[key]] = value
        def keys(self):
            return tuple(indextofield[i] for i in range(len(self)))
        def values(self):
            return tuple(self._data)
        def items(self):
            return zip(self.keys(), self.values())
        def __contains__(self, item):
            return item in fieldtoindex
        def __iter__(self):
            return iter(fieldtoindex)
        def __len__(self):
            return len(self._data)
        def __repr__(self):
            return "_td:" + {k:v for k,v in self.items()}.__repr__()
    assert dictish(TicDatDataRow)
    return TicDatDataRow


class Sloc(object):
    """
    A utility class for the slicing on pandas Series.
    Works just like .loc, except doesn't exception out when
    encountering an empty slice.
    **All** credit for this class goes to the inimitable IL.
    https://github.com/pydata/pandas/issues/10695
    """
    def __init__(self, s):
        """
        In general there is no need to create this object explicitly.
        TicDatFactory.copy_to_pandas can create them for each of your
        data columns, or you can use the add_sloc utility function.
        :param s: a Series object.
        :return:
        """
        verify(pd, "pandas needs to be installed in order to enable pandas functionality")
        # as of this writing, the DataFrame doesn't handle references like df[:,"item"] correctly
        verify(isinstance(s, pd.Series), "sloc only implemented for Series")
        self._s = s
    def __getitem__(self, key):
        try:
            return self._s.loc[key]
        except Exception as e:
            if containerish(key) and any(isinstance(k, slice) and
                                         (k.start == k.step == k.stop == None) for k in key):
                return pd.Series([], dtype=numpy.float64)
            raise e
    @staticmethod
    def add_sloc(s):
        """
        adds an .sloc attribute to a the series or to every column of the data frame
        :param s: either a series or a data frame
        :return: s if .sloc could be added, None otherwise
        """
        verify(pd, "pandas needs to be installed in order to enable pandas functionality")
        if isinstance(s.index, pd.MultiIndex) :
        # sloc functionality really makes sense only for a MultiIndex
            if isinstance(s, pd.DataFrame):
            # adding sloc just to the columns of the DataFrame and not to the DataFrame itself.
                for c in s.columns:
                    Sloc.add_sloc(s[c])
            else:
                s.sloc = Sloc(s)
            return s

class LogFile(object) :
    """
    Utility class for writing log files.
    Also enables writing on-the-fly tables into log files.
    """
    def __init__(self, path):
        self._f = open(path, "w") if path else None
    def write(self, *args, **kwargs):
        self._f.write(*args, **kwargs) if self._f else None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    def close(self):
        self._f.close()if self._f else None
    def log_table(self, table_name, seq, formatter = lambda _ : "%s"%_,
                  max_write = 10) :
        """
        Writes a table to the log file. Extremely useful functionality for
        on the fly errors, warnings and diagnostics.
        :param log_table : the name to be given to the logged table
        :param seq: An iterable of iterables. The first iterable
                    lists the field names for the table. The remaining iterables
                    list the column values for each row. The outer iterable
                    is thus of length num_rows + 1, while each of the inner
                    iterables are of length num_cols.
        :param formatter: a function used to turn column entries into strings
        :param max_write: the maximum number of table entries to write
                          to the actual log file.
        :return:
        """
        verify(containerish(seq) and all(map(containerish, seq)),
               "seq needs to be container of containers")
        verify(len(seq) >= 1, "seq missing initial header row")
        verify(max(map(len, seq)) == min(map(len, seq)),
               "each row of seq needs to be the same length as the header row")
        self.write("Table %s:\n"%table_name)
        if len(seq[0]) <= 2:
            ljust = 30
        elif len(seq[0]) == 3:
            ljust = 25
        elif len(seq[0]) == 4:
            ljust = 20
        else:
            ljust = 18
        if len(seq) - 1 > max_write:
          self.write("(Showing first %s entries out of %s in total)\n"
                     %(max_write, len(seq)-1))
        for row in list(seq)[:max_write+1]:
            self.write("".join(formatter(_).ljust(ljust) for _ in row) + "\n")
        self.write("\n")

class Progress(object):
    """
    Utility class for indicating progress.
    """
    def __init__(self, quiet = False):
        self._quiet = quiet
    def numerical_progress(self, theme, progress):
        """
        indicate generic progress
        :param theme: string describing the type of progress being advanced
        :param progress: numerical indicator to the degree of progress advanced
        :return: False if GUI indicates solve should gracefully finish, True otherwise
        """
        verify(stringish(theme), "type_ needs to be string")
        verify(numericish(progress), "progress needs to be numerical")
        if not self._quiet:
             print("%s:%s"%(theme.ljust(40), "{:.5f}".format(progress)))
        return True
    def mip_progress(self, theme, lower_bound, upper_bound):
        """
        indicate progress towards solving a MIP via converging upper and lower bounds
        :param theme: string describing the type of MIP solve underway
        :param lower_bound: the best current lower bound to the MIP objective
        :param upper_bound: the best current upper bound to the MIP objective
        :return: False if GUI indicates solve should gracefully finish, True otherwise
        """
        verify(stringish(theme), "type_ needs to be string")
        verify(all(map(numericish, (lower_bound, upper_bound))),
               "lower_bound, upper_bound need to be numeric")
        verify(lower_bound - abs(lower_bound) * .00001 <= upper_bound,
               "lower_bound can't be bigger than upper_bound")
        if not self._quiet:
             print("%s:%s:%s"%(theme.ljust(30), "{:.5f}".format(lower_bound).ljust(20),
                               "{:.5f}".format(upper_bound)))
        return True
    def gurobi_call_back_factory(self, theme, model) :
        """
        Allow a Gurobi  model to call mip_progress. **Only for minimize**
        :param theme: string describing the type of MIP solve underway
        :param model: a Gurobi model (or ticdat.Model.core_model)
        :return: a call_back function that can be passed to Model.optimize
        """
        verify(gu, "gurobipy is not installed and properly licensed")
        def rtn(gu_model, where) :
            assert gu_model is model
            if where == gu.GRB.callback.MIP:
                ub = model.cbGet(gu.GRB.callback.MIP_OBJBST)
                lb = model.cbGet(gu.GRB.callback.MIP_OBJBND)
                keep_going = self.mip_progress(theme, lb, ub)
                if not keep_going :
                    model.terminate()
        return rtn
    def add_cplex_listener(self, theme, model):
        '''
        Allow a CPLEX model to call mip_progress. **Only for minimize**
        :param theme: short descriptive string
        :param model: cplex.Model object (or ticdat.Model.core_model)
        :return:
        '''
        verify(cplexprogress, "docplex is not installed")
        super_self = self
        class MyListener(cplexprogress.ProgressListener):
            def notify_progress(self, progress_data):
                # this is assuming a minimization problem.
                ub = float("inf") if progress_data.current_objective is None else progress_data.current_objective
                keep_going = super_self.mip_progress(theme, progress_data.best_bound, ub)
                if not keep_going:
                    self.abort()
        model.add_progress_listener(MyListener())

EPSILON = 1e-05

def per_error(x1, x2) :
    x1 = float(x1) if numericish(x1) else x1
    x2 = float(x2) if numericish(x2) else x2
    if (x1 < 0) and (x2 < 0) :
        return per_error(-x1, -x2)
    if x1 == float("inf") :
        return 0 if (x2 == float("inf")) else x1
    SMALL_NOT_ZERO = 1e-10
    assert(EPSILON>SMALL_NOT_ZERO)
    abs1 = abs(x1)
    abs2 = abs(x2)
    # is it safe to divide by the bigger absolute value
    if max(abs1, abs2) > SMALL_NOT_ZERO:
        rtn = ((max(x1, x2) - min(x1, x2)) / max(abs1, abs2))
        return rtn
    return 0

def nearly_same(x1, x2, epsilon) :
    return per_error(x1, x2) < epsilon

RowPredicateInfo = namedtuple("RowPredicateInfo", ["predicate", "predicate_kwargs_maker",
                                                   "predicate_failure_response"])

def does_new_fk_complete_circle(native_tbl, foreign_tbl, tdf):
    fks = defaultdict(set)
    for fk in tdf.foreign_keys:
        fks[fk.native_table].add(fk)
    rtn = []
    def process_table(t, already_seen):
        if t == native_tbl:
            rtn[:] = [True]
        elif t not in already_seen:
            for fk in fks.get(t, ()):
                process_table(fk.foreign_table, already_seen + [t])
    process_table(foreign_tbl, [])
    return bool(rtn)