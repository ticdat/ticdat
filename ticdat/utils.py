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
import datetime as datetime_
try:
    import dateutil
except:
    dateutil = None

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

def dat_restricted(table_list):
    '''
    Decorator factory used to decorate action functions (or solve function) to restrict the access to the
    tables in the input_schema
    :param table_list: A list of tables that are a subset input_schema.all_tables
    :return: A decorator that can be applied to a function to fine-tune how ticdat controls its access to the
             input_schema
    Example usage
        @dat_restricted(['table_one', 'table_five'])
        def some_action(dat):
           # the action
        ticdat will pass a dat object that only has table_one and table_five as attributes. If a dat object
        is returned back for writing, any attributes other than table_one, table_five will be ignored.
    Note that the input_schema is not known by this decorator_factory, and thus table_list can't be sanity checked
    at the time the function is decorated. ticdat will sanity check the table_list when the decorated function is
    used by ticdat.standard_main (the Enframe-ticdat code will also perform an equivalent check, as will any ticdat
    supporting platform).
    As the function will be decorated with a dat_restricted attribute, a programmer is allowed to avoid the decorator
    factory and simply do the following instead.
        def some_action(dat):
           # the action
        some_action.dat_restricted = table_list
    Although  this will work the same, you are encouraged to use the dat_restricted decorator factory for better
    readability.
    dat_restricted can be used to decorate the solve function, in which case standard_main and Enframe will do the
    expected thing and pass a dat object that is restricted to table_list.
    '''
    verify(containerish(table_list) and table_list and all(isinstance(_, str) for _ in table_list),
           "table_list needs to be a non-empty container of strings")
    def dat_restricted_decorator(func): # no need to use functools.wraps since not actually wrapping.
        func.dat_restricted = tuple(table_list)
        return func
    return dat_restricted_decorator

def sln_restricted(table_list):
    '''
    Decorator factory used to decorate action functions (or solve function) to restrict the access to the
    tables in the solution_schema
    :param table_list: A list of tables that are a subset solution_schema.all_tables
    :return: A decorator that can be applied to a function to fine-tune how ticdat controls its access to the
             solution_schema
    Example usage
        @sln_restricted(['table_one', 'table_five'])
        def some_action(sln):
           # the action
        ticdat will pass a sln object that only has table_one and table_five as attributes. If {"sln":sln}
        is returned back for writing, any sln attributes other than table_one, table_five will be ignored.
    Note that the solution_schema is not known by this decorator_factory, and thus table_list can't be sanity checked
    at the time the function is decorated. ticdat will sanity check the table_list when the decorated function is
    used by ticdat.standard_main (the Enframe-ticdat code will also perform an equivalent check, as will any ticdat
    supporting platform).
    As the function will be decorated with a sln_restricted attribute, a programmer is allowed to avoid the decorator
    factory and simply do the following instead.
        def some_action(sln):
           # the action
        some_action.sln_restricted = table_list
    Although  this will work the same, you are encouraged to use the sln_restricted decorator factory for better
    readability.
    sln_restricted can be used to decorate the solve function, in which case standard_main and Enframe will do the
    expected thing and handle only the table_list attributes of the returned sln object.
    '''
    verify(containerish(table_list) and table_list and all(isinstance(_, str) for _ in table_list),
           "table_list needs to be a non-empty container of strings")
    def sln_restricted_decorator(func): # no need to use functools.wraps since not actually wrapping.
        func.sln_restricted = tuple(table_list)
        return func
    return sln_restricted_decorator

def clone_a_anchillary_info_schema(schema, table_restrictions):
    '''
    :param schema: the result of calling _.schema(include_ancillary_info=True) when _ is a
    TicDatFactory or PanDatFactory
    :param table_restrictions: None (indicating a simple clone) or a sublist of the tables in schema.
    :return: a clone of schema, except with the tables outside of table_restrictions removed (unlesss
    table_restrictions is None, in which case schema is returned).
    '''
    if table_restrictions is None:
        return schema
    verify(containerish(table_restrictions) and table_restrictions and
           all(isinstance(_, str) for _ in table_restrictions), "table_restrictions needs to be a container of strings")
    verify(dictish(schema) and set(table_restrictions).issubset(schema.get("tables_fields", [])),
           "table_restrictions needs to be a subset of schema['tables_fields']")
    rtn = {}
    for k, v in schema.items():
        if k in ["tables_fields", "default_values", "data_types"]:
            rtn[k] = {_k:_v for _k, _v in v.items() if _k in table_restrictions}
        elif k == "foreign_keys":
            rtn[k] = tuple(fk for fk in v if set(fk[:2]).issubset(table_restrictions))
        elif k == "parameters":
            rtn[k] = v if k in table_restrictions else {}
        else:
            assert k == "infinity_io_flag", f"{k} is unpexted part of schema"
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
            if rtn is not None:
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

def _clone_to_restricted_as_needed(function, schema, name):
    if not hasattr(function, name):
        return schema, set()
    restricted = getattr(function, name)
    verify(containerish(restricted) and restricted and
           all(isinstance(_, str) for _ in restricted), f"{name} needs to be a container of strings")
    verify(set(restricted).issubset(schema.all_tables), f"{restricted} needs to be a subset of {schema.all_tables}")
    if set(restricted) == set(schema.all_tables):
        return schema, set()
    return schema.clone(table_restrictions=restricted), set(restricted)
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

    "python engine_file.py --input <input_file_or_dir> --output <output_file_or_dir>"

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
    """
    # See EnframeOfflineHandler for  details for how to configure the enframe.json file
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
        print ("python %s --help --input <input file or dir> --output <output file or dir>"%file_name +
               " --enframe <enframe_config.json> --action <action_function>")
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:e:a:", ["help", "input=", "output=", "enframe=", "action="])
    except getopt.GetoptError as err:
        print (str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    input_file, output_file, enframe_config, enframe_handler, action_name = "input.xlsx", "output.xlsx", "", None, None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--input"):
            input_file = a
        elif o in ("-o", "--output"):
            output_file = a
        elif o in ("-e", "--enframe"):
            enframe_config = a
        elif o in ("-a", "--action"):
            action_name = a
        else:
            verify(False, "unhandled option")
    original_input_schema = input_schema
    if action_name:
        module = sys.modules[solve.__module__.split('.')[0]]
        verify(hasattr(module, action_name), f"{action_name} is not an attribute of the module")
        action_func = getattr(module, action_name)
        verify(callable(action_func), f"{action_name} is not callable")
        action_func_args = inspect.getfullargspec(action_func).args
        verify({"dat", "sln"}.intersection(action_func_args),
               f"{action_name} needs at least one of 'dat', 'sln' as arguments")
        input_schema, input_restrictions = _clone_to_restricted_as_needed(action_func, input_schema, "dat_restricted")
        solution_schema, solution_restrictions = _clone_to_restricted_as_needed(action_func, solution_schema,
                                                                                "sln_restricted")
    else:
        input_schema, input_restrictions = _clone_to_restricted_as_needed(solve, input_schema, "dat_restricted")
        solution_schema, solution_restrictions = _clone_to_restricted_as_needed(solve, solution_schema,
                                                                                "sln_restricted")

    if enframe_config:
        enframe_handler = make_enframe_offline_handler(enframe_config, input_schema, solution_schema,
                                                       solve if not action_name else action_func)
        if enframe_handler.solve_type.lower() ==  "Copy Input to Postgres".lower():
            input_schema, input_restrictions = original_input_schema, set()
        if "solve" in enframe_handler.solve_type.lower() and solution_restrictions:
            print("\nNote - only the following subset of tables will be written to the local Enframe DB\n" +
                  str(solution_restrictions) + "\n")
        verify(enframe_handler, "-e/--enframe command line functionality requires additional Enframe specific package")
        if enframe_handler.solve_type == "Proxy Enframe Solve":
            if action_name:
                enframe_handler.perform_action_with_function()
            else:
                enframe_handler.proxy_enframe_solve()
            print(f"Enframe proxy solve executed with {enframe_config}" +
                  (f" and action {action_name}" if action_name else ""))
            return

    recognized_extensions = (".json", ".xls", ".xlsx", ".db")
    if create_routine == "create_tic_dat":
        recognized_extensions += (".sql", ".mdb", ".accdb")
    file_or_dir = lambda f: "file" if any(f.endswith(_) for _ in recognized_extensions) else "directory"
    if not (os.path.exists(input_file)):
        print("%s is not a valid input file or directory"%input_file)
        return

    print("input data from %s %s"%(file_or_dir(input_file), input_file))
    dat = _get_dat_object(tdf=input_schema, create_routine=create_routine, file_path=input_file,
                          file_or_directory=file_or_dir(input_file),
                          check_for_dups=create_routine == "create_tic_dat")
    if enframe_handler:
        enframe_handler.copy_input_dat(dat)
        print(f"Input data copied from {input_file} to the postgres DB defined by {enframe_config}")
        if enframe_handler.solve_type == "Copy Input to Postgres and Solve":
            if action_name:
                enframe_handler.perform_action_with_function()
            else:
                enframe_handler.proxy_enframe_solve()
            print(f"Enframe proxy solve executed with {enframe_config}" +
                  (f" and action {action_name}" if action_name else ""))
        return

    print("output %s %s"%(file_or_dir(output_file), output_file))
    write_func, write_kwargs = _get_write_function_and_kwargs(tdf=solution_schema, file_path=output_file,
                                                              file_or_directory=file_or_dir(output_file),
                                                              case_space_table_names=case_space_table_names)
    if not action_name:
        sln = solve(dat)
        verify(not (sln is not None and safe_apply(bool)(sln) is None),
               "The solve (or action) function should return either a TicDat/PanDat object (for success), " +
               "or something falsey (to indicate failure)")
        if sln:
            print("%s output %s %s"%("Overwriting" if os.path.exists(output_file) else "Creating",
                                     file_or_dir(output_file), output_file))
            write_func(sln, output_file, **write_kwargs)
        else:
            print("No solution was created!")
        return
    print("solution data from %s %s"%(file_or_dir(output_file), output_file))

    kwargs = {}
    if "dat" in action_func_args:
        kwargs["dat"] = dat
    if "sln" in action_func_args:
        sln = _get_dat_object(tdf=solution_schema, create_routine=create_routine, file_path=output_file,
                              file_or_directory=file_or_dir(output_file),
                              check_for_dups=create_routine == "create_tic_dat")
        kwargs["sln"] = sln
    rtn = action_func(**kwargs)
    def quickie_good_obj(dat, tdf):
        return all(hasattr(dat, t) for t in tdf.all_tables)
    def dat_write(dat):
        w_func, w_kwargs = _get_write_function_and_kwargs(tdf=input_schema, file_path=input_file,
                                                          file_or_directory=file_or_dir(input_file),
                                                          case_space_table_names=case_space_table_names)
        print("%s input %s %s" % ("Overwriting" if os.path.exists(input_file) else "Creating",
                                   file_or_dir(input_file), input_file))
        w_func(dat, input_file, **w_kwargs)
    if rtn:
        if isinstance(rtn, dict):
            verify({"dat", "sln"}.intersection(rtn), "The returned dict is missing both 'dat' and 'sln' keys")
            if "dat" in rtn:
                verify(quickie_good_obj(rtn["dat"], input_schema), "rtn['dat'] fails sanity check")

                dat_write(rtn["dat"])
            if "sln" in rtn:
                verify(quickie_good_obj(rtn["sln"], solution_schema), "rtn['sln'] fails sanity check")
                print("%s output %s %s" % ("Overwriting" if os.path.exists(output_file) else "Creating",
                                           file_or_dir(output_file), output_file))
                write_func(rtn["sln"], output_file, **write_kwargs)
        else:
            verify(quickie_good_obj(rtn, input_schema), "rtn fails sanity check")
            dat_write(rtn)
    else:
        print(f"{action_func} failed to return anything!")

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

def make_enframe_offline_handler(enframe_config, input_schema, solution_schema, core_func):
    try:
        from framework_utils.ticdat_deployer import EnframeOfflineHandler
    except:
        try:
            from enframe_offline_handler import EnframeOfflineHandler
        except:
            EnframeOfflineHandler = None
    if EnframeOfflineHandler:
        return EnframeOfflineHandler(enframe_config, input_schema, solution_schema, core_func)

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

def dictish(x): return all(hasattr(x, _) for _ in
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


def td_row_factory(table, key_field_names, data_field_names, default_values={}):
    assert dictish(default_values) and set(default_values).issubset(data_field_names)
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
    x1 = float(x1)
    x2 = float(x2)
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