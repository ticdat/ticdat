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

try:
    import pandas as pd
    from pandas import DataFrame
except:
    pd = DataFrame =  None
import inspect

# likely replace this with some sort of sys.platform call that makes a good guess
development_deployed_environment = False

def standard_main(input_schema, solution_schema, solve):
    """
     provides standardized command line functionality for a ticdat solve engine
    :param input_schema: a TicDatFactory defining the input schema
    :param solution_schema: a TicDatFactory defining the output schema
    :param solve: a function that takes a input_schema.TicDat object and
                  returns a solution_schema.TicDat object
    :return: None
    Implements a command line signature of
    "python engine_file.py --input <input_file_or_dir> --output <output_file_or_dir>
    For the input/output command line arguments.
    --> endings in ".xls" or ".xlsx" imply reading/writing Excel files
    --> endings in ".mdb" or ".accdb" imply reading/writing Access files
    --> ending in ".db" imply reading/writing SQLite database files
    --> ending in ".sql" imply reading/writing SQLite text files rendered in
        schema-less SQL statements
    --> ending in ".json" imply reading/writing .json files
    --> otherwise, the assumption is that an input/output directory is being specified,
        which will be used for reading/writing .csv files.
        (Recall that .csv format is implemented as one-csv-file-per-table, so an entire
        model will be stored in a directory containing a series of .csv files)
    Defaults are input.xlsx, output.xlsx
    """
    verify(all(isinstance(_, ticdat.TicDatFactory) for _ in (input_schema, solution_schema)),
               "input_schema and solution_schema both need to be TicDatFactory objects")
    verify(callable(solve), "solve needs to be a function")
    _args = inspect.getargspec(solve).args
    verify(_args and len(_args) == 1, "solve needs to take just one argument")
    file_name = sys.argv[0]
    def usage():
        print ("python %s --help --input <input file or dir> --output <output file or dir>"%
               file_name)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:o:", ["help", "input=", "output="])
    except getopt.GetoptError as err:
        print (str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    input_file, output_file = "input.xlsx", "output.xlsx"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--input"):
            input_file = a
        elif o in ("-o", "--output"):
            output_file = a
        else:
            verify(False, "unhandled option")
    file_or_dir = lambda f :"file" if any(f.endswith(_) for _ in
                            (".json", ".xls", ".xlsx", ".db", ".sql", ".mdb", ".accdb")) \
                  else "directory"
    if not (os.path.exists(input_file)):
        print("%s is not a valid input file or directory"%input_file)
    else:
        print("input %s %s : output %s %s"%(file_or_dir(input_file), input_file,
                                            file_or_dir(output_file), output_file))
        dat = None
        if os.path.isfile(input_file) and file_or_dir(input_file) == "file":
            if input_file.endswith(".json"):
                assert not input_schema.json.find_duplicates(input_file), "duplicate rows found"
                dat = input_schema.json.create_tic_dat(input_file)
            if input_file.endswith(".xls") or input_file.endswith(".xlsx"):
                assert not input_schema.xls.find_duplicates(input_file), "duplicate rows found"
                dat = input_schema.xls.create_tic_dat(input_file)
            if input_file.endswith(".db"):
                assert not input_schema.sql.find_duplicates(input_file), "duplicate rows found"
                dat = input_schema.sql.create_tic_dat(input_file)
            if input_file.endswith(".sql"):
                # no way to check a .sql file for duplications
                dat = input_schema.sql.create_tic_dat_from_sql(input_file)
            if input_file.endswith(".mdb") or input_file.endswith(".accdb"):
                assert not input_schema.mdb.find_duplicates(input_file), "duplicate rows found"
                dat = input_schema.mdb.create_tic_dat(input_file)
        elif os.path.isdir(input_file) and file_or_dir(input_file) == "directory":
            assert not input_schema.csv.find_duplicates(input_file), "duplicate rows found"
            dat = input_schema.csv.create_tic_dat(input_file)
        verify(dat, "Failed to read from and/or recognize %s"%input_file)
        sln = solve(dat)
        if sln:
            print("%s output %s %s"%("Overwriting" if os.path.exists(output_file) else "Creating",
                                     file_or_dir(output_file), output_file))
            if output_file.endswith(".json"):
                solution_schema.json.write_file(sln, output_file, allow_overwrite=True)
            elif output_file.endswith(".xls") or output_file.endswith(".xlsx"):
                solution_schema.xls.write_file(sln, output_file, allow_overwrite=True)
            elif output_file.endswith(".db"):
                solution_schema.sql.write_db_data(sln, output_file, allow_overwrite=True)
            elif output_file.endswith(".sql"):
                solution_schema.sql.write_sql_file(sln, output_file, allow_overwrite=True)
            elif output_file.endswith(".mdb") or output_file.endswith(".accdb"):
                solution_schema.mdb.write_file(sln, output_file, allow_overwrite=True)
            else:
                solution_schema.csv.write_directory(sln, output_file, allow_overwrite=True)
        else:
            print("No solution was created!")

def verify(b, msg) :
    if not b :
        raise TicDatError(msg)

try:
    import gurobipy as gu
    verify(set(gu.tuplelist(((1,2), (2,3),(3,2))).select("*", 2))
               == {(1, 2), (3, 2)}, "")
except:
    gu = None

try:
    import docplex.mp.progress as cplexprogress
except:
    cplexprogress = None

def find_denormalized_sub_table_failures(table, pk_fields, data_fields):
    """
    checks to see if the table argument contains a denormalized sub-table
    indexed by pk_fields with data fields data_fields
    :param table: The table to study. Can either be a pandas DataFrame or a
                  or a container of consistent {field_name:value} dictionaries.
    :param pk_fields: The pk_fields of the sub-table. Needs to be fields
                      (but not necc primary key fields) of the table.
    :param data_fileds: The data fields of the sub-table. Needs to be fields
                        (but not necc data fields) of the table.
    :return: A dictionary indexed by the pk_fields values in the table
             that are associated with improperly denormalized table rows. The
             values of the return dictionary are themselves dictionaries indexed
             by data fields. The values of the inner dictionary are
             tuples of the different distinct values found for the data field
             at the different rows with common primary key field values.
             The inner dictionaries are pruned so that only tuples of length >1
             are included, and the return dictionary is pruned so that only
             entries with at least one non-pruned inner dictionary is included.
             Thus, a table that has a properly denormalized (pk_fields, data_fields)
             sub-table will return an empty dictionary.
    """
    pk_fields = (pk_fields,) if stringish(pk_fields) else pk_fields
    data_fields = (data_fields,) if stringish(data_fields) else data_fields
    verify(containerish(pk_fields) and all(map(stringish, pk_fields)),
           "pk_fields needs to be either a field name or a container of field names")
    verify(containerish(data_fields) and all(map(stringish, data_fields)),
           "data_fields needs to be either a field name or a container of field names")
    verify(len(set(pk_fields).union(data_fields)) == len(pk_fields) + len(data_fields),
           "there are duplicate field names amongst pk_fields, data_fields")
    if DataFrame and isinstance(table, DataFrame):
        verify(hasattr(table, "columns"), "table missing columns")
        for f in tuple(pk_fields) + tuple(data_fields):
            verify(f in table.columns, "%s isn't a column for table"%f)
        tdf = ticdat.TicDatFactory(t = [[],tuple(pk_fields)+tuple(data_fields)])
        dat = tdf.TicDat(t = table)
        return find_denormalized_sub_table_failures(dat.t, pk_fields, data_fields)
    verify(containerish(table) and all(map(dictish, table)),
           "table needs to either be a pandas.DataFrame or a container of {field_name:value} dictionaries")
    rtn = defaultdict(lambda : defaultdict(set))
    for row in table:
        for f in tuple(pk_fields) + tuple(data_fields):
            verify(f in row, "%s isn't a key for one of the inner dictionaries of table"%f)
            verify(hasattr(row[f], "__hash__"),
                   "the values for field %s all need to be hashable"%f)
        pk = row[pk_fields[0]] if len(pk_fields) == 1 else tuple(row[f] for f in pk_fields)
        for f in data_fields:
            rtn[pk][f].add(row[f])
    for k,v in list(rtn.items()):
        rtn[k] = {f:tuple(s) for f,s in v.items() if len(s) > 1}
        if not rtn[k]:
            del(rtn[k])
    return dict(rtn)

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
    if hasattr(tdf,'lingo_prepend'):
        rtn.lingo_prepend = tdf.lingo_prepend
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
        if gu:
            self._gu = gu.tuplelist(self._indicies)
            self._indicies = None
        self.clear()

    def slice(self, *args):
        """
        Perform a multi-index slice. (Not to be confused with the native Python slice)
        :param *args a series of index values or '*'. The latter means 'match every value'
        :return: a list of tuples which match  args.
        :caveat will run faster if gurobipy is available
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
                return pd.Series([])
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
    Utility class for writing log files to the Opalytics Cloud Platform.
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
                          to the actual log file. In the Opalytics Cloud Platform,
                          the log file will link to a scrollable, sortable grid
                          with all the table entries.
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
    Utility class for indicating progress to the Opalytics Cloud Platform.
    Also enables writing on-the-fly tables into log files.
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
        verify(lower_bound * 0.99999 <= upper_bound,
               "lower_bound can't be bigger than upper_bound")
        if not self._quiet:
             print("%s:%s:%s"%(theme.ljust(30), "{:.5f}".format(lower_bound).ljust(20),
                               "{:.5f}".format(upper_bound)))
        return True
    def gurobi_call_back_factory(self, theme, model) :
        """
        create a MIP call back handler for Gurobi
        :param theme: string describing the type of MIP solve underway
        :param model: a Gurobi model
        :return: a call_back function that can be passed to optimize
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
        verify(cplexprogress, "docplex is not installed")
        super_self = self
        class MyListener(cplexprogress.ProgressListener):
            def notify_progress(self, progress_data):
                keep_going = super_self.mip_progress(theme, progress_data.best_bound,
                                                     progress_data.current_objective)
                if not keep_going:
                    self.abort()
        model.add_progress_listener(MyListener())