"""
general utility module
PEP8
"""
from numbers import Number
try:
    import pandas as pd
    from pandas import DataFrame
except:
    pd = DataFrame =  None

def do_it(g): # just walks through everything in a gen - I like the syntax this enables
    for x in g :
        pass

class TicDatError(Exception) :
    pass

def debug_break():
    import ipdb; ipdb.set_trace()

_memo = []
def memo(x) :
    do_it(_memo.pop() for _ in list(_memo))
    _memo.append(x)

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

def freezable_factory(baseClass, freezeAttr) :
    class _Freezeable(baseClass) :
        def __setattr__(self, key, value):
            if not getattr(self, freezeAttr, False):
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


def verify(b, msg) :
    if not b :
        raise TicDatError(msg)

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
                    Sloc.add_sloc(getattr(s,c))
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
             print "%s:%s"%(theme.ljust(40), "{:.5f}".format(progress))
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
             print "%s:%s:%s"%(theme.ljust(30), "{:.5f}".format(lower_bound).ljust(20),
                               "{:.5f}".format(upper_bound))
        return True
