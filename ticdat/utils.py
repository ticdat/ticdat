"""
general utility module
PEP8
"""
from numbers import Number

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

def dictish(x): return all(hasattr(x, _) for _ in
                           ("__getitem__", "keys", "values", "items", "__contains__", "__len__"))
def stringish(x): return all(hasattr(x, _) for _ in ("lower", "upper", "strip"))
def containerish(x): return all(hasattr(x, _) for _ in ("__iter__", "__len__", "__contains__")) \
                                and not stringish(x)
def generatorish(x): return all(hasattr(x, _) for _ in ("__iter__", "next")) \
                            and not (containerish(x) or dictish(x))
def numericish(x) : return isinstance(x, Number) and not isinstance(x, bool)

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
