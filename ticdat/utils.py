

def doIt(g): # just walks through everything in a gen - I like the syntax this enables
    for x in g :
        pass

class TicDatError(Exception) :
    pass

def debugBreak():
    import ipdb; ipdb.set_trace()

_memo = []
def memo(x) :
    doIt(_memo.pop() for _ in list(_memo))
    _memo.append(x)

dictish = lambda x : all(hasattr(x, _) for _ in ("__getitem__", "keys", "values", "items", "__contains__", "__len__"))
stringish = lambda x : all(hasattr(x, _) for _ in ("lower", "upper", "strip"))
containerish = lambda x : all(hasattr(x, _) for _ in ("__iter__", "__len__", "__getitem__")) and not stringish(x)
generatorish = lambda x : all(hasattr(x, _) for _ in ("__iter__", "next")) and not (containerish(x) or dictish(x))

def freezableFactory(baseClass, freezeAttr) :
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


_FreezableDictBase = freezableFactory(dict, "_attributesFrozen")
class FreezeableDict(_FreezableDictBase) :
    def __setattr__(self, key, value):
        if key == "_dataFrozen" and value :
            return super(_FreezableDictBase, self).__setattr__(key, value)
        return super(FreezeableDict, self).__setattr__(key, value)
    def __setitem__(self, key, value):
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).__setitem__(key, value)
        raise TicDatError("Can't edit a " + self.__class__.__name__)
    def __delitem__(self, key):
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).__delitem__(key)
        raise TicDatError("Can't edit a " + self.__class__.__name__)
    def update(self, *args, **kwargs) :
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).update(*args, **kwargs)
        raise TicDatError("Can't edit a " + self.__class__.__name__)
    def pop(self, *args, **kwargs) :
        if not getattr(self, "_dataFrozen", False) :
            return super(FreezeableDict, self).pop(*args, **kwargs)
        raise TicDatError("Can't edit a " + self.__class__.__name__)

class FrozenDict(FreezeableDict) :
    def __init__(self, *args, **kwargs):
        super(FrozenDict, self).__init__(*args, **kwargs)
        self._dataFrozen = True # need to do first, obviously
        self._attributesFrozen  = True

def deepFreezeContainer(x) :
    if stringish(x) or not hasattr(x, "__contains__") :
        return x
    if hasattr(x, "keys") and hasattr(x, "values") :
        return FrozenDict({deepFreezeContainer(k) : deepFreezeContainer(v) for k,v in x.items()})
    if hasattr(x, "__getitem__") :
        return tuple(map(deepFreezeContainer, x))
    return frozenset(map(deepFreezeContainer,x))


def verify(b, msg) :
    if not b :
        raise TicDatError(msg)

def ticDataRowFactory(table, keyFieldNames, dataFieldNames, defaultValues={}):
    assert dictish(defaultValues) and set(defaultValues).issubset(dataFieldNames)
    assert not set(keyFieldNames).intersection(dataFieldNames)
    if not dataFieldNames:
        def makeFreezeableDict(x=()) : # need a freezeable dict not a frozen dict here so can still link foreign keys
            verify(containerish(x) and len(x) == 0, "Attempting to add non-empty data to %s"%table)
            return FreezeableDict()
        return makeFreezeableDict
    fieldToIndex = {x:dataFieldNames.index(x) for x in dataFieldNames}
    indexToField = {v:k for k,v in fieldToIndex.items()}
    class TicDatDataRow(freezableFactory(object, "_attributesFrozen")) :
        def __init__(self, x):
            self._data = [0] * len(fieldToIndex) # since ticDat targeting numerical analysis, 0 is good default default
            if dictish(x) :
                verify(set(x.keys()).issubset(fieldToIndex), "Applying inappropriate data field names to %s"%table)
                for f,i in fieldToIndex.items():
                    if f in defaultValues :
                        self._data[i] = defaultValues[f]
                for f,_d in x.items():
                    self[f] = _d
            elif containerish(x) :
                verify(len(x) == len(self), "%s requires each row to have %s data values"%(table, len(self)))
                for i in range(len(self)):
                    self._data[i] = x[i]
            else:
                verify(len(self) ==1, "%s requires each row to have %s data values"%(table, len(self)))
                self._data[0] = x
        def __getitem__(self, item):
            verify(item in fieldToIndex, "Key error : %s not data field name for table %s"%(item, table))
            return self._data[fieldToIndex[item]]
        def __setitem__(self, key, value):
            verify(key in fieldToIndex, "Key error : %s not data field name for table %s"%(key, table))
            if getattr(self, "_dataFrozen", False) :
                raise TicDatError("Can't edit a frozen TicDatDataRow")
            self._data[fieldToIndex[key]] = value
        def keys(self):
            return tuple(indexToField[i] for i in range(len(self)))
        def values(self):
            return tuple(self._data)
        def items(self):
            return zip(self.keys(), self.values())
        def __contains__(self, item):
            return item in fieldToIndex
        def __iter__(self):
            return iter(fieldToIndex)
        def __len__(self):
            return len(self._data)
        def __repr__(self):
            return "_td:" + {k:v for k,v in self.items()}.__repr__()
    assert dictish(TicDatDataRow)
    return TicDatDataRow
