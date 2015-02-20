import itertools
import os
import fnmatch
import unittest

_debug = []
def _asserting() :
    _debug.append(())
    return _debug
assert _asserting()

def DEBUG() :
    return bool(_debug)

def doIt(g): # just walks through everything in a gen - I like the syntax this
    for x in g : #enables
        pass

class TicDatError(Exception) :
    pass

def debugBreak():
    import ipdb; ipdb.set_trace()

_memo = []
def memo(x) :
    doIt(_memo.pop() for _ in list(_memo))
    _memo.append(x)

def failToDebugger(cls) :
    def _failToDebugger(x) :
        if not (x) :
            import ipdb; ipdb.set_trace()
            assert(x)
    cls.assertTrue = lambda s,x : _failToDebugger(x)
    cls.assertFalse = lambda s,x : _failToDebugger(not x)
    cls.failToDebugger = True
    return cls

def runSuite(cls, **kwargs ):
    # call with fastOnly = True or fastOnly = False as the only args
    assert (len(kwargs) == 1) and ('fastOnly' in kwargs)
    fastOnly = kwargs['fastOnly']
    assert (fastOnly == True) or (fastOnly == False)
    _rtn = [getattr(cls, x) for x in dir(cls)
           if x.startswith("test")]
    assert all(callable(x) for x in _rtn)
    _rtn = [x.__name__ for x in _rtn if
             ((not fastOnly) or
              (not getattr(x, "slow", False))) and
             ((DEBUG()) or
              (not getattr(x, "skipForRelease", False)))]

    suite = unittest.TestSuite()
    for x in _rtn :
        suite.addTest(cls(x))
    if "failToDebugger" in dir(cls) and cls.failToDebugger :
        print "!!! Debugging suite for " + str(cls) + " !!!\n"
        suite.debug()
        print "!!! Debugged suite for " + str(cls) + " !!!\n"
    else :
        unittest.TextTestRunner().run(suite)


def unixWildCardDir(dirPath, unixStyleStr, fullPath = True, collapseSingleton = True, allowLinks = False) :
    assert os.path.isdir(dirPath)
    rtn = [x for x in os.listdir(dirPath) if fnmatch.fnmatch(x, unixStyleStr)]
    if (fullPath) :
        rtn = [os.path.abspath(os.path.join(dirPath, x)) for x in rtn]
    if (not allowLinks) :
        rtn = [x for x in rtn if not os.path.islink(x) ]
    if ( (len(rtn) == 1)  and collapseSingleton):
        return rtn[0]
    return rtn

def findAllUnixWildCard(directory, unixStyleStr, fullPath = True, recursive=True, allowLinks = False, skippedDirs = None):
    _skippedDirs = skippedDirs or [] # Py does scary stuff with mutable default arguments
    assert(os.path.isdir(directory))
    assert (not _skippedDirs)  or (recursive), "skipping directories makes sense only for recursive"
    rtn = unixWildCardDir(directory, unixStyleStr, fullPath = fullPath,
                          collapseSingleton = False, allowLinks = allowLinks)
    dirList=os.listdir(directory)

    for fname in dirList:
        fname = os.path.join(directory,  fname)
        if ( (os.path.isdir(fname)) and recursive and fname not in _skippedDirs) :
            rtn = rtn + findAllUnixWildCard(fname, unixStyleStr, recursive = True,
                                            allowLinks = allowLinks, skippedDirs = _skippedDirs)
    return rtn

def findAllFiles(path, extensions) :
    assert os.path.isdir(path)
    return list(itertools.chain(*[findAllUnixWildCard(path, "*" + x) for x in extensions]))

def zipBackUp(sourceFilePaths, destDir, destBaseName):
    import zipfile
    assert len(sourceFilePaths) and os.path.isdir(destDir)

    appender = 0
    backUpFilePath = lambda : os.path.join(destDir, destBaseName + str(appender) + ".zip")
    while os.path.exists(backUpFilePath()) :
        appender += 1
    print "backing up " + str(len(sourceFilePaths)) + " files to " + backUpFilePath()
    zf = zipfile.ZipFile(backUpFilePath(), 'w')
    for x in set(sourceFilePaths) :
        zf.write(x)
    zf.close()

def deepFlatten(x) :
    # does a FULL recursive flatten.
    # this works for 2.7, will need to be replaced for 3
    # make sure replaced version works equally well with tuples as lists
    import compiler
    return tuple(compiler.ast.flatten(x))

def shallowFlatten(x) :
    return tuple(itertools.chain(*x))

dictish = lambda x : all(hasattr(x, _) for _ in ("__getitem__", "keys", "values", "items", "__contains__", "__len__"))
stringish = lambda x : all(hasattr(x, _) for _ in ("lower", "upper", "strip"))
containerish = lambda x : all(hasattr(x, _) for _ in ("__iter__", "__len__", "__getitem__")) and not stringish(x)

def goodTicDatObject(ticDatObject, tableList = None, badMessageHolder=None):
    if tableList is None :
        tableList = tuple(x for x in dir(ticDatObject) if not x.startswith("_") and
                          not callable(getattr(ticDatObject, x)))
    badMessages = badMessageHolder if badMessageHolder is not None else  []
    assert hasattr(badMessages, "append")
    def _hasAttr(t) :
        if not hasattr(ticDatObject, t) :
            badMessages.append(t + " not an attribute.")
            return False
        return True
    evalGood = (_hasAttr(t) and goodTicDatTable(getattr(ticDatObject, t),
                lambda x : badMessages.append(t + " : " + x)) for t in tableList)
    if (badMessageHolder is not None) : # if logging, then evaluate all, else shortcircuit
        evalGood = list(evalGood)
    return all(evalGood)

def goodTicDatTable(ticDatTable, badMessageHandler = lambda x : None):
    if not dictish(ticDatTable) :
        badMessageHandler("Not a dict-like object.")
        return False
    if not len(ticDatTable) :
        return True
    def keyLen(k) :
        if not containerish(k) :
            return "singleton"
        try:
            rtn = len(k)
        except :
            rtn = 0
        return rtn
    if not all(keyLen(k) == keyLen(ticDatTable.keys()[0]) for k in ticDatTable.keys()) :
        badMessageHandler("Inconsistent key lengths")
        return False
    if not all(dictish(x) for x in ticDatTable.values()) :
        badMessageHandler("At least one value is not a dict-like object")
        return False
    if not all(set(x.keys()) == set(ticDatTable.values()[0].keys()) for x in ticDatTable.values()) :
        badMessageHandler("Inconsistent field name keys.")
        return False
    return True


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

def checkSchema(primaryKeyFields, dataFields):
    def _checkArg(arg, argName):
        verify(dictish(arg), "%s needs to be dict"%argName)
        verify(all(stringish(x) for x in arg.keys()), "The keys for %s should be table names")
        verify(not any(x.startswith("_") for x in arg.keys()), "%s contains a table names starting with underscore")
        verify(all(stringish(x) or (containerish(x) and all(stringish(y) for y in x))
                   for x in arg.values()), "The values for %s should be field names or containers filled with field names")
        return FrozenDict({k : (v,) if stringish(v) else tuple(v) for k,v in arg.items()})

    primaryKeyFields = _checkArg(primaryKeyFields, "primaryKeyFields")
    dataFields = _checkArg(dataFields, "dataFields")

    for pt in set(primaryKeyFields).intersection(dataFields) :
        verify(not set(primaryKeyFields[pt]).intersection(dataFields[pt]),
               "Table %s has a non empty intersection between its primary key field names and its data field names"%pt)
    for t in dataFields:
        # at some point we will support support tables without primary keys
        verify(t in primaryKeyFields, "Table %s doesn't have any primary key fields"%t)
    for t in primaryKeyFields:
        verify(len(primaryKeyFields) > 0, "Table %s doesn't have any primary key fields"%t)
    return (primaryKeyFields, dataFields)

def ticDataRowFactory(table, keyFieldNames, dataFieldNames, defaultValues={}):
    assert dictish(defaultValues) and set(defaultValues).issubset(dataFieldNames)
    assert not set(keyFieldNames).intersection(dataFieldNames)
    if not dataFieldNames:
        def makeFrozenDict(*args, **kwargs) :
            verify(len(args) == len(kwargs) == 0, "Attempting to add non-empty data to %s"%table)
            return FrozenDict()
        return makeFrozenDict
    fieldToIndex = {x:dataFieldNames.index(x) for x in dataFieldNames}
    indexToField = {v:k for k,v in fieldToIndex.items()}
    class TicDatDataRow(freezableFactory(object, "_attributesFrozen")) :
        def __init__(self, x):
            self._data = [None] * len(fieldToIndex)
            if dictish(x) :
                verify(set(x.keys()).issubset(fieldToIndex), "Applying inappropriate data field names to %s"%table)

                for f,i in fieldToIndex.items():
                    if f in defaultValues :
                        self._data[i] = defaultValues[f]
                for f,_d in x.items():
                    self[f] = _d
            elif containerish(x) :
                verify(len(x) == len(self), "%s has requires each row to have %s data values"%(table, len(self)))
                for i in range(len(self)):
                    self._data[i] = x[i]
            else:
                verify(len(self) ==1, "%s has requires each row to have %s data values"%(table, len(self)))
                self._data[0] = x
        def __getitem__(self, item):
            verify(item in fieldToIndex, "Key error : %s not data field name for table %s"%(item, table))
            return self._data[fieldToIndex[item]]
        def __setitem__(self, key, value):
            verify(key in fieldToIndex, "Key error : %s not data field name for table %s"%(key, table))
            if getattr(self, "_dataFrozen", False) :
                raise Exception("Can't edit a frozen TicDatDataRow")
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

#assert is magic. It  can't be passed around directly. Lambda encapsulation lets us pass around a proxy. Yay lambda
def assertTrue(x) :
    assert (x)

def assertFalse(x) :
    assert (not x)



def assertTicDatTablesSame(t1, t2, _goodTicDatTable = goodTicDatTable,
                           _assertTrue = assertTrue, _assertFalse = assertFalse) :
    _assertTrue(set(t1) == set(t2))
    _assertTrue(_goodTicDatTable(t1) and _goodTicDatTable(t2))
    for k1,v1 in t1.items() :
        v2 = t2[k1]
        if dictish(v1) != dictish(v2) and dictish(v2) :
            v2, v1 = v1, v2
        if dictish(v1) and dictish(v2) :
            _assertTrue(set(v1) == set(v2))
            for _k1 in v1 :
                _assertTrue(v1[_k1] == v2[_k1])
        elif dictish(v1) and containerish(v2) :
            _assertTrue(sorted(v1.values()) == sorted(v2))
        elif dictish(v1) :
            _assertTrue(len(v1) == 1 and v1.values()[0] == v2)
        else :
            if containerish(v1) != containerish(v2) and containerish(v2) :
                v2, v1 = v1, v2
            if containerish(v1) and containerish(v2) :
                _assertTrue(len(v1) == len(v2))
                _assertTrue(all(v1[x] == v2[x] for x in range(len(v1))))
            elif containerish(v1) :
                _assertTrue(len(v1) == 1 and v1[0] == v2)
            else :
                _assertTrue(v1 == v2)


