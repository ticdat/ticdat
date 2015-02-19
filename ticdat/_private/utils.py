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
        if stringish(k) :
            return 0
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
            raise Exception("can't set attributes to a frozen " + self.__class__.__name__)
        def __delattr__(self, item):
            if not getattr(self, freezeAttr, False):
                return super(_Freezeable, self).__delattr__(item)
            raise Exception("can't del attributes to a frozen " + self.__class__.__name__)
    return _Freezeable



