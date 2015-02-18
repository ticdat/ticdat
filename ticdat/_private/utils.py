import itertools
import os
import fnmatch

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
