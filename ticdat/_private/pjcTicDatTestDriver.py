# PJC hates nose. What an idiot

import ticdat._private.utils as utils
import ticdat.testing.ticdattestutils as tdu
import os
try:
    __IPYTHON__
    _IPYHTON =  True
except NameError:
    _IPYHTON =  False

import ticdat.testing.ticdattestutils as testutils


if _IPYHTON :
    print "!!IPYTHON!!"

backingUp = True

filterByReject = None

# override code
#filterByReject = lambda f :  True    # this line rejects everything, easy way to bypass all tests#
#filterByReject = lambda f :  not "slow" in f.lower()  # True means rejection


def diagnosticPause(msg) :
    print "!!! HEY " + str(msg) + " !!!! "
    #os.system("pause")  # windows friendly pause
    raw_input("Press enter to continue") # mac friendly pause


if filterByReject :
    diagnosticPause("applying a special filter")


# only py allows code so phucking cool as this
passesTheFilter = lambda f : (not filterByReject) or (not filterByReject(f))

# run the appropriate tests
for f in sorted(utils.findAllUnixWildCard(tdu._codeDir(), "*test*.py"), key = lambda x : "slow." in x) :
 if os.path.basename(f).startswith("test") or os.path.basename(f).startswith("slow.test") :
    print os.path.basename(f), passesTheFilter(f)
    if passesTheFilter(f)  :
        diagnosticPause(f + "\nrunning")
        try :
          execfile(f)
          diagnosticPause(os.path.basename(f) + " check it")
        except SystemExit:
          pass


def projectFiles() :

    myProjectFiles = [os.path.abspath(x) for x in utils.deepFlatten((
        utils.findAllUnixWildCard(os.path.join(tdu._codeDir(), ".."), "*.py"),
        tdu.getDataFiles() ))
        if x]
    assert myProjectFiles, "nothing found"
    assert len(myProjectFiles) < 200
    return myProjectFiles

def performBackUp(gDrive):
    utils.zipBackUp(projectFiles(), gDrive, "ticDatSrc")


# the gDrive backing up is just a convenience, so hardcoding paths for my convenience
# for now just a singleton, can put other paths here if needed
gDrivesPython= "/Users/petercacioppi/Google_Drive/Python/ticdat"
performBackUp(gDrivesPython) if backingUp and os.path.exists(gDrivesPython) else None

import shutil
# !!!!!!!!!!!!!!!!!!!! BEGIN KEEP ME !!!!!!!!!!!!!!!!!!!!!!!!!!
# remove the extra shizzle
_filesWithExtension = lambda ext  : tuple(os.path.abspath(x) for y in
    (utils.findAllUnixWildCard(y, "*" + ext) for y in (os.path.join(tdu._codeDir(), ".."),))
    for x in y)
map(os.remove, utils.deepFlatten((_filesWithExtension(".pyc"), _filesWithExtension(".pyo"))))
map(os.remove, utils.deepFlatten(utils.findAllUnixWildCard(os.path.join(tdu._codeDir(), ".."), "gurobi.log")))
utils.doIt(shutil.rmtree(z) if os.path.isdir(z) else None for x in range(3)
    for z in (os.path.join(os.path.join(tdu._codeDir(), *([".."]*x)), ".idea"),))
# !!!!!!!!!!!!!!!!!!!! END KEEP ME !!!!!!!!!!!!!!!!!!!!!!!!!!