import os
import inspect
import ticdat._private.utils as utils


def _codeFile() :
    return os.path.realpath(os.path.abspath(inspect.getsourcefile(_codeFile)))
def _codeDir():
    return os.path.dirname(__codeFile)
__codeFile = _codeFile()

_testingModelsDir = os.path.abspath(os.path.join(_codeDir(), "data"))

_validDataExtensions = (".csv", ".xls", ".xlsx")

def getDataFiles() :
    rtn = (utils.findAllFiles(_testingModelsDir, _validDataExtensions) )
    return rtn
