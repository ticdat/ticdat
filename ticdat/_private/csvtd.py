"""
Read/write ticDat objects from/to csv files. Requires csv module (which is typically standard)
"""

from ticdat._private.utils import freezableFactory, TicDatError, verify, containerish, stringish
import os

try:
    import csv
    importWorked=True
except:
    importWorked=False

def _tryFloat(x) :
    try :
        return float(x)
    except :
        return x

class CsvTicFactory(freezableFactory(object, "_isFrozen")) :
    def __init__(self, ticDatFactory):
        assert importWorked, "don't create this otherwise"
        self.ticDatFactory = ticDatFactory
        assert not set(ticDatFactory.dataFields).difference(ticDatFactory.primaryKeyFields), \
            "not expecting tables with no primary key fields"
        self._isFrozen = True
    def createTicDat(self, dirPath, dialect='excel'):
        return self.ticDatFactory.TicDat(**self._createTicDat(dirPath, dialect))
    def createFrozenTicDat(self, dirPath, dialect='excel'):
        return self.ticDatFactory.FrozenTicDat(**self._createTicDat(dirPath, dialect))
    def _createTicDat(self, dirPath, dialect):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dirPath), "Invalid directory path %s"%dirPath)
        return {t : self._createTable(dirPath, t, dialect) for t in self.ticDatFactory.primaryKeyFields}
    def _createTable(self, dirPath, table, dialect):
        filePath = os.path.join(dirPath, table + ".csv")
        verify(os.path.isfile(filePath), "Could not find file path %s for table %s"%(filePath, table))
        tdf = self.ticDatFactory
        fieldnames=tdf.primaryKeyFields[table] + tdf.dataFields.get(table, ())
        rtn = {}
        with open(filePath) as csvfile:
            for r in csv.DictReader(csvfile, dialect=dialect) :
                verify(set(r.keys()).issuperset(fieldnames), "Failed to find the required field names for %s"%table)
                primaryKey = _tryFloat(r[tdf.primaryKeyFields[table][0]]) if len(tdf.primaryKeyFields[table])==1 else \
                    tuple(_tryFloat(r[_]) for _ in tdf.primaryKeyFields[table])
                rtn[primaryKey] = tuple(_tryFloat(r[_]) for _ in tdf.dataFields.get(table,()))
        return rtn

    def writeDirectory(self, ticDat, dirPath, allowOverwrite = True, dialect='excel'):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(not os.path.isfile(dirPath), "A file is not a valid directory path")
        tdf = self.ticDatFactory
        msg = []
        if not self.ticDatFactory.goodTicDatObject(ticDat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        if not allowOverwrite:
            for t in tdf.primaryKeyFields :
                f = os.path.join(dirPath, t + ".csv")
                verify(not os.path.exists(f), "The %s path exists and overwrite is not allowed"%f)
        if not os.path.isdir(dirPath) :
            os.mkdir(dirPath)
        for t in tdf.primaryKeyFields :
            f = os.path.join(dirPath, t + ".csv")
            with open(f, 'w') as csvfile:
                 writer = csv.DictWriter(csvfile,dialect=dialect, fieldnames=tdf.primaryKeyFields[t] + tdf.dataFields.get(t, ()) )
                 writer.writeheader()
                 for primaryKey,dataRow in getattr(ticDat, t).items() :
                     primaryKeyDict = {f:v for f,v in zip(tdf.primaryKeyFields[t],
                                        primaryKey if containerish(primaryKey) else (primaryKey,))}
                     writer.writerow(dict(dataRow, **primaryKeyDict))