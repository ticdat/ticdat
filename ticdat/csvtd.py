"""
Read/write ticDat objects from/to csv files. Requires csv module (which is typically standard)
"""

from utils import freezableFactory, TicDatError, verify, containerish, dictish, debugBreak
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
    def __init__(self, tic_dat_factory):
        assert importWorked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, dirPath, dialect='excel'):
        return self.tic_dat_factory.TicDat(**self._createTicDat(dirPath, dialect))
    def create_frozen_tic_dat(self, dirPath, dialect='excel'):
        return self.tic_dat_factory.FrozenTicDat(**self._createTicDat(dirPath, dialect))
    def _createTicDat(self, dirPath, dialect):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dirPath), "Invalid directory path %s"%dirPath)
        return {t : self._createTable(dirPath, t, dialect) for t in self.tic_dat_factory.all_tables}
    def _createTable(self, dirPath, table, dialect):
        filePath = os.path.join(dirPath, table + ".csv")
        verify(os.path.isfile(filePath), "Could not find file path %s for table %s"%(filePath, table))
        tdf = self.tic_dat_factory
        fieldnames=tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
        if table in tdf.generator_tables:
            def rtn() :
                verify(os.path.isfile(filePath), "Could not find file path %s for table %s"%(filePath, table))
                with open(filePath) as csvfile:
                    for r in csv.DictReader(csvfile, dialect=dialect) :
                        verify(set(r.keys()).issuperset(fieldnames),
                               "Failed to find the required field names for %s"%table)
                        yield tuple(_tryFloat(r[_]) for _ in tdf.data_fields[table])
        else:
            rtn = {} if tdf.primary_key_fields.get(table) else []
            with open(filePath) as csvfile:
                for r in csv.DictReader(csvfile, dialect=dialect) :
                    verify(set(r.keys()).issuperset(fieldnames), "Failed to find the required field names for %s"%table)
                    if tdf.primary_key_fields.get(table) :
                        primaryKey = _tryFloat(r[tdf.primary_key_fields[table][0]]) \
                            if len(tdf.primary_key_fields[table])==1 else \
                            tuple(_tryFloat(r[_]) for _ in tdf.primary_key_fields[table])
                        rtn[primaryKey] = tuple(_tryFloat(r[_]) for _ in tdf.data_fields.get(table,()))
                    else:
                        rtn.append(tuple(_tryFloat(r[_]) for _ in tdf.data_fields[table]))
        return rtn

    def write_directory(self, tic_dat, dir_path, allow_overwrite = True, dialect='excel'):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(not os.path.isfile(dir_path), "A file is not a valid directory path")
        tdf = self.tic_dat_factory
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        if not allow_overwrite:
            for t in tdf.all_tables :
                f = os.path.join(dir_path, t + ".csv")
                verify(not os.path.exists(f), "The %s path exists and overwrite is not allowed"%f)
        if not os.path.isdir(dir_path) :
            os.mkdir(dir_path)
        for t in tdf.all_tables :
            f = os.path.join(dir_path, t + ".csv")
            with open(f, 'w') as csvfile:
                 writer = csv.DictWriter(csvfile,dialect=dialect, fieldnames=
                        tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ()) )
                 writer.writeheader()
                 _t =  getattr(tic_dat, t)
                 if dictish(_t) :
                     for primaryKey, dataRow in _t.items() :
                         primaryKeyDict = {f:v for f,v in zip(tdf.primary_key_fields[t],
                                            primaryKey if containerish(primaryKey) else (primaryKey,))}
                         writer.writerow(dict(dataRow, **primaryKeyDict))
                 else :
                     for dataRow in (_t if containerish(_t) else _t()) :
                         writer.writerow(dict(dataRow))