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
    """
    Primary class for reading/writing csv files with ticDat objects.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A CsvTicDatFactory will automatically be associated with the parent
        TicDatFactory if your system has the required csv package.
        :param tic_dat_factory:
        :return:
        """
        assert importWorked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, dirPath, dialect='excel', headers_present = True):
        """
        Create a TicDat object from the csv files in a directory
        :param dirPath: the directory containing the .csv files.
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain the column headers?
        :return: a TicDat object populated by the matching files.
        !!NB!! missing files resolve to an empty table, but missing fields on matching files throw an Exception!!
        """
        return self.tic_dat_factory.TicDat(**self._createTicDat(dirPath, dialect, headers_present))
    def create_frozen_tic_dat(self, dirPath, dialect='excel', headers_present = True):
        """
        Create a FrozenTicDat object from the csv files in a directory
        :param dirPath: the directory containing .csv files whose names match the table names in
                        the ticDat schema
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain the column headers?
        :return: a TicDat object populated by the matching files.
        !!NB!! missing files resolve to an empty table, but missing fields on matching files throw an Exception!!
        """
        return self.tic_dat_factory.FrozenTicDat(**self._createTicDat(dirPath, dialect, headers_present))
    def _createTicDat(self, dirPath, dialect, headers_present):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dirPath), "Invalid directory path %s"%dirPath)
        rtn =  {t : self._createTable(dirPath, t, dialect, headers_present) for t in self.tic_dat_factory.all_tables}
        return {k:v for k,v in rtn.items() if v}
    def _createTable(self, dirPath, table, dialect, headers_present):
        filePath = os.path.join(dirPath, table + ".csv")
        if not os.path.isfile(filePath) :
            return
        tdf = self.tic_dat_factory
        fieldnames=tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
        dictReaderArgs = dict({"fieldnames":fieldnames} if not headers_present else{},**{"dialect": dialect})
        def verifyFieldsByCount() :
            verify(os.path.isfile(filePath), "Could not find file path %s for table %s"%(filePath, table))
            with open(filePath) as csvfile :
                trialReader = csv.reader(csvfile, dialect=dialect)
                for row in trialReader:
                    verify(len(row) == len(fieldnames), "Need %s columns for table %s"%(len(fieldnames), table))
                    return
        if table in tdf.generator_tables:
            def rtn() :
                verifyFieldsByCount() if not headers_present else None
                with open(filePath) as csvfile:
                    for r in csv.DictReader(csvfile, **dictReaderArgs) :
                        verify(set(r.keys()).issuperset(fieldnames),
                               "Failed to find the required field names for %s"%table)
                        yield tuple(_tryFloat(r[_]) for _ in tdf.data_fields[table])
        else:
            verifyFieldsByCount() if not headers_present else None
            rtn = {} if tdf.primary_key_fields.get(table) else []
            with open(filePath) as csvfile:
                for r in csv.DictReader(csvfile, **dictReaderArgs) :
                    verify(set(r.keys()).issuperset(fieldnames), "Failed to find the required field names for %s"%table)
                    if tdf.primary_key_fields.get(table) :
                        primaryKey = _tryFloat(r[tdf.primary_key_fields[table][0]]) \
                            if len(tdf.primary_key_fields[table])==1 else \
                            tuple(_tryFloat(r[_]) for _ in tdf.primary_key_fields[table])
                        rtn[primaryKey] = tuple(_tryFloat(r[_]) for _ in tdf.data_fields.get(table,()))
                    else:
                        rtn.append(tuple(_tryFloat(r[_]) for _ in tdf.data_fields[table]))
        return rtn

    def write_directory(self, tic_dat, dir_path, allow_overwrite = False, dialect='excel', write_header = True):
        """
        write the ticDat data to a collection of csv files
        :param tic_dat: the data object
        :param dir_path: the directory in which to write the csv files
        :param allow_overwrite: boolean - are we allowed to overwrite existing files?
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param write_header: Boolean. Should the header information be written as the first row?
        :return:
        """
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
                 writer.writeheader() if write_header else None
                 _t =  getattr(tic_dat, t)
                 if dictish(_t) :
                     for primaryKey, dataRow in _t.items() :
                         primaryKeyDict = {f:v for f,v in zip(tdf.primary_key_fields[t],
                                            primaryKey if containerish(primaryKey) else (primaryKey,))}
                         writer.writerow(dict(dataRow, **primaryKeyDict))
                 else :
                     for dataRow in (_t if containerish(_t) else _t()) :
                         writer.writerow(dict(dataRow))