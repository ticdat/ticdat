"""
Read/write ticDat objects from/to csv files.
PEP8
"""

import os
from utils import freezable_factory, TicDatError, verify, containerish, dictish
import utils

try:
    import csv
    import_worked=True
except:
    import_worked=False

def _try_float(x) :
    try :
        return float(x)
    except :
        return x

class CsvTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing csv files with ticDat objects.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A CsvTicDatFactory will
        automatically be associated with the parent TicDatFactory if your
        system has the required csv package.
        :param tic_dat_factory:
        :return:
        """
        assert import_worked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, dir_path, dialect='excel', headers_present = True):
        """
        Create a TicDat object from the csv files in a directory
        :param dir_path: the directory containing the .csv files.
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain the
                                column headers?
        :return: a TicDat object populated by the matching files.
        caveats: Missing files resolve to an empty table, but missing fields on
                 matching files throw an Exception.
                 Data field values (but not primary key values) will be coerced
                 into floats if possible.
        """
        return self.tic_dat_factory.TicDat(**self._create_tic_dat(dir_path, dialect,
                                                                  headers_present))
    def create_frozen_tic_dat(self, dir_path, dialect='excel', headers_present = True):
        """
        Create a FrozenTicDat object from the csv files in a directory
        :param dir_path: the directory containing .csv files.
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain
                                the column headers?
        :return: a TicDat object populated by the matching files.
        caveats: Missing files resolve to an empty table, but missing fields on
                 matching files throw an Exception.
                 Data field values (but not primary key values) will be coerced
                 into floats if possible.
        """
        return self.tic_dat_factory.FrozenTicDat(**self._create_tic_dat(dir_path, dialect,
                                                    headers_present))
    def _create_tic_dat(self, dir_path, dialect, headers_present):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dir_path), "Invalid directory path %s"%dir_path)
        rtn =  {t : self._create_table(dir_path, t, dialect, headers_present)
                for t in self.tic_dat_factory.all_tables}
        return {k:v for k,v in rtn.items() if v}
    def _create_table(self, dir_path, table, dialect, headers_present):
        file_path = os.path.join(dir_path, table + ".csv")
        if not os.path.isfile(file_path) :
            return
        tdf = self.tic_dat_factory
        fieldnames=tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
        dict_rdr_args = dict({"fieldnames":fieldnames} if not headers_present else{},
                             **{"dialect": dialect})
        def verify_fields_by_cnt() :
            verify(os.path.isfile(file_path),
                   "Could not find file path %s for table %s"%(file_path, table))
            with open(file_path) as csvfile :
                trial_rdr = csv.reader(csvfile, dialect=dialect)
                for row in trial_rdr:
                    verify(len(row) == len(fieldnames),
                           "Need %s columns for table %s"%(len(fieldnames), table))
                    return
        if table in tdf.generator_tables:
            def rtn() :
                verify_fields_by_cnt() if not headers_present else None
                with open(file_path) as csvfile:
                    for r in csv.DictReader(csvfile, **dict_rdr_args) :
                        verify(set(r.keys()).issuperset(fieldnames),
                               "Failed to find the required field names for %s"%table)
                        yield tuple(_try_float(r[_]) for _ in tdf.data_fields[table])
        else:
            verify_fields_by_cnt() if not headers_present else None
            rtn = {} if tdf.primary_key_fields.get(table) else []
            with open(file_path) as csvfile:
                for r in csv.DictReader(csvfile, **dict_rdr_args) :
                    verify(set(r.keys()).issuperset(fieldnames),
                           "Failed to find the required field names for %s"%table)
                    if tdf.primary_key_fields.get(table) :
                        p_key = _try_float(r[tdf.primary_key_fields[table][0]]) \
                            if len(tdf.primary_key_fields[table])==1 else \
                            tuple(_try_float(r[_]) for _ in tdf.primary_key_fields[table])
                        rtn[p_key] = tuple(_try_float(r[_]) for _ in tdf.data_fields.get(table,()))
                    else:
                        rtn.append(tuple(_try_float(r[_]) for _ in tdf.data_fields[table]))
        return rtn

    def write_directory(self, tic_dat, dir_path, allow_overwrite = False, dialect='excel',
                        write_header = True):
        """
        write the ticDat data to a collection of csv files
        :param tic_dat: the data object
        :param dir_path: the directory in which to write the csv files
        :param allow_overwrite: boolean - are we allowed to overwrite existing
                                files?
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param write_header: Boolean. Should the header information be written
                             as the first row?
        :return:
        """
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(not os.path.isfile(dir_path), "A file is not a valid directory path")
        tdf = self.tic_dat_factory
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
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
                     for p_key, data_row in _t.items() :
                         primaryKeyDict = {f:v for f,v in zip(tdf.primary_key_fields[t],
                                            p_key if containerish(p_key) else (p_key,))}
                         writer.writerow(dict(data_row, **primaryKeyDict))
                 else :
                     for data_row in (_t if containerish(_t) else _t()) :
                         writer.writerow(dict(data_row))