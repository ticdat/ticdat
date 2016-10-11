"""
Read/write ticDat objects from/to csv files.
PEP8
"""

import os
from ticdat.utils import DataFrame, create_generic_free
from ticdat.utils import freezable_factory, TicDatError, verify, containerish, dictish
from collections import defaultdict
from itertools import product

try:
    import csv
except:
    csv = None

_can_unit_test = csv

def _try_float(x) :
    try :
        return float(x)
    except :
        return x

class CsvTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing csv files with ticDat objects.
    Your system will need the csv package if you want to use this class.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A CsvTicDatFactory will
        automatically be associated with the csv attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, dir_path, dialect='excel', headers_present = True,
                       freeze_it = False):
        """
        Create a TicDat object from the csv files in a directory
        :param dir_path: the directory containing the .csv files.
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain the
                                column headers?
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the matching files.
        caveats: Missing files resolve to an empty table, but missing fields on
                 matching files throw an Exception.
                 Data field values (but not primary key values) will be coerced
                 into floats if possible.
        """
        verify(csv, "csv needs to be installed to use this subroutine")
        tdf = self.tic_dat_factory
        verify(headers_present or not tdf.generic_tables,
               "headers need to be present to read generic tables")
        verify(DataFrame or not tdf.generic_tables,
               "Strange absence of pandas despite presence of generic tables")
        rtn =  self.tic_dat_factory.TicDat(**self._create_tic_dat(dir_path, dialect,
                                                                  headers_present))
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn
    def _create_tic_dat(self, dir_path, dialect, headers_present):
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dir_path), "Invalid directory path %s"%dir_path)
        rtn =  {t : self._create_table(dir_path, t, dialect, headers_present)
                for t in self.tic_dat_factory.all_tables}
        return {k:v for k,v in rtn.items() if v}
    def find_duplicates(self, dir_path, dialect='excel', headers_present = True):
        """
        Find the row counts for duplicated rows.
        :param dir_path: the directory containing .csv files.
        :param dialect: the csv dialect. Consult csv documentation for details.
        :param headers_present: Boolean. Does the first row of data contain
                                the column headers?
        :return: A dictionary whose keys are the table names for the primary key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered
                 in the table, and the value is the count of records in the
                 Excel sheet with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        caveats: Missing files resolve to an empty table, but missing fields (data or primary key) on
                 matching files throw an Exception.
        """
        verify(csv, "csv needs to be installed to use this subroutine")
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(os.path.isdir(dir_path), "Invalid directory path %s"%dir_path)
        tdf = self.tic_dat_factory
        rtn = {t:defaultdict(int) for t,_ in tdf.primary_key_fields.items()
               if _ and self._get_file_path(dir_path, t)}
        for t in rtn:
            with open(self._get_file_path(dir_path, t)) as csvfile:
                for r in self._get_data(csvfile, t, dialect, headers_present):
                    p_key = r[tdf.primary_key_fields[t][0]] \
                            if len(tdf.primary_key_fields[t])==1 else \
                            tuple(r[_] for _ in tdf.primary_key_fields[t])
                    rtn[t][p_key] += 1
        for t in list(rtn.keys()):
            rtn[t] = {k:v for k,v in rtn[t].items() if v > 1}
            if not rtn[t]:
                del(rtn[t])
        return rtn
    def _get_file_path(self, dir_path, table):
        rtn = [path for f in os.listdir(dir_path) for path in [os.path.join(dir_path, f)]
               if os.path.isfile(path) and
               f.lower().replace(" ", "_") == "%s.csv"%table.lower()]
        verify(len(rtn) <= 1, "duplicate .csv files found for %s"%table)
        if rtn:
            return rtn[0]
    def _get_data(self, csvfile, table, dialect, headers_present):
        tdf = self.tic_dat_factory
        fieldnames=tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
        assert fieldnames or table in self.tic_dat_factory.generic_tables
        for row in csv.DictReader(csvfile, dialect = dialect,
                            **({"fieldnames":fieldnames} if not headers_present else {})):
            if not headers_present:
                verify(len(row) == len(fieldnames),
                   "Need %s columns for table %s"%(len(fieldnames), table))
                yield {f: _try_float(row[f]) for f in fieldnames}
            else:
                key_matching = defaultdict(list)
                for k,f in product(row.keys(), fieldnames or row.keys()):
                    if k.lower() ==f.lower():
                        key_matching[f].append(k)
                fieldnames = fieldnames or row.keys()
                for f in fieldnames:
                    verify(f in key_matching, "Unable to find field name %s for table %s"%(f, table))
                    verify(len(key_matching[f]) <= 1,
                           "Duplicate field names found for field %s table %s"%(f, table))
                yield {f: _try_float(row[key_matching[f][0]]) for f in fieldnames}

    def _create_table(self, dir_path, table, dialect, headers_present):
        file_path = self._get_file_path(dir_path, table)
        if not (file_path and  os.path.isfile(file_path)) :
            return
        tdf = self.tic_dat_factory
        if table in tdf.generator_tables:
            def rtn() :
                with open(file_path) as csvfile:
                    for r in self._get_data(csvfile, table, dialect, headers_present):
                        yield tuple(r[_] for _ in tdf.data_fields[table])
        else:
            rtn = {} if tdf.primary_key_fields.get(table) else []
            with open(file_path) as csvfile:
                for r in self._get_data(csvfile, table, dialect, headers_present) :
                    if tdf.primary_key_fields.get(table) :
                        p_key = r[tdf.primary_key_fields[table][0]] \
                                if len(tdf.primary_key_fields[table]) == 1 else \
                                tuple(r[_] for _ in tdf.primary_key_fields[table])
                        rtn[p_key] = tuple(r[_] for _ in tdf.data_fields.get(table,()))
                    elif table in tdf.generic_tables:
                        rtn.append(r)
                    else:
                        rtn.append(tuple(r[_] for _ in tdf.data_fields[table]))
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
        verify(csv, "csv needs to be installed to use this subroutine")
        verify(dialect in csv.list_dialects(), "Invalid dialect %s"%dialect)
        verify(not os.path.isfile(dir_path), "A file is not a valid directory path")
        if self.tic_dat_factory.generic_tables:
            dat, tdf = create_generic_free(tic_dat, self.tic_dat_factory)
            return tdf.csv.write_directory(dat, dir_path, allow_overwrite, dialect, write_header)
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