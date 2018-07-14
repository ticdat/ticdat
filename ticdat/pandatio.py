# coding=utf-8
try:
    import sqlite3 as sql
except:
    sql = None

import os
from ticdat.utils import freezable_factory, verify, case_space_to_pretty, pd, TicDatError, FrozenDict, all_fields
from ticdat.utils import all_underscore_replacements
from itertools import product
from collections import defaultdict

_longest_sheet = 30 # seems to be an Excel limit with pandas

def _sql_con(dbFile):
    verify(sql, "sqlite3 needs to be installed")
    con = sql.connect(dbFile)
    return con

def _brackets(l) :
    return ["[%s]"%_ for _ in l]

class SqlPanFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing SQLite files with panDat objects.
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A SqlPanFactory will
        automatically be associated with the sql attribute of the parent
        PanDatFactory.
        :param pan_dat_factory:
        :return:
        """
        self.pan_dat_factory = pan_dat_factory
        self._isFrozen = True
    def create_pan_dat(self, db_file_path):
        """
        Create a PanDat object from a SQLite database file
        :param db_file_path: A SQLite DB File.
        :return: a PanDat object populated by the matching tables.
        caveats: Missing tables and missing fields throw an Exception.
                 Table names are matched with case-space insensitivity, but spaces
                 are respected for field names.
                 (ticdat supports whitespace in field names but not table names).
        """
        verify(os.path.exists(db_file_path) and not os.path.isdir(db_file_path),
               "%s not a file path"%db_file_path)
        rtn = {}
        for t, s in self._get_table_names(db_file_path).items():
            with _sql_con(db_file_path) as con:
                rtn[t] = pd.read_sql(sql="Select * from [%s]"%s, con=con)
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        verify(not missing_fields, "The following are (table, field) pairs missing from the %s file.\n%s"%
               (db_file_path, missing_fields))
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _get_table_names(self, db_file_path):
        rtn = {}
        with _sql_con(db_file_path) as con:
            def try_name(name):
                try :
                    con.execute("Select * from [%s]"%name)
                except :
                    return False
                return True
            for table in self.pan_dat_factory.all_tables:
                rtn[table] = [t for t in all_underscore_replacements(table) if try_name(t)]
                verify(len(rtn[table]) >= 1, "Unable to recognize table %s in SQLite file %s"%
                                  (table, db_file_path))
                verify(len(rtn[table]) <= 1, "Duplicate tables found for table %s in SQLite file %s"%
                                  (table, db_file_path))
                rtn[table] = rtn[table][0]
        return rtn
    def write_file(self, pan_dat, file_path, if_exists='replace', case_space_table_names=False):
        """
        write the panDat data to an excel file
        :param pan_dat: the PanDat object to write
        :param file_path: The file path of the excel file to create
        :param if_exists: ‘fail’, ‘replace’ or ‘append’. How to behave if the table already exists
        :param case_space_table_names: boolean - make best guesses how to add spaces and upper case
                                          characters to table names
        :return:
        caveats: The row names (index) isn't written. The default pandas schema generation is used,
                 and thus foreign key relationships aren't written. (The code to generate foreign keys
                 is written and tested as part of TicDatFactory, and thus this shortcoming could be
                 easily rectified if need be).
        """
        # note - pandas has an unfortunate tendency to push types into SQLlite columns. This can result in
        # writing-reading round trips converting your numbers to text if they are mixed type columns.
        # Can address this more fully if it turns into a problem.
        verify(pd, "pandas not installed")
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid SQLLite file path")
        case_space_table_names = case_space_table_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        with _sql_con(file_path) as con:
            for t in self.pan_dat_factory.all_tables:
                getattr(pan_dat, t).to_sql(name=case_space_to_pretty(t) if case_space_table_names else t,
                                           con=con, if_exists=if_exists, index=False)

class XlsPanFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Excel files with panDat objects.
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A XlsPanFactory will
        automatically be associated with the xls attribute of the parent
        PanDatFactory.
        :param pan_dat_factory:
        :return:
        """
        self.pan_dat_factory = pan_dat_factory
        self._isFrozen = True

    def create_pan_dat(self, xls_file_path):
        """
        Create a PanDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :return: a PanDat object populated by the matching sheets.
        caveats: Missing sheets resolve to an empty table, but missing fields
                 on matching sheets throw an Exception.
                 Table names are matched to sheets with with case-space insensitivity, but spaces and
                 case are respected for field names.
                 (ticdat supports whitespace in field names but not table names).
        """
        rtn = {}
        for t, s in self._get_sheet_names(xls_file_path).items():
            rtn[t] = pd.read_excel(xls_file_path, s)
        missing_tables = {t for t in self.pan_dat_factory.all_tables if t not in rtn}
        if missing_tables:
            print ("The following table names could not be found in the %s file.\n%s\n"%
                   (xls_file_path,"\n".join(missing_tables)))
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        verify(not missing_fields, "The following are (table, field) pairs missing from the %s file.\n%s"%
               (xls_file_path, missing_fields))
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _get_sheet_names(self, xls_file_path):
        sheets = defaultdict(list)
        try :
            xl = pd.ExcelFile(xls_file_path)
        except Exception as e:
            raise TicDatError("Unable to open %s as xls file : %s"%(xls_file_path, e.message))
        for table, sheet in product(self.pan_dat_factory.all_tables, xl.sheet_names) :
            if table.lower()[:_longest_sheet] == sheet.lower().replace(' ', '_')[:_longest_sheet]:
                sheets[table].append(sheet)
        duplicated_sheets = tuple(_t for _t,_s in sheets.items() if len(_s) > 1)
        verify(not duplicated_sheets, "The following sheet names were duplicated : " +
               ",".join(duplicated_sheets))
        sheets = FrozenDict({k:v[0] for k,v in sheets.items()})
        return sheets
    def write_file(self, pan_dat, file_path, allow_overwrite = False, case_space_sheet_names = False):
        """
        write the panDat data to an excel file
        :param pan_dat: the PanDat object to write
        :param file_path: The file path of the excel file to create
        :param allow_overwrite: boolean - are we allowed to overwrite an
                                existing file?
              case_space_sheet_names: boolean - make best guesses how to add spaces and upper case
                                      characters to sheet names
        :return:
        caveats: The row names (index) isn't written.
        """
        verify(pd, "pandas not installed")
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid xls file path")
        verify(allow_overwrite or not os.path.exists(file_path),
               "The %s path exists and overwrite is not allowed"%file_path)
        case_space_sheet_names = case_space_sheet_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        writer = pd.ExcelWriter(file_path)
        for t in self.pan_dat_factory.all_tables:
            getattr(pan_dat, t).to_excel(writer, case_space_to_pretty(t) if case_space_sheet_names else t,
                                         index=False)
        writer.save()
