# coding=utf-8
try:
    import sqlite3 as sql
except:
    sql = None
import json

import os
from ticdat.utils import freezable_factory, verify, case_space_to_pretty, pd, TicDatError, FrozenDict, all_fields
from ticdat.utils import all_underscore_replacements, stringish, dictish
from itertools import product
from collections import defaultdict
import inspect

_longest_sheet = 30 # seems to be an Excel limit with pandas

def _sql_con(dbFile):
    verify(sql, "sqlite3 needs to be installed")
    con = sql.connect(dbFile)
    return con

def _brackets(l) :
    return ["[%s]"%_ for _ in l]

class _DummyContextManager(object):
    def __init__(self, *args, **kwargs):
        pass
    def __enter__(self, *execinfo) :
        return self
    def __exit__(self, *excinfo) :
        pass

class JsonPanFactory(freezable_factory(object, "_isFrozen")):
    """
    Primary class for reading/writing json data with panDat objects.
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A JsonPanFactory will
        automatically be associated with the json attribute of the parent
        PanDatFactory.
        :param pan_dat_factory:
        :return:
        """
        self.pan_dat_factory = pan_dat_factory
        to_json_args = inspect.getargspec(pd.DataFrame.to_json).args
        assert "orient" in to_json_args
        self._modern_pandas = "index" in to_json_args
        self._isFrozen = True
    def create_pan_dat(self, path_or_buf, fill_missing_fields=False, orient='split', **kwargs):
        """
        Create a PanDat object from a SQLite database file
        :param path_or_buf:  a valid JSON string or file-like
        :param fill_missing_fields: boolean. If truthy, missing fields will be filled in
                                    with their default value. Otherwise, missing fields
                                    throw an Exception.
        :param orient: Indication of expected JSON string format. See pandas.read_json for more details.
        :param kwargs: additional named arguments to pass to pandas.read_json
        :return: a PanDat object populated by the matching tables.
        caveats: Missing tables always throw an Exception.
                 Table names are matched with case-space insensitivity, but spaces
                 are respected for field names.
                 (ticdat supports whitespace in field names but not table names).
                 +- "inf", "-inf" strings will be converted to +-float("inf")
        """
        if os.path.exists(path_or_buf):
            verify(os.path.isfile(path_or_buf), "%s appears to be a directory and not a file." % path_or_buf)
            with open(path_or_buf, "r") as f:
                loaded_dict = json.load(f)
        else:
            verify(stringish(path_or_buf), "%s isn't a string" % path_or_buf)
            loaded_dict = json.loads(path_or_buf)
        verify(dictish(loaded_dict), "path_or_buf to json.load as a dict")
        verify(all(map(dictish, loaded_dict.values())),
               "the json.load result doesn't resolve to a dictionary whose values are themselves dictionaries")

        tbl_names = self._get_table_names(loaded_dict)
        verify("orient" not in kwargs, "orient should be passed as a non-kwargs argument")
        rtn = {t: pd.read_json(json.dumps(loaded_dict[f]), orient=orient, **kwargs) for t,f in tbl_names.items()}
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        if fill_missing_fields:
            for t,f in missing_fields:
                rtn[t][f] = self.pan_dat_factory.default_values[t][f]
        verify(fill_missing_fields or not missing_fields,
               "The following (table, field) pairs are missing fields.\n%s" % [(t, f) for t,f in missing_fields])
        for v in rtn.values():
            v.replace("inf", float("inf"), inplace=True)
            v.replace("-inf", -float("inf"), inplace=True)
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _get_table_names(self, loaded_dict):
        rtn = {}
        for table in self.pan_dat_factory.all_tables:
            rtn[table] = [c for c in loaded_dict if c.lower().replace(" ", "_") == table.lower()]
            verify(len(rtn[table]) >= 1, "Unable to recognize table %s" % table)
            verify(len(rtn[table]) <= 1, "Multiple dictionary key choices found for table %s" % table)
            rtn[table] = rtn[table][0]
        return rtn
    def write_file(self, pan_dat, json_file_path, case_space_table_names=False, orient='split',
                   index=False, indent=None, sort_keys=False, **kwargs):
        """
        write the panDat data to a collection of csv files
        :param pan_dat: the PanDat object to write
        :param json_file_path: the json file into which the data is to be written. If falsey, will return a
                               JSON  string
        :param case_space_table_names: boolean - make best guesses how to add spaces and upper case
                                       characters to table names
        :param orient: Indication of expected JSON string format. See pandas.to_json for more details.
        :param index: boolean - whether or not to write the index.
        :param indent: None. See json.dumps
        :param sort_keys: See json.dumps
        :param kwargs: additional named arguments to pass to pandas.to_json
        :return:
        caveats:  +-float("inf") will be converted to "inf", "-inf"
        """
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify("orient" not in kwargs, "orient should be passed as a non-kwargs argument")
        verify("index" not in kwargs, "index should be passed as a non-kwargs argument")

        if self._modern_pandas:
            # FYI - pandas Exception: ValueError: 'index=False' is only valid when 'orient' is 'split' or 'table'
            kwargs["index"] = index if orient in ("split", "table") else True
        case_space_table_names = case_space_table_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        rtn = {}
        for t in self.pan_dat_factory.all_tables:
            df = getattr(pan_dat, t).replace(float("inf"), "inf").replace(-float("inf"), "-inf")
            k = case_space_to_pretty(t) if case_space_table_names else t
            rtn[k] = json.loads(df.to_json(path_or_buf=None, orient=orient, **kwargs))
            if orient == 'split' and not index:
                rtn[k].pop("index", None)
        if json_file_path:
            with open(json_file_path, "w") as f:
                json.dump(rtn, f, indent=indent, sort_keys=sort_keys)
        else:
            return json.dumps(rtn, indent=indent, sort_keys=sort_keys)

class CsvPanFactory(freezable_factory(object, "_isFrozen")):
    """
    Primary class for reading/writing csv files with panDat objects.
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A CsvPanFactory will
        automatically be associated with the csv attribute of the parent
        PanDatFactory.
        :param pan_dat_factory:
        :return:
        """
        self.pan_dat_factory = pan_dat_factory
        self._isFrozen = True
    def create_pan_dat(self, dir_path, fill_missing_fields=False, **kwargs):
        """
        Create a PanDat object from a SQLite database file
        :param db_file_path: the directory containing the .csv files.
        :param fill_missing_fields: boolean. If truthy, missing fields will be filled in
                                    with their default value. Otherwise, missing fields
                                    throw an Exception.
        :param kwargs: additional named arguments to pass to pandas.read_csv
        :return: a PanDat object populated by the matching tables.
        caveats: Missing tables always throw an Exception.
                 Table names are matched with case-space insensitivity, but spaces
                 are respected for field names.
                 (ticdat supports whitespace in field names but not table names).
        """
        verify(os.path.isdir(dir_path), "%s not a directory path"%dir_path)
        tbl_names = self._get_table_names(dir_path)
        rtn = {t: pd.read_csv(f, **kwargs) for t,f in tbl_names.items()}
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        if fill_missing_fields:
            for t,f in missing_fields:
                rtn[t][f] = self.pan_dat_factory.default_values[t][f]
        verify(fill_missing_fields or not missing_fields,
               "The following (table, file_name, field) triplets are missing fields.\n%s" %
               [(t, os.path.basename(tbl_names[t]), f) for t,f in missing_fields])
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _get_table_names(self, dir_path):
        rtn = {}
        for table in self.pan_dat_factory.all_tables:
            rtn[table] = [path for f in os.listdir(dir_path) for path in [os.path.join(dir_path, f)]
                          if os.path.isfile(path) and
                          f.lower().replace(" ", "_") == "%s.csv"%table.lower()]
            verify(len(rtn[table]) >= 1, "Unable to recognize table %s" % table)
            verify(len(rtn[table]) <= 1, "Multiple possible csv files found for table %s" % table)
            rtn[table] = rtn[table][0]
        return rtn
    def write_directory(self, pan_dat, dir_path, case_space_table_names=False, index=False, **kwargs):
        """
        write the panDat data to a collection of csv files
        :param pan_dat: the PanDat object to write
        :param dir_path: the directory in which to write the csv files
                             Set to falsey if using con argument.
        :param case_space_table_names: boolean - make best guesses how to add spaces and upper case
                                       characters to table names
        :param index: boolean - whether or not to write the index.
        :param kwargs: additional named arguments to pass to pandas.to_csv
        :return:
        caveats: The row names (index) isn't written (unless kwargs indicates it should be).
        """
        verify(not os.path.isfile(dir_path), "A file is not a valid directory path")
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify("index" not in kwargs, "index should be passed as a non-kwargs argument")
        kwargs["index"] = index
        case_space_table_names = case_space_table_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        if not os.path.isdir(dir_path) :
            os.mkdir(dir_path)
        for t in self.pan_dat_factory.all_tables :
            f = os.path.join(dir_path, (case_space_to_pretty(t) if case_space_table_names else t) + ".csv")
            getattr(pan_dat, t).to_csv(f, **kwargs)

class SqlPanFactory(freezable_factory(object, "_isFrozen")):
    """
    Primary class for reading/writing SQLite files (and sqlalchemy.engine.Engine objects) with panDat objects.
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
    def create_pan_dat(self, db_file_path, con=None, fill_missing_fields=False):
        """
        Create a PanDat object from a SQLite database file
        :param db_file_path: A SQLite DB File. Set to falsey if using con argument
        :param con: sqlalchemy.engine.Engine or sqlite3.Connection.
                    Set to falsey if using db_file_path argument.
        :param fill_missing_fields: boolean. If truthy, missing fields will be filled in
                                    with their default value. Otherwise, missing fields
                                    throw an Exception.
        :return: a PanDat object populated by the matching tables.
        caveats: Missing tables always throw an Exception.
                 Table names are matched with case-space insensitivity, but spaces
                 are respected for field names.
                 (ticdat supports whitespace in field names but not table names).
        """
        verify(bool(db_file_path) != bool(con),
               "use either the con argument or the db_file_path argument but not both")
        if db_file_path:
            verify(os.path.exists(db_file_path) and not os.path.isdir(db_file_path),
                   "%s not a file path"%db_file_path)
        rtn = {}
        con_maker = lambda: _sql_con(db_file_path) if db_file_path else _DummyContextManager(con)
        with con_maker() as _:
            con_ = con or _
            for t, s in self._get_table_names(con_).items():
                rtn[t] = pd.read_sql(sql="Select * from [%s]"%s, con=con_)
        missing_fields = {(t, f) for t in rtn for f in all_fields(self.pan_dat_factory, t)
                          if f not in rtn[t].columns}
        if fill_missing_fields:
            for t,f in missing_fields:
                rtn[t][f] = self.pan_dat_factory.default_values[t][f]
        verify(fill_missing_fields or not missing_fields,
               "The following are (table, field) pairs missing from the %s file.\n%s" % (db_file_path, missing_fields))
        rtn = self.pan_dat_factory.PanDat(**rtn)
        msg = []
        assert self.pan_dat_factory.good_pan_dat_object(rtn, msg.append), str(msg)
        return rtn
    def _get_table_names(self, con):
        rtn = {}
        def try_name(name):
            try :
                con.execute("Select * from [%s]"%name)
            except :
                return False
            return True
        for table in self.pan_dat_factory.all_tables:
            rtn[table] = [t for t in all_underscore_replacements(table) if try_name(t)]
            verify(len(rtn[table]) >= 1, "Unable to recognize table %s" % table)
            verify(len(rtn[table]) <= 1, "Multiple possible tables found for table %s" % table)
            rtn[table] = rtn[table][0]
        return rtn
    def write_file(self, pan_dat, db_file_path, con=None, if_exists='replace', case_space_table_names=False):
        """
        write the panDat data to an excel file
        :param pan_dat: the PanDat object to write
        :param db_file_path: The file path of the SQLite file to create.
                             Set to falsey if using con argument.
        :param con: sqlalchemy.engine.Engine or sqlite3.Connection.
                    Set to falsey if using db_file_path argument
        :param if_exists: ‘fail’, ‘replace’ or ‘append’. How to behave if the table already exists
        :param case_space_table_names: boolean - make best guesses how to add spaces and upper case
                                          characters to table names
        :return:
        caveats: The row names (index) isn't written. The default pandas schema generation is used,
                 and thus foreign key relationships aren't written. (The code to generate foreign keys
                 is written and tested as part of TicDatFactory, and thus this shortcoming could be
                 easily rectified if need be).
        """
        # note - pandas has an unfortunate tendency to push types into SQLite columns. This can result in
        # writing-reading round trips converting your numbers to text if they are mixed type columns.
        verify(bool(db_file_path) != bool(con),
               "use either the con argument or the db_file_path argument but not both")
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        if db_file_path:
            verify(not os.path.isdir(db_file_path), "A directory is not a valid SQLLite file path")
        case_space_table_names = case_space_table_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        con_maker = lambda: _sql_con(db_file_path) if db_file_path else _DummyContextManager(con)
        with con_maker() as _:
            con_ = con or _
            for t in self.pan_dat_factory.all_tables:
                getattr(pan_dat, t).to_sql(name=case_space_to_pretty(t) if case_space_table_names else t,
                                           con=con_, if_exists=if_exists, index=False)

class XlsPanFactory(freezable_factory(object, "_isFrozen")):
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

    def create_pan_dat(self, xls_file_path, fill_missing_fields=False):
        """
        Create a PanDat object from an Excel file
        :param xls_file_path: An Excel file containing sheets whose names match
                              the table names in the schema.
        :param fill_missing_fields: boolean. If truthy, missing fields will be filled in
                                    with their default value. Otherwise, missing fields
                                    throw an Exception.
        :return: a PanDat object populated by the matching sheets.
        caveats: Missing sheets resolve to an empty table, but missing fields
                 on matching sheets throw an Exception (unless fill_missing_fields is falsey).
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
        if fill_missing_fields:
            for t,f in missing_fields:
                rtn[t][f] = self.pan_dat_factory.default_values[t][f]
        verify(fill_missing_fields or not missing_fields,
               "The following are (table, field) pairs missing from the %s file.\n%s" % (xls_file_path, missing_fields))
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
    def write_file(self, pan_dat, file_path, case_space_sheet_names=False):
        """
        write the panDat data to an excel file
        :param pan_dat: the PanDat object to write
        :param file_path: The file path of the excel file to create
        :param case_space_sheet_names: boolean - make best guesses how to add spaces and upper case
                                      characters to sheet names
        :return:
        caveats: The row names (index) isn't written.
        """
        msg = []
        verify(self.pan_dat_factory.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s"%"\n".join(msg))
        verify(not os.path.isdir(file_path), "A directory is not a valid xls file path")
        case_space_sheet_names = case_space_sheet_names and \
                                 len(set(self.pan_dat_factory.all_tables)) == \
                                 len(set(map(case_space_to_pretty, self.pan_dat_factory.all_tables)))
        writer = pd.ExcelWriter(file_path)
        for t in self.pan_dat_factory.all_tables:
            getattr(pan_dat, t).to_excel(writer, case_space_to_pretty(t) if case_space_sheet_names else t,
                                         index=False)
        writer.save()
