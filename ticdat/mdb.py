"""
Read/write ticDat objects from mdb files. Requires the pypyodbc module
PEP8
"""
import os
import sys
import inspect
import shutil
import ticdat.utils as utils
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish
from ticdat.utils import debug_break, numericish, all_underscore_replacements, find_duplicates
from ticdat.utils import create_duplicate_focused_tdf


try:
    import pypyodbc as py
except:
    py = None

try:
    import pyodbc
except:
    pyodbc = None

def _code_dir():
    return os.path.dirname(os.path.abspath(inspect.getsourcefile(_code_dir)))

def _standard_verify(generic_tables_present):
    verify(pyodbc or py,
        "pyodbc or pypyodbc (or both) needs to be installed to use this subroutine")
    # notes on implementing generic tables here
    # --> In general, look at sqlitetd.py for examples of how generic tables are handled
    # -->--> When reading data, the fields present need to be queried. See
    #        _check_tables_fields for how that can be done with a select *
    # -->--> When writing data, simply exploit utils.create_generic_free
    #        mdb.py has a good example of how that works in this context.
    verify(not generic_tables_present,
           "Generic tables functionality currently not implemented for mdb/accdb files.")


_connect = (pyodbc or py).connect if (pyodbc or py) else None

_write_new_file_works = py and (sys.platform in ('win32','cli'))
_can_mdb_unit_test = py and _write_new_file_works
_can_accdb_unit_test = pyodbc and os.path.isfile(os.path.join(_code_dir(), "blank.accdb"))

_dbq = "*.mdb, *.accdb"

def _connection_str(file):
    verify(file.endswith(".mdb") or file.endswith(".accdb"),
           "%s doesn't end with an expected file ending."%file)
    return 'Driver={Microsoft Access Driver (%s)};DBQ=%s'%(_dbq, os.path.abspath(file))

_mdb_inf = 1e+100
assert _mdb_inf < float("inf"), "sanity check on inf"
def _write_data(x) :  return max(min(x, _mdb_inf), -_mdb_inf) if numericish(x) else x

def _read_data(x) :
    if utils.numericish(x) :
        if x >= _mdb_inf :
            return float("inf")
        if x <= -_mdb_inf :
            return -float("inf")
    return x

def _brackets(l) :
    return ["[%s]"%_ for _ in l]

class MdbTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Access/MDB files with ticDat objects.
    Your system will need the required pypyodbc package if you want to actually
    do something with it.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A MdbTicDatFactory will
        automatically be associated with the mdb attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._duplicate_focused_tdf = create_duplicate_focused_tdf(tic_dat_factory)
        self._isFrozen = True
    def create_tic_dat(self, mdb_file_path, freeze_it = False):
        """
        Create a TicDat object from an Access MDB file
        :param mdb_file_path: An Access db with a consistent schema.
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the matching tables.
        caveats : Numbers with absolute values larger than 1e+100 will
                  be read as float("inf") or float("-inf")
        """
        _standard_verify(self.tic_dat_factory.generic_tables)
        rtn =  self.tic_dat_factory.TicDat(**self._create_tic_dat(mdb_file_path))
        if freeze_it:
            return self.tic_dat_factory.freeze_me(rtn)
        return rtn
    def find_duplicates(self, mdb_file_path):
        """
        Find the row counts for duplicated rows.
        :param mdb_file_path: An Access db with a consistent schema.
        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the mdb table with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        _standard_verify(self.tic_dat_factory.generic_tables)
        if not self._duplicate_focused_tdf:
            return {}
        return find_duplicates(self._duplicate_focused_tdf.mdb.create_tic_dat(mdb_file_path),
                              self._duplicate_focused_tdf)
    def _get_table_names(self, db_file_path, tables):
        rtn = {}
        with _connect(_connection_str(db_file_path)) as con:
            def try_name(name):
                with con.cursor() as cur:
                  try :
                    cur.execute("Select * from [%s]"%name)
                  except :
                    return False
                return True
            for table in tables:
                rtn[table] = [t for t in all_underscore_replacements(table) if try_name(t)]
                verify(len(rtn[table]) >= 1, "Unable to recognize table %s in MS Access file %s"%
                                  (table, db_file_path))
                verify(len(rtn[table]) <= 1, "Duplicate tables found for table %s in MS Access file %s"%
                                  (table, db_file_path))
                rtn[table] = rtn[table][0]
        return rtn
    def _check_tables_fields(self, mdb_file_path, tables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(mdb_file_path), "%s isn't a valid file path"%mdb_file_path)
        try :
            _connect(_connection_str(mdb_file_path))
        except Exception as e:
            raise TDE("Unable to open %s as MS Access file : %s"%(mdb_file_path, e.message))
        table_names = self._get_table_names(mdb_file_path, tables)
        with _connect(_connection_str(mdb_file_path)) as con:
            for table in tables:
              with con.cursor() as cur:
                cur.execute("Select * from [%s]"%table_names[table])
                fields = set(_[0].lower() for _ in cur.description)
                for field in tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ()):
                    verify(field.lower() in fields,
                        "Unable to recognize field %s in table %s for file %s"%
                        (field, table, mdb_file_path))
        return table_names

    def _create_gen_obj(self, mdbFilePath, table, table_name):
        tdf = self.tic_dat_factory
        def tableObj() :
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with _connect(_connection_str(mdbFilePath)) as con:
              with con.cursor() as cur :
                cur.execute("Select %s from [%s]"%(", ".join(_brackets(tdf.data_fields[table])),
                                                   table_name))
                for row in cur.fetchall():
                  yield list(map(_read_data, row))
        return tableObj
    def _create_tic_dat(self, mdbFilePath):
        tdf = self.tic_dat_factory
        table_names = self._check_tables_fields(mdbFilePath, tdf.all_tables)
        rtn = {}
        with _connect(_connection_str(mdbFilePath)) as con:
            for table in set(tdf.all_tables).difference(tdf.generator_tables) :
                fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
                rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
                with con.cursor() as cur :
                    cur.execute("Select %s from [%s]"%(", ".join(_brackets(fields)),
                                 table_names[table]))
                    for row in cur.fetchall():
                        pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                        data = list(map(_read_data,
                                    row[len(tdf.primary_key_fields.get(table, ())):]))
                        if dictish(rtn[table]) :
                            rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                        else :
                            rtn[table].append(data)
        for table in tdf.generator_tables :
            rtn[table] = self._create_gen_obj(mdbFilePath, table, table_names[table])
        return rtn
    @property
    def can_write_new_file(self):
        """
        :return: True if this environment can write to a new mdb database files,
                 False otherwise
        """
        return _write_new_file_works
    def write_schema(self, mdb_file_path, **field_types):
        """
        :param mdb_file_path: The file path of the mdb database to create
        :param field_types: Named arguments are table names. Argument values
                            are mapping of field name to field type.
                            Allowable field types are text, double and int
                            If missing, primary key fields are text, and data
                            fields are double
        :return:
        """
        verify(not self.tic_dat_factory.generic_tables,
               "generic_tables are not compatible with write_schema. " +
               "Use write_file instead.")
        _standard_verify(self.tic_dat_factory.generic_tables)
        verify(dictish(field_types), "field_types should be a dict")
        for k,v in field_types.items() :
            verify(k in self.tic_dat_factory.all_tables, "%s isn't a table name"%k)
            verify(dictish(v), "Need a mapping from field names to field types for %s"%k)
            for fld,type_ in v.items() :
                verify(fld in self.tic_dat_factory.primary_key_fields.get(k, ()) +
                          self.tic_dat_factory.data_fields.get(k, ()),
                       "%s isn't a field name for table %s"%(fld, k))
                verify(type_ in ("text", "double", "int"),
                       "For table %s, field %s, %s isn't one of (text, double, int)"%(k, fld, type_))
        get_fld_type = lambda tbl, fld, default : field_types.get(tbl, {}).get(fld, default)
        if not os.path.exists(mdb_file_path) :
            verify(mdb_file_path.endswith(".mdb") or mdb_file_path.endswith(".accdb"),
                   "For file creation, specify either an .mdb or .accdb file name")
            if mdb_file_path.endswith(".mdb"):
                verify(py, "pypyodbc needs to be installed to create a new .mdb file")
                verify(self.can_write_new_file, "Creating a new file not enabled for this OS")
                py.win_create_mdb(mdb_file_path)
            else:
                blank_accdb = os.path.join(_code_dir(), "blank.accdb")
                verify(os.path.exists(blank_accdb) and os.path.isfile(blank_accdb),
                    "You need to run accdb_create_setup.py as a post pip install operation " +
                    "to configure writing to new .accdb files.")
                shutil.copy(blank_accdb, mdb_file_path)
        with _connect(_connection_str(mdb_file_path)) as con:
            for t in self.tic_dat_factory.all_tables:
                str = "Create TABLE [%s] (\n"%t
                strl = ["[%s] %s"%(f, get_fld_type(t, f, "text")) for
                        f in self.tic_dat_factory.primary_key_fields.get(t, ())] + \
                       ["[%s] %s"%(f, get_fld_type(t, f, "double"))
                        for f in self.tic_dat_factory.data_fields.get(t, ())]
                if self.tic_dat_factory.primary_key_fields.get(t) :
                    strl.append("PRIMARY KEY(%s)"%",".join
                        (_brackets(self.tic_dat_factory.primary_key_fields[t])))
                str += ",\n".join(strl) + "\n);"
                con.cursor().execute(str).commit()
    def write_file(self, tic_dat, mdb_file_path, allow_overwrite = False):
        """
        write the ticDat data to an SQLite database file
        :param tic_dat: the data object to write
        :param mdb_file_path: the file path of the SQLite database to populate
        :param allow_overwrite: boolean - are we allowed to overwrite pre-existing data
        :return:
        caveats : Numbers with absolute values larger than 1e+100 will
                  be written as 1e+100 or -1e+100
        NB - thrown Exceptions of the form "Data type mismatch in criteria expression"
             generally result either from Access's inability to store different data
             types in the same field, or from a mismatch between the data object
             and the default field types ticdat uses when creating an Access schema.
             For the latter, feel free to call the write_schema function on the data
             file first with explicitly identified field types.
        """
        _standard_verify(self.tic_dat_factory.generic_tables)
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(mdb_file_path), "A directory is not a valid Access file path")
        if not os.path.exists(mdb_file_path) :
            self.write_schema(mdb_file_path)
        table_names = self._check_tables_fields(mdb_file_path, self.tic_dat_factory.all_tables)
        with _connect(_connection_str(mdb_file_path)) as con:
            for t in self.tic_dat_factory.all_tables:
                verify(table_names[t] == t, "Failed to find table %s in path %s"%
                                            (t, mdb_file_path))
                if not allow_overwrite :
                    with con.cursor() as cur :
                        cur.execute("Select * from %s"%t)
                        verify(not any(True for _ in cur.fetchall()),
                            "allow_overwrite is False, but there are already data records in %s"%t)
                con.cursor().execute("Delete from %s"%t).commit() if allow_overwrite else None
                _t = getattr(tic_dat, t)
                if dictish(_t) :
                    primary_keys = tuple(self.tic_dat_factory.primary_key_fields[t])
                    for pk_row, sql_data_row in _t.items() :
                        _items = sql_data_row.items()
                        fields = _brackets(primary_keys + tuple(x[0] for x in _items))
                        data_row = ((pk_row,) if len(primary_keys)==1 else pk_row) + \
                                  tuple(_write_data(x[1]) for x in _items)
                        assert len(data_row) == len(fields)
                        str = "INSERT INTO %s (%s) VALUES (%s)"%\
                              (t, ",".join(fields), ",".join("?" for _ in fields))
                        con.cursor().execute(str, data_row).commit()
                else :
                    for sql_data_row in (_t if containerish(_t) else _t()) :
                        str = "INSERT INTO %s (%s) VALUES (%s)"%(t,
                          ",".join(_brackets(sql_data_row.keys())),
                          ",".join(["?"]*len(sql_data_row)))
                        con.cursor().execute(str,tuple(map(_write_data, sql_data_row.values())))
