"""
Read/write ticDat objects from mdb files. Requires the pypyodbc module
PEP8
"""
import os
import sys
import utils
from utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish
from utils import  debug_break, numericish

try:
    import pypyodbc as py
    import_worked=True
except:
    import_worked=False

_write_new_file_works = sys.platform in ('win32','cli')

def _connection_str(file):
    return 'Driver={Microsoft Access Driver (*.mdb)};DBQ=%s'%os.path.abspath(file)

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



class MdbTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Access/MDB files with ticDat objects.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A MdbTicDatFactory will
        automatically be associated with the parent TicDatFactory if your
        system has the required pypyodbc packages.
        :param tic_dat_factory:
        :return:
        """
        assert import_worked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
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
        Rtn = self.tic_dat_factory.TicDat
        if freeze_it:
            Rtn = self.tic_dat_factory.FrozenTicDat
        return Rtn(**self._create_tic_dat(mdb_file_path))
    def _check_tables_fields(self, mdb_file_path, tables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(mdb_file_path), "%s isn't a valid file path"%mdb_file_path)
        try :
            py.connect(_connection_str(mdb_file_path))
        except Exception as e:
            raise TDE("Unable to open %s as SQLite file : %s"%(mdb_file_path, e.message))
        with py.connect(_connection_str(mdb_file_path)) as con:
            for table in tables:
              with con.cursor() as cur:
                try :
                    cur.execute("Select * from %s"%table)
                except :
                    raise TDE("Unable to recognize table %s in SQLite file %s"
                              %(table, mdb_file_path))
                fields = set(_[0].lower() for _ in cur.description)
                for field in tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ()):
                    verify(field.lower() in fields,
                        "Unable to recognize field %s in table %s for file %s"%
                        (field, table, mdb_file_path))

    def _create_gen_obj(self, mdbFilePath, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            self._check_tables_fields(mdbFilePath, (table,))
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with py.connect(_connection_str(mdbFilePath)) as con:
              with con.cursor() as cur :
                cur.execute("Select %s from %s"%(", ".join(tdf.data_fields[table]), table))
                for row in cur.fetchall():
                  yield map(_read_data, row)
        return tableObj
    def _create_tic_dat(self, mdbFilePath):
        tdf = self.tic_dat_factory
        self._check_tables_fields(mdbFilePath, tdf.all_tables)
        rtn = {}
        with py.connect(_connection_str(mdbFilePath)) as con:
            for table in set(tdf.all_tables).difference(tdf.generator_tables) :
                fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
                rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
                with con.cursor() as cur :
                    cur.execute("Select %s from %s"%(", ".join(fields), table))
                    for row in cur.fetchall():
                        pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                        data = map(_read_data, row[len(tdf.primary_key_fields.get(table, ())):])
                        if dictish(rtn[table]) :
                            rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                        else :
                            rtn[table].append(data)
        for table in tdf.generator_tables :
            rtn[table] = self._create_gen_obj(mdbFilePath, table)
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
        :param field_types: A mapping of table, field pairs to field types.
                            Allowable field types are text, float and int
                            If missing, primary key fields are text, and data
                            fields are float
        :return:
        """
        verify(dictish(field_types), "field_types should be a dict")
        for k,v in field_types.items() :
            verify(k in self.tic_dat_factory.all_tables, "%s isn't a table name"%k)
            verify(dictish(v), "Need a mapping from field names to field types for %s"%k)
            for fld,type_ in v.items() :
                verify(fld in self.tic_dat_factory.primary_key_fields.get(k[0], ()) +
                          self.tic_dat_factory.data_fields.get(k[0], ()),
                       "%s isn't a field name for table %s"%(fld, k))
                verify(type_ in ("text", "float", "int"),
                       "For table %s, field %s, %s isn't one of (text, float, int)"%(k, fld, type_))
        get_fld_type = lambda tbl, fld, default : field_types.get(tbl, {}).get(fld, default)
        if not os.path.exists(mdb_file_path) :
            verify(self.can_write_new_file, "Writing to a new file not enabled")
            py.win_create_mdb(mdb_file_path)
        with py.connect(_connection_str(mdb_file_path)) as con:
            for t in self.tic_dat_factory.all_tables:
                str = "Create TABLE %s (\n"%t
                strl = ["%s %s"%(f, get_fld_type(t, f, "text")) for
                        f in self.tic_dat_factory.primary_key_fields.get(t, ())] + \
                       ["%s %s"%(f, get_fld_type(t, f, "float"))
                        for f in self.tic_dat_factory.data_fields.get(t, ())]
                if self.tic_dat_factory.primary_key_fields.get(t) :
                    strl.append("PRIMARY KEY(%s)"%",".join
                        (self.tic_dat_factory.primary_key_fields[t]))
                str += ",\n".join(strl) +  "\n);"
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
        """
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(mdb_file_path), "A directory is not a valid Access file path")
        if not os.path.exists(mdb_file_path) :
            self.write_schema(mdb_file_path)
        self._check_tables_fields(mdb_file_path, self.tic_dat_factory.all_tables)
        with py.connect(_connection_str(mdb_file_path)) as con:
            for t in self.tic_dat_factory.all_tables:
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
                        fields = primary_keys + tuple(x[0] for x in _items)
                        data_row = ((pk_row,) if len(primary_keys)==1 else pk_row) + \
                                  tuple(_write_data(x[1]) for x in _items)
                        assert len(data_row) == len(fields)
                        str = "INSERT INTO %s (%s) VALUES (%s)"%\
                              (t, ",".join(fields), ",".join("?" for _ in fields))
                        con.cursor().execute(str, data_row).commit()
                else :
                    for sql_data_row in (_t if containerish(_t) else _t()) :
                        str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(sql_data_row.keys()),
                          ",".join(["?"]*len(sql_data_row)))
                        con.cursor().execute(str,tuple(map(_write_data, sql_data_row.values())))
