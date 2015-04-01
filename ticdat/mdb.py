"""
Read/write ticDat objects from mdb files. Requires the pypyodbc module
"""

from utils import freezableFactory, TicDatError, verify, stringish, dictish, containerish, debugBreak
import os
import sys
import utils



try:
    import pypyodbc as py
    importWorked=True
except:
    importWorked=False

_writeNewFileWorks = sys.platform in ('win32','cli')

def _connectionString(file):
    return 'Driver={Microsoft Access Driver (*.mdb)};DBQ=%s'%os.path.abspath(file)

_mdbInfinity = 1e+100
def _writeData(x) :
    if x == float("inf") :
        return _mdbInfinity
    if x == -float("inf") :
        return -_mdbInfinity
    return x

def _readData(x) :
    if utils.numericish(x) :
        if x >= _mdbInfinity :
            return float("inf")
        if x <= -_mdbInfinity :
            return -float("inf")
    return x



class MdbTicFactory(freezableFactory(object, "_isFrozen")) :
    """
    Primary class for reading/writing Access/MDB files with ticDat objects.
    """
    def __init__(self, ticDatFactory):
        """
        Don't call this function explicitly. A MdbTicDatFactory will automatically be associated with the parent
        TicDatFactory if your system has the required pypyodbc packages.
        :param ticDatFactory:
        :return:
        """
        assert importWorked, "don't create this otherwise"
        self.tic_dat_factory = ticDatFactory
        self._isFrozen = True
    def create_tic_dat(self, mdbFilePath):
        """
        Create a TicDat object from an Access MDB file
        :param mdbFilePath: An Access db with a consistent schema.
        :return: a TicDat object populated by the matching tables.
        """
        return self.tic_dat_factory.TicDat(**self._createTicDat(mdbFilePath))
    def create_frozen_tic_dat(self, mdbFilePath):
        """
        Create a FrozenTicDat object from an Access MDB file
        :param mdbFilePath:An Access db with a consistent schema.
        :return: a TicDat object populated by the matching table.
        """
        return self.tic_dat_factory.FrozenTicDat(**self._createTicDat(mdbFilePath))
    def _checkTablesAndFields(self, mdbFilePath, allTables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(mdbFilePath), "%s isn't a valid file path"%mdbFilePath)
        try :
            py.connect(_connectionString(mdbFilePath))
        except Exception as e:
            raise TDE("Unable to open %s as SQLite file : %s"%(mdbFilePath, e.message))
        with py.connect(_connectionString(mdbFilePath)) as con:
            for table in allTables :
              with con.cursor() as cur:
                try :
                    cur.execute("Select * from %s"%table)
                except :
                    raise TDE("Unable to recognize table %s in SQLite file %s"%(table, mdbFilePath))
                fields = set(_[0].lower() for _ in cur.description)
                for field in tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ()) :
                    verify(field.lower() in fields,
                        "Unable to recognize field %s in table %s for file %s"%(field, table, mdbFilePath))

    def _createGeneratorObj(self, mdbFilePath, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            self._checkTablesAndFields(mdbFilePath, (table,))
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with py.connect(_connectionString(mdbFilePath)) as con:
              with con.cursor() as cur :
                cur.execute("Select %s from %s"%(", ".join(tdf.data_fields[table]), table))
                for row in cur.fetchall():
                  yield map(_readData, row)
        return tableObj
    def _createTicDat(self, mdbFilePath):
        tdf = self.tic_dat_factory
        self._checkTablesAndFields(mdbFilePath, tdf.all_tables)
        rtn = {}
        with py.connect(_connectionString(mdbFilePath)) as con:
            for table in set(tdf.all_tables).difference(tdf.generator_tables) :
                fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
                rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
                with con.cursor() as cur :
                    cur.execute("Select %s from %s"%(", ".join(fields), table))
                    for row in cur.fetchall():
                        pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                        data = map(_readData, row[len(tdf.primary_key_fields.get(table, ())):])
                        if dictish(rtn[table]) :
                            rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                        else :
                            rtn[table].append(data)
        for table in tdf.generator_tables :
            rtn[table] = self._createGeneratorObj(mdbFilePath, table)
        return rtn
    @property
    def can_write_new_file(self):
        """
        :return: True if this environment can write to a new mdb database files, false otherwise
        """
        return _writeNewFileWorks
    def write_schema(self, mdbFilePath, **fieldTypes):
        """
        :param mdbFilePath: the file path of the mdb database to create
        :param fieldTypes: a mapping of table, field pairs to field types. Allowable field types are text, float and int
                           if missing, primary key fields are text, and data fields are float
        :return:
        """
        verify(dictish(fieldTypes), "fieldTypes should be a dict")
        for k,v in fieldTypes.items() :
            verify(k in self.tic_dat_factory.all_tables, "%s isn't a table name"%k)
            verify(dictish(v), "Need a mapping from field names to field types for %s"%k)
            for fld,typ in v.items() :
                verify(fld in self.tic_dat_factory.primary_key_fields.get(k[0], ()) +
                          self.tic_dat_factory.data_fields.get(k[0], ()),
                       "%s isn't a field name for table %s"%(fld, k))
                verify(typ in ("text", "float", "int"), "For table %s, field %s, %s isn't one of (text, float, int)"
                        %(k, fld, typ))
        getFieldType = lambda tbl, fld, default : fieldTypes.get(tbl, {}).get(fld, default)
        if not os.path.exists(mdbFilePath) :
            verify(self.can_write_new_file, "Writing to a new file not enabled")
            py.win_create_mdb(mdbFilePath)
        with py.connect(_connectionString(mdbFilePath)) as con:
            for t in self.tic_dat_factory.all_tables:
                str = "Create TABLE %s (\n"%t
                strl = ["%s %s"%(f, getFieldType(t, f, "text")) for
                        f in self.tic_dat_factory.primary_key_fields.get(t, ())] + \
                       ["%s %s"%(f, getFieldType(t, f, "float"))
                        for f in self.tic_dat_factory.data_fields.get(t, ())]
                if self.tic_dat_factory.primary_key_fields.get(t) :
                    strl.append("PRIMARY KEY(%s)"%",".join(self.tic_dat_factory.primary_key_fields[t]))
                str += ",\n".join(strl) +  "\n);"
                con.cursor().execute(str).commit()
    def write_file(self, ticDat, mdbFilePath, allow_overwrite = False):
        """
        write the ticDat data to an SQLite database file
        :param ticDat: the data object to write
        :param mdbFilePath: the file path of the SQLite database to populate
        :param allow_overwrite: boolean - are we allowed to overwrite pre-existing data
        :return:
        """
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(ticDat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(mdbFilePath), "A directory is not a valid Access file path")
        if not os.path.exists(mdbFilePath) :
            self.write_schema(mdbFilePath)
        self._checkTablesAndFields(mdbFilePath, self.tic_dat_factory.all_tables)
        with py.connect(_connectionString(mdbFilePath)) as con:
            for t in self.tic_dat_factory.all_tables:
                if not allow_overwrite :
                    with con.cursor() as cur :
                        cur.execute("Select * from %s"%t)
                        verify(not any(True for _ in cur.fetchall()),
                            "allow_overwrite is False, but there are already data records in %s"%t)
                con.cursor().execute("Delete from %s"%t).commit() if allow_overwrite else None
                _t = getattr(ticDat, t)
                if dictish(_t) :
                    primaryKeys = tuple(self.tic_dat_factory.primary_key_fields[t])
                    for pkRow, sqlDataRow in _t.items() :
                        _items = sqlDataRow.items()
                        fields = primaryKeys + tuple(x[0] for x in _items)
                        dataRow = ((pkRow,) if len(primaryKeys)==1 else pkRow) + \
                                  tuple(_writeData(x[1]) for x in _items)
                        assert len(dataRow) == len(fields)
                        str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(fields), ",".join("?" for _ in fields))
                        con.cursor().execute(str, dataRow).commit()
                else :
                    for sqlDataRow in (_t if containerish(_t) else _t()) :
                        str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(sqlDataRow.keys()),
                          ",".join(["?"]*len(sqlDataRow)))
                        con.cursor().execute(str,tuple(map(_writeData, sqlDataRow.values())))
