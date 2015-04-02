"""
Read/write ticDat objects from SQLite files. Requires the sqlite3 module
PEP8
"""
import os
import utils
from utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish

try:
    import sqlite3 as sql
    import_worked=True
except:
    import_worked=False

def _read_data_format(x) :
    if stringish(x) and x.lower() in ("inf", "-inf") :
        return float(x)
    if stringish(x) and x.lower()  == "true" :
        return True
    if stringish(x) and x.lower()  == "false" :
        return False
    return x

def _insert_format(x) :
    # note that [1==True, 0==False, 1 is not True, 0 is not False] is all part of Python
    if x in (float("inf"), -float("inf")) or stringish(x) or (x is True) or (x is False):
        return  "'%s'"%x
    if x is None:
        return "null"
    return  str(x)

def _sql_con(dbFile, foreign_keys = True) :
    con = sql.connect(dbFile)
    con.execute("PRAGMA foreign_keys = %s"%("ON" if foreign_keys else "OFF"))
    con.commit() # remember - either close or commit to push changes!!
    return con

class SQLiteTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing SQLite files with ticDat objects.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A SQLiteTicFactory will
        automatically be associated with the parent TicDatFactory if your
        system has the required sqlite3 packages.
        :param tic_dat_factory:
        :return:
        """
        assert import_worked, "don't create this otherwise"
        self.tic_dat_factory = tic_dat_factory
        self._isFrozen = True
    def create_tic_dat(self, sql_file_path):
        """
        Create a TicDat object from a SQLite file
        :param sql_file_path: A SQLite db with a consistent schema.
        :return: a TicDat object populated by the matching tables.
        caveats : "inf" and "-inf" (case insensitive) are read as floats
        """
        return self.tic_dat_factory.TicDat(**self._create_tic_dat(sql_file_path))
    def create_frozen_tic_dat(self, sql_file_path):
        """
        Create a FrozenTicDat object from an SQLite file
        :param sql_file_path:A SQLite db with a consistent schema.
        :return: a TicDat object populated by the matching table.
        caveats : "inf" and "-inf" (case insensitive) are read as floats
        """
        return self.tic_dat_factory.FrozenTicDat(**self._create_tic_dat(sql_file_path))
    def _check_tables_fields(self, sql_file_path, tables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(sql_file_path), "%s isn't a valid file path"%sql_file_path)
        try :
            sql.connect(sql_file_path)
        except Exception as e:
            raise TDE("Unable to open %s as SQLite file : %s"%(sql_file_path, e.message))
        with sql.connect(sql_file_path) as con:
            for table in tables :
                try :
                    con.execute("Select * from %s"%table)
                except :
                    raise TDE("Unable to recognize table %s in SQLite file %s"%
                              (table, sql_file_path))
                for field in tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ()):
                    try :
                        con.execute("Select %s from %s"%(field,table))
                    except :
                        raise TDE("Unable to recognize field %s in table %s for file %s"%
                                  (field, table, sql_file_path))
    def _create_gen_obj(self, sql_file_path, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            self._check_tables_fields(sql_file_path, (table,))
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with sql.connect(sql_file_path) as con:
                for row in con.execute("Select %s from %s"%
                                        (", ".join(tdf.data_fields[table]), table)):
                    yield map(_read_data_format, row)
        return tableObj
    def _create_tic_dat(self, sql_file_path):
        tdf = self.tic_dat_factory
        self._check_tables_fields(sql_file_path, tdf.all_tables)
        rtn = {}
        with sql.connect(sql_file_path) as con:
            for table in set(tdf.all_tables).difference(tdf.generator_tables) :
                fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
                rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
                for row in con.execute("Select %s from %s"%(", ".join(fields), table)) :
                    pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                    data = map(_read_data_format, row[len(tdf.primary_key_fields.get(table, ())):])
                    if dictish(rtn[table]) :
                        rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                    else :
                        rtn[table].append(data)
        for table in tdf.generator_tables :
            rtn[table] = self._create_gen_obj(sql_file_path, table)
        return rtn
    def _ordered_tables(self):
        rtn = []
        def processTable(t) :
            if t not in rtn:
                for fks in self.tic_dat_factory.foreign_keys.get(t, ()) :
                    processTable(fks["foreignTable"])
                rtn.append(t)
        map(processTable, self.tic_dat_factory.all_tables)
        return tuple(rtn)
    def write_schema(self, sqlFilePath):
        """
        :param sqlFilePath: the file path of the SQLite database to create
        :return:
        """
        with _sql_con(sqlFilePath, foreign_keys=False) as con:
            for t in self._ordered_tables() :
                str = "Create TABLE %s (\n"%t
                strl = [f for f in self.tic_dat_factory.primary_key_fields.get(t, ())] + \
                       [f + " default %s"%self.tic_dat_factory.default_values.get(t, {}).get(f, 0)
                        for f in self.tic_dat_factory.data_fields.get(t, ())]
                for fks in self.tic_dat_factory.foreign_keys.get(t, ()) :
                    nativefields, foreignfields = zip(* (fks["mappings"].items()))
                    strl.append("FOREIGN KEY(%s) REFERENCES %s(%s)"%(",".join(nativefields),
                                 fks["foreignTable"], ",".join(foreignfields)))
                if self.tic_dat_factory.primary_key_fields.get(t) :
                    strl.append("PRIMARY KEY(%s)"%(",".join
                                                   (self.tic_dat_factory.primary_key_fields[t])))
                str += ",\n".join(strl) + "\n);"
                con.execute(str)
    def write_file(self, tic_dat, sql_file_path, allow_overwrite = False):
        """
        write the ticDat data to an SQLite database file
        :param tic_dat: the data object to write
        :param sql_file_path: the file path of the SQLite database to populate
        :param allow_overwrite: boolean - are we allowed to overwrite pre-existing data
        :return:
        caveats : float("inf"), float("-inf") are written as "inf", "-inf"
        """
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(sql_file_path), "A directory is not a valid SQLite file path")
        if not os.path.exists(sql_file_path) :
            self.write_schema(sql_file_path)
        self._check_tables_fields(sql_file_path, self.tic_dat_factory.all_tables)
        with _sql_con(sql_file_path, foreign_keys=False) as con:
            for t in self.tic_dat_factory.all_tables:
                verify(allow_overwrite or not any(True for _ in  con.execute("Select * from %s"%t)),
                        "allow_overwrite is False, but there are already data records in %s"%t)
                con.execute("Delete from %s"%t) if allow_overwrite else None
                _t = getattr(tic_dat, t)
                if dictish(_t) :
                 primarykeys = tuple(self.tic_dat_factory.primary_key_fields[t])
                 for pkRow, sqldatarow in _t.items() :
                    _items = sqldatarow.items()
                    fields = primarykeys + tuple(x[0] for x in _items)
                    dataRow = ((pkRow,) if len(primarykeys)==1 else pkRow) + \
                              tuple(x[1] for x in _items)
                    assert len(dataRow) == len(fields)
                    str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(fields),
                          ",".join("%s" for _ in fields))
                    con.execute(str%tuple(map(_insert_format, dataRow)))
                else :
                 for sqldatarow in (_t if containerish(_t) else _t()) :
                    str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(sqldatarow.keys()),
                          ",".join(["%s"]*len(sqldatarow)))
                    con.execute(str%tuple(map(_insert_format, sqldatarow.values())))





