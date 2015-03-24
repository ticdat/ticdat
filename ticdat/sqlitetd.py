from utils import freezableFactory, TicDatError, verify, stringish, dictish, containerish, debugBreak
import os
import utils



try:
    import sqlite3 as sql
    importWorked=True
except:
    importWorked=False

def _readDataFormat(x) :
    if stringish(x) and x.lower() in ("inf", "-inf") :
        return float(x)
    if stringish(x) and x.lower()  == "true" :
        return True
    if stringish(x) and x.lower()  == "false" :
        return False
    return x

def _insertFormat(x) :
    # note that [1==True, 0==False, 1 is not True, 0 is not False] is all part of Python
    if x in (float("inf"), -float("inf")) or stringish(x) or (x is True) or (x is False):
        return  "'%s'"%x
    if x is None:
        return "null"
    return  str(x)

def _sqlConnect(dbFile, foreignKeys = True) :
    con = sql.connect(dbFile)
    con.execute("PRAGMA foreign_keys = %s"%("ON" if foreignKeys else "OFF"))
    con.commit() # remember - either close or commit to push changes!!
    return con

class SQLiteTicFactory(freezableFactory(object, "_isFrozen")) :
    """
    Primary class for reading/writing SQLite files with ticDat objects.
    """
    def __init__(self, ticDatFactory):
        """
        Don't call this function explicitly. A XlsTicDatFactory will automatically be associated with the parent
        TicDatFactory if your system has the required xlrd, xlwt packages.
        :param ticDatFactory:
        :return:
        """
        assert importWorked, "don't create this otherwise"
        self.tic_dat_factory = ticDatFactory
        self._isFrozen = True
    def create_tic_dat(self, sqlFilePath):
        """
        Create a TicDat object from an Excel file
        :param sqlFilePath: A SQLite db with a consistent schema.
        :return: a TicDat object populated by the matching tables.
        """
        return self.tic_dat_factory.TicDat(**self._createTicDat(sqlFilePath))
    def create_frozen_tic_dat(self, sqlFilePath):
        """
        Create a FrozenTicDat object from an Excel file
        :param sqlFilePath:A SQLite db with a consistent schema.
        :return: a TicDat object populated by the matching table.
        """
        return self.tic_dat_factory.FrozenTicDat(**self._createTicDat(sqlFilePath))
    def _checkTablesAndFields(self, sqlFilePath, allTables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(sqlFilePath), "%s isn't a valid file path"%sqlFilePath)
        try :
            sql.connect(sqlFilePath)
        except Exception as e:
            raise TDE("Unable to open %s as SQLite file : %s"%(sqlFilePath, e.message))
        with sql.connect(sqlFilePath) as con:
            for table in allTables :
                try :
                    con.execute("Select * from %s"%table)
                except :
                    raise TDE("Unable to recognize table %s in SQLite file %s"%(table, sqlFilePath))
                for field in tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ()) :
                    try :
                        con.execute("Select %s from %s"%(field,table))
                    except :
                        raise TDE("Unable to recognize field %s in table %s for file %s"%(field, table, sqlFilePath))
    def _createGeneratorObj(self, sqlFilePath, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            self._checkTablesAndFields(sqlFilePath, (table,))
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with sql.connect(sqlFilePath) as con:
                for row in con.execute("Select %s from %s"%(", ".join(tdf.data_fields[table]), table)) :
                    yield map(_readDataFormat, row)
        return tableObj
    def _createTicDat(self, sqlFilePath):
        tdf = self.tic_dat_factory
        self._checkTablesAndFields(sqlFilePath, tdf.all_tables)
        rtn = {}
        with sql.connect(sqlFilePath) as con:
            for table in set(tdf.all_tables).difference(tdf.generator_tables) :
                fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
                rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
                for row in con.execute("Select %s from %s"%(", ".join(fields), table)) :
                        pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                        data = map(_readDataFormat, row[len(tdf.primary_key_fields.get(table, ())):])
                        if dictish(rtn[table]) :
                            rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                        else :
                            rtn[table].append(data)
        for table in tdf.generator_tables :
            rtn[table] = self._createGeneratorObj(sqlFilePath, table)
        return rtn
    def _orderedTables(self):
        rtn = []
        def processTable(t) :
            if t not in rtn:
                for fks in self.tic_dat_factory.foreign_keys.get(t, ()) :
                    processTable(fks["foreignTable"])
                rtn.append(t)
        map(processTable, self.tic_dat_factory.all_tables)
        return tuple(rtn)
    def write_file(self, ticDat, sqlFilePath, allow_overwrite = False):
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(ticDat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid ticDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(sqlFilePath), "A directory is not a valid SQLite file path")
        if not os.path.exists(sqlFilePath) :
            with _sqlConnect(sqlFilePath, foreignKeys=False) as con:
                for t in self._orderedTables() :
                    str = "Create TABLE %s (\n"%t
                    strl = [f for f in self.tic_dat_factory.primary_key_fields.get(t, ())] + \
                           [f + " default %s"%self.tic_dat_factory.default_values.get(t, {}).get(f, 0)
                            for f in self.tic_dat_factory.data_fields.get(t, ())]
                    for fks in self.tic_dat_factory.foreign_keys.get(t, ()) :
                        nativeFields, foreignFields = zip(* (fks["mappings"].items()))
                        strl.append("FOREIGN KEY(%s) REFERENCES %s(%s)"%(",".join(nativeFields),
                                     fks["foreignTable"], ",".join(foreignFields)))
                    if self.tic_dat_factory.primary_key_fields.get(t) :
                        strl.append("PRIMARY KEY(%s)"%",".join(self.tic_dat_factory.primary_key_fields[t]))
                    str += ",\n".join(strl) +  "\n);"
                    con.execute(str)

        self._checkTablesAndFields(sqlFilePath, self.tic_dat_factory.all_tables)
        with _sqlConnect(sqlFilePath, foreignKeys=False) as con:
            for t in self.tic_dat_factory.all_tables:
                verify(allow_overwrite or not any(True for _ in  con.execute("Select * from %s"%t)),
                        "allow_overwrite is False, but there are already data records in %s"%t)
                con.execute("Delete from %s"%t) if allow_overwrite else None
                _t = getattr(ticDat, t)
                if dictish(_t) :
                 primaryKeys = tuple(self.tic_dat_factory.primary_key_fields[t])
                 for pkRow, sqlDataRow in _t.items() :
                    _items = sqlDataRow.items()
                    fields = primaryKeys + tuple(x[0] for x in _items)
                    dataRow = ((pkRow,) if len(primaryKeys)==1 else pkRow) + tuple(x[1] for x in _items)
                    assert len(dataRow) == len(fields)
                    str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(fields),
                          ",".join("%s" for _ in fields))
                    con.execute(str%tuple(map(_insertFormat, dataRow)))
                else :
                 for sqlDataRow in (_t if containerish(_t) else _t()) :
                    str = "INSERT INTO %s (%s) VALUES (%s)"%(t, ",".join(sqlDataRow.keys()),
                          ",".join(["%s"]*len(sqlDataRow)))
                    con.execute(str%tuple(map(_insertFormat, sqlDataRow.values())))





