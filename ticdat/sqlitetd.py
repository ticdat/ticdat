import utils as utls
from utils import freezableFactory, TicDatError, verify, stringish, dictish
import os
from collections import defaultdict
from itertools import product


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
        return self.ticDatFactory.TicDat(**self._createTicDat(sqlFilePath))
    def create_frozen_tic_dat(self, sqlFilePath):
        """
        Create a FrozenTicDat object from an Excel file
        :param sqlFilePath:A SQLite db with a consistent schema.
        :return: a TicDat object populated by the matching table.
        """
        return self.ticDatFactory.FrozenTicDat(**self._createTicDat(sqlFilePath))
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
                        TDE("Unable to recognize field %s in table %s for file %s"%(field, table, sqlFilePath))
    def _createGeneratorObj(self, sqlFilePath, table):
        tdf = self.tic_dat_factory
        def tableObj() :
            self._checkTablesAndFields(sqlFilePath, (table,))
            assert (not tdf.primary_key_fields.get(table)) and (tdf.primary_key_fields.get(table))
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
    def write_data(self, ticDat, sqlFilePath, allow_overwrite = False):
        if not os.path.exists(sqlFilePath) :
            # create the schema in the file path
            pass




