"""
Read/write ticDat objects from SQLite files. Requires the sqlite3 module
PEP8
"""
import os
from collections import defaultdict
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, dictish, containerish
from ticdat.utils import FrozenDict, all_underscore_replacements, find_duplicates
from ticdat.utils import create_duplicate_focused_tdf, create_generic_free

try:
    import sqlite3 as sql
except:
    sql = None

_can_unit_test = sql

def _read_data_format(x) :
    if stringish(x) and x.lower() in ("inf", "-inf") :
        return float(x)
    if stringish(x) and x.lower()  == "true" :
        return True
    if stringish(x) and x.lower()  == "false" :
        return False
    return x

def _fix_str(x):
    """
    can't fix all the strings. won't work right if some jerk wants to insert '' or some other multiple
    of consecutive of '. need bound parameters for that, which precludes storing things in readable sql
    """
    rtn = []
    for i,c in enumerate(x):
        preceeding = x[i-1] if x else ""
        following = x[i+1] if i < len(x)-1 else ""
        if c != "'" or "'" in (preceeding, following):
            rtn.append(c)
        else:
            rtn.append("''")
    return "".join(rtn)

def _insert_format(x) :
    # note that [1==True, 0==False, 1 is not True, 0 is not False] is all part of Python
    if stringish(x):
        return "'%s'"%_fix_str(x)
    if x in (float("inf"), -float("inf")) or (x is True) or (x is False):
        return "'%s'"%x
    if x is None:
        return "null"
    return  str(x)

def _sql_con(dbFile, foreign_keys = True) :
    con = sql.connect(dbFile)
    con.execute("PRAGMA foreign_keys = %s"%("ON" if foreign_keys else "OFF"))
    con.commit() # remember - either close or commit to push changes!!
    return con

def _brackets(l) :
    return ["[%s]"%_ for _ in l]

class SQLiteTicFactory(freezable_factory(object, "_isFrozen")) :
    """
    Primary class for reading/writing SQLite files with ticDat objects.
    You need the sqlite3 package to be installed to use it.
    """
    def __init__(self, tic_dat_factory):
        """
        Don't call this function explicitly. A SQLiteTicFactory will
        automatically be associated with the sql attribute of the parent
        TicDatFactory.
        :param tic_dat_factory:
        :return:
        """
        self.tic_dat_factory = tic_dat_factory
        self._duplicate_focused_tdf = create_duplicate_focused_tdf(tic_dat_factory)
        self._isFrozen = True
    def _Rtn(self, freeze_it):
        if freeze_it:
            return lambda *args, **kwargs : self.tic_dat_factory.freeze_me(
                    self.tic_dat_factory.TicDat(*args, **kwargs))
        return self.tic_dat_factory.TicDat
    def create_tic_dat(self, db_file_path, freeze_it = False):
        """
        Create a TicDat object from a SQLite database file
        :param db_file_path: A SQLite db with a consistent schema.
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the matching tables.
        caveats : "inf" and "-inf" (case insensitive) are read as floats
        """
        verify(sql, "sqlite3 needs to be installed to use this subroutine")
        return self._Rtn(freeze_it)(**self._create_tic_dat(db_file_path))
    def create_tic_dat_from_sql(self, sql_file_path, includes_schema = False,
                                freeze_it = False):
        """
        Create a TicDat object from an SQLite sql text file
        :param sql_file_path: A text file containing SQLite compatible SQL statements delimited by ;
        :param includes_schema: boolean - does the sql_file_path contain schema generating SQL?
        :param freeze_it: boolean. should the returned object be frozen?
        :return: a TicDat object populated by the db created from the SQL
        """
        verify(sql, "sqlite3 needs to be installed to use this subroutine")
        return self._Rtn(freeze_it)(**self._create_tic_dat_from_sql(
                    sql_file_path, includes_schema))
    def find_duplicates(self, db_file_path):
        """
        Find the row counts for duplicated rows.
        :param db_file_path: A SQLite db with a consistent schema.
        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the SQLite table with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        verify(sql, "sqlite3 needs to be installed to use this subroutine")
        if not self._duplicate_focused_tdf:
            return {}
        return find_duplicates(self._duplicate_focused_tdf.sql.create_tic_dat(db_file_path),
                              self._duplicate_focused_tdf)
    def _fks(self):
        rtn = defaultdict(set)
        for fk in self.tic_dat_factory.foreign_keys:
            rtn[fk.native_table].add(fk)
        return FrozenDict({k:tuple(v) for k,v in rtn.items()})
    def _create_tic_dat_from_sql(self, sql_file_path, includes_schema):
        verify(os.path.exists(sql_file_path), "%s isn't a valid file path"%sql_file_path)
        verify(not self.tic_dat_factory.generator_tables,
               "recovery of generator tables from sql files not yet implemented")
        tdf = self.tic_dat_factory
        with sql.connect(":memory:") as con:
            if not includes_schema:
                for str in self._get_schema_sql(set(tdf.all_tables).
                                                difference(tdf.generic_tables)):
                    con.execute(str)
            with open(sql_file_path, "r") as f:
                for str in f.read().split(";"):
                    con.execute(str)
            return self._create_tic_dat_from_con(con,
                        {t:t for t in self.tic_dat_factory.all_tables})
    def _get_table_names(self, db_file_path, tables):
        rtn = {}
        with sql.connect(db_file_path) as con:
            def try_name(name):
                try :
                    con.execute("Select * from [%s]"%name)
                except :
                    return False
                return True
            for table in tables:
                rtn[table] = [t for t in all_underscore_replacements(table) if try_name(t)]
                verify(len(rtn[table]) >= 1, "Unable to recognize table %s in SQLite file %s"%
                                  (table, db_file_path))
                verify(len(rtn[table]) <= 1, "Duplicate tables found for table %s in SQLite file %s"%
                                  (table, db_file_path))
                rtn[table] = rtn[table][0]
        return rtn
    def _check_tables_fields(self, db_file_path, tables):
        tdf = self.tic_dat_factory
        TDE = TicDatError
        verify(os.path.exists(db_file_path), "%s isn't a valid file path"%db_file_path)
        try :
            sql.connect(db_file_path)
        except Exception as e:
            raise TDE("Unable to open %s as SQLite file : %s"%(db_file_path, e.message))
        table_names = self._get_table_names(db_file_path, tables)
        with sql.connect(db_file_path) as con:
            for table in tables :
                for field in tdf.primary_key_fields.get(table, ()) + \
                             tdf.data_fields.get(table, ()):
                    try :
                        con.execute("Select [%s] from [%s]"%(field,table_names[table]))
                    except :
                        raise TDE("Unable to recognize field %s in table %s for file %s"%
                                  (field, table, db_file_path))
        return table_names
    def _create_gen_obj(self, db_file_path, table, table_name):
        tdf = self.tic_dat_factory
        def tableObj() :
            assert (not tdf.primary_key_fields.get(table)) and (tdf.data_fields.get(table))
            with sql.connect(db_file_path) as con:
                for row in con.execute("Select %s from [%s]"%
                        (", ".join(_brackets(tdf.data_fields[table])), table_name)):
                    yield list(map(_read_data_format, row))
        return tableObj
    def _create_tic_dat(self, db_file_path):
        tdf = self.tic_dat_factory
        table_names = self._check_tables_fields(db_file_path, tdf.all_tables)
        with sql.connect(db_file_path) as con:
            rtn = self._create_tic_dat_from_con(con, table_names)
        for table in tdf.generator_tables :
            rtn[table] = self._create_gen_obj(db_file_path, table, table_names[table])
        return rtn
    def _create_tic_dat_from_con(self, con, table_names):
        tdf = self.tic_dat_factory
        rtn = {}
        for table in set(tdf.all_tables).difference(tdf.generator_tables) :
            fields = tdf.primary_key_fields.get(table, ()) + tdf.data_fields.get(table, ())
            if not fields:
                assert table in tdf.generic_tables
                fields = tuple(x[1] for x in con.execute("PRAGMA table_info(%s)"%table))
            rtn[table]= {} if tdf.primary_key_fields.get(table, ())  else []
            for row in con.execute("Select %s from [%s]"%(", ".join(_brackets(fields)),
                                                          table_names[table])):
                if table in tdf.generic_tables:
                    rtn[table].append({f:_read_data_format(d) for f,d in zip(fields,row)})
                else:
                    pk = row[:len(tdf.primary_key_fields.get(table, ()))]
                    data = list(map(_read_data_format,
                                    row[len(tdf.primary_key_fields.get(table, ())):]))

                    if dictish(rtn[table]) :
                        rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                    else :
                        rtn[table].append(data)
        return rtn
    def _ordered_tables(self):
        rtn = []
        fks = self._fks()
        def processTable(t) :
            if t not in rtn:
                for fk in fks.get(t, ()) :
                    processTable(fk.foreign_table)
                rtn.append(t)
        list(map(processTable, self.tic_dat_factory.all_tables))
        return tuple(rtn)
    def _get_schema_sql(self, tables):
        assert not set(self.tic_dat_factory.generic_tables).intersection(tables)
        rtn = []
        fks = self._fks()
        for t in [_ for _ in self._ordered_tables() if _ in tables]:
            str = "Create TABLE [%s] (\n"%t
            strl = _brackets(self.tic_dat_factory.primary_key_fields.get(t, ())) + \
                   ["[%s]"%f + " default [%s]"%self.tic_dat_factory.default_values.get(t, {}).get(f, 0)
                    for f in self.tic_dat_factory.data_fields.get(t, ())]
            for fk in fks.get(t, ()) :
                nativefields, foreignfields = zip(*(fk.nativetoforeignmapping().items()))
                strl.append("FOREIGN KEY(%s) REFERENCES [%s](%s)"%(",".join(_brackets(nativefields)),
                             fk.foreign_table, ",".join(_brackets(foreignfields))))
            if self.tic_dat_factory.primary_key_fields.get(t) :
                strl.append("PRIMARY KEY(%s)"%(",".join(_brackets(self.tic_dat_factory.primary_key_fields[t]))))
            str += ",\n".join(strl) + "\n);"
            rtn.append(str)
        return tuple(rtn)
    def _get_data(self, tic_dat, as_sql):
        rtn = []
        for t in self.tic_dat_factory.all_tables:
            _t = getattr(tic_dat, t)
            if dictish(_t) :
                primarykeys = tuple(self.tic_dat_factory.primary_key_fields[t])
                for pkrow, sqldatarow in _t.items() :
                    _items = list(sqldatarow.items())
                    fields = primarykeys + tuple(x[0] for x in _items)
                    datarow = ((pkrow,) if len(primarykeys)==1 else pkrow) + \
                              tuple(x[1] for x in _items)
                    assert len(datarow) == len(fields)
                    str = "INSERT INTO [%s] (%s) VALUES (%s)"%(t, ",".join(_brackets(fields)),
                          ",".join("%s" if as_sql else "?" for _ in fields))
                    if as_sql:
                        rtn.append((str%tuple(map(_insert_format, datarow))) + ";")
                    else:
                        rtn.append((str, datarow))
            else :
                for sqldatarow in (_t if containerish(_t) else _t()) :
                    k,v = zip(*sqldatarow.items())
                    str = "INSERT INTO [%s] (%s) VALUES (%s)"%\
                             (t, ",".join(_brackets(k)), ",".join(
                                ["%s" if as_sql else "?"]*len(sqldatarow)))
                    if as_sql:
                        rtn.append((str%tuple(map(_insert_format, v))) + ";")
                    else:
                        rtn.append((str,v))
        return tuple(rtn)
    def write_db_schema(self, db_file_path):
        """
        :param db_file_path: the file path of the SQLite database to create
        :return:
        """
        verify(not self.tic_dat_factory.generic_tables,
               "generic_tables are not compatible with write_db_schema. " +
               "Use write_db_data instead.")
        with _sql_con(db_file_path, foreign_keys=False) as con:
            for str in self._get_schema_sql(self.tic_dat_factory.all_tables):
                con.execute(str)
    def write_db_data(self, tic_dat, db_file_path, allow_overwrite = False):
        """
        write the ticDat data to an SQLite database file
        :param tic_dat: the data object to write
        :param db_file_path: the file path of the SQLite database to populate
        :param allow_overwrite: boolean - are we allowed to overwrite pre-existing data
        :return:
        caveats : float("inf"), float("-inf") are written as "inf", "-inf"
        """
        verify(sql, "sqlite3 needs to be installed to use this subroutine")
        msg = []
        if not self.tic_dat_factory.good_tic_dat_object(tic_dat, lambda m : msg.append(m)) :
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not os.path.isdir(db_file_path), "A directory is not a valid SQLite file path")
        if self.tic_dat_factory.generic_tables:
             dat, tdf = create_generic_free(tic_dat, self.tic_dat_factory)
             return tdf.sql.write_db_data(dat, db_file_path, allow_overwrite)
        if not os.path.exists(db_file_path) :
            self.write_db_schema(db_file_path)
        table_names = self._check_tables_fields(db_file_path, self.tic_dat_factory.all_tables)
        with _sql_con(db_file_path, foreign_keys=False) as con:
            for t in self.tic_dat_factory.all_tables:
                verify(table_names[t] == t, "Failed to find table %s in path %s"%
                                            (t, db_file_path))
                verify(allow_overwrite or not any(True for _ in  con.execute("Select * from %s"%t)),
                        "allow_overwrite is False, but there are already data records in %s"%t)
                con.execute("Delete from %s"%t) if allow_overwrite else None
            for sql_str, data in self._get_data(tic_dat, as_sql=False):
                con.execute(sql_str, list(data))

    def write_sql_file(self, tic_dat, sql_file_path, include_schema = False,
                       allow_overwrite = False):
        """
        write the sql for the ticDat data to a text file
        :param tic_dat: the data object to write
        :param sql_file_path: the path of the text file to hold the sql statements for the data
        :param include_schema: boolean - should we write the schema sql first?
        :param allow_overwrite: boolean - are we allowed to overwrite pre-existing file
        :return:
        caveats : float("inf"), float("-inf") are written as "inf", "-inf"
        """
        verify(sql, "sqlite3 needs to be installed to use this subroutine")
        verify(allow_overwrite or not os.path.exists(sql_file_path),
               "The %s path exists and overwrite is not allowed"%sql_file_path)
        must_schema = set(self.tic_dat_factory.all_tables if include_schema else [])
        if self.tic_dat_factory.generic_tables:
             gt = self.tic_dat_factory.generic_tables
             dat, tdf = create_generic_free(tic_dat, self.tic_dat_factory)
             return tdf.sql._write_sql_file(dat, sql_file_path, must_schema.union(gt))
        return self._write_sql_file(tic_dat, sql_file_path, must_schema)

    def _write_sql_file(self, tic_dat, sql_file_path, schema_tables):
        with open(sql_file_path, "w") as f:
            for str in self._get_schema_sql(schema_tables) + \
                       self._get_data(tic_dat, as_sql=True):
                f.write(str + "\n")

