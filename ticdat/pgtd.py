"""
Read/write ticDat objects from PostGres database. Requires the sqlalchemy module
"""

from collections import defaultdict
import math
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, FrozenDict, find_duplicates
from ticdat.utils import create_duplicate_focused_tdf, dictish, numericish, safe_apply
try:
    import sqlalchemy as sa
except:
    sa = None
try:
    import psycopg2
except:
    psycopg2 = None
try:
    import pandas as pd
except:
    pd = None

_can_unit_test = bool(sa)
# SELECT * FROM pg_get_keywords()  WHERE catdesc = 'reserved'; created _the_reserved_words
_the_reserved_words = {_.lower() for _ in ["asymmetric", "session_user", "initially", "table", "user", "desc",
    "collate", "primary", "current_role", "do", "trailing", "in", "case", "then", "only", "end", "leading", "analyze",
    "constraint", "offset", "union", "limit", "some", "asc", "else", "intersect", "for", "current_time", "create",
    "returning", "analyse", "foreign", "grant", "deferrable", "using", "all", "any", "current_user", "check",
    "current_catalog", "into", "and", "or", "array", "symmetric", "where", "from", "localtime", "cast", "group",
    "references", "localtimestamp", "not", "true", "column", "to", "null", "current_timestamp", "when", "fetch", "as",
    "placing", "order", "select", "except", "default", "current_date", "window", "false", "unique", "both", "distinct",
    "having", "on", "variadic", "lateral", "with"]}


# CUIDADO CUIDADO CUIDADO I wrote some ticdat_deployer code that referred to the following private function
def _pg_name(name):
    rtn = [_ if _.isalnum() else "_" for _ in name.lower()]
    if rtn and rtn[0].isdigit():
        rtn[0] = "_"
    return "".join(rtn)

def _active_fld_tables(engine, schema, active_fld):
    return {_[0] for _ in engine.execute("SELECT table_name FROM information_schema.columns " +
            f"WHERE table_schema = '{schema}' and column_name = '{active_fld}'")}

class _PostgresFactory(freezable_factory(object, "_isFrozen"),):
    def __init__(self, tdf):
        self.tdf = tdf
        self._isFrozen = True

    def _check_good_pgtd_compatible_table_field_names(self):
        all_fields = lambda t: self.tdf.primary_key_fields.get(t, ()) + self.tdf.data_fields.get(t, ())
        for t in self.tdf.all_tables: # play nice with the table/field names or don't play at all
            verify(_pg_name(t) == t,
                   f"Table {t} doesn't obey a postgres friendly naming convention." +
                   f"It should be have been named {_pg_name(t)}\n" +
                   "This is a postgres specific requirement. See pgsql doc string for more info.")
            verify(len(all_fields(t)) == len(set(map(_pg_name, all_fields(t)))),
                   f"Table {t} has field names that collide with each other under case/space insensitivity.\n" +
                   "This is a postgres specific requirement. See pgsql doc string for more info.")
            # a little testing indicated that the problem is with reserved words as fields, but not tables
            reserved_word_collision = {_ for _ in all_fields(t) if _.lower() in _the_reserved_words}
            verify(not reserved_word_collision, f"The following field names from table {t} collide with PostGres " +
                   f"reserved words {reserved_word_collision}")

    def check_tables_fields(self, engine, schema, error_on_missing_table=False):
        '''
        throws a TicDatError if there there isn't a postgres schema in engine with the proper tables and fields.
        :param engine: has an .execute method
        :param schema: string that represents a postgres schema
        :param error_on_missing_table: boolean - should an error be thrown for missing tables? If falsey, then
               print a warning instead.
        :return: A list of missing tables. Will raise TicDatError if there are missing tables and
                 error_on_missing_table is truthy.
        '''
        tdf = self.tdf
        verify(schema in [row[0] for row in engine.execute("select schema_name from information_schema.schemata")],
               f"Schema {schema} is missing from engine {engine}")
        pg_tables = [row[0] for row in engine.execute(
            f"select table_name from information_schema.tables where table_schema ='{schema}'")]
        missing_tables = []
        for table in tdf.all_tables:
            if table in pg_tables:
                pg_fields = [row[0] for row in engine.execute(f"""SELECT column_name FROM information_schema.columns 
                             WHERE table_schema = '{schema}' AND table_name = '{table}'""")]
                for field in tdf.primary_key_fields.get(table, ()) + \
                             tdf.data_fields.get(table, ()):
                    matches = [f for f in pg_fields if f == _pg_name(field)]
                    verify(len(matches) == 1,
                           f"Unable to recognize {table}.{_pg_name(field)} in postgres schema {schema}")
            else:
                missing_tables.append(table)
        verify(not (missing_tables and error_on_missing_table),
               f"Unable to recognize tables {missing_tables} in postgres schema {schema}")
        if missing_tables:
            print ("The following table names could not be found in the %s schema.\n%s\n"%
                   (schema,"\n".join(missing_tables)))
        return missing_tables
    def _fks(self):
        rtn = defaultdict(set)
        for fk in self.tdf.foreign_keys:
            rtn[fk.native_table].add(fk)
        return FrozenDict({k: tuple(v) for k, v in rtn.items()})

    def _ordered_tables(self):
        rtn = []
        fks = self._fks()
        def process_table(t, already_seen=None):
            already_seen = already_seen or [] # emergency fail for circular reference to avoid endless recursion
            if t not in rtn + already_seen:
                for fk in fks.get(t, ()):
                    process_table(fk.foreign_table, already_seen+[t])
                rtn.append(t)

        list(map(process_table, self.tdf.all_tables))
        return tuple(rtn)

    def _get_schema_sql(self, tables, schema, forced_field_types):
        rtn = []
        fks = self._fks()

        def get_fld_type(t, f, default_type):
            if (t, f) in forced_field_types:
                return forced_field_types[t, f]
            if t == "parameters" and self.tdf.parameters:
                return "text"
            fld_type = self.tdf.data_types.get(t, {}).get(f)
            if not fld_type:
                return default_type
            if fld_type.datetime:
                return "timestamp"
            verify(not (fld_type.number_allowed and fld_type.strings_allowed),
                   f"Select one of string or numeric for {t}.{f} if declaring type and using postgres")
            if fld_type.strings_allowed:
                return 'text'
            if fld_type.number_allowed:
                if fld_type.must_be_int:
                    return 'integer'
                else:
                    return 'float'
            else:
                TicDatError(f"Allow one of text or numeric for {t}.{f} if declaring type and using postgres")

        def db_default(t, f):
            rtn = self.tdf.default_values[t][f]
            if forced_field_types.get((t, f)) in ("bool", "boolean"):
                return bool(rtn)
            if rtn is None or rtn == "":
                return "NULL"
            if numericish(rtn) and abs(rtn) == float("inf") and numericish(self.tdf.infinity_io_flag):
                return math.copysign(self.tdf.infinity_io_flag, rtn)
            if stringish(rtn) and rtn:
                return f"'{rtn}'"
            return rtn

        def nullable(t, f):
            fld_type = self.tdf.data_types.get(t, {}).get(f)
            if not fld_type:
                return True
            if fld_type.number_allowed and self.tdf.infinity_io_flag is None :
                return True
            return fld_type.nullable

        def default_sql_str(t, f):
            if forced_field_types.get((t, f)) == "text" and stringish(self.tdf.default_values[t][f]):
                return f" DEFAULT {db_default(t, f)}"
            fld_type = self.tdf.data_types.get(t, {}).get(f)
            if (fld_type and fld_type.datetime) or get_fld_type(t, f, '') == "bytea":
                return ""
            return f" DEFAULT {db_default(t, f)}"

        for t in [_ for _ in self._ordered_tables() if _ in tables]:
            str = f"CREATE TABLE {schema}.{t} (\n"
            strl = [f"{_pg_name(f)} " + get_fld_type(t, f, 'text') for f in
                    self.tdf.primary_key_fields.get(t, ())] + \
                   [f"{_pg_name(f)} " + get_fld_type(t, f, 'float') +
                    (f"{' NOT NULL' if not nullable(t,f) else ''}") + default_sql_str(t, f)
                    for f in self.tdf.data_fields.get(t, ())]
            if self.tdf.primary_key_fields.get(t):
                strl.append(f"PRIMARY KEY ({','.join(map(_pg_name, self.tdf.primary_key_fields[t]))})")
            for fk in fks.get(t, ()):
                nativefields, foreignfields = zip(*(fk.nativetoforeignmapping().items()))
                strl.append(f"FOREIGN KEY ({','.join(map(_pg_name, nativefields))}) REFERENCES " +
                            f"{schema}.{fk.foreign_table} ({','.join(map(_pg_name, foreignfields))})")
            str += ",\n".join(strl) + "\n);"
            rtn.append(str)
        return tuple(rtn)

    def write_schema(self, engine, schema, forced_field_types=None, include_ancillary_info=True):
        """
        :param engine: typically a sqlalchemy database engine with drivertype postgres (really just needs an .execute)

        :param schema: a string naming the postgres schema to populate (will create if needed)

        :param forced_field_types : A dictionary mappying (table, field) to a field type
                                    Absent forcing, types are inferred from tic_dat_factory.data_types if possible,
                                    and set via the assumption that PK fields are text and data fields are floats if
                                    not.
        :param  include_ancillary_info : boolean. If False, no primary key or foreign key info will be written
        :return:
        """
        self._check_good_pgtd_compatible_table_field_names()
        forced_field_types = forced_field_types or {}
        all_fields = lambda t: self.tdf.primary_key_fields.get(t, ()) + self.tdf.data_fields.get(t, ())
        good_forced_field_type_entry = lambda k, v: isinstance(k, tuple) and len(k) == 2 \
                        and k[1] in all_fields(k[0]) and v in \
                        ["text", "integer", "float", "bool", "boolean", "timestamp", "date", "bytea"]
        verify(dictish(forced_field_types) and
               all(good_forced_field_type_entry(k, v) for k,v in forced_field_types.items()),
               "bad forced_field_types argument")
        if not include_ancillary_info:
            from ticdat import TicDatFactory
            tdf = TicDatFactory(**{t: [[], pks + dfs] for t, (pks, dfs) in self.tdf.schema().items()})
            tdf.set_infinity_io_flag(self.tdf.infinity_io_flag)
            for t, dts in self.tdf.data_types.items():
                for f, dt in dts.items():
                    tdf.set_data_type(t, f, *dt)
            for t, dfvs in self.tdf.default_values.items():
                for f, dfv in dfvs.items():
                    tdf.set_default_value(t, f, dfv)
            forced_field_types_ = {(t, f): "text" for t, (pks, dfs) in self.tdf.schema().items() for f in pks
                       if f not in tdf.data_types.get(t, {})}
            forced_field_types_.update(forced_field_types)
            return PostgresTicFactory(tdf).write_schema(engine, schema, forced_field_types_)

        verify(not getattr(self.tdf, "generic_tables", None),
               "TicDat for postgres does not yet support generic tables")

        if schema not in [row[0] for row in engine.execute("select schema_name from information_schema.schemata")]:
            engine.execute(sa.schema.CreateSchema(schema))
        for str in self._get_schema_sql(self.tdf.all_tables, schema, forced_field_types):
            engine.execute(str)

    def _handle_prexisting_rows(self, engine, schema, pre_existing_rows):
        verify(isinstance(pre_existing_rows, dict), "pre_existing_rows needs to dict")
        verify(set(pre_existing_rows).issubset(self.tdf.all_tables), "bad pre_existing_rows keys")
        verify(set(pre_existing_rows.values()).issubset({'delete', 'append'}), "bad pre_existing_rows values")
        pre_existing_rows = dict({t:"delete" for t in self.tdf.all_tables}, **pre_existing_rows)
        # need to iterate from leaves (children) upwards to avoid breaking foreign keys with delete
        for t in reversed(self._ordered_tables()):
            if pre_existing_rows[t] == "delete":
                try:
                    engine.execute(f"truncate table {schema}.{t}") # postgres truncate will fail on FKs re:less
                except Exception as e:
                    assert "foreign key" in str(e), "truncate should only fail due to foreign key issues"
                    engine.execute(f"DELETE FROM {schema}.{t}")

class PostgresTicFactory(_PostgresFactory):
    """
    Primary class for reading/writing PostGres databases with TicDat objects.
    You need the sqlalchemy package to be installed to use it.

    Don't create this object explicitly. A PostgresTicFactory will automatically be associated with the
    pgsql attribute of the parent TicDatFactory.

    postgres doesn't support brackets, and putting spaces in postgres field names is frowned upon.
    https://bit.ly/2xWLZL3.
    You **are** encouraged to continue to use field names like "Min Nutrition" in your ticdat Python code, and the
    pgtd code here will match such fields up with postgres field names like min_nutrition when reading/writing from
    a postgres DB. (Non alphamnumeric characters in general, and not just spaces, are replaced with underscores
    for generating PGSQL field names)
    """
    def __init__(self, tic_dat_factory):
        """
        Don't create this object explicitly. A PostgresTicFactory will
        automatically be associated with the pgsql attribute of the parent
        TicDatFactory.

        :param tic_dat_factory:

        :return:
        """
        self._duplicate_focused_tdf = create_duplicate_focused_tdf(tic_dat_factory)
        super().__init__(tic_dat_factory)

    def _read_data_cell(self, t, f, x):
        return self.tdf._general_read_cell(t, f, x)

    def _write_data_cell(self, t, f, x):
        rtn = self.tdf._infinity_flag_write_cell(t, f, x)
        if numericish(rtn):
            rtn = float(rtn) if safe_apply(int)(rtn) != rtn else int(rtn)
        return rtn

    def _Rtn(self, freeze_it):
        def _rtn(*args, **kwargs):
            rtn = self.tdf._parameter_table_post_read_adjustment(self.tdf.TicDat(*args, **kwargs))
            if freeze_it:
                return self.tdf.freeze_me(rtn)
            return rtn
        return _rtn

    def create_tic_dat(self, engine, schema, freeze_it=False, active_fld=""):
        """
        Create a TicDat object from a PostGres connection

        :param engine: A sqlalchemy connection to the PostGres database

        :param schema : The name of the schema to read from

        :param freeze_it: boolean. should the returned object be frozen?

        :param active_fld: if provided, a string for a boolean filter field.
                           Must be compliant w PG naming conventions, which are different from ticdat field naming
                           conventions. Typically developer can ignore this argument, designed for expert support.

        :return: a TicDat object populated by the matching tables. Missing tables issue a warning and resolve
                 to empty.

        """
        verify(sa, "sqlalchemy needs to be installed to use this subroutine")
        verify(_pg_name(active_fld) ==  active_fld, "active_fld needs to be compliant with PG naming conventions")
        self._check_good_pgtd_compatible_table_field_names()
        return self._Rtn(freeze_it)(**self._create_tic_dat(engine, schema, active_fld))

    def _create_tic_dat(self, engine, schema, active_fld):
        tdf = self.tdf
        verify(len(tdf.generic_tables) == 0,
               "Generic tables have not been enabled for postgres")
        verify(len(tdf.generator_tables) == 0,
               "Generator tables have not been enabled for postgres")
        rtn = self._create_tic_dat_from_con(engine, schema, active_fld)
        return rtn

    def _create_tic_dat_from_con(self, engine, schema, active_fld):
        tdf = self.tdf
        active_fld_tables = _active_fld_tables(engine, schema, active_fld) if active_fld else set()
        missing_tables = self.check_tables_fields(engine, schema)
        rtn = {}
        for table in set(tdf.all_tables).difference(missing_tables):
            rtn[table] = {} if tdf.primary_key_fields.get(table) else []
            assert tdf.primary_key_fields.get(table) or tdf.data_fields.get(table), "since no generic tables"
            fields = [_pg_name(f) for f in tdf.primary_key_fields.get(table, ()) +
                      tdf.data_fields.get(table, ())]
            for row in engine.execute(f"Select {', '.join(fields)} from {schema}.{table}" +
                                      (f" where {active_fld} is True" if table in active_fld_tables else "")):
                if tdf.primary_key_fields.get(table):
                    pk = [self._read_data_cell(table, f, x) for f,x in
                          zip(tdf.primary_key_fields[table], row[:len(tdf.primary_key_fields[table])])]
                    data = [self._read_data_cell(table, f, x) for f, x in
                            zip(tdf.data_fields[table], row[len(tdf.primary_key_fields[table]):])]
                    rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                else:
                    rtn[table].append([self._read_data_cell(table, f, x) for f, x in zip(tdf.data_fields[table], row)])

        return rtn

    def find_duplicates(self, engine, schema, active_fld=""):
        """
        Find the row counts for duplicated rows.

        :param engine: A sqlalchemy Engine object that can connect to our postgres instance

        :param schema: Name of the schema within the engine's database to use

        :param active_fld: if provided, a string for a boolean filter field.
                           Must be compliant w PG naming conventions, which are different from ticdat field naming
                           conventions. Typically developer can ignore this argument, designed for expert support.

        :return: A dictionary whose keys are table names for the primary-ed key tables.
                 Each value of the return dictionary is itself a dictionary.
                 The inner dictionary is keyed by the primary key values encountered in the table,
                 and the value is the count of records in the postgres table with this primary key.
                 Row counts smaller than 2 are pruned off, as they aren't duplicates
        """
        verify(sa, "sqlalchemy needs to be installed to use this subroutine")
        self._check_good_pgtd_compatible_table_field_names()
        if not self._duplicate_focused_tdf:
            return {}

        return find_duplicates(PostgresTicFactory(self._duplicate_focused_tdf).create_tic_dat(
                                engine, schema, active_fld=active_fld), self._duplicate_focused_tdf)


    def _get_data(self, tic_dat, schema, active_fld, active_fld_tables, dump_format="list"):
        """This function creates sql for writing data to postgres"""
        assert dump_format in ["list", "dict"]
        rtn = [] if dump_format == "list" else defaultdict(list)
        for t in self._ordered_tables():
            _t = getattr(tic_dat, t)
            primarykeys = tuple(self.tdf.primary_key_fields.get(t, ()))
            for the_data in (_t.items() if primarykeys else _t):
                if primarykeys:
                    pkrow, sqldatarow = the_data
                    # sqldatarow will always yield keys, values in TicDatFactory defined order
                    fields = primarykeys + tuple(sqldatarow.keys())
                    pkrow = (pkrow,) if len(primarykeys) == 1 else pkrow
                    datarow = tuple(self._write_data_cell(t, f, x) for f,x in zip(primarykeys, pkrow)) + \
                              tuple(self._write_data_cell(t, f, x) for f,x in sqldatarow.items())
                else:
                    fields = tuple(the_data.keys())
                    datarow = tuple(self._write_data_cell(t, f, x) for f,x in the_data.items())
                assert len(datarow) == len(fields)
                fields = list(map(_pg_name, fields))
                if t in active_fld_tables:
                    fields.append(active_fld)
                    datarow = datarow + (True,)
                if dump_format == "list":
                    str = f"INSERT INTO {schema}.{t} ({','.join(fields)}) VALUES ({','.join('%s' for _ in fields)})"
                    rtn.append((str, datarow))
                else:
                    str = f"INSERT INTO {schema}.{t} ({','.join(fields)}) VALUES %s"
                    rtn[str].append(datarow)
        return tuple(rtn) if dump_format == "list" else dict(rtn)

    def write_data(self, tic_dat, engine, schema, dsn=None, pre_existing_rows=None, active_fld=""):
        """
        write the ticDat data to a PostGres database

        :param tic_dat: the data object to write

        :param engine: a sqlalchemy database engine with drivertype postgres

        :param schema: the postgres schema to write to (call self.write_schema explicitly as needed)

        :param dsn: optional - if truthy, a dict that can be unpacked as arguments to
                    psycopg2.connect. Will speed up bulk writing compared to engine.execute
                    If truthy and not a dict, then will be passed directly to psycopg2.connect as the sole argument.

        :param pre_existing_rows: if provided, a dict mapping table name to either "delete" or "append"
                                  default behavior is "delete"

        :param active_fld: if provided, a string for a boolean filter field which will be populated with True.
                           Must be compliant w PG naming conventions, which are different from ticdat field naming
                           conventions. Typically developer can ignore this argument, designed for expert support.
        :return:
        """
        verify(sa, "sqalchemy needs to be installed to use this subroutine")
        verify(engine.name=='postgresql',
               "a sqlalchemy engine with drivername='postgres' is required")
        verify(not dsn or psycopg2, "need psycopg2 to use the faster dsn write option")
        verify(_pg_name(active_fld) ==  active_fld, "active_fld needs to be compliant with PG naming conventions")
        active_f_tables = _active_fld_tables(engine, schema, active_fld) if active_fld else set()
        self._check_good_pgtd_compatible_table_field_names()
        msg = []
        if not self.tdf.good_tic_dat_object(tic_dat, lambda m: msg.append(m)):
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not self.tdf.generic_tables,
               "TicDat for postgres does not yet support generic tables")
        self.check_tables_fields(engine, schema, error_on_missing_table=True) # call self.write_schema as needed
        self._handle_prexisting_rows(engine, schema, pre_existing_rows or {})
        if dsn:
            connect_kwargs = dsn if dsn and dictish(dsn) else {}
            connect_args = [dsn] if dsn and not dictish(dsn) else []
            with psycopg2.connect(*connect_args, **connect_kwargs) as db:
                with db.cursor() as cursor:
                    for k, v in self._get_data(tic_dat, schema, active_fld, active_f_tables, dump_format="dict").items():
                        psycopg2.extras.execute_values(cursor, k, v)
        else:
            all_dat = self._get_data(tic_dat, schema, active_fld, active_f_tables)
            if len(all_dat) > 1000:
                print("***pgtd.py not using most efficient data writing technique**")
            for sql_str, data in all_dat:
                engine.execute(sql_str, data)


class PostgresPanFactory(_PostgresFactory):
    """
    Primary class for reading/writing PostGres databases with PanDat objects.

    Don't create this object explicitly. A PostgresPanFactory will automatically be associated with the
    pgsql attribute of the parent PanDatFactory.

    Will need to have pandas installed to do anything.

    postgres doesn't support brackets, and putting spaces in postgres field names is frowned upon.
    https://bit.ly/2xWLZL3.
    You **are** encouraged to continue to use field names like "Min Nutrition" in your ticdat Python code, and the
    pgtd code here will match such fields up with postgres field names like min_nutrition when reading/writing from
    a postgres DB. (Non alphamnumeric characters in general, and not just spaces, are replaced with underscores
    for generating PGSQL field names).
    """
    def __init__(self, pan_dat_factory):
        """
        Don't create this object explicitly. A PostgresPanFactory will
        automatically be associated with the pgsql attribute of the parent
        PanDatFactory.

        :return:
        """
        super().__init__(pan_dat_factory)

    def create_pan_dat(self, engine, schema, active_fld=""):
        """
        Create a PanDat object from a PostGres connection

        :param engine: A sqlalchemy connection to the PostGres database

        :param schema : The name of the schema to read from

        :param active_fld: if provided, a string for a boolean filter field.
                           Must be compliant w PG naming conventions, which are different from ticdat field naming
                           conventions. Typically developer can ignore this argument, designed for expert support.

        :return: a PanDat object populated by the matching tables. Missing tables issue a warning and resolve
                 to empty.
        """
        self._check_good_pgtd_compatible_table_field_names()
        verify(_pg_name(active_fld) ==  active_fld, "active_fld needs to be compliant with PG naming conventions")
        missing_tables = self.check_tables_fields(engine, schema)
        active_fld_tables = _active_fld_tables(engine, schema, active_fld) if active_fld else set()
        rtn = {}
        for table in set(self.tdf.all_tables).difference(missing_tables):
            fields = [(f, _pg_name(f)) for f in self.tdf.primary_key_fields.get(table, ()) +
                      self.tdf.data_fields.get(table, ())]
            rtn[table] = pd.read_sql(sql=f"Select {', '.join([pgf for f, pgf in fields])} from {schema}.{table}" +
                                         (f" where {active_fld} is True" if table in active_fld_tables else ""),
                                     con=engine)
            rtn[table].rename(columns={pgf: f for f, pgf in fields}, inplace=True)

        rtn = self.tdf.PanDat(**rtn)
        msg = []
        assert self.tdf.good_pan_dat_object(rtn, msg.append), str(msg)
        return self.tdf._general_post_read_adjustment(rtn, push_parameters_to_be_valid=True)

    def write_data(self, pan_dat, engine, schema, pre_existing_rows=None, active_fld="",
                   progress=None, table_specific_context_manager=None):
        '''
        write the PanDat data to a postgres database

        :param pan_dat: a PanDat object

        :param engine: A sqlalchemy connection to the PostGres database

        :param schema: The postgres schema to write to (call self.write_schema explicitly as needed)

        :param pre_existing_rows: if provided, a dict mapping table name to either "delete" or "append"
                                  default behavior is "delete"

        :param active_fld: if provided, a string for a boolean filter field which will be populated with True.
                           Must be compliant w PG naming conventions, which are different from ticdat field naming
                           conventions. Typically developer can ignore this argument, designed for expert support.

        :param progress: if provided, a ticdat.Progress object that is called every time a table is uploaded

        :param table_specific_context_manager: if provided, a dict mapping table name to a context manager factory.
                That is to say, table_specific_context_manager.values() should be zero argument calleables that return
                context manager objects. The DataFrame.to_sql statement writing to the database will happen within this
                resulting context manager for every entry in this dict.
                This is an expert level only feature - don't use it without studying this whole subroutine

        :return:
        '''
        verify(_pg_name(active_fld) ==  active_fld, "active_fld needs to be compliant with PG naming conventions")
        table_specific_context_manager = table_specific_context_manager or {}
        verify(isinstance(table_specific_context_manager, dict), "table_specific_context_manager needs to be dict")
        verify(set(table_specific_context_manager).issubset(self.tdf.all_tables),
               "bad table_specific_context_manager keys")
        verify(all(map(callable, table_specific_context_manager.values())),
               "values of table_specific_context_manager should be context managers (and thus calleable)")
        verify(all(all(hasattr(v, a) for a in ["__enter__", "__exit__"]) for v in
                   table_specific_context_manager.values()),
               "values of table_specific_context_manager should be context managers")
        class EmptyContextManager(object):
            def __enter__(self, *execinfo):
                pass
            def __exit__(self, *excinfo):
                pass
        table_specific_context_manager = {t: table_specific_context_manager.get(t, EmptyContextManager)
                                          for t in self.tdf.all_tables}
        active_field_tables = _active_fld_tables(engine, schema, active_fld) if active_fld else set()
        self._check_good_pgtd_compatible_table_field_names()
        msg = []
        verify(self.tdf.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s" %"\n".join(msg))
        self.check_tables_fields(engine, schema, error_on_missing_table=True) # call self.write_schema as needed
        self._handle_prexisting_rows(engine, schema, pre_existing_rows or {})
        pan_dat = self.tdf._pre_write_adjustment(pan_dat)
        to_upload = self._ordered_tables()
        for i, table in enumerate(to_upload):
            df = getattr(pan_dat, table).copy(deep=True)
            fields = self.tdf.primary_key_fields.get(table, ()) + self.tdf.data_fields.get(table, ())
            df.rename(columns={f: _pg_name(f) for f in fields}, inplace=True)
            if table in active_field_tables:
                df[active_fld] = True
            with table_specific_context_manager[table]():
                df.to_sql(name=table, schema=schema, con=engine, if_exists="append", index=False)
            if progress and not progress.numerical_progress("Uploading...", 100.*i/len(to_upload)):
                break
