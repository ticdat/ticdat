"""
Read/write ticDat objects from PostGres database. Requires the sqlalchemy module
"""

from collections import defaultdict
from ticdat.utils import freezable_factory, TicDatError, verify, stringish, FrozenDict, find_duplicates
from ticdat.utils import create_duplicate_focused_tdf, dictish
import time
import os
import json
import sys
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

# CUIDADO CUIDADO CUIDADO I wrote some ticdat_deployer code that referred to the following private function
def _pg_name(name):
    rtn_ = [_ if _.isalnum() else "_" for _ in name.lower()]
    return "".join(rtn_)

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

        def processTable(t):
            if t not in rtn:
                for fk in fks.get(t, ()):
                    processTable(fk.foreign_table)
                rtn.append(t)

        list(map(processTable, self.tdf.all_tables))
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
            if rtn is None:
                return "NULL"
            return rtn

        def nullable(t, f):
            fld_type = self.tdf.data_types.get(t, {}).get(f)
            if not fld_type:
                return True
            if fld_type.number_allowed and self.tdf.infinity_io_flag is None :
                return True
            return fld_type.nullable

        def default_sql_str(t, f):
            fld_type = self.tdf.data_types.get(t, {}).get(f)
            if fld_type and fld_type.datetime:
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
                        ["text", "integer", "float", "bool", "boolean", "timestamp"]
        verify(dictish(forced_field_types) and
               all(good_forced_field_type_entry(k, v) for k,v in forced_field_types.items()),
               "bad forced_field_types argument")
        if not include_ancillary_info:
            from ticdat import TicDatFactory
            tdf = TicDatFactory(**{t: [[], pks + dfs] for t, (pks, dfs) in self.tdf.schema().items()})
            for t, dts in self.tdf.data_types.items():
                for f, dt in dts.items():
                    tdf.set_data_type(t, f, *dt)
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
        return self.tdf._infinity_flag_write_cell(t, f, x)

    def _Rtn(self, freeze_it):
        def _rtn(*args, **kwargs):
            rtn = self.tdf._parameter_table_post_read_adjustment(self.tdf.TicDat(*args, **kwargs))
            if freeze_it:
                return self.tdf.freeze_me(rtn)
            return rtn
        return _rtn

    def create_tic_dat(self, engine, schema, freeze_it=False):
        """
        Create a TicDat object from a PostGres connection

        :param engine: A sqlalchemy connection to the PostGres database

        :param schema : The name of the schema to read from

        :param freeze_it: boolean. should the returned object be frozen?

        :return: a TicDat object populated by the matching tables. Missing tables issue a warning and resolve
                 to empty.

        """
        verify(sa, "sqlalchemy needs to be installed to use this subroutine")
        self._check_good_pgtd_compatible_table_field_names()
        return self._Rtn(freeze_it)(**self._create_tic_dat(engine, schema))

    def _create_tic_dat(self, engine, schema):
        tdf = self.tdf
        verify(len(tdf.generic_tables) == 0,
               "Generic tables have not been enabled for postgres")
        verify(len(tdf.generator_tables) == 0,
               "Generator tables have not been enabled for postgres")
        rtn = self._create_tic_dat_from_con(engine, schema)
        return rtn

    def _create_tic_dat_from_con(self, engine, schema):
        tdf = self.tdf
        missing_tables = self.check_tables_fields(engine, schema)
        rtn = {}
        for table in set(tdf.all_tables).difference(missing_tables):
            rtn[table] = {} if tdf.primary_key_fields.get(table) else []
            assert tdf.primary_key_fields.get(table) or tdf.data_fields.get(table), "since no generic tables"
            fields = [_pg_name(f) for f in tdf.primary_key_fields.get(table, ()) +
                      tdf.data_fields.get(table, ())]
            for row in engine.execute(f"Select {', '.join(fields)} from {schema}.{table}"):
                if tdf.primary_key_fields.get(table):
                    pk = [self._read_data_cell(table, f, x) for f,x in
                          zip(tdf.primary_key_fields[table], row[:len(tdf.primary_key_fields[table])])]
                    data = [self._read_data_cell(table, f, x) for f, x in
                            zip(tdf.data_fields[table], row[len(tdf.primary_key_fields[table]):])]
                    rtn[table][pk[0] if len(pk) == 1 else tuple(pk)] = data
                else:
                    rtn[table].append([self._read_data_cell(table, f, x) for f, x in zip(tdf.data_fields[table], row)])

        return rtn

    def find_duplicates(self, engine, schema):
        """
        Find the row counts for duplicated rows.

        :param engine: A sqlalchemy Engine object that can connect to our postgres instance

        :param schema: Name of the schema within the engine's database to use

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

        return find_duplicates(PostgresTicFactory(self._duplicate_focused_tdf).create_tic_dat(engine, schema),
                               self._duplicate_focused_tdf)


    def _get_data(self, tic_dat, schema, dump_format="list"):
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
                if dump_format == "list":
                    str = f"INSERT INTO {schema}.{t} ({','.join(fields)}) VALUES ({','.join('%s' for _ in fields)})"
                    rtn.append((str, datarow))
                else:
                    str = f"INSERT INTO {schema}.{t} ({','.join(fields)}) VALUES %s"
                    rtn[str].append(datarow)
        return tuple(rtn) if dump_format == "list" else dict(rtn)

    def write_data(self, tic_dat, engine, schema, dsn=None, pre_existing_rows=None):
        """
        write the ticDat data to a PostGres database

        :param tic_dat: the data object to write

        :param engine: a sqlalchemy database engine with drivertype postgres

        :param schema: the postgres schema to write to (call self.write_schema explicitly as needed)

        :param dsn: optional - if truthy, a dict that can be unpacked as arguments to
                    psycopg2.connect. Will speed up bulk writing compared to engine.execute

        :param pre_existing_rows: if provided, a dict mapping table name to either "delete" or "append"
                                  default behavior is "delete"

        :return:
        """
        verify(sa, "sqalchemy needs to be installed to use this subroutine")
        verify(engine.name=='postgresql',
               "a sqlalchemy engine with drivername='postgres' is required")
        verify(not dsn or psycopg2, "need psycopg2 to use the faster dsn write option")
        verify(dictish(dsn or {}), "if provided - dsn needs to be a dict")
        self._check_good_pgtd_compatible_table_field_names()
        msg = []
        if not self.tdf.good_tic_dat_object(tic_dat, lambda m: msg.append(m)):
            raise TicDatError("Not a valid TicDat object for this schema : " + " : ".join(msg))
        verify(not self.tdf.generic_tables,
               "TicDat for postgres does not yet support generic tables")
        self.check_tables_fields(engine, schema, error_on_missing_table=True) # call self.write_schema as needed
        self._handle_prexisting_rows(engine, schema, pre_existing_rows or {})
        if dsn:
            with psycopg2.connect(**dsn) as db:
                with db.cursor() as cursor:
                    for k, v in self._get_data(tic_dat, schema, dump_format="dict").items():
                        psycopg2.extras.execute_values(cursor, k, v)
        else:
            all_dat = self._get_data(tic_dat, schema)
            if len(all_dat) > 1000:
                print("***pgtd.py not using most efficient data writing technique**")
            for sql_str, data in all_dat:
                engine.execute(sql_str, data)


class PostgresPanFactory(_PostgresFactory):
    """
    Primary class for reading/writing PostGres databases with PanDat objects.

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

    def create_pan_dat(self, engine, schema):
        """
        Create a PanDat object from a PostGres connection
        :param engine: A sqlalchemy connection to the PostGres database
        :param schema : The name of the schema to read from
        :return: a PanDat object populated by the matching tables. Missing tables issue a warning and resolve
                 to empty.
        """
        self._check_good_pgtd_compatible_table_field_names()
        missing_tables = self.check_tables_fields(engine, schema)
        rtn = {}
        for table in set(self.tdf.all_tables).difference(missing_tables):
            fields = [(f, _pg_name(f)) for f in self.tdf.primary_key_fields.get(table, ()) +
                      self.tdf.data_fields.get(table, ())]
            rtn[table] = pd.read_sql(sql=f"Select {', '.join([pgf for f, pgf in fields])} from {schema}.{table}",
                                     con=engine)
            rtn[table].rename(columns={pgf: f for f, pgf in fields}, inplace=True)

        rtn = self.tdf.PanDat(**rtn)
        msg = []
        assert self.tdf.good_pan_dat_object(rtn, msg.append), str(msg)
        return self.tdf._general_post_read_adjustment(rtn, push_parameters_to_be_valid=True)

    def write_data(self, pan_dat, engine, schema, pre_existing_rows=None):
        '''
        write the PanDat data to a postgres database
        :param pan_dat: a PanDat object
        :param engine: A sqlalchemy connection to the PostGres database
        :param schema: The postgres schema to write to (call self.write_schema explicitly as needed)
        :param pre_existing_rows: if provided, a dict mapping table name to either "delete" or "append"
                                  default behavior is "delete"
        :return:
        '''
        self._check_good_pgtd_compatible_table_field_names()
        msg = []
        verify(self.tdf.good_pan_dat_object(pan_dat, msg.append),
               "pan_dat not a good object for this factory : %s" %"\n".join(msg))
        self.check_tables_fields(engine, schema, error_on_missing_table=True) # call self.write_schema as needed
        self._handle_prexisting_rows(engine, schema, pre_existing_rows or {})
        pan_dat = self.tdf._infinity_flag_pre_write_adjustment(pan_dat)
        for table in self._ordered_tables():
            df = getattr(pan_dat, table).copy(deep=True)
            fields = self.tdf.primary_key_fields.get(table, ()) + self.tdf.data_fields.get(table, ())
            df.rename(columns={f: _pg_name(f) for f in fields}, inplace=True)
            df.to_sql(name=table, schema=schema, con=engine, if_exists="append", index=False)

class EnframeOfflineHandler(object):
    def __init__(self, confg_file, input_schema, solution_schema, solve, engine_object=None):
        """
        :param confg_file: an appropriate json file
            example enframe.json file.
            {"postgres_url": "postgresql://postgres@127.0.0.1:64452/test",
             "postgres_schema": "test_schema"}
            Optional enframe.json keys
            -> solve_type : can be "Proxy Enframe Solve", "Copy Input To Postgres" or "Copy Input to Postgres and Solve"
                            defaults to "Copy Input to Postgres and Solve"
            -> master_schema: can be any string. If an empty string, then a master_schema isn't used. Defaults to
                              "reports".
        :param input_schema: the input_schema
        :param solution_schema: the solution_schema
        :param solve: the solve function
        :param engine_object: this will be passed only for unit testing, normally it is deduced from solve
        """
        try:
            from framework_utils.ticdat_deployer import TicDatDeployer
        except :
            try:
                from enframe_ticdat_deployer import TicDatDeployer
            except:
                TicDatDeployer = None
        self._engine = None
        verify(TicDatDeployer, "Need some local package that can find TicDatDeployer.")
        verify(sa, "sqlalchemy needs to be installed to use PostGres")
        verify(os.path.isfile(confg_file), f"{confg_file} isn't a valid file path")
        with open(confg_file, "r") as _:
            d = json.load(_)
        verify(dictish(d), f"{confg_file} doesn't resolve to a dict")
        recognized_keys = {"postgres_url", "postgres_schema", "solve_type", "master_schema"}
        ignored_keys = set(d).difference(recognized_keys)
        if ignored_keys:
            print(f"\n****\nThe following entries from {confg_file} will be ignored.\n{ignored_keys}\n****\n")
        self._postgres_url = d["postgres_url"]
        self._postgres_schema = d["postgres_schema"]
        verify(stringish(self._postgres_schema) and self._postgres_schema,
               "postgres_schema should refer to a non-empty string")
        self.solve_type = d.get("solve_type", "Copy Input to Postgres and Solve")
        verify(self.solve_type in ["Proxy Enframe Solve", "Copy Input To Postgres", "Copy Input to Postgres and Solve"],
           "solve_type must be 'Proxy Enframe Solve', 'Copy Input To Postgres' or 'Copy Input to Postgres and Solve'")
        self._master_schema = d.get("master_schema", "reports")
        verify(stringish(self._master_schema), "master_schema should refer to a string")
        verify(self._master_schema != self._postgres_schema, "master_schema should not refer to postgres_schema")
        if engine_object:
            m = engine_object
        else:
            m = sys.modules[solve.__module__]
            if m.__package__:
                m = sys.modules[m.__package__]
        for n, o in [["input_schema", input_schema], ["solution_schema", solution_schema], ["solve", solve]]:
            verify(getattr(m, n, None) is o or (engine_object and n == "solve"),
                   f"failure to resolve {n} as a proper attribute of the engine")
        self._tdd = TicDatDeployer.duck_type_create(m)
        self._tdd_data = self._tdd.ticdat_helpful_data()
        self._python_engine = m
        engine_fail = ""
        try:
            self._engine = sa.create_engine(self._postgres_url)
        except Exception as e:
            engine_fail = str(e)
        verify(not engine_fail, "Failed to create postgres engine\n" +
               f"URL : {self._postgres_url}\nException : {engine_fail}")
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._engine:
            self._engine.dispose()
    def _get_forced_field_types(self, config_type):
        pgsql, tdf, renamings = {
            "input": (self._tdd_data.input_pgtd, self._python_engine.input_schema, self._tdd_data.input_renamings),
            "output": (self._tdd_data.solution_pgtd, self._python_engine.solution_schema,
                       self._tdd_data.solution_renamings)}[config_type]
        kwarg = {}
        enframe_attr = getattr(self._python_engine, {"input": "enframe_input_config",
                                                     "output": "enframe_output_config"}[config_type], {})
        if "type_for_complex_fields" in enframe_attr:
            kwarg = {"type_for_complex_fields": enframe_attr["type_for_complex_fields"]}
        forced_field_types = {}
        mapping_dict = {"text":"text", "int":"integer", "float":"float", "datetime":"timestamp"}
        for t in tdf.all_tables:
            for f in tdf.schema()[t][0] + tdf.schema()[t][1]:
                forced_field_types[renamings[t], f] = mapping_dict[self._tdd.get_data_type(config_type, t, f, **kwarg)]
        return forced_field_types
    def _write_schema_as_needed(self, pgsql, forced_field_types=None):
        from ticdat.utils import TicDatError

        has_schema = True
        try:
            pgsql.check_tables_fields(self._engine, self._postgres_schema, error_on_missing_table=True)
        except TicDatError:
            has_schema = False
        if not has_schema:

            pgsql.write_schema(self._engine, self._postgres_schema, include_ancillary_info=False,
                               forced_field_types=forced_field_types)
    def copy_input_dat(self, dat):
       assert self.solve_type == "Copy Input To Postgres"
       tdf = self._python_engine.input_schema
       parameters_schema = tdf.schema().get("parameters")
       renamed_parameters_schema = self._tdd_data.input_pgtd.tdf.schema().get("parameters")
       ffd = {k:v for k, v in self._get_forced_field_types("input").items() if k[0] != "parameters"}
       if parameters_schema:
           ffd.update({("parameters", _[0]):"text" for _ in renamed_parameters_schema})
       self._write_schema_as_needed(self._tdd_data.input_pgtd, forced_field_types=ffd)
       self._write_schema_as_needed(self._tdd_data.small_integrity_pgtd)
       from ticdat import TicDatFactory
       if isinstance(tdf, TicDatFactory):
            assert tdf.good_tic_dat_object(dat)
            renamed_dat = self._tdd_data.input_pgtd.tdf.TicDat()
            for t in tdf.all_tables:
                if t == "parameters":
                    for k ,v in dat.parameters.items():
                        renamed_dat.parameters[k] = next(iter(v.values()))
                else:
                    setattr(renamed_dat, self._tdd._input_renamings[t], getattr(dat, t))
            self._tdd_data.input_pgtd.write_data(renamed_dat, self._engine, self._postgres_schema,
                                                 dsn=self._try_get_dsn())
       else:
            assert tdf.good_pan_dat_object(dat)
            renamed_dat = self._tdd_data.input_pgtd.tdf.PanDat()
            for t in tdf.all_tables:
                if t == "parameters":
                    renamed_dat.parameters = dat.parameters.rename(columns={e[0]:t[0] for e,t in zip(
                        parameters_schema, renamed_parameters_schema)})
                else:
                    setattr(renamed_dat, self._tdd_data.input_renamings[t], getattr(dat, t))
            self._tdd_data.input_pgtd.write_data(renamed_dat, self._engine, self._postgres_schema)
    def _try_get_dsn(self):
        if not psycopg2:
            return None
        try:
            rtn = psycopg2.connect(self._postgres_url).get_dsn_parameters()
        except:
            return None
        return rtn
    def proxy_enframe_solve(self):
        _time = time.time()
        seconds = lambda: "{0:.2f}".format(time.time() - _time)
        class MockDb(object):
            engine = self._engine
            schema = self._postgres_schema
        result_type, result = self._tdd.get_input_dat_with_integrity_checking(MockDb())
        if result_type == "Integrity Failures":
            print("Various data integrity problems were found.")
            for k, v in result.items():
                print(k.ljust(30) + " : " + str(v))
            return
        assert result_type in ["TicDat Data Object", "PanDat Data Object"]
        dat = result
        print(f"-->Launch-to-dat time {seconds()} seconds.")
        # at some point could add a bunch of cool stuff  here - progress and dumping log tables to .csv files for ex
        sln = self._python_engine.solve(dat)
        print(f"--> Launch-to-solve time {seconds()} seconds.")
        if sln: # writing might be slowed down because we have no DSN - can make that optional and add it later
            self._write_schema_as_needed(self._tdd_data.solution_pgtd, self._get_forced_field_types("output"))
            self._tdd.write_solution_dat_to_postgres(sln, self._engine, self._postgres_schema, dsn=self._try_get_dsn())
        else:
            print("No solution found")






