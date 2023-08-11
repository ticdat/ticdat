import ticdat.utils as utils
from ticdat import TicDatFactory, PanDatFactory
from ticdat.testing.ticdattestutils import pan_dat_maker
from ticdat.pgtd import _can_unit_test, PostgresTicFactory, PostgresPanFactory, _pg_name
from ticdat.testing.ticdattestutils import flagged_as_run_alone, fail_to_debugger, memo
import time
import datetime
import math
import pickle
import os
import inspect
import json
from itertools import product
def _this_directory() :
    return os.path.dirname(os.path.realpath(os.path.abspath(inspect.getsourcefile(_this_directory))))

import unittest
try:
    import sqlalchemy as sa
except:
    sa = None
try:
    import testing.postgresql as testing_postgresql
except:
    testing_postgresql = None
try:
    import pandas as pd
except:
    pd = None
try:
    import dateutil, dateutil.parser
except:
    dateutil = None

diet_schema = TicDatFactory (
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity"]])
diet_schema.add_foreign_key("nutrition_quantities", "foods", ["Food", "Name"])
diet_schema.add_foreign_key("nutrition_quantities", "categories",
                            ["Category", "Name"])
diet_schema.set_data_type("categories", "Min Nutrition", min=0, max=float("inf"),
                          inclusive_min=True, inclusive_max=False)
diet_schema.set_data_type("categories", "Max Nutrition", min=0, max=float("inf"),
                          inclusive_min=True, inclusive_max=True)
diet_schema.set_data_type("foods", "Cost", min=0, max=float("inf"),
                          inclusive_min=True, inclusive_max=False)
diet_schema.set_data_type("nutrition_quantities", "Quantity", min=0, max=float("inf"),
                          inclusive_min=True, inclusive_max=False)
diet_dat = diet_schema.TicDat(**
{'foods': [['hamburger', 2.49],
  ['salad', 2.49],
  ['hot dog', 1.5],
  ['fries', 1.89],
  ['macaroni', 2.09],
  ['chicken', 2.89],
  ['milk', 0.89],
  ['ice cream', 1.59],
  ['pizza', 1.99]],
 'categories': [['protein', 91, float("inf")],
  ['calories', 1800, 2200.0],
  ['fat', 0, 65.0],
  ['sodium', 0, 1779.0]],
 'nutrition_quantities': [['ice cream', 'protein', 8],
  ['ice cream', 'fat', 10],
  ['fries', 'sodium', 270],
  ['fries', 'calories', 380],
  ['hamburger', 'fat', 26],
  ['macaroni', 'sodium', 930],
  ['hot dog', 'sodium', 1800],
  ['chicken', 'sodium', 1190],
  ['salad', 'calories', 320],
  ['ice cream', 'calories', 330],
  ['milk', 'sodium', 125],
  ['salad', 'sodium', 1230],
  ['pizza', 'sodium', 820],
  ['pizza', 'protein', 15],
  ['pizza', 'calories', 320],
  ['hamburger', 'calories', 410],
  ['milk', 'fat', 2.5],
  ['salad', 'protein', 31],
  ['milk', 'protein', 8],
  ['macaroni', 'fat', 10],
  ['salad', 'fat', 12],
  ['hot dog', 'fat', 32],
  ['chicken', 'fat', 10],
  ['chicken', 'protein', 32],
  ['fries', 'protein', 4],
  ['pizza', 'fat', 12],
  ['milk', 'calories', 100],
  ['ice cream', 'sodium', 180],
  ['chicken', 'calories', 420],
  ['hamburger', 'sodium', 730],
  ['macaroni', 'calories', 320],
  ['fries', 'fat', 19],
  ['hot dog', 'calories', 560],
  ['hot dog', 'protein', 20],
  ['macaroni', 'protein', 12],
  ['hamburger', 'protein', 24]]})

#@fail_to_debugger
class TestPostres(unittest.TestCase):
    can_run = False

    def setUp(self):
        try:
            self.postgresql = testing_postgresql.Postgresql()
            self.engine = sa.create_engine(self.postgresql.url())
            self.engine_fail = None
        except Exception as e:
            self.postgresql = self.engine = None
            self.engine_fail = e
        if self.engine_fail:
            print(f"!!!!Engine failed to load due to {self.engine_fail}")
        if self.engine and utils.safe_apply(lambda: test_schema in sa.inspect(self.engine).get_schema_names())():
            self.engine.execute(sa.schema.DropSchema(test_schema, cascade=True))

    def tearDown(self):
        if self.postgresql:
            self.engine.dispose()
            self.postgresql.stop()

    def test_datetime_defaults(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(orders=[["Name"], ["Deliver By"]])
        tdf.set_data_type("orders", "Deliver By", datetime=True)
        tdf.set_default_value("orders", "Deliver By", "Jan 1 2019 1 PM")
        schema = test_schema + "_dt_dfs_1"
        tdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False,
                               forced_field_types={("orders", "Deliver By"): "text"})
        self.engine.execute(f"Insert into {schema}.orders (name) values ('blah')")
        dat = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(dat._len_dict() == {"orders": 1})
        self.assertFalse(tdf.find_data_type_failures(dat))

    def test_datetime_defaults_2(self): # for Foresta
        if not self.can_run:
            return
        tdf = TicDatFactory(orders=[[], ["Name", "Deliver By"]])
        tdf.set_data_type("orders", "Deliver By", datetime=True)
        tdf.set_default_value("orders", "Deliver By", "Jan 1 2019 1 PM")
        tdf2 = tdf.clone()
        tdf2.set_data_type("orders", "Deliver By", datetime=True, nullable=True)
        schema = test_schema + "_dt_dfs_2"
        tdf2.pgsql.write_schema(self.engine, schema, include_ancillary_info=True,
                               forced_field_types={("orders", "Name"): "text", ("orders", "Deliver By"): "text"})
        self.engine.execute(f"Insert into {schema}.orders (name) values ('blah')")
        dat = tdf2.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(dat._len_dict() == {"orders": 1})
        self.assertFalse(tdf.find_data_type_failures(dat))

    def test_pgtd_active_dups(self):
        if not self.can_run:
            return
        schema = test_schema+"_act_dups"
        tdf_1 = TicDatFactory(t_one=[[], ["Field One", "Field Two", "Da Active"]],
                              t_two=[[], ["Field One", "Da Active"]])
        dat = tdf_1.TicDat(t_one = [["a", "b", True], ["a", "c", True], ["a", "b", False], ["a", "d", True]],
                           t_two = [["a", True], ["b", False], ["a", False], ["b", False], ["a", False]])
        self.assertTrue(len(dat.t_one) == 4 and len(dat.t_two) == 5)
        tdf_1.pgsql.write_schema(self.engine, schema, include_ancillary_info=False, forced_field_types=
            {(t, f): "boolean" if "Active" in f else "text" for t, (pks, dfs) in tdf_1.schema().items()
             for f in pks + dfs})
        tdf_1.pgsql.write_data(dat, self.engine, schema)
        self.assertTrue(tdf_1._same_data(dat, tdf_1.pgsql.create_tic_dat(self.engine, schema), epsilon=1e-8))
        tdf = TicDatFactory(t_one= [["Field One", "Field Two"], []],
                            t_two= [["Field One"], []])
        self.assertTrue(tdf.pgsql.find_duplicates(self.engine, schema))
        self.assertFalse(tdf.pgsql.find_duplicates(self.engine, schema, active_fld="da_active"))

    def test_context_manager_write(self):
        cntxt_events = set()
        outer_self = self
        def context_manager_factory(t):
            class ContextManager(object):
                def __enter__(self, *execinfo):
                    outer_self.assertFalse((t, "enter") in cntxt_events)
                    cntxt_events.add((t, "enter"))
                def __exit__(self, *excinfo):
                    outer_self.assertFalse((t, "exit") in cntxt_events)
                    cntxt_events.add((t, "exit"))
            return ContextManager
        schema = test_schema+"_cntx_mgr"
        pdf = PanDatFactory.create_from_full_schema(diet_schema.schema(include_ancillary_info=True))
        pan_dat = diet_schema.copy_to_pandas(diet_dat, reset_index=True)
        pdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        cntx_arg = {t: context_manager_factory(t) for t in ["foods", "nutrition_quantities"]}
        self.assertFalse(cntxt_events)
        pdf.pgsql.write_data(pan_dat, self.engine, schema, table_specific_context_manager=cntx_arg)
        self.assertTrue(cntxt_events == {(t, e) for t, e in product(cntx_arg, ["enter", "exit"])})
        pan_dat_2 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(pan_dat, pan_dat_2, epsilon=1e-10))

    def test_pgtd_active(self):
        if not self.can_run:
            return
        schema = test_schema+"_active"
        tdf = TicDatFactory(**{k:[pks, (["active_fld"] if k == "categories" else []) + dfs]
                                      for k, (pks, dfs) in diet_schema.schema().items()})
        tdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False,
                               forced_field_types={('categories', 'active_fld'):'boolean'})
        tdf = diet_schema.clone()
        dat = tdf.copy_tic_dat(diet_dat)
        dat.categories["junk"] = {}
        tdf.pgsql.write_data(dat, self.engine, schema, active_fld="active_fld")
        self.assertTrue(set(_[0] for _ in self.engine.execute(f"Select active_fld from {schema}.categories")) == {True})
        self.engine.execute(f"Update {schema}.categories set active_fld = False where name = 'junk'")
        dat_2 = tdf.pgsql.create_tic_dat(self.engine, schema, active_fld="active_fld")
        self.assertTrue(tdf._same_data(dat_2, diet_dat, epsilon=1e-10))

        pdf = PanDatFactory.create_from_full_schema(diet_schema.schema(include_ancillary_info=True))
        pan_dat = tdf.copy_to_pandas(diet_dat, drop_pk_columns=False)
        pan_dat_2 = pdf.pgsql.create_pan_dat(self.engine, schema, active_fld="active_fld")
        self.assertTrue(pdf._same_data(pan_dat, pan_dat_2, epsilon=1e-10))
        self.assertTrue(set(_[0] for _ in self.engine.execute(f"Select active_fld from {schema}.categories")) ==
                        {True, False})
        pdf.pgsql.write_data(pan_dat, self.engine, schema, active_fld="active_fld")
        self.assertTrue(set(_[0] for _ in self.engine.execute(f"Select active_fld from {schema}.categories")) == {True})

    def test_issue_68(self):
        if not self.can_run:
            return
        tdf = diet_schema.clone()
        pgtf = tdf.pgsql
        pgtf.write_schema(self.engine, test_schema, include_ancillary_info=False)
        dat = tdf.copy_tic_dat(diet_dat)
        import numpy
        dat.categories["protein"]["Max Nutrition"] = numpy.int64(200)
        dat.categories["fat"]["Max Nutrition"] = numpy.float64(65)
        pgtf.write_data(dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(dat, pg_tic_dat))

    def test_issue_68_pd(self):
        # kind of a dumb test since the numpy types tend to be the ones pandas creates naturally, but no harm
        # in being rigorous
        if not self.can_run:
            return
        tdf = diet_schema.clone()
        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        pgtf = pdf.pgsql
        pgtf.write_schema(self.engine, test_schema, include_ancillary_info=False)
        dat = tdf.copy_tic_dat(diet_dat)
        import numpy
        dat.categories["protein"]["Max Nutrition"] = numpy.int64(200)
        dat.categories["fat"]["Max Nutrition"] = numpy.float64(65)
        pan_dat = pdf.copy_pan_dat(tdf.copy_to_pandas(dat, drop_pk_columns=False))
        pgtf.write_data(pan_dat, self.engine, test_schema)
        pg_pan_dat = pgtf.create_pan_dat(self.engine, test_schema)
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))
        pan_dat.categories["Max Nutrition"] = utils.faster_df_apply(pan_dat.categories,
                                                               lambda row: numpy.int64(row["Max Nutrition"]))
        pan_dat.foods["Cost"] = utils.faster_df_apply(pan_dat.foods, lambda row: numpy.float64(row["Cost"]))
        pgtf.write_data(pan_dat, self.engine, test_schema)
        pg_pan_dat = pgtf.create_pan_dat(self.engine, test_schema)
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))

    def test_wtf(self):
        schema = "wtf"
        tdf = TicDatFactory(table_one=[["Cost per Distance", "Cost per Hr. (in-transit)"], ["Stuff"]],
                            table_two=[["This", "That"], ["Tho"]])
        tdf.pgsql.write_schema(self.engine, schema)
        data = [["a", "b", 1], ["dd", "ee", 10], ["023", "210", 102.1]]
        tic_dat = tdf.TicDat(table_one=data, table_two=data)
        tdf.pgsql.write_data(tic_dat, self.engine, schema, dsn=self.postgresql.dsn())
        pg_tic_dat = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(tic_dat, pg_tic_dat))

    def test_diet_dsn(self):
        if not self.can_run:
            return
        pgtf = diet_schema.pgsql
        pgtf.write_schema(self.engine, test_schema)
        pgtf.write_data(diet_dat, self.engine, test_schema, dsn=self.postgresql.dsn())
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat))

    def test_diet_no_inf_flagging(self):
        pgtf = diet_schema.pgsql
        pgtf.write_schema(self.engine, test_schema, include_ancillary_info=False)
        if not self.can_run:
            return
        for dsn in [None, self.postgresql.dsn()]:
            pgtf.write_data(diet_dat, self.engine, test_schema, dsn=dsn)
            self.assertTrue(sorted([_ for _ in self.engine.execute(f"Select * from {test_schema}.categories")]) ==
              [('calories', 1800.0, 2200.0), ('fat', 0.0, 65.0), ('protein', 91.0, float("inf")),
               ('sodium', 0.0, 1779.0)])
            self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
            pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
            self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat))

    def test_diet_no_inf_pd_flagging(self):
        pdf = PanDatFactory.create_from_full_schema(diet_schema.schema(include_ancillary_info=True))
        pan_dat = diet_schema.copy_to_pandas(diet_dat, drop_pk_columns=False)
        pgpf = pdf.pgsql
        pgpf.write_schema(self.engine, test_schema, include_ancillary_info=False)
        pgpf.write_data(pan_dat, self.engine, test_schema)
        self.assertTrue(sorted([_ for _ in self.engine.execute(f"Select * from {test_schema}.categories")]) ==
                    [('calories', 1800.0, 2200.0), ('fat', 0.0, 65.0), ('protein', 91.0, float("inf")),
                     ('sodium', 0.0, 1779.0)])
        pg_pan_dat = pgpf.create_pan_dat(self.engine, test_schema)
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))

    def test_diet(self):
        if not self.can_run:
            return
        tdf = diet_schema.clone()
        tdf.set_infinity_io_flag(999999999)
        pgtf = tdf.pgsql
        pgtf.write_schema(self.engine, test_schema, include_ancillary_info=False)
        pgtf.write_data(diet_dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat))

        tdf = diet_schema.clone()
        tdf.set_infinity_io_flag(None)
        pgtf_null_inf = tdf.pgsql
        pg_tic_dat_none_inf = pgtf_null_inf.create_tic_dat(self.engine, test_schema)
        self.assertFalse(diet_schema._same_data(diet_dat, pg_tic_dat_none_inf))
        pg_tic_dat_none_inf.categories["protein"]["Max Nutrition"] = float("inf")
        self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat_none_inf))

        dat2 = diet_schema.copy_tic_dat(diet_dat)
        dat2.foods["za"] = dat2.foods.pop("pizza")
        pgtf.write_data(dat2, self.engine, test_schema, pre_existing_rows={"foods": "append"})
        self.assertTrue(set(pgtf.find_duplicates(self.engine, test_schema)) == {'foods'})
        dat3 = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(set(dat3.foods).issuperset(dat2.foods) and set(dat3.foods).issuperset(diet_dat.foods))
        self.assertTrue(set(dat3.foods).difference(diet_dat.foods) == {'za'})
        self.assertTrue(set(dat3.foods).difference(dat2.foods) == {'pizza'})

        pgtf.write_data(dat2, self.engine, test_schema, pre_existing_rows={"nutrition_quantities": "append"})
        self.assertTrue(set(pgtf.find_duplicates(self.engine, test_schema)) == {'nutrition_quantities'})
        dat4 = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(dat2, dat4))

        test_schema_2 = test_schema +  "_none_inf"
        pgtf_null_inf.write_schema(self.engine, test_schema_2)
        pgtf_null_inf.write_data(pg_tic_dat_none_inf, self.engine, test_schema_2)
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema_2)
        self.assertFalse(diet_schema._same_data(diet_dat, pg_tic_dat))
        pg_tic_dat.categories["protein"]["Max Nutrition"] = float("inf")
        self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat))
        pg_tic_dat_none_inf = pgtf_null_inf.create_tic_dat(self.engine, test_schema_2)
        self.assertTrue(diet_schema._same_data(diet_dat, pg_tic_dat_none_inf))

        tdf = TicDatFactory(**diet_schema.schema()) # not clone so losing the data types
        tdf.set_infinity_io_flag(None)
        pgtf_null_inf = tdf.pgsql
        pg_tic_dat_none_inf = pgtf_null_inf.create_tic_dat(self.engine, test_schema_2)
        self.assertFalse(diet_schema._same_data(diet_dat, pg_tic_dat_none_inf))
        self.assertTrue(pg_tic_dat_none_inf.categories["protein"]["Max Nutrition"] is None)

    def test_big_diet(self):
        now = time.time()
        if not self.can_run:
            return
        pgtf = diet_schema.pgsql
        big_dat = diet_schema.copy_tic_dat(diet_dat)
        for k in range(int(1e5)):
            big_dat.categories[str(k)] = [0,100]
        pgtf.write_schema(self.engine, test_schema)
        pgtf.write_data(big_dat, self.engine, test_schema, dsn=self.postgresql.dsn())
        print(f"\n**** tdf writing {big_dat._len_dict()} : {time.time()-now}****\n")
        now = time.time()
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        print(f"**** tdf reading {big_dat._len_dict()} : {time.time()-now}****")
        self.assertTrue(diet_schema._same_data(big_dat, pg_tic_dat))

        med_dat = diet_schema.copy_tic_dat(diet_dat)
        for k in range(int(2e3)):
            med_dat.categories[str(k)] = [0,100]
        # big enough to trigger a warning message if writing out without dsn
        pgtf.write_data(med_dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(med_dat, pg_tic_dat))

    def test_big_diet_two(self):
        now = time.time()
        if not self.can_run:
            return
        pgtf = diet_schema.pgsql
        big_dat = diet_schema.copy_tic_dat(diet_dat)
        for k in range(int(1e5)):
            big_dat.categories[str(k)] = [0,100]
        pgtf.write_schema(self.engine, test_schema)
        pgtf.write_data(big_dat, self.engine, test_schema, dsn=self.postgresql.url())
        print(f"\ntdf writing {big_dat._len_dict()} {time.time()-now}*!!*\n")
        now = time.time()
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        print(f"\ntdf reading and dup {big_dat._len_dict()} {time.time()-now}****\n")
        self.assertTrue(diet_schema._same_data(big_dat, pg_tic_dat))

        med_dat = diet_schema.copy_tic_dat(diet_dat)
        for k in range(int(2e3)):
            med_dat.categories[str(k)] = [0,100]
        # big enough to trigger a warning message if writing out without dsn
        pgtf.write_data(med_dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(diet_schema._same_data(med_dat, pg_tic_dat))

    def test_schema(self):
        if not self.can_run:
            return
        pgtf = diet_schema.pgsql
        pg_schema = pgtf._get_schema_sql(diet_schema.all_tables, "schema", forced_field_types={})
        all_fields = {t: pkfs + dfs for t, (pkfs, dfs) in diet_schema.schema().items()}
        for t, fields in all_fields.items():
            self.assertTrue(len([x for x in pg_schema if f"CREATE TABLE schema.{t}" in x]) == 1)
            self.assertFalse(any(f in x for f in fields for x in pg_schema))
            self.assertTrue(any(_pg_name(f) in x for f in fields for x in pg_schema))
            self.assertTrue(any(all(_pg_name(f) in x for f in fields) and
                                f"CREATE TABLE schema.{t}" in x for x in pg_schema))

    def test_ints_and_strings_and_lists(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(t_one = [[], ["str_field", "int_field"]],
                            t_two = [["str_field", "int_field"], []])
        for t in tdf.all_tables:
            tdf.set_data_type(t, "str_field", strings_allowed=['This', 'That'], number_allowed=False)
            tdf.set_data_type(t, "int_field", must_be_int=True)
        dat = tdf.TicDat(t_one = [["This", 1], ["That", 2], ["This", 111], ["That", 211]],
                         t_two = [["This", 10], ["That", 9]])
        self.assertFalse(tdf.find_data_type_failures(dat))
        self.assertTrue(len(dat.t_one) == 4)
        self.assertTrue(len(dat.t_two) == 2)
        pgtf = tdf.pgsql
        pgtf.write_schema(self.engine, test_schema)
        pgtf.write_data(dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(tdf._same_data(dat, pg_tic_dat))

    def test_true_false(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(table = [["pkf"], ["df1", "df2"]])
        tdf.set_data_type("table", "df2", min=-float("inf"))
        dat = tdf.TicDat(table = [["d1", True, 100], ["d2", False, 200], ["d3", False, -float("inf")]])
        self.assertTrue(len(dat.table) == 3)
        self.assertFalse(tdf.find_data_type_failures(dat))
        pgtf = tdf.pgsql
        ex = None
        try:
            pgtf.write_data(None, self.engine, test_schema)
        except utils.TicDatError as te:
            ex = str(te)
        self.assertTrue(ex and "Not a valid TicDat object" in ex)
        pgtf.write_schema(self.engine, test_schema, forced_field_types={("table", "df1"): "bool"})
        pgtf.write_data(dat, self.engine, test_schema)
        self.assertFalse(pgtf.find_duplicates(self.engine, test_schema))
        pg_tic_dat = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(tdf._same_data(dat, pg_tic_dat))

    def test_dups(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(one = [["a"],["b", "c"]],
                            two = [["a", "b"],["c"]],
                            three = [["a", "b", "c"],[]])
        tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
        td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], [5, 1, 2]]
                            for t in tdf.all_tables})
        tdf2.pgsql.write_schema(self.engine, test_schema)
        tdf2.pgsql.write_data(td, self.engine, test_schema)
        dups = tdf.pgsql.find_duplicates(self.engine, test_schema)
        self.assertTrue(dups == {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})

    def test_pd_progress(self):
        if not self.can_run:
            return
        schema = "t_pg_d_pd"
        tdf = diet_schema
        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        pdf.set_infinity_io_flag(1e12)
        pgpf = pdf.pgsql
        pan_dat = pan_dat_maker(tdf.schema(), diet_dat)
        pgpf.write_schema(self.engine, schema, include_ancillary_info=False)
        class MyProgress(utils.Progress):
            def __init__(self):
                self.uploaded=0
                super().__init__(quiet=True)
            def numerical_progress(self, theme, progress):
                self.uploaded+=1
                return self.uploaded < 2
        progress = MyProgress()
        pgpf.write_data(pan_dat, self.engine, schema, progress=progress)
        self.assertTrue(progress.uploaded == 2)
        pg_pan_dat = pgpf.create_pan_dat(self.engine, schema)
        self.assertFalse(pdf._same_data(pan_dat, pg_pan_dat))
        self.assertTrue(pg_pan_dat._len_dict() == {'categories': 4, 'foods': 9})

    def test_diet_pd(self):
        if not self.can_run:
            return
        schema = "test_pg_diet"
        tdf = diet_schema
        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        pdf.set_infinity_io_flag(1e12)
        pgpf = pdf.pgsql
        pan_dat = pan_dat_maker(tdf.schema(), diet_dat)
        pgpf.write_schema(self.engine, schema, include_ancillary_info=False)
        pgpf.write_data(pan_dat, self.engine, schema)
        pg_pan_dat = pgpf.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))
        pdf.set_infinity_io_flag(None)
        pg_pan_dat_none_inf = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertFalse(pdf._same_data(pan_dat, pg_pan_dat_none_inf))
        pg_pan_dat_none_inf.categories.loc[pg_pan_dat_none_inf.categories["Name"] == "protein", "Max Nutrition"] = \
            float("inf")
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat_none_inf))

        pdf.set_infinity_io_flag("N/A")
        dat2 = diet_schema.copy_tic_dat(diet_dat)
        dat2.foods["za"] = dat2.foods.pop("pizza")
        dat2 = pan_dat_maker(tdf.schema(), dat2)
        pgpf.write_data(dat2, self.engine, schema, pre_existing_rows={"foods": "append"})
        dat3 = pgpf.create_pan_dat(self.engine, schema)
        self.assertTrue(set(pdf.find_duplicates(dat3)) == {'foods'})
        self.assertTrue(set(dat3.foods["Name"]).issuperset(dat2.foods["Name"]))
        self.assertTrue(set(dat3.foods["Name"]).issuperset(pan_dat.foods["Name"]))
        self.assertTrue(set(dat3.foods["Name"]).difference(pan_dat.foods["Name"]) == {'za'})
        self.assertTrue(set(dat3.foods["Name"]).difference(dat2.foods["Name"]) == {'pizza'})
        pgpf.write_data(dat2, self.engine, schema, pre_existing_rows={"nutrition_quantities": "append"})
        dat4 = pgpf.create_pan_dat(self.engine, schema)
        self.assertTrue(set(pdf.find_duplicates(dat4)) == {'nutrition_quantities'} and not pdf.find_duplicates(dat2))
        dat4.nutrition_quantities = dat4.nutrition_quantities[:36]
        self.assertFalse(pdf.find_duplicates(dat4))
        self.assertTrue(pdf._same_data(dat2, dat4))

        test_schema_2 = schema +  "_none_inf"
        pdf.set_infinity_io_flag(None)
        pgpf.write_schema(self.engine, test_schema_2)
        pgpf.write_data(pan_dat, self.engine, test_schema_2)
        pdf.set_infinity_io_flag("N/A")
        pg_pan_dat = pgpf.create_pan_dat(self.engine, test_schema_2)
        self.assertFalse(pdf._same_data(pan_dat, pg_pan_dat))
        pg_pan_dat.categories.loc[pg_pan_dat.categories["Name"] == "protein", "Max Nutrition"] = float("inf")
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))
        pdf.set_infinity_io_flag(None)
        pg_pan_dat_none_inf = pgpf.create_pan_dat(self.engine, test_schema_2)
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat_none_inf))

        pdf_ = PanDatFactory(**diet_schema.schema()) # doesnt have data types
        pdf_.set_infinity_io_flag(None)
        pgpf_null_inf = pdf_.pgsql
        pg_pan_dat_none_inf = pgpf_null_inf.create_pan_dat(self.engine, test_schema_2)
        self.assertFalse(pdf._same_data(pan_dat, pg_pan_dat_none_inf))
        self.assertTrue(math.isnan(pg_pan_dat_none_inf.categories[pg_pan_dat_none_inf.categories["Name"] == "protein"]
                        ["Max Nutrition"][0]))

    def test_big_diet_pd(self):
        if not self.can_run:
            return
        tdf = diet_schema
        pdf = PanDatFactory(**tdf.schema())
        pgpf = PostgresPanFactory(pdf)
        big_dat = diet_schema.copy_tic_dat(diet_dat)
        for k in range(int(1e5)):
            big_dat.categories[str(k)] = [0,100]
        pan_dat = pan_dat_maker(tdf.schema(), big_dat)
        schema = "test_pg_big_diet"
        now = time.time()
        pgpf.write_schema(self.engine, schema)
        pgpf.write_data(pan_dat, self.engine, schema)
        print(f"\npdf writing {big_dat._len_dict()} : {time.time()-now}**&&**\n")
        now = time.time()
        pg_pan_dat = pgpf.create_pan_dat(self.engine, schema)
        print(f"\npdf reading {big_dat._len_dict()} : {time.time()-now}**&&**\n")
        self.assertTrue(pdf._same_data(pan_dat, pg_pan_dat))

    def test_big_demand_lol(self):
        demand_lol_path = os.path.join(_this_directory(), "demand_lol.json")
        with open(demand_lol_path, "r") as f:
            demand_lol = json.load(f)
        self.assertTrue(set(map(len, demand_lol)) == {6}) # all rows of length six
        demand_lol = demand_lol * int(2000)
        self.assertTrue(len(demand_lol) == 1208000) # 1200K  records is a solid stress test
        print(len(demand_lol))
        pdf = PanDatFactory(demand=[["Site", "Proudct", "Time Period"], ["Min Demand", "Max Demand", "Revenue"]])
        pan_dat = pdf.PanDat(demand=demand_lol)
        schema = "test_big_demand_lol"
        pdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        now = time.time()
        pdf.pgsql.write_data(pan_dat, self.engine, schema)
        print(f"\npdf writing {len(demand_lol)} : {time.time()-now}**&&**\n")

    def test_extra_fields_pd(self):
        pdf = PanDatFactory(boger = [["a"], ["b", "c"]])
        pdf.set_default_value("boger", "c", "hoya")
        dat = pdf.PanDat(boger = pd.DataFrame({"a": [1, 2, 3], "b":[4, 5,6], "c":['a', 'b', 'c']}))
        schema = "test_pd_extra_fields"
        pdf.pgsql.write_schema(self.engine, schema, forced_field_types={("boger", "c"): "text",
                                                                                      ("boger", "a"):"float"})
        pdf.pgsql.write_data(dat, self.engine, schema)
        pdf2 = PanDatFactory(boger=[["a"], ["b"]])
        dat2 = pdf2.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(list(dat2.boger["a"]) == [1.0, 2.0, 3.0] and list(dat2.boger["b"]) == [4.0, 5.0, 6.0])
        dat2_2 = pdf2.PanDat(boger = pd.DataFrame({"a": [10, 300], "b":[40, 60]}))
        pdf2.pgsql.write_data(dat2_2, self.engine, schema)
        dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(list(dat.boger["a"]) == [10, 300] and list(dat.boger["b"]) == [40, 60])
        self.assertTrue(set(dat.boger["c"]) == {'hoya'})

        pdf2 = PanDatFactory(boger=[["b"], ["a"]])
        dat2_2 = pdf2.PanDat(boger = pd.DataFrame({"b":[41, 61], "a": [21, 31]}))
        pdf2.pgsql.write_data(dat2_2, self.engine, schema)
        dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(list(dat.boger["a"]) == [21, 31] and list(dat.boger["b"]) == [41, 61])
        self.assertTrue(set(dat.boger["c"]) == {'hoya'})

        pdf2 = PanDatFactory(boger=[["b"], ["z", "a"]])
        dat2_2 = pdf2.PanDat(boger = pd.DataFrame({"z": ["this", "that", "other"], "b":[42, 61, 1], "a": [22, 31, 2]}))
        self.assertTrue(list(dat2_2.boger.columns) == ['b', 'z', 'a'])
        # extra columns won't work when they are part of the PanDatFactory!
        ex = []
        try:
            pdf2.pgsql.write_data(dat2_2, self.engine, schema)
        except Exception as e:
            ex.append(str(e))
        self.assertTrue("Unable to recognize" in ex[0])

        pdf2 = PanDatFactory(boger=[["b"], ["a"]])
        pdf2.pgsql.write_data(dat2_2, self.engine, schema)
        dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(list(dat.boger["a"]) == [22, 31, 2] and list(dat.boger["b"]) == [42, 61, 1])
        self.assertTrue(set(dat.boger["c"]) == {'hoya'})


    def test_time_stamp(self):
        tdf = TicDatFactory(table=[["Blah"],["Timed Info"]])
        tdf.set_data_type("table", "Timed Info", nullable=True)
        tdf.set_default_value("table", "Timed Info", None)
        dat = tdf.TicDat()
        dat.table[1] = dateutil.parser.parse("2014-05-01 18:47:05.069722")
        dat.table[2] = dateutil.parser.parse("2014-05-02 18:47:05.178768")
        pgtf = tdf.pgsql
        pgtf.write_schema(self.engine, test_schema,
                          forced_field_types={('table', 'Blah'):"integer", ('table', 'Timed Info'):"timestamp"})
        pgtf.write_data(dat, self.engine, test_schema, dsn=self.postgresql.dsn())
        dat_2 = pgtf.create_tic_dat(self.engine, test_schema)
        self.assertTrue(tdf._same_data(dat, dat_2))
        self.assertTrue(all(isinstance(row["Timed Info"], datetime.datetime) for row in dat_2.table.values()))
        self.assertFalse(any(isinstance(k, datetime.datetime) for k in dat_2.table))

        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        def same_data(pan_dat, pan_dat_2):
            df1, df2 = pan_dat.table, pan_dat_2.table
            if list(df1["Blah"]) != list(df2["Blah"]):
                return False
            for dt1, dt2 in zip(df1["Timed Info"], df2["Timed Info"]):
                delta = dt1 - dt2
                if abs(delta.total_seconds()) > 1e-6:
                    return False
            return True
        pan_dat = pdf.pgsql.create_pan_dat(self.engine, test_schema)
        pan_dat_2 = pan_dat_maker(tdf.schema(), dat_2)
        self.assertTrue(same_data(pan_dat, pan_dat_2))
        for df in [_.table for _ in [pan_dat, pan_dat_2]]:
            for i in range(len(df)):
                self.assertFalse(isinstance(df.loc[i, "Blah"], datetime.datetime))
                self.assertTrue(isinstance(df.loc[i, "Timed Info"], datetime.datetime))

        pan_dat.table.loc[1, "Timed Info"] = dateutil.parser.parse("2014-05-02 18:48:05.178768")
        self.assertFalse(same_data(pan_dat, pan_dat_2))
        pdf.pgsql.write_data(pan_dat, self.engine, test_schema)
        pan_dat_2 = pdf.pgsql.create_pan_dat(self.engine, test_schema)
        self.assertTrue(same_data(pan_dat, pan_dat_2))

        dat.table[2] = dateutil.parser.parse("2014-05-02 18:48:05.178768")
        self.assertFalse(tdf._same_data(dat, dat_2))

    def test_missing_tables(self):
        schema = test_schema + "_missing_tables"
        tdf_1 = TicDatFactory(this = [["Something"],["Another"]])
        pdf_1 = PanDatFactory(**tdf_1.schema())
        tdf_2 = TicDatFactory(**dict(tdf_1.schema(), that=[["What", "Ever"],[]]))
        pdf_2 = PanDatFactory(**tdf_2.schema())
        dat = tdf_1.TicDat(this=[["a", 2],["b", 3],["c", 5]])
        pan_dat = tdf_1.copy_to_pandas(dat, drop_pk_columns=False)
        tdf_1.pgsql.write_schema(self.engine, schema)
        tdf_1.pgsql.write_data(dat, self.engine, schema)
        pg_dat = tdf_2.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf_1._same_data(dat, pg_dat))
        pg_pan_dat = pdf_2.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf_1._same_data(pan_dat, pg_pan_dat))

    def testNullsAndInf(self):
        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, nullable=True)
        dat = tdf.TicDat(table = [[None, 100], [200, 109], [0, 300], [300, None], [400, 0]])
        schema = test_schema + "_bool_defaults"
        tdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        tdf.pgsql.write_data(dat, self.engine, schema)

        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(dat, dat_1))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        tdf.set_infinity_io_flag(None)
        dat_inf = tdf.TicDat(table = [[float("inf"), 100], [200, 109], [0, 300], [300, float("inf")], [400, 0]])
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)

        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        tdf.pgsql.write_data(dat_inf, self.engine, schema)
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        tdf.set_infinity_io_flag(None)
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat_inf, dat_1))
        dat_inf = tdf.TicDat(table = [[float("-inf"), 100], [200, 109], [0, 300], [300, -float("inf")], [400, 0]])
        self.assertTrue(tdf._same_data(dat_inf, dat_1))

    def testDietWithInfFlagging(self):
        tdf = diet_schema.clone()
        dat = tdf.copy_tic_dat(diet_dat)
        tdf.set_infinity_io_flag(999999999)
        schema = test_schema + "_diet_inf_flagging"
        tdf.pgsql.write_schema(self.engine, schema)
        tdf.pgsql.write_data(dat, self.engine, schema)
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = tdf.clone()
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = TicDatFactory(**diet_schema.schema())
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat, dat_1))
        self.assertTrue(dat_1.categories["protein"]["Max Nutrition"] == 999999999)
        dat_1.categories["protein"]["Max Nutrition"] = float("inf")
        self.assertTrue(tdf._same_data(dat, dat_1))

    def testNullsPd(self):
        pdf = PanDatFactory(table=[[], ["field one", "field two"]])
        for f in ["field one", "field two"]:
            pdf.set_data_type("table", f, nullable=True)
        dat = pdf.PanDat(table = {"field one": [None, 200, 0, 300, 400], "field two": [100, 109, 300, None, 0]})
        schema = test_schema + "_bool_defaults_pd"
        pdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        pdf.pgsql.write_data(dat, self.engine, schema)

        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat, dat_1, nans_are_same_for_data_rows=True))

        pdf = PanDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            pdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        pdf.set_infinity_io_flag(None)
        dat_inf = pdf.PanDat(table = {"field one": [float("inf"), 200, 0, 300, 400],
                                      "field two": [100, 109, 300, float("inf"), 0]})
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)

        self.assertTrue(pdf._same_data(dat_inf, dat_1))
        pdf.pgsql.write_data(dat_inf, self.engine, schema)
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat_inf, dat_1))

        pdf = PanDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            pdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        pdf.set_infinity_io_flag(None)
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertFalse(pdf._same_data(dat_inf, dat_1))
        dat_inf = pdf.PanDat(table = {"field one": [-float("inf"), 200, 0, 300, 400],
                                      "field two": [100, 109, 300, -float("inf"), 0]})
        self.assertTrue(pdf._same_data(dat_inf, dat_1))

    def testDietWithInfFlaggingPd(self):
        pdf = PanDatFactory.create_from_full_schema(diet_schema.schema(include_ancillary_info=True))
        dat = diet_schema.copy_to_pandas(diet_dat, drop_pk_columns=False)
        pdf.set_infinity_io_flag(999999999)
        schema = test_schema + "_diet_inf_flagging_pd"
        pdf.pgsql.write_schema(self.engine, schema)
        pdf.pgsql.write_data(dat, self.engine, schema)
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat, dat_1))
        pdf = pdf.clone()
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat, dat_1))
        tdf = PanDatFactory(**diet_schema.schema())
        dat_1 = tdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat, dat_1))
        protein = dat_1.categories["Name"] == "protein"
        self.assertTrue(list(dat_1.categories[protein]["Max Nutrition"])[0] == 999999999)
        dat_1.categories.loc[protein, "Max Nutrition"] = float("inf")
        self.assertTrue(tdf._same_data(dat, dat_1))

    def test_parameters(self):
        schema = test_schema + "_parameters"
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.add_parameter("Something", 100)
        tdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        tdf.pgsql.write_schema(self.engine, schema)
        tdf.pgsql.write_data(dat, self.engine, schema)
        dat_ = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(dat, dat_))

    def test_parameters_pd(self):
        schema = test_schema + "_parameters_pd"
        pdf = PanDatFactory(parameters=[["Key"], ["Value"]])
        pdf.add_parameter("Something", 100)
        pdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = TicDatFactory(**pdf.schema()).TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        dat = TicDatFactory(**pdf.schema()).copy_to_pandas(dat, drop_pk_columns=False)
        pdf.pgsql.write_schema(self.engine, schema)
        pdf.pgsql.write_data(dat, self.engine, schema)
        dat_ = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat, dat_))

    def testDateTime(self):
        schema = test_schema + "_datetime"
        tdf = TicDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        tdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        tdf.add_parameter("p2", None, datetime=True, nullable=True)
        tdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        tdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)

        dat = tdf.TicDat(table_with_stuffs = [[dateutil.parser.parse("July 11 1972"), None],
                                              [datetime.datetime.now(), dateutil.parser.parse("Sept 11 2011")]],
                         parameters = [["p1", "7/11/1911"], ["p2", None]])
        self.assertFalse(tdf.find_data_type_failures(dat) or tdf.find_data_row_failures(dat))

        tdf.pgsql.write_schema(self.engine, schema)
        tdf.pgsql.write_data(dat, self.engine, schema)
        dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat, dat_1,  nans_are_same_for_data_rows=True))
        self.assertTrue(all(len(getattr(dat, t)) == len(getattr(dat_1, t)) for t in tdf.all_tables))
        self.assertFalse(tdf.find_data_type_failures(dat_1) or tdf.find_data_row_failures(dat_1))
        self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], datetime.datetime))
        self.assertTrue(all(isinstance(_, datetime.datetime) for _ in dat_1.table_with_stuffs))
        self.assertTrue(len([_ for _ in dat_1.table_with_stuffs if pd.isnull(_)]) == 0)
        self.assertTrue(all(isinstance(_, datetime.datetime) or pd.isnull(_) for v in dat_1.table_with_stuffs.values()
                            for _ in v.values()))
        self.assertTrue(len([_ for v in dat_1.table_with_stuffs.values() for _ in v.values() if pd.isnull(_)]) == 1)
        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        pan_dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        dat_2 = pdf.copy_to_tic_dat(pan_dat)
        # pandas can be a real PIA sometimes, hacking around some weird downcasting
        for k in list(dat_2.table_with_stuffs):
            dat_2.table_with_stuffs[pd.Timestamp(k)] = dat_2.table_with_stuffs.pop(k)
        self.assertTrue(tdf._same_data(dat_1, dat_2, nans_are_same_for_data_rows=True))

        pdf.pgsql.write_data(pan_dat, self.engine, schema)
        dat_3 = pdf.copy_to_tic_dat(pdf.pgsql.create_pan_dat(self.engine, schema))
        for k in list(dat_3.table_with_stuffs):
            dat_3.table_with_stuffs[pd.Timestamp(k)] = dat_3.table_with_stuffs.pop(k)
        self.assertTrue(tdf._same_data(dat_1, dat_3, nans_are_same_for_data_rows=True))

    def test_nullables(self):
        schema = test_schema + "nullables"
        pdf = PanDatFactory(table_with_stuffs = [["field one"], ["field two"]])
        pdf.set_data_type("table_with_stuffs", "field one")
        pdf.set_data_type("table_with_stuffs", "field two", number_allowed=False, strings_allowed='*', nullable=True)
        tdf = TicDatFactory.create_from_full_schema(pdf.schema(include_ancillary_info=True))
        tic_dat = tdf.TicDat(table_with_stuffs=[[101, "022"], [202, None], [303, "111"]])
        dat = tdf.copy_to_pandas(tic_dat, drop_pk_columns=False)
        self.assertFalse(tdf.find_data_type_failures(tic_dat))
        self.assertFalse(pdf.find_data_type_failures(dat))

        pdf.pgsql.write_schema(self.engine, schema)
        pdf.pgsql.write_data(dat, self.engine, schema)
        dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(dat, dat_1,  nans_are_same_for_data_rows=True))
        tic_dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf._same_data(tic_dat, tic_dat_1, nans_are_same_for_data_rows=True))

    def testIssue45(self):
        schema = test_schema + "issue45"
        pdf = PanDatFactory(data=[["a"], ["b"]])
        pdf.set_data_type("data", "b", number_allowed=False, strings_allowed='*')
        tdf = TicDatFactory.create_from_full_schema(pdf.schema(include_ancillary_info=True))
        tic_dat = tdf.TicDat(data = [[2,"1"],[4,"3"], [44,"022"]])
        dat = tdf.copy_to_pandas(tic_dat, drop_pk_columns=False)
        self.assertFalse(tdf.find_data_type_failures(tic_dat))
        self.assertFalse(pdf.find_data_type_failures(dat))
        pdf.pgsql.write_schema(self.engine, schema, forced_field_types={("data", "a"): "integer"})
        pdf.pgsql.write_data(dat, self.engine, schema)
        def two_checks():
            dat_1 = pdf.pgsql.create_pan_dat(self.engine, schema)
            self.assertTrue(pdf._same_data(dat, dat_1))
            tic_dat_1 = tdf.pgsql.create_tic_dat(self.engine, schema)
            self.assertTrue(tdf._same_data(tic_dat, tic_dat_1))
        two_checks()
        tdf.pgsql.write_data(tic_dat, self.engine, schema)
        two_checks()

    def testIssue55(self):
        schema = test_schema + "issue45"
        tdf = TicDatFactory(boger=[["A Fields"], ["556f"]])
        tdf.pgsql.write_schema(self.engine, schema)

    def testCircularFks(self):
        schema = test_schema + "circular_fks"
        tdf = TicDatFactory(table_one=[["A Field"], []], table_two=[["B Field"],[]], table_three=[["C Field"], []])
        tdf.add_foreign_key("table_one", "table_two", ["A Field", "B Field"])
        tdf.add_foreign_key("table_two", "table_three", ["B Field", "C Field"])
        tdf.add_foreign_key("table_three", "table_one", ["C Field", "A Field"])
        tdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        t_ = [["a"], ["b"], ["c"]]
        dat = tdf.TicDat(table_one=t_, table_two=t_, table_three=t_)
        tdf.pgsql.write_data(dat, self.engine, schema)

    def test_extra_pdf_column(self):
        schema = test_schema + "extra_cols"
        pdf = PanDatFactory(foo=[[], ['a', 'b']], bar=[['d'], ['i']])
        for t, [pks, dfs] in pdf.schema().items():
            for f in pks+dfs:
                pdf.set_data_type(t, f, number_allowed=True, strings_allowed=[])
        foo_df = pd.DataFrame({'a': [1, 2], 'b': [3, 4], 'c': [5, 6]})  # notice that I added an extra column 'c'
        bar_df = pd.DataFrame({'d': [10, 20], 'h': [30, 40], 'i': [50, 60]})
        pan_dat = pdf.PanDat(foo=foo_df, bar=bar_df)
        pgpf = pdf.pgsql
        pgpf.write_schema(self.engine, schema, include_ancillary_info=False)
        pgpf.write_data(pan_dat, self.engine, schema)
        pan_dat_2 = pgpf.create_pan_dat(self.engine, schema)
        self.assertTrue(pdf._same_data(pan_dat, pan_dat_2, epsilon=1e-4))
        self.assertTrue(set(pan_dat.foo.columns).difference(pan_dat_2.foo.columns) == {'c'})

    def testDateTimeTwo(self):
        schema = test_schema + "_date_two"
        tdf = TicDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        tdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        tdf.add_parameter("p2", None, datetime=True, nullable=True)
        tdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        tdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)

        n = str(datetime.datetime.now())
        dat = tdf.TicDat(table_with_stuffs = [[dateutil.parser.parse("July 11 1972"), None],
                                              [dateutil.parser.parse(n[0:n.find(" ")]),
                                               dateutil.parser.parse("Sept 11 2011")]],
                         parameters = [["p1", "7/11/1911"], ["p2", None]])
        self.assertFalse(tdf.find_data_type_failures(dat) or tdf.find_data_row_failures(dat))

        tdf.pgsql.write_schema(self.engine, schema,
                               forced_field_types={("table_with_stuffs", "field one"): "date",
                                                   ("table_with_stuffs", "field two"): "date"})
        tdf.pgsql.write_data(dat, self.engine, schema)

        dat_2 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat, dat_2, nans_are_same_for_data_rows=True))
        dat.parameters['p1']['b'] = dateutil.parser.parse(dat.parameters['p1']['b'])
        self.assertTrue(tdf._same_data(dat, dat_2, nans_are_same_for_data_rows=True))

        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        pan_dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        dat_3 = pdf.copy_to_tic_dat(pan_dat)
        self.assertTrue(tdf._same_data(dat, dat_3, nans_are_same_for_data_rows=True))

    def test_pgtd_pandas_none_null(self):
        # https://rb.gy/t6m88 (issue 5 on pjcpjc/ro_h) is particularly interested in getting the PG NULL to map onto
        # None with PanDatFactory
        if not self.can_run:
            return
        schema = test_schema+"_none_null"
        tdf_1 = TicDatFactory(t_one=[["Field One"], ["Field Two"]])
        tdf_1.set_data_type("t_one", "Field Two", strings_allowed='*', number_allowed=False, nullable=True)
        dat = tdf_1.TicDat(t_one = [["a", None], ["b", "b"], [None, "c"], ["c", "c"]])
        self.assertTrue(len(dat.t_one) == 4)
        tdf_1.pgsql.write_schema(self.engine, schema, include_ancillary_info=False)
        tdf_1.pgsql.write_data(dat, self.engine, schema)
        dat_2 = tdf_1.pgsql.create_tic_dat(self.engine, schema)
        self.assertTrue(tdf_1._same_data(dat, dat_2))
        pdf = PanDatFactory.create_from_full_schema(tdf_1.schema(include_ancillary_info=True))
        pan_dat = pdf.pgsql.create_pan_dat(self.engine, schema)
        dat_3 = pdf.copy_to_tic_dat(pan_dat)
        self.assertTrue(tdf_1._same_data(dat, dat_3))
        self.assertTrue((dat_3.t_one["a"]['Field Two'] == None) and None in dat_3.t_one)

    def test_pickle_w_text_columns(self):
        # this text in response to a request for blob field type support. I'd rather not support blobs
        # if I don't have to, but supporting pickle.dumps is legit
        if not self.can_run:
            return
        schema = test_schema+"_pickle"
        tdf = TicDatFactory(t_one=[["Field One"], ["Field Two"]])
        dat = tdf.TicDat(t_one = [["a", pickle.dumps([1, 2])], ["b", pickle.dumps({"a": [1, 2], "b": [1, 2, 3]})]])
        pdf = tdf.clone(clone_factory=PanDatFactory)
        pan_dat = pdf.PanDat(**{t: getattr(tdf.copy_to_pandas(dat, reset_index=True), t) for t in tdf.all_tables})
        self.assertTrue(len(dat.t_one) == len(pan_dat.t_one) == 2)
        tdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False,
                               forced_field_types={("t_one", "Field Two"): "bytea"})
        tdf.pgsql.write_data(dat, self.engine, schema)
        dat_2 = tdf.pgsql.create_tic_dat(self.engine, schema)
        self.assertFalse(tdf._same_data(dat, dat_2)) # its  weird this way, but no matter, we only care about pickle
        self.assertTrue(set(dat.t_one) == set(dat_2.t_one))
        for k, r in dat.t_one.items():
            self.assertTrue(pickle.loads(r["Field Two"]) == pickle.loads(dat_2.t_one[k]["Field Two"]))

        schema = test_schema+"_pickle_pdf"
        pdf.pgsql.write_schema(self.engine, schema, include_ancillary_info=False,
                               forced_field_types={("t_one", "Field Two"): "bytea"})
        pdf.pgsql.write_data(pan_dat, self.engine, schema)
        pan_dat_2 = pdf.pgsql.create_pan_dat(self.engine, schema)
        dat_3 = pdf.copy_to_tic_dat(pan_dat)
        dat_4 = pdf.copy_to_tic_dat(pan_dat_2)
        self.assertFalse(tdf._same_data(dat_3, dat_4))
        self.assertTrue(set(dat_3.t_one) == set(dat_4.t_one))
        for k, r in dat_3.t_one.items():
            self.assertTrue(pickle.loads(r["Field Two"]) == pickle.loads(dat_4.t_one[k]["Field Two"]))

test_schema = 'test'


# Run the tests.
if __name__ == "__main__":
    if not _can_unit_test:
        print("!!!!!!!!!FAILING POSTGRES UNIT TESTS DUE TO FAILURE TO LOAD SQLALCHEMY!!!!!!!!")
    else:
        TestPostres.can_run = True
    unittest.main()