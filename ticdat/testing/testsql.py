import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanPath, addNetflowForeignKeys, addDietForeignKeys, flagged_as_run_alone
from ticdat.testing.ticdattestutils import spacesData, spacesSchema, dietSchemaWeirdCase, dietSchemaWeirdCase2
from ticdat.testing.ticdattestutils import copyDataDietWeirdCase, copyDataDietWeirdCase2, am_on_windows
from ticdat.sqlitetd import _can_unit_test, sql
import datetime
try:
    import dateutil, dateutil.parser
except:
    dateutil=None

import shutil
import unittest

#@fail_to_debugger
class TestSql(unittest.TestCase):
    can_run = False
    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        if am_on_windows: # working around issue ticdat/ticdat#1
            try:
                shutil.rmtree(_scratchDir)
            except:
                pass
        else:
            shutil.rmtree(_scratchDir)
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def _test_generic_copy(self, ticDat, tdf, skip_tables=None):
        assert all(tdf.primary_key_fields.get(t) for t in tdf.all_tables)
        path = makeCleanDir(os.path.join(_scratchDir, "generic_copy"))
        replace_name  = lambda f : "name_" if f == "name" else f
        clean_tdf = TicDatFactory(**{t:[list(map(replace_name, pks)), dfs]
                                     for t,(pks, dfs) in tdf.schema().items()})

        temp_tdf = TicDatFactory(**{t:v if t in (skip_tables or []) else '*'
                                    for t,v in clean_tdf.schema().items()})
        temp_dat = temp_tdf.TicDat(**{t:getattr(ticDat, t) for t in (skip_tables or [])})
        for t in temp_tdf.generic_tables:
            setattr(temp_dat, t, getattr(clean_tdf.copy_to_pandas(ticDat, drop_pk_columns=False) ,t))

        temp_tdf.sql.write_db_data(temp_dat, os.path.join(path, "f.db"))
        temp_tdf.sql.write_sql_file(temp_dat, os.path.join(path, "f1.sql"), include_schema=False)
        temp_tdf.sql.write_sql_file(temp_dat, os.path.join(path, "f2.sql"), include_schema=True)

        for file_name, includes_schema in [("f.db", False), ("f1.sql", False), ("f2.sql", True)]:
            file_path = os.path.join(path, file_name)
            if file_path.endswith(".db"):
                self.assertFalse(temp_tdf.sql.find_duplicates(file_path))
                read_dat = temp_tdf.sql.create_tic_dat(file_path)
            else:
                read_dat = temp_tdf.sql.create_tic_dat_from_sql(file_path, includes_schema)
            generic_free_dat, _ = utils.create_generic_free(read_dat, temp_tdf)
            check_dat = clean_tdf.TicDat()
            for t in temp_tdf.generic_tables:
                for r in getattr(generic_free_dat, t):
                    pks = clean_tdf.primary_key_fields[t]
                    getattr(check_dat, t)[r[pks[0]] if len(pks) == 1 else tuple(r[_] for _ in pks)] = \
                        {df:r[df] for df in clean_tdf.data_fields.get(t, [])}
            for t in (skip_tables or []):
                for k,v in getattr(generic_free_dat, t).items():
                    getattr(check_dat, t)[k] = v
            self.assertTrue(clean_tdf._same_data(check_dat, clean_tdf.copy_tic_dat(ticDat)))

    def testDups(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(one = [["a"],["b", "c"]],
                            two = [["a", "b"],["c"]],
                            three = [["a", "b", "c"],[]])
        tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
        td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                            for t in tdf.all_tables})
        f = makeCleanPath(os.path.join(_scratchDir, "testDups.db"))
        tdf2.sql.write_db_data(td, f)
        dups = tdf.sql.find_duplicates(f)
        self.assertTrue(dups ==  {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})

    def testDiet(self):
        if not self.can_run:
            return
        def doTheTests(tdf) :
            ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
            filePath = makeCleanPath(os.path.join(_scratchDir, "diet.db"))
            tdf.sql.write_db_data(ticDat, filePath)
            self.assertFalse(tdf.sql.find_duplicates(filePath))
            sqlTicDat = tdf.sql.create_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            def changeit() :
                sqlTicDat.categories["calories"]["minNutrition"]=12
            changeit()
            self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

            self.assertTrue(self.firesException(lambda : tdf.sql.write_db_data(ticDat, filePath)))
            tdf.sql.write_db_data(ticDat, filePath, allow_overwrite=True)
            sqlTicDat = tdf.sql.create_tic_dat(filePath, freeze_it=True)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            self.assertTrue(self.firesException(changeit))
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

            filePath = makeCleanPath(os.path.join(_scratchDir, "diet.sql"))
            tdf.sql.write_sql_file(ticDat, filePath)
            sqlTicDat = tdf.sql.create_tic_dat_from_sql(filePath)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            changeit()
            self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

            tdf.sql.write_sql_file(ticDat, filePath, include_schema=True, allow_overwrite=True)
            sqlTicDat = tdf.sql.create_tic_dat_from_sql(filePath, includes_schema=True, freeze_it=True)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            self.assertTrue(self.firesException(changeit))
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

        doTheTests(TicDatFactory(**dietSchema()))

        tdf = TicDatFactory(**dietSchema())
        self.assertFalse(tdf.foreign_keys)
        tdf.set_default_values(categories =  {'maxNutrition': float("inf"), 'minNutrition': 0.0},
                               foods =  {'cost': 0.0},
                               nutritionQuantities =  {'qty': 0.0})
        addDietForeignKeys(tdf)
        ordered = tdf.sql._ordered_tables()
        self.assertTrue(ordered.index("categories") < ordered.index("nutritionQuantities"))
        self.assertTrue(ordered.index("foods") < ordered.index("nutritionQuantities"))

        ticDat = tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["nutritionQuantities"])
        origTicDat = tdf.copy_tic_dat(ticDat)
        self.assertTrue(tdf._same_data(ticDat, origTicDat))
        self.assertFalse(tdf.find_foreign_key_failures(ticDat))
        ticDat.nutritionQuantities['hot dog', 'boger'] = ticDat.nutritionQuantities['junk', 'protein'] = -12
        self.assertTrue(tdf.find_foreign_key_failures(ticDat) ==
        {('nutritionQuantities', 'foods', ('food', 'name'), 'many-to-one'): (('junk',), (('junk', 'protein'),)),
         ('nutritionQuantities', 'categories', ('category', 'name'), 'many-to-one'):
             (('boger',), (('hot dog', 'boger'),))})

        self.assertFalse(tdf._same_data(ticDat, origTicDat))
        tdf.remove_foreign_key_failures(ticDat)
        self.assertFalse(tdf.find_foreign_key_failures(ticDat))
        self.assertTrue(tdf._same_data(ticDat, origTicDat))

        doTheTests(tdf)

    def testWeirdDiets(self):
        if not self.can_run:
            return
        filePath = os.path.join(_scratchDir, "weirdDiet.db")
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.sql.write_db_data(dat2, filePath , allow_overwrite=True)
        self.assertFalse(tdf2.sql.find_duplicates(filePath))
        sqlTicDat = tdf.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))


        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.sql.write_db_data(dat3, makeCleanPath(filePath))
        with sql.connect(filePath) as con:
            con.execute("ALTER TABLE nutrition_quantities RENAME TO [nutrition quantities]")

        sqlTicDat2 = tdf3.sql.create_tic_dat(filePath)
        self.assertTrue(tdf3._same_data(dat3, sqlTicDat2))
        with sql.connect(filePath) as con:
            con.execute("create table nutrition_quantities(boger)")

        self.assertTrue(self.firesException(lambda : tdf3.sql.create_tic_dat(filePath)))

    def testNetflow(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        ordered = tdf.sql._ordered_tables()
        self.assertTrue(ordered.index("nodes") < min(ordered.index(_) for _ in ("arcs", "cost", "inflow")))
        self.assertTrue(ordered.index("commodities") < min(ordered.index(_) for _ in ("cost", "inflow")))
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["arcs", "nodes"])
        filePath = os.path.join(_scratchDir, "netflow.sql")
        tdf.sql.write_db_data(ticDat, filePath)
        self.assertFalse(tdf.sql.find_duplicates(filePath))
        sqlTicDat = tdf.sql.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
        def changeIt() :
            sqlTicDat.inflow['Pencils', 'Boston']["quantity"] = 12
        self.assertTrue(self.firesException(changeIt))
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

        sqlTicDat = tdf.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
        self.assertFalse(self.firesException(changeIt))
        self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

        pkHacked = netflowSchema()
        pkHacked["nodes"][0] = ["nimrod"]
        tdfHacked = TicDatFactory(**pkHacked)
        ticDatHacked = tdfHacked.TicDat(**{t : getattr(ticDat, t) for t in tdf.all_tables})
        tdfHacked.sql.write_db_data(ticDatHacked, makeCleanPath(filePath))
        self.assertFalse(tdfHacked.sql.find_duplicates(filePath))
        self.assertTrue(self.firesException(lambda : tdfHacked.sql.write_db_data(ticDat, filePath)))
        tdfHacked.sql.write_db_data(ticDat, filePath, allow_overwrite =True)
        self.assertTrue("Unable to recognize field name in table nodes" in
                        self.firesException(lambda  :tdf.sql.create_tic_dat(filePath)))

        ticDatNew = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})

        ticDatNew.cost['Pencils', 'booger', 'wooger'] =  10
        ticDatNew.cost['junker', 'Detroit', 'New York'] =  20
        ticDatNew.cost['bunker', 'Detroit', 'New Jerk'] =  20
        ticDatNew.arcs['booger', 'wooger'] =  112
        self.assertTrue({f[:2] + f[2][:1] : set(v.native_pks) for
                         f,v in tdf.find_foreign_key_failures(ticDatNew).items()} ==
        {('arcs', 'nodes', u'destination'): {('booger', 'wooger')},
         ('arcs', 'nodes', u'source'): {('booger', 'wooger')},
         ('cost', 'commodities', u'commodity'): {('bunker', 'Detroit', 'New Jerk'),
                                                 ('junker', 'Detroit', 'New York')},
         ('cost', 'nodes', u'destination'): {('bunker', 'Detroit', 'New Jerk'),
                                             ('Pencils', 'booger', 'wooger')},
         ('cost', 'nodes', u'source'): {('Pencils', 'booger', 'wooger')}})

        ticDat3 = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        ticDat3.arcs['Detroit', 'Boston'] = float("inf")
        ticDat3.arcs['Denver',  'Boston'] = float("inf")
        self.assertFalse(tdf._same_data(ticDat3, ticDat))
        tdf.sql.write_db_data(ticDat3, makeCleanPath(filePath))
        ticDat4 = tdf.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat3, ticDat4))

    def testSilly(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())
        schema2 = sillyMeSchema()
        schema2["b"][0] = ("bField2", "bField1", "bField3")
        schema3 = sillyMeSchema()
        schema3["a"][1] = ("aData2", "aData3", "aData1")
        schema4 = sillyMeSchema()
        schema4["a"][1] = ("aData1", "aData3")
        schema5 = sillyMeSchema()
        _tuple = lambda x : tuple(x) if utils.containerish(x) else (x,)
        for t in ("a", "b") :
            schema5[t][1] = _tuple(schema5[t][1]) + _tuple(schema5[t][0])
        schema5["a"][0], schema5["b"][0] =  (),  []
        schema6 = sillyMeSchema()
        schema6["d"] =  [["dField"],()]

        tdf2, tdf3, tdf4, tdf5, tdf6 = (TicDatFactory(**x) for x in (schema2, schema3, schema4, schema5, schema6))
        tdf5.set_generator_tables(("a","c"))
        tdf5 = tdf5.clone()
        filePath = os.path.join(_scratchDir, "silly.db")
        tdf.sql.write_db_data(ticDat, filePath)
        self.assertFalse(tdf.sql.find_duplicates(filePath))

        ticDat2 = tdf2.sql.create_tic_dat(filePath)
        self.assertFalse(tdf._same_data(ticDat, ticDat2))

        ticDat3 = tdf3.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat3))

        ticDat4 = tdf4.sql.create_tic_dat(filePath)
        for t in ["a","b"]:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.sql.create_tic_dat(filePath)
        self.assertTrue(tdf5._same_data(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

        self.assertTrue(tdf._same_data(ticDat, tdf6.sql.create_tic_dat(filePath)))
        ticDat.a["theboger"] = (1, None, 12)
        if am_on_windows:
            filePath = filePath.replace("silly.db", "silly_2.db") # working around issue ticdat/ticdat#1
        tdf.sql.write_db_data(ticDat, makeCleanPath(filePath))
        ticDatNone = tdf.sql.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)

    def testInjection(self):
        if not self.can_run:
            return
        problems = [ "'", "''", '"', '""']
        tdf = TicDatFactory(boger = [["a"], ["b"]])
        dat = tdf.TicDat()
        for v,k in enumerate(problems):
            dat.boger[k]=v
            dat.boger[v]=k
        filePath = makeCleanPath(os.path.join(_scratchDir, "injection.db"))
        tdf.sql.write_db_data(dat, filePath)
        dat2 = tdf.sql.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat,dat2))

        filePath = makeCleanPath(os.path.join(_scratchDir, "injection.sql"))
        tdf.sql.write_sql_file(dat, filePath)
        self.assertTrue(firesException(lambda : tdf.sql.create_tic_dat_from_sql(filePath)))

    def testSpacey(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**spacesSchema())
        dat = tdf.TicDat(**spacesData())
        filePath = makeCleanPath(os.path.join(_scratchDir, "spacey.db"))
        tdf.sql.write_db_data(dat, filePath)
        dat2 = tdf.sql.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat,dat2))

        with sql.connect(filePath) as con:
            for t in tdf.all_tables:
                con.execute("ALTER TABLE %s RENAME TO [%s]"%(t, t.replace("_", " ")))
        dat3 = tdf.sql.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat, dat3))

    def testDefaults(self):
        tdf = TicDatFactory(one=[["a"],["b", "c"]], two=[["a", "b"],["c"]], three=[["a", "b", "c"],[]])
        dat = tdf.TicDat(one=[[1, 2, 3],[4, 5, 6]], two=[[1, 2, 3],[4 ,5, 6]], three=[[1, 2, 3], [4, 5, 6]])
        filePath = makeCleanPath(os.path.join(_scratchDir, "defaults.sql"))
        tdf.sql.write_sql_file(dat, filePath)

        tdf2 = TicDatFactory(one=[["a"],["b", "c"]], two=[["a", "b"],["c"]], three=[["a", "b", "c"],["d"]])
        dat2 = tdf2.TicDat(one=dat.one, two=dat.two, three={k:{} for k in dat.three})
        dat22 = tdf2.sql.create_tic_dat_from_sql(filePath)
        self.assertTrue(tdf2._same_data(dat2, dat22))



        tdf2 = TicDatFactory(one=[["a"],["b", "c"]], two=[["a", "b"],["c"]], three=[["a", "b", "c"],["d"]])
        tdf2.set_default_value("three", "d", float("inf"))
        dat2_b = tdf2.TicDat(one=dat.one, two=dat.two, three={k:{} for k in dat.three})
        dat22_b = tdf2.sql.create_tic_dat_from_sql(filePath)
        self.assertTrue(tdf2._same_data(dat2_b, dat22_b))

        self.assertFalse(tdf2._same_data(dat2, dat2_b))

    def testBooleansAndNulls(self):
        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        dat = tdf.TicDat(table = [[None, 100], [200, True], [False, 300], [300, None], [400, False]])
        file_one = os.path.join(_scratchDir, "boolDefaults.sql")
        file_two = os.path.join(_scratchDir, "boolDefaults.db")
        tdf.sql.write_sql_file(dat, file_one)
        tdf.sql.write_db_data(dat, file_two)
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        dat_2 = tdf.sql.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        tdf.set_infinity_io_flag(None)
        dat_inf = tdf.TicDat(table = [[float("inf"), 100], [200, True], [False, 300], [300, float("inf")],
                                      [400, False]])
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        dat_2 = tdf.sql.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))
        tdf.sql.write_sql_file(dat_inf, makeCleanPath(file_one))
        tdf.sql.write_db_data(dat_inf, file_two, allow_overwrite=True)
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        dat_2 = tdf.sql.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        tdf.set_infinity_io_flag(None)
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        dat_2 = tdf.sql.create_tic_dat(file_two)
        self.assertFalse(tdf._same_data(dat_inf, dat_1))
        self.assertFalse(tdf._same_data(dat_inf, dat_2))
        dat_inf = tdf.TicDat(table = [[float("-inf"), 100], [200, True], [False, 300], [300, -float("inf")],
                                      [400, False]])
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))

    def testDietWithInfFlagging(self):
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        tdf.set_infinity_io_flag(999999999)
        file_one = os.path.join(_scratchDir, "dietInfFlag.sql")
        file_two = os.path.join(_scratchDir, "dietInfFlag.db")
        tdf.sql.write_sql_file(dat, file_one)
        tdf.sql.write_db_data(dat, file_two)
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        dat_2 = tdf.sql.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))
        tdf = tdf.clone()
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = TicDatFactory(**dietSchema())
        dat_1 = tdf.sql.create_tic_dat_from_sql(file_one)
        self.assertFalse(tdf._same_data(dat, dat_1))

    def test_parameters(self):
        filePath = os.path.join(_scratchDir, "parameters")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.add_parameter("Something", 100)
        tdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        tdf.sql.write_sql_file(dat, filePath+".sql")
        dat_ = tdf.sql.create_tic_dat_from_sql(filePath+".sql")
        self.assertTrue(tdf._same_data(dat, dat_))
        tdf.sql.write_db_data(dat, filePath+".db")
        dat_ = tdf.sql.create_tic_dat(filePath+".db")
        self.assertTrue(tdf._same_data(dat, dat_))

    def test_missing_tables(self):
        path = os.path.join(_scratchDir, "missing")
        tdf_1 = TicDatFactory(this = [["Something"],["Another"]])
        tdf_2 = TicDatFactory(**dict(tdf_1.schema(), that=[["What", "Ever"],[]]))
        dat = tdf_1.TicDat(this=[["a", 2],["b", 3],["c", 5]])
        tdf_1.sql.write_sql_file(dat, path+".sql")
        sql_dat = tdf_2.sql.create_tic_dat_from_sql(path+".sql")
        self.assertTrue(tdf_1._same_data(dat, sql_dat))
        tdf_1.sql.write_db_data(dat, path+".db")
        sql_dat = tdf_2.sql.create_tic_dat(path+".db")
        self.assertTrue(tdf_1._same_data(dat, sql_dat))

    def testDateTime(self):
        tdf = TicDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        tdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        tdf.add_parameter("p2", None, datetime=True, nullable=True)
        tdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        tdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)

        dat = tdf.TicDat(table_with_stuffs = [["July 11 1972", None],
                                              [datetime.datetime.now(), dateutil.parser.parse("Sept 11 2011")]],
                         parameters = [["p1", "7/11/1911"], ["p2", None]])
        self.assertFalse(tdf.find_data_type_failures(dat) or tdf.find_data_row_failures(dat))

        path = os.path.join(_scratchDir, "datetime.db")
        tdf.sql.write_db_data(dat, path)
        dat_1 = tdf.sql.create_tic_dat(path)
        self.assertFalse(tdf._same_data(dat, dat_1))
        self.assertFalse(tdf.find_data_type_failures(dat_1) or tdf.find_data_row_failures(dat_1))
        self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], datetime.datetime))
        self.assertTrue(all(isinstance(_, datetime.datetime) for _ in dat_1.table_with_stuffs))
        self.assertTrue(all(isinstance(_, datetime.datetime) or _ is None for v in dat_1.table_with_stuffs.values()
                            for _ in v.values()))
        path = os.path.join(_scratchDir, "datetime.sql")
        tdf.sql.write_sql_file(dat, path)
        dat_2 = tdf.sql.create_tic_dat_from_sql(path)
        self.assertTrue(tdf._same_data(dat_1, dat_2, nans_are_same_for_data_rows=True))

_scratchDir = TestSql.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING SQL UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not _can_unit_test :
        print("!!!!!!!!!FAILING SQL UNIT TESTS DUE TO FAILURE TO LOAD SQL LIBRARIES!!!!!!!!")
    else:
        TestSql.can_run = True
    unittest.main()
