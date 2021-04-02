import os

import ticdat.utils as utils
import shutil
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, dietSchemaWeirdCase
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, copyDataDietWeirdCase
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, sillyMeDataTwoTables, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanDir, dietSchemaWeirdCase2, copyDataDietWeirdCase2
from ticdat.testing.ticdattestutils import flagged_as_run_alone
import unittest
from ticdat.csvtd import _can_unit_test
import datetime
try:
    import dateutil, dateutil.parser
except:
    dateutil=None

#@fail_to_debugger
class TestCsv(unittest.TestCase):
    can_run = False

    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_scratchDir)
    def firesException(self, f, troubleshoot=False):
        if troubleshoot:
            import ipdb
            ipdb.set_trace()
            f()
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

        temp_tdf.csv.write_directory(temp_dat, path)
        self.assertFalse(temp_tdf.csv.find_duplicates(path))
        read_dat = temp_tdf.csv.create_tic_dat(path)
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

    def testDiet(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["nutritionQuantities"])
        dirPath = os.path.join(_scratchDir, "diet")
        tdf.csv.write_directory(ticDat,dirPath)
        self.assertFalse(tdf.csv.find_duplicates(dirPath))
        csvTicDat = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))
        def change() :
            csvTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(firesException(change))
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))

        self.assertTrue(self.firesException(lambda  :
            tdf.csv.write_directory(ticDat, dirPath, dialect="excel_t")
                                    ).endswith("Invalid dialect excel_t"))

        tdf.csv.write_directory(ticDat, dirPath, dialect="excel-tab", allow_overwrite=True)
        self.assertTrue(self.firesException(lambda : tdf.csv.create_tic_dat(dirPath, freeze_it=True)))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True, dialect="excel-tab")
        self.assertTrue(firesException(change))
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.csv.write_directory(dat2, dirPath, allow_overwrite=True)
        csvTicDat2 = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat2))
        os.rename(os.path.join(dirPath, "nutritionquantities.csv"),
                  os.path.join(dirPath, "nutritionquantities.csv".upper()))
        csvTicDat2 = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat2))

        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.csv.write_directory(dat3, dirPath, allow_overwrite=True)
        os.rename(os.path.join(dirPath, "nutrition_quantities.csv"),
                  os.path.join(dirPath, "nutrition quantities.csv"))
        csvDat3 = tdf3.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf3._same_data(dat3, csvDat3))
        shutil.copy(os.path.join(dirPath, "nutrition quantities.csv"),
                    os.path.join(dirPath, "nutrition_quantities.csv"))
        self.assertTrue(self.firesException(lambda : tdf3.csv.create_tic_dat(dirPath)))

    def testMissingTable(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k != "nutritionQuantities"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        dirPath = os.path.join(_scratchDir, "diet_missing")
        tdf2.csv.write_directory(ticDat2, makeCleanDir(dirPath))
        ticDat3 = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities)
        self.assertTrue(ticDat3.categories and ticDat3.foods)

        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k == "categories"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        tdf2.csv.write_directory(ticDat2, makeCleanDir(dirPath))
        ticDat3 = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities or ticDat3.foods)
        self.assertTrue(ticDat3.categories)

    def testSillyTwoTables(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.set_data_type("a", "aField", strings_allowed='*', number_allowed=True)
        ticDat = tdf.TicDat(**sillyMeDataTwoTables())
        dirPath = os.path.join(_scratchDir, "sillyTwoTables")
        tdf.csv.write_directory(ticDat,dirPath)
        self.assertFalse(tdf.csv.find_duplicates(dirPath))
        csvTicDat = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

    def testNetflow(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["arcs", "nodes"])
        dirPath = os.path.join(_scratchDir, "netflow")
        tdf.csv.write_directory(ticDat, dirPath)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertFalse(tdf.csv.find_duplicates(dirPath))
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it= True, headers_present=False)
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))
        tdf.csv.write_directory(ticDat, dirPath, write_header=False,allow_overwrite=True)
        self.assertTrue(self.firesException(lambda : tdf.csv.create_tic_dat(dirPath, freeze_it=True)))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, headers_present=False, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        # the casting to floats is controlled by data types and default values
        ticDat.nodes[12] = {}
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))
        tdf2 = TicDatFactory(**netflowSchema())
        tdf2.set_data_type("nodes", "name", strings_allowed='*', number_allowed=True)
        csvTicDat = tdf2.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        del(ticDat.nodes[12])
        ticDat.nodes['12'] = {}
        self.assertTrue(firesException(lambda : tdf.csv.write_directory(ticDat, dirPath)))
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

    def testSilly(self):
        if not self.can_run:
            return
        def doTest(headersPresent) :
            tdf = TicDatFactory(**sillyMeSchema())
            for t, flds in tdf.primary_key_fields.items():
                for f in flds:
                    tdf.set_data_type(t, f, number_allowed=True, strings_allowed='*')
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
            schema5["a"][0], schema5["b"][0] = (), []
            schema5b = sillyMeSchema()
            for t in ("a", "b") :
                schema5b[t][1] = _tuple(schema5b[t][0]) + _tuple(schema5b[t][1])
            schema5b["a"][0], schema5b["b"][0] = (), []
            schema6 = sillyMeSchema()
            schema6["d"] = [("dField",),[]]

            tdf2, tdf3, tdf4, tdf5, tdf5b, tdf6 = (TicDatFactory(**x) for x in
                            (schema2, schema3, schema4, schema5, schema5b, schema6))
            for tdf_ in [ tdf2, tdf3, tdf4, tdf5, tdf5b, tdf6]:
                for t, flds in tdf_.primary_key_fields.items():
                    for f in flds:
                        tdf_.set_data_type(t, f, number_allowed=True, strings_allowed='*')
            tdf5.set_generator_tables(["a", "c"])
            tdf5b.set_generator_tables(("a", "c"))


            dirPath = makeCleanDir(os.path.join(_scratchDir, "silly"))
            tdf.csv.write_directory(ticDat, dirPath, write_header=headersPresent)

            ticDat2 = tdf2.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertFalse if headersPresent else self.assertTrue)(tdf._same_data(ticDat, ticDat2))

            ticDat3 = tdf3.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(tdf._same_data(ticDat, ticDat3))

            if headersPresent :
                ticDat4 = tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)
                for t in ("a", "b") :
                    for k,v in getattr(ticDat4, t).items() :
                        for _k, _v in v.items() :
                            self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                        if set(v) == set(getattr(ticDat, t)[k]) :
                            self.assertTrue(t == "b")
                        else :
                            self.assertTrue(t == "a")
            else :
                self.assertTrue(self.firesException(lambda :
                                    tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)))

            ticDat5 = tdf5.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(
                                                    tdf5._same_data(tdf._keyless(ticDat), ticDat5))
            self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

            ticDat5b = tdf5b.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf5b._same_data(tdf._keyless(ticDat), ticDat5b))
            self.assertTrue(callable(ticDat5b.a) and callable(ticDat5b.c) and not callable(ticDat5b.b))


            ticDat6 = tdf6.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf._same_data(ticDat, ticDat6))
            self.assertTrue(firesException(lambda : tdf6._same_data(ticDat, ticDat6)))
            self.assertTrue(hasattr(ticDat6, "d") and utils.dictish(ticDat6.d))
            allDataTdf = TicDatFactory(**{t:[[], tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ())]
                             for t in tdf.all_tables})

            def writeData(data):
                td = allDataTdf.TicDat(a = data, b=data, c=data)
                allDataTdf.csv.write_directory(td, dirPath, allow_overwrite=True, write_header=headersPresent)

            writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)])
            ticDatMan = tdf.csv.create_tic_dat(dirPath, headers_present=headersPresent, freeze_it=True)
            self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
            self.assertTrue(ticDatMan.b[(1, 20, 30)]["bData"] == 40)
            rowCount = tdf.csv.find_duplicates(dirPath, headers_present= headersPresent)
            self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)


            writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40), (1,20,30,12)])
            rowCount = tdf.csv.find_duplicates(dirPath, headers_present=headersPresent)
            self.assertTrue(set(rowCount) == {'a', 'b'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==3)
            self.assertTrue(set(rowCount["b"]) == {(1,20,30)} and rowCount["b"][1,20,30]==2)



        utils.do_it(doTest(x) for x in (True, False))

    def test_numericish_text(self):
        dir_path = os.path.join(_scratchDir, "numericish")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        dat = tdf.TicDat(parameters=[["a", "100"], ["b", "010"], [3, "200"], ["d", "020"]])
        def round_trip():
            tdf.csv.write_directory(dat, makeCleanDir(dir_path))
            return tdf.csv.create_tic_dat(dir_path)
        dat2 = round_trip()
        self.assertFalse(tdf._same_data(dat, dat2))
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.set_data_type("parameters", "Key", strings_allowed='*', number_allowed=True)
        tdf.set_default_value("parameters", "Value", "")
        dat2 = round_trip()
        self.assertTrue(tdf._same_data(dat, dat2))
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.set_data_type("parameters", "Value", strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters=[["a", "100"], ["b", "010"], ["c", "200"], ["d", "020"]])
        dat2 = round_trip()
        self.assertTrue(tdf._same_data(dat, dat2))

    def test_empty_text_none(self):
        dir_path = os.path.join(_scratchDir, "empty_text")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        dat_n = tdf.TicDat(parameters=[[None, 100], ["b", 10.01], ["three", 200], ["d", None]])
        dat_s = tdf.TicDat(parameters=[["", 100], ["b", 10.01], ["three", 200], ["d", ""]])
        def round_trip():
            tdf.csv.write_directory(dat_n, makeCleanDir(dir_path))
            return tdf.csv.create_tic_dat(dir_path)
        dat2 = round_trip()
        self.assertTrue(tdf._same_data(dat_s, dat2) and not tdf._same_data(dat_n, dat2))
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.set_data_type("parameters", "Key", nullable=True)
        tdf.set_default_value("parameters", "Value", None) # this default alone will mess with number reading
        dat2 = round_trip()
        self.assertFalse(tdf._same_data(dat_s, dat2) or tdf._same_data(dat_n, dat2))
        self.assertTrue(any(r["Value"] is None for r in dat2.parameters.values()))
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.set_data_type("parameters", "Key", nullable=True)
        tdf.set_data_type("parameters", "Value", nullable=True, must_be_int=True)
        dat2 = round_trip()
        self.assertTrue(not tdf._same_data(dat_s, dat2) and tdf._same_data(dat_n, dat2))

    def test_parameters(self):
        dir_path = os.path.join(_scratchDir, "parameters_csv")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.add_parameter("Something", 100)
        tdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        tdf.csv.write_directory(dat, dir_path)
        dat_ = tdf.csv.create_tic_dat(dir_path)
        self.assertTrue(tdf._same_data(dat, dat_))


    def testCaseSpaceTableNames(self):
        tdf = TicDatFactory(table_one = [["a"],["b", "c"]], table_two = [["this", "that"],[]])
        dir_path = os.path.join(_scratchDir, "case_space")
        dat = tdf.TicDat(table_one = [['a', 2, 3], ['b', 5, 6]], table_two = [["a", "b"],["c", "d"], ["x", "z"]])
        tdf.csv.write_directory(dat, makeCleanDir(dir_path), case_space_table_names=True)
        self.assertTrue(all(os.path.exists(os.path.join(dir_path, _+".csv")) for _ in ["Table One", "Table Two"]))
        self.assertFalse(any(os.path.exists(os.path.join(dir_path, _+".csv")) for _ in ["table_one", "table_two"]))
        self.assertTrue(tdf._same_data(dat, tdf.csv.create_tic_dat(dir_path)))
        tdf.csv.write_directory(dat, makeCleanDir(dir_path), case_space_table_names=False)
        self.assertFalse(any(os.path.exists(os.path.join(dir_path, _+".csv")) for _ in ["Table One", "Table Two"]))
        self.assertTrue(all(os.path.exists(os.path.join(dir_path, _+".csv")) for _ in ["table_one", "table_two"]))
        self.assertTrue(tdf._same_data(dat, tdf.csv.create_tic_dat(dir_path)))

    def testNulls(self):
        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, nullable=True)
        dat = tdf.TicDat(table = [[None, 100], [200, "this"], ["that", 300], [300, None], [400, "that"]])
        dir_path = os.path.join(_scratchDir, "boolDefaults")
        tdf.csv.write_directory(dat, dir_path)
        dat_1 = tdf.csv.create_tic_dat(dir_path)
        self.assertTrue(tdf._same_data(dat, dat_1))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        tdf.set_infinity_io_flag(None)
        dat_inf = tdf.TicDat(table = [[float("inf"), 100], [200, "this"], ["that", 300], [300, float("inf")],
                                      [400, "that"]])
        dat_1 = tdf.csv.create_tic_dat(dir_path)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        tdf.csv.write_directory(dat_inf, makeCleanDir(dir_path))
        dat_1 = tdf.csv.create_tic_dat(dir_path)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        tdf.set_infinity_io_flag(None)
        dat_1 = tdf.csv.create_tic_dat(dir_path)
        self.assertFalse(tdf._same_data(dat_inf, dat_1))
        dat_inf = tdf.TicDat(table = [[float("-inf"), 100], [200, "this"], ["that", 300], [300, -float("inf")],
                                      [400, "that"]])
        self.assertTrue(tdf._same_data(dat_inf, dat_1))

    def testDietWithInfFlagging(self):
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        tdf.set_infinity_io_flag(999999999)
        path = os.path.join(_scratchDir, "dietInfFlag")
        tdf.csv.write_directory(dat, path)
        dat_1 = tdf.csv.create_tic_dat(path)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = tdf.clone()
        dat_1 = tdf.csv.create_tic_dat(path)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = TicDatFactory(**dietSchema())
        dat_1 = tdf.csv.create_tic_dat(path)
        self.assertFalse(tdf._same_data(dat, dat_1))

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

        path = os.path.join(_scratchDir, "datetime")
        tdf.csv.write_directory(dat, path)
        dat_1 = tdf.csv.create_tic_dat(path)
        self.assertFalse(tdf._same_data(dat, dat_1))
        self.assertFalse(tdf.find_data_type_failures(dat_1) or tdf.find_data_row_failures(dat_1))
        self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], datetime.datetime))
        self.assertTrue(all(isinstance(_, datetime.datetime) for _ in dat_1.table_with_stuffs))
        self.assertTrue(all(isinstance(_, datetime.datetime) or _ is None for v in dat_1.table_with_stuffs.values()
                            for _ in v.values()))

    def testIssue45(self):
        raw_tdf = TicDatFactory(data=[["a"], ["b"]])
        tdf_nums = TicDatFactory(data=[["a"], ["b"]])
        tdf_nums.set_data_type("data", "a")
        tdf_strs = TicDatFactory(data=[["a"], ["b"]])
        tdf_strs.set_data_type("data", "b", strings_allowed='*', number_allowed=False)
        dat_nums = tdf_nums.TicDat(data = [[1,2],[3,4], [22, 44]])
        dat_strs = tdf_nums.TicDat(data = [["1","2"],["3","4"], ["022", "0044"]])
        dirs = [os.path.join(_scratchDir, _) for _ in ["dat_nums_csv", "dat_strs_csv"]]
        raw_tdf.csv.write_directory(dat_nums, dirs[0])
        dat_nums_2 = tdf_nums.csv.create_tic_dat(dirs[0])
        raw_tdf.csv.write_directory(dat_strs, dirs[1])
        dat_strs_2 = tdf_strs.csv.create_tic_dat(dirs[1])
        self.assertTrue(raw_tdf._same_data(dat_nums, dat_nums_2))
        self.assertTrue(raw_tdf._same_data(dat_strs, dat_strs_2))

_scratchDir = TestCsv.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING CSV UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not _can_unit_test :
        print("!!!!!!!!!FAILING CSV UNIT TESTS DUE TO FAILURE TO LOAD CSV LIBRARIES!!!!!!!!")
    else:
        TestCsv.can_run = True

    unittest.main()

