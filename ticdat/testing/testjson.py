import os
import shutil
from ticdat.ticdatfactory import TicDatFactory
from ticdat.pandatfactory import PanDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, dietSchemaWeirdCase
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, copyDataDietWeirdCase
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, sillyMeDataTwoTables, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanDir, dietSchemaWeirdCase2, copyDataDietWeirdCase2, makeCleanPath
from ticdat.testing.ticdattestutils import flagged_as_run_alone

import unittest
from ticdat.jsontd import _can_unit_test, json
import datetime
try:
    import dateutil, dateutil.parser
except:
    dateutil=None

#@fail_to_debugger
class TestJson(unittest.TestCase):
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

    def testDiet(self):
        if not self.can_run:
            return
        for kwargs in [{"verbose": True}, {"verbose": False}, {"to_pandas": True}]:
            tdf = TicDatFactory(**dietSchema())
            ticDat = tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
            ticDat.categories["fat"]["minNutrition"] = -float("inf") # violates integrity but better testing
            ticDat = tdf.freeze_me(ticDat)
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "diet")), "file.json")
            tdf.json.write_file(ticDat, writePath, **kwargs)
            self.assertFalse(tdf.json.find_duplicates(writePath))
            jsonTicDat = tdf.json.create_tic_dat(writePath)
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat, epsilon=1e-5))

            def change() :
                jsonTicDat.categories["calories"]["minNutrition"]=12
            self.assertFalse(firesException(change))
            self.assertFalse(tdf._same_data(ticDat, jsonTicDat, epsilon=1e-5))
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertTrue(firesException(change))
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat, epsilon=1e-5))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.json.write_file(dat2, writePath, allow_overwrite=True)
        jsonTicDat2 = tdf.json.create_tic_dat(writePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, jsonTicDat2))

        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.json.write_file(dat3, writePath, allow_overwrite=True)
        with open(writePath, "r") as f:
            jdict = json.load(f)
        jdict["nutrition quantities"] = jdict["nutrition_quantities"]
        del(jdict["nutrition_quantities"])
        with open(writePath, "w") as f:
            json.dump(jdict, f)
        jsonDat3 = tdf3.json.create_tic_dat(writePath)
        self.assertTrue(tdf3._same_data(dat3, jsonDat3))
        jdict["nutrition_quantities"] = jdict["nutrition quantities"]
        with open(writePath, "w") as f:
            json.dump(jdict, f)
        self.assertTrue(self.firesException(lambda : tdf3.json.create_tic_dat(writePath)))

    def testSillyTwoTables(self):
        if not self.can_run:
            return

        for kwargs in [{"verbose": True}, {"verbose": False}, {"to_pandas": True}]:

            tdf = TicDatFactory(**sillyMeSchema())
            ticDat = tdf.TicDat(**sillyMeDataTwoTables())
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "sillyTwoTables")), "file.json")
            tdf.json.write_file(ticDat, writePath, **kwargs)
            self.assertFalse(tdf.json.find_duplicates(writePath))
            jsonTicDat = tdf.json.create_tic_dat(writePath)
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

    def testMissingTable(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k != "nutritionQuantities"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        filePath = os.path.join(_scratchDir, "diet_missing.json")
        tdf2.json.write_file(ticDat2, filePath, allow_overwrite=True)
        ticDat3 = tdf.json.create_tic_dat(filePath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities)
        self.assertTrue(ticDat3.categories and ticDat3.foods)

        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k == "categories"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        tdf2.json.write_file(ticDat2, filePath, allow_overwrite=True)
        ticDat3 = tdf.json.create_tic_dat(filePath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities or ticDat3.foods)
        self.assertTrue(ticDat3.categories)

    def testNetflow(self):
        if not self.can_run:
            return
        for verbose in [True, False]:
            tdf = TicDatFactory(**netflowSchema())
            ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
            self.assertTrue(tdf._same_data(ticDat, tdf.json.create_tic_dat(
                            tdf.json.write_file(ticDat, "")), epsilon=0.0001))

            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "netflow")), "file.json")
            tdf.json.write_file(ticDat, writePath, verbose=verbose)
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertFalse(tdf.json.find_duplicates(writePath))
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

            ticDat.nodes[12] = {}
            tdf.json.write_file(ticDat, writePath, verbose=verbose, allow_overwrite=True)
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

            # unlike csv, json format respects strings that are floatable
            del(ticDat.nodes[12])
            ticDat.nodes['12'] = {}
            self.assertTrue(firesException(lambda : tdf.json.write_file(ticDat, writePath, verbose=verbose)))
            tdf.json.write_file(ticDat, writePath, allow_overwrite=True, verbose=verbose)
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

    def testDups(self):
        if not self.can_run:
            return
        for kwargs in [{"verbose":True}, {}, {"to_pandas": True}]:
            tdf = TicDatFactory(one = [["a"],["b", "c"]],
                                two = [["a", "b"],["c"]],
                                three = [["a", "b", "c"],[]])
            tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
            td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                                for t in tdf.all_tables})
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "dups")), "file.json")
            tdf2.json.write_file(td, writePath, **kwargs)
            dups = tdf.json.find_duplicates(writePath)
            self.assertTrue(dups == {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})

    def testSilly(self):
        if not self.can_run:
            return
        for verbose in [True, False]:
            tdf = TicDatFactory(**sillyMeSchema())
            ticDat = tdf.TicDat(**sillyMeData())
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "netflow")), "file.json")
            tdf.json.write_file(ticDat, writePath, verbose=verbose)
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertFalse(tdf.json.find_duplicates(writePath))
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

    def testBooleansAndNulls(self):
        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        dat = tdf.TicDat(table = [[None, 100], [200, True], [False, 300], [300, None], [400, False]])
        file_one = os.path.join(_scratchDir, "boolDefaults_1.json")
        file_two = os.path.join(_scratchDir, "boolDefaults_2.json")
        tdf.json.write_file(dat, file_one, verbose=True)
        tdf.json.write_file(dat, file_two, verbose=False)
        dat_1 = tdf.json.create_tic_dat(file_one)
        dat_2 = tdf.json.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        tdf.set_infinity_io_flag(None)
        dat_inf = tdf.TicDat(table = [[float("inf"), 100], [200, True], [False, 300], [300, float("inf")],
                                      [400, False]])
        dat_1 = tdf.json.create_tic_dat(file_one)
        dat_2 = tdf.json.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))
        tdf.json.write_file(dat_inf, file_one, verbose=True, allow_overwrite=True)
        tdf.json.write_file(dat_inf, file_two, verbose=False, allow_overwrite=True)
        dat_1 = tdf.json.create_tic_dat(file_one)
        dat_2 = tdf.json.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        tdf.set_infinity_io_flag(None)
        dat_1 = tdf.json.create_tic_dat(file_one)
        dat_2 = tdf.json.create_tic_dat(file_two)
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
        file_one = os.path.join(_scratchDir, "dietInfFlag_1.json")
        file_two = os.path.join(_scratchDir, "dietInfFlag_2.json")
        tdf.json.write_file(dat, file_one, verbose=True)
        tdf.json.write_file(dat, file_two, verbose=False)
        dat_1 = tdf.json.create_tic_dat(file_one)
        dat_2 = tdf.json.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))
        tdf = tdf.clone()
        dat_1 = tdf.json.create_tic_dat(file_one)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = TicDatFactory(**dietSchema())
        dat_1 = tdf.json.create_tic_dat(file_one)
        self.assertFalse(tdf._same_data(dat, dat_1))


    def test_parameters(self):
        path = os.path.join(_scratchDir, "parameters.json")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.add_parameter("Something", 100)
        tdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters = [["Something", float("inf")], ["Different", "inf"]])
        tdf.json.write_file(dat, path)
        dat_ = tdf.json.create_tic_dat(path)
        self.assertTrue(tdf._same_data(dat, dat_))
        tdf2 = TicDatFactory(parameters=[["Key"], ["Value"]])
        pdf = PanDatFactory(parameters=[["Key"], ["Value"]])
        dat = tdf.TicDat(parameters = [["Something", 2]])
        dat_ = tdf2.json.create_tic_dat(tdf.json.write_file(dat, ""))
        self.assertTrue(tdf._same_data(dat, dat_))
        dat_2 = pdf.copy_to_tic_dat(pdf.json.create_pan_dat(tdf.json.write_file(dat, "", to_pandas=True)))
        self.assertTrue(tdf._same_data(dat, dat_2))



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

        file_one = os.path.join(_scratchDir, "datetime.json")
        tdf.json.write_file(dat, file_one)
        dat_1 = tdf.json.create_tic_dat(file_one)
        self.assertFalse(tdf._same_data(dat, dat_1))
        self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], datetime.datetime))
        self.assertTrue(all(isinstance(_, datetime.datetime) for _ in dat_1.table_with_stuffs))
        self.assertTrue(all(isinstance(_, datetime.datetime) or _ is None for v in dat_1.table_with_stuffs.values()
                            for _ in v.values()))


_scratchDir = TestJson.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not _can_unit_test :
        print("!!!!!!!!!FAILING JSON UNIT TESTS DUE TO FAILURE TO LOAD JSON LIBRARIES!!!!!!!!")
    else:
        TestJson.can_run = True

    unittest.main()