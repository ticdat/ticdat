import os
import shutil
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, dietSchemaWeirdCase
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, copyDataDietWeirdCase
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanDir, dietSchemaWeirdCase2, copyDataDietWeirdCase2
import unittest
from ticdat.jsontd import _can_unit_test, json

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
        for verbose in [True, False]:
            tdf = TicDatFactory(**dietSchema())
            ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "diet")), "file.json")
            tdf.json.write_file(ticDat, writePath, verbose=verbose)
            self.assertFalse(tdf.json.find_duplicates(writePath))
            jsonTicDat = tdf.json.create_tic_dat(writePath)
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

            def change() :
                jsonTicDat.categories["calories"]["minNutrition"]=12
            self.assertFalse(firesException(change))
            self.assertFalse(tdf._same_data(ticDat, jsonTicDat))
            jsonTicDat = tdf.json.create_tic_dat(writePath, freeze_it=True)
            self.assertTrue(firesException(change))
            self.assertTrue(tdf._same_data(ticDat, jsonTicDat))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.json.write_file(dat2, writePath, allow_overwrite=True, verbose=verbose)
        jsonTicDat2 = tdf.json.create_tic_dat(writePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, jsonTicDat2))

        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.json.write_file(dat3, writePath, allow_overwrite=True, verbose=verbose)
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

    def testNetflow(self):
        if not self.can_run:
            return
        for verbose in [True, False]:
            tdf = TicDatFactory(**netflowSchema())
            ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})

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
        for verbose in [True, False]:
            tdf = TicDatFactory(one = [["a"],["b", "c"]],
                                two = [["a", "b"],["c"]],
                                three = [["a", "b", "c"],[]])
            tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
            td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                                for t in tdf.all_tables})
            writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "dups")), "file.json")
            tdf2.json.write_file(td, writePath, verbose=verbose)
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

_scratchDir = TestJson.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not _can_unit_test :
        print("!!!!!!!!!FAILING JSON UNIT TESTS DUE TO FAILURE TO LOAD JSON LIBRARIES!!!!!!!!")
    else:
        TestJson.can_run = True

    unittest.main()