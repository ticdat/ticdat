import os
import ticdat.utils as utils
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
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        writePath = os.path.join(makeCleanDir(os.path.join(_scratchDir, "diet")), "file.json")
        tdf.json.write_file(ticDat, writePath)
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


_scratchDir = TestJson.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING JSON UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not _can_unit_test :
        print("!!!!!!!!!FAILING JSON UNIT TESTS DUE TO FAILURE TO LOAD JSON LIBRARIES!!!!!!!!")
    else:
        TestJson.can_run = True

    unittest.main()