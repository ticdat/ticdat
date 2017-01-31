import os
import ticdat.utils as utils
import shutil
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, dietSchemaWeirdCase
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, copyDataDietWeirdCase
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanDir, dietSchemaWeirdCase2, copyDataDietWeirdCase2
import unittest
from ticdat.jsontd import _can_unit_test

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