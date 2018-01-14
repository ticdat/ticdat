import sys
import unittest
import ticdat.utils as utils
from ticdat import LogFile, Progress
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, sillyMeDataTwoTables, fail_to_debugger
from ticdat.testing.ticdattestutils import spacesSchema, spacesData, flagged_as_run_alone
import os
import itertools
import shutil

def create_inputset_mock(tdf, dat):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    class RtnObject(object):
        schema = {t:"not needed for mock object" for t in tdf.all_tables}
        def getTable(self, t):
            return getattr(temp_dat, t).reset_index(drop=True)
    return RtnObject()

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestOpalytics(unittest.TestCase):
    can_run = bool(utils.DataFrame)
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def testDiet(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.copy_tic_dat(dietData()))
        inputset = create_inputset_mock(tdf, ticDat)
        self.assertFalse(tdf.opalytics.find_duplicates(inputset))
        ticDat2 = tdf.opalytics.create_tic_dat(inputset)
        self.assertTrue(tdf._same_data(ticDat, ticDat2))

        def change() :
            ticDat2.categories["calories"]["minNutrition"]=12
        self.assertFalse(firesException(change))
        self.assertFalse(tdf._same_data(ticDat, ticDat2))

        ticDat2 = tdf.opalytics.create_tic_dat(inputset, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDat2))
        self.assertTrue(firesException(change))
        self.assertTrue(tdf._same_data(ticDat, ticDat2))

        tdf2 = TicDatFactory(**{k:[pks, list(dfs) + ["dmy"]] for k,(pks, dfs) in tdf.schema().items()})
        _dat = tdf2.copy_tic_dat(ticDat)
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf2, _dat))))

        ex = self.firesException(lambda: tdf2.opalytics.create_tic_dat(inputset))
        self.assertTrue("field dmy can't be found" in ex)


    def testSillyTwoTables(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf, ticDat))))

        ticDat = tdf.TicDat(**sillyMeDataTwoTables())
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf, ticDat))))

    def testNetflow(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.copy_tic_dat(netflowData())
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf, ticDat))))

        ticDat.nodes[12] = {}
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf, ticDat))))

    def testSpaces(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(create_inputset_mock(tdf, ticDat))))

    def testDups(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(one = [["a"],["b", "c"]],
                            two = [["a", "b"],["c"]],
                            three = [["a", "b", "c"],[]])
        tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
        td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                            for t in tdf.all_tables})
        dups = tdf.opalytics.find_duplicates(create_inputset_mock(tdf2, td))
        self.assertTrue(dups == {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})


# Run the tests.
if __name__ == "__main__":

    if not utils.DataFrame :
        print("!!!!!!!!!FAILING OPALYTICS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")

    unittest.main()
