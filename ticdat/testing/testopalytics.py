import sys
import unittest
import ticdat.utils as utils
from ticdat import LogFile, Progress
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, sillyMeDataTwoTables, fail_to_debugger
from ticdat.testing.ticdattestutils import spacesSchema, spacesData, flagged_as_run_alone, addDietForeignKeys
import os
from itertools import product

def hack_name(s):
    rtn = list(s)
    for i in range(len(s)):
        if s[i] == "_":
            rtn[i] = " "
        elif i%3==0:
            if s[i].lower() == s[i]:
                rtn[i] = s[i].upper()
            else:
                rtn[i] = s[i].lower()
    return "".join(rtn)

def create_inputset_mock(tdf, dat, hack_table_names=False, includeActiveEnabled = True):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    replaced_name = {t:hack_name(t) if hack_table_names else t for t in tdf.all_tables}
    original_name = {v:k for k,v in replaced_name.items()}
    if includeActiveEnabled:
        class RtnObject(object):
            schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
            def getTable(self, t, includeActive=False):
                rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
                if includeActive:
                    rtn["_active"] = True
                return rtn
    else:
        class RtnObject(object):
            schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
            def getTable(self, t):
                rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
                return rtn
    return RtnObject()

def create_inputset_mock_with_active_hack(tdf, dat, hack_table_names=False):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    replaced_name = {t:hack_name(t) if hack_table_names else t for t in tdf.all_tables}
    original_name = {v:k for k,v in replaced_name.items()}
    class RtnObject(object):
        schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
        def getTable(self, t, includeActive=False):
            rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
            if "_active" in rtn.columns and not includeActive:
                rtn.drop("_active", axis=1, inplace=True)
            return rtn
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
        for hack, raw_data, activeEnabled in list(product(*(([True, False],)*3))):
            tdf = TicDatFactory(**dietSchema())
            ticDat = tdf.freeze_me(tdf.copy_tic_dat(dietData()))
            inputset = create_inputset_mock(tdf, ticDat, hack, activeEnabled)
            self.assertFalse(tdf.opalytics.find_duplicates(inputset, raw_data=raw_data))
            ticDat2 = tdf.opalytics.create_tic_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))

            def change() :
                ticDat2.categories["calories"]["minNutrition"]=12
            self.assertFalse(firesException(change))
            self.assertFalse(tdf._same_data(ticDat, ticDat2))

            ticDat2 = tdf.opalytics.create_tic_dat(inputset, freeze_it=True, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))
            self.assertTrue(firesException(change))
            self.assertTrue(tdf._same_data(ticDat, ticDat2))

            tdf2 = TicDatFactory(**{k:[pks, list(dfs) + ["dmy"]] for k,(pks, dfs) in tdf.schema().items()})
            _dat = tdf2.copy_tic_dat(ticDat)
            self.assertTrue(tdf._same_data(ticDat,
                                           tdf.opalytics.create_tic_dat(create_inputset_mock(tdf2, _dat, hack),
                                                                        raw_data=raw_data)))

            ex = self.firesException(lambda: tdf2.opalytics.create_tic_dat(inputset, raw_data=raw_data))
            self.assertTrue("field dmy can't be found" in ex)

    def testDietCleaning(self):
        sch = dietSchema()
        sch["categories"][-1].append("_active")
        tdf1 = TicDatFactory(**dietSchema())
        tdf2 = TicDatFactory(**sch)

        ticDat2 = tdf2.copy_tic_dat(dietData())
        for v in ticDat2.categories.values():
            v["_active"] = True
        ticDat2.categories["fat"]["_active"] = False
        ticDat1 = tdf1.copy_tic_dat(dietData())

        input_set = create_inputset_mock_with_active_hack(tdf2, ticDat2)
        self.assertTrue(tdf1._same_data(tdf1.opalytics.create_tic_dat(input_set, raw_data=True), ticDat1))

        ticDatPurged = tdf1.opalytics.create_tic_dat(input_set)
        self.assertFalse(tdf1._same_data(ticDatPurged, ticDat1))

        ticDat1.categories.pop("fat")
        tdf1.remove_foreign_keys_failures(ticDat1)

        self.assertTrue(tdf1._same_data(ticDatPurged, ticDat1))

    def testDietCleaningTwo(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.set_data_type("categories", "maxNutrition", min=66, inclusive_max=True)
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.categories.pop("fat")
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))
        tdf.remove_foreign_keys_failures(ticDat)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testDietCleaningThree(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.categories.pop("fat")
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))
        tdf.remove_foreign_keys_failures(ticDat)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))


    def testDietCleaningFive(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        tdf.set_data_type("categories", "minNutrition", max=0, inclusive_max=True)
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.categories.pop("fat")
        ticDat.categories.pop("calories")
        ticDat.categories.pop("protein")

        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))
        tdf.remove_foreign_keys_failures(ticDat)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testDietCleaningFour(self):
        tdf = TicDatFactory(**dietSchema())
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())
        ticDat.categories.pop("fat")
        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))
        tdf.remove_foreign_keys_failures(ticDat)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testSillyCleaningOne(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.set_data_type("c", "cData4", number_allowed=False, strings_allowed=['d'])
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testSillyCleaningTwo(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] == 'd')
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testSillyCleaningThree(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] != 4)
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] != 24)
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        self.assertTrue(tdf._same_data(tdf.opalytics.create_tic_dat(input_set, raw_data=True), ticDat))

        ticDatPurged = tdf.opalytics.create_tic_dat(input_set)
        self.assertFalse(tdf._same_data(ticDatPurged, ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(ticDatPurged, ticDat))

    def testSillyTwoTables(self):
        if not self.can_run:
            return
        for hack, raw_data in list(product(*(([True, False],)*2))):
            tdf = TicDatFactory(**sillyMeSchema())
            ticDat = tdf.TicDat(**sillyMeData())
            self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(
                create_inputset_mock(tdf, ticDat, hack), raw_data=raw_data)))

            ticDat = tdf.TicDat(**sillyMeDataTwoTables())
            self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(
                create_inputset_mock(tdf, ticDat, hack), raw_data=raw_data)))

    def testNetflow(self):
        if not self.can_run:
            return
        for hack, raw_data in list(product(*(([True, False],)*2))):
            tdf = TicDatFactory(**netflowSchema())
            ticDat = tdf.copy_tic_dat(netflowData())
            self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(
                create_inputset_mock(tdf, ticDat, hack), raw_data=raw_data)))

            ticDat.nodes[12] = {}
            self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(
                create_inputset_mock(tdf, ticDat, hack), raw_data=raw_data)))

    def testSpaces(self):
        if not self.can_run:
            return
        for hack, raw_data in list(product(*(([True, False],)*2))):
            tdf = TicDatFactory(**spacesSchema())
            ticDat = tdf.TicDat(**spacesData())
            self.assertTrue(tdf._same_data(ticDat, tdf.opalytics.create_tic_dat(
                create_inputset_mock(tdf, ticDat, hack), raw_data=raw_data)))

    def testDups(self):
        if not self.can_run:
            return
        for hack, raw_data in list(product(*(([True, False],)*2))):
            tdf = TicDatFactory(one = [["a"],["b", "c"]],
                                two = [["a", "b"],["c"]],
                                three = [["a", "b", "c"],[]])
            tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
            td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                                for t in tdf.all_tables})
            dups = tdf.opalytics.find_duplicates(create_inputset_mock(tdf2, td, hack), raw_data=raw_data)
            self.assertTrue(dups == {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})


# Run the tests.
if __name__ == "__main__":

    if not utils.DataFrame :
        print("!!!!!!!!!FAILING OPALYTICS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")

    unittest.main()
