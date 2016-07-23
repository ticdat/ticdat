import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import fail_to_debugger, spacesSchema, addNetflowForeignKeys
import shutil
import unittest
from ticdat.mdb import _connection_str, py, pyodbc
_can_unit_test = py or pyodbc

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestMdbReadOnly(unittest.TestCase):
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def testSimplest(self):
        if not _can_unit_test:
            return
        tdf = TicDatFactory(simple_table = [["pk1"],["df1", "df2"]])
        dat = tdf.mdb.create_tic_dat("simplest.accdb")
        self.assertTrue(len(dat.simple_table) == 3 and dat.simple_table[3]["df2"] == 2)

    def testDups(self):
        if not _can_unit_test:
            return
        tdf = TicDatFactory(one = [["a"],["b, c"]],
                            two = [["a", "b"],["c"]],
                            three = [["a", "b", "c"],[]])
        dups = tdf.mdb.find_duplicates("dups.accdb")
        self.assertTrue(dups ==  {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})

    def testDiet(self):
        if not _can_unit_test:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        filePath = "diet.accdb"
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
        mdbTicDat = tdf.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        def changeit() :
            mdbTicDat.categories["calories"]["minNutrition"]=12
        changeit()
        self.assertFalse(tdf._same_data(ticDat, mdbTicDat))
        mdbTicDat = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        self.assertTrue(self.firesException(changeit))
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))

    def testNetflow(self):
        if not _can_unit_test:
            return
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.all_tables}))
        filePath = "netflow.accdb"
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
        mdbTicDat = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        def changeIt() :
            mdbTicDat.inflow['Pencils', 'Boston']["quantity"] = 12
        self.assertTrue(self.firesException(changeIt))
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))

        mdbTicDat = tdf.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        self.assertFalse(self.firesException(changeIt))
        self.assertFalse(tdf._same_data(ticDat, mdbTicDat))

        pkHacked = netflowSchema()
        pkHacked["nodes"][0] = ["nimrod"]
        tdfHacked = TicDatFactory(**pkHacked)
        self.assertTrue("Unable to recognize field nimrod in table nodes" in
                        self.firesException(lambda  :tdfHacked.mdb.create_tic_dat(filePath)))

    def testSpacey(self):
        if not _can_unit_test:
            return
        tdf = TicDatFactory(**spacesSchema())
        spacesData =  {
        "a_table" : {1 : {"a Data 3":3, "a Data 2":2, "a Data 1":1},
                     22 : (1.1, 12, 12), 0.23 : (11, 12, 11)},
        "b_table" : {("1", "2", "3") : 1, ("a", "b", "b") : 12},
        "c_table" : (("1", "2", "3", 4),
                      {"c Data 4":55, "c Data 2":"b", "c Data 3":"c", "c Data 1":"a"},
                      ("a", "b", "12", 24) ) }

        dat = tdf.TicDat(**spacesData)
        filePath = "spaces.accdb"
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
        dat2 = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat,dat2))


# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not _can_unit_test:
        print("!!!!!!!!!FAILING MDB READ UNIT TESTS DUE TO FAILURE TO LOAD MDB LIBRARIES!!!!!!!!")
    elif not all(os.path.exists("%s.accdb"%n) for n in ("simplest", "diet", "netflow", "dups", "spaces")):
        print("!!!!!!!!!FAILING MDB READ UNIT TESTS DUE TO MISSING DATA FILES!!!!!!!!")
        # these files are easy to create (simplest is just simple, and testmdb has commented out code
        # that makes them as .mdbs, which can then be converted by an Office account)
        # also, pcacioppi Google Drive has a copy of them in the ticdat folder
    unittest.main()
