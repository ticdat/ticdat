import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, failToDebugger
from ticdat.testing.ticdattestutils import makeCleanPath, addNetflowForeignKeys, addDietForeignKeys
import shutil
import unittest
#uncomment decorator to drop into debugger for assertTrue, assertFalse failures

#@failToDebugger
class TestMdb(unittest.TestCase):
    canRun = False
    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_scratchDir)
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testDiet(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        filePath = makeCleanPath(os.path.join(_scratchDir, "diet.mdb"))
        tdf.mdb.write_file(ticDat, filePath)
        mdbTicDat = tdf.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        def changeit() :
            mdbTicDat.categories["calories"]["minNutrition"]=12
        changeit()
        self.assertFalse(tdf._same_data(ticDat, mdbTicDat))

        self.assertTrue(self.firesException(lambda : tdf.mdb.write_file(ticDat, filePath)))
        tdf.mdb.write_file(ticDat, filePath, allow_overwrite=True)
        mdbTicDat = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))
        self.assertTrue(self.firesException(changeit))
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))

    def testNetflow(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.all_tables}))
        filePath = os.path.join(_scratchDir, "netflow.mdb")
        tdf.mdb.write_file(ticDat, filePath)
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
        ticDatHacked = tdfHacked.TicDat(**{t : getattr(ticDat, t) for t in tdf.all_tables})
        tdfHacked.mdb.write_file(ticDatHacked, makeCleanPath(filePath))
        self.assertTrue(self.firesException(lambda : tdfHacked.mdb.write_file(ticDat, filePath)))
        tdfHacked.mdb.write_file(ticDat, filePath, allow_overwrite =True)
        self.assertTrue("Unable to recognize field name in table nodes" in
                        self.firesException(lambda  :tdf.mdb.create_tic_dat(filePath)))

    def testSilly(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())
        filePath = os.path.join(_scratchDir, "silly.mdb")
        self.assertTrue(firesException(lambda : tdf.mdb.write_file(ticDat, makeCleanPath(filePath))))
        def sillyMeCleanData() :
            return {
                "a" : {"1" : (1, 2, "3"), "b" : (12, 12.2, "twelve"), "c" : (11, 12, "thirt")},
                "b" : {(1, 2, "3") : 1, (3, 4, "b") : 12},
                "c" : ((1, "2", 3, 4), (0.2, "b", 0.3, 0.4), (1.2, "b", 12, 24) )
            }
        ticDat = tdf.TicDat(**sillyMeCleanData())
        self.assertTrue(firesException(lambda : tdf.mdb.write_file(ticDat, makeCleanPath(filePath))))
        def makeCleanSchema() :
            tdf.mdb.write_schema(makeCleanPath(filePath), a={"aData3" : "text"},
                        b = {"bField1" : "int", "bField2" : "int"}, c={"cData2" : "text"})
            return filePath
        tdf.mdb.write_file(ticDat, makeCleanSchema())
        mdbTicDat = tdf.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))

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

        ticDat2 = tdf2.mdb.create_tic_dat(filePath)
        self.assertFalse(tdf._same_data(ticDat, ticDat2))

        ticDat3 = tdf3.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat3))

        ticDat4 = tdf4.mdb.create_tic_dat(filePath)
        for t in ["a","b"]:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf5._same_data(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

        self.assertTrue("table d" in self.firesException(lambda  : tdf6.mdb.create_tic_dat(filePath)))

        ticDat.a["theboger"] = (1, None, "twelve")
        tdf.mdb.write_file(ticDat, makeCleanSchema())
        ticDatNone = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)

_scratchDir = TestMdb.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not hasattr(td, "mdb") :
        print "!!!!!!!!!FAILING MDB UNIT TESTS DUE TO FAILURE TO LOAD MDB LIBRARIES!!!!!!!!"
    elif not td.mdb.can_write_new_file :
        print "!!!!!!!!!FAILING MDB UNIT TESTS DUE TO FAILURE TO WRITE NEW MDB FILES!!!!!!!!"
    else :
        TestMdb.canRun= True
    unittest.main()
