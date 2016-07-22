import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanPath, addNetflowForeignKeys, addDietForeignKeys
from ticdat.testing.ticdattestutils import spacesSchema, dietSchemaWeirdCase, dietSchemaWeirdCase2
from ticdat.testing.ticdattestutils import copyDataDietWeirdCase, copyDataDietWeirdCase2
import shutil
import unittest
from ticdat.mdb import _connection_str, _can_mdb_unit_test, py
import ticdat.mdb as tdmdb
_orig_dbq = tdmdb._dbq

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestMdb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # uncomment the following line to run on old test machines
        #tdmdb._dbq = "*.mdb"
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        tdmdb._dbq = _orig_dbq
        shutil.rmtree(_scratchDir)
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def testDups(self):
        if not _can_mdb_unit_test:
            return
        tdf = TicDatFactory(one = [["a"],["b, c"]],
                            two = [["a", "b"],["c"]],
                            three = [["a", "b", "c"],[]])
        tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
        td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], [11, 1, 2]]
                            for t in tdf.all_tables})
        f = makeCleanPath(os.path.join(_scratchDir, "testDups.mdb"))
        tdf2.mdb.write_file(td, f)
        #shutil.copy(f, "dups.mdb") #uncomment to make readonly test file as .mdb
        dups = tdf.mdb.find_duplicates(f)
        self.assertTrue(dups ==  {'three': {(1, 2, 2): 2}, 'two': {(1, 2): 3}, 'one': {1: 3, 2: 2}})

    def testDiet(self):
        if not _can_mdb_unit_test:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        filePath = makeCleanPath(os.path.join(_scratchDir, "diet.mdb"))
        tdf.mdb.write_file(ticDat, filePath)
        #shutil.copy(filePath, "diet.mdb") #uncomment to make readonly test file as .mdb
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
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
        if not _can_mdb_unit_test:
            return
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.all_tables}))
        filePath = os.path.join(_scratchDir, "netflow.mdb")
        tdf.mdb.write_file(ticDat, filePath)
        #shutil.copy(filePath, "netflow.mdb") #uncomment to make readonly test file as .mdb
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
        ticDatHacked = tdfHacked.TicDat(**{t : getattr(ticDat, t) for t in tdf.all_tables})
        tdfHacked.mdb.write_file(ticDatHacked, makeCleanPath(filePath))
        self.assertTrue(self.firesException(lambda : tdfHacked.mdb.write_file(ticDat, filePath)))
        tdfHacked.mdb.write_file(ticDat, filePath, allow_overwrite =True)
        self.assertTrue("Unable to recognize field name in table nodes" in
                        self.firesException(lambda  :tdf.mdb.create_tic_dat(filePath)))

    def testSilly(self):
        if not _can_mdb_unit_test:
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
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
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

    def testInjection(self):
        if not _can_mdb_unit_test:
            return
        problems = [ "'", "''", '"', '""']
        tdf = TicDatFactory(boger = [["a"], ["b"]])
        dat = tdf.TicDat()
        for v,k in enumerate(problems):
            dat.boger[k]=str(v)
            dat.boger[str(v)]=k
        filePath = makeCleanPath(os.path.join(_scratchDir, "injection.mdb"))
        tdf.mdb.write_schema(filePath, boger = {"b":"text"})
        tdf.mdb.write_file(dat, filePath)
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
        dat2 = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat,dat2))

    def testSpacey(self):
        if not _can_mdb_unit_test:
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
        filePath = makeCleanPath(os.path.join(_scratchDir, "spacey.mdb"))
        tdf.mdb.write_schema(filePath, a_table = {"a Field":"double"},
                                       c_table = {"c Data 1":"text", "c Data 2":"text",
                                                  "c Data 3":"text", "c Data 4":"int"})
        tdf.mdb.write_file(dat, filePath)
        self.assertFalse(tdf.mdb.find_duplicates(filePath))
        dat2 = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat,dat2))

        with py.connect(_connection_str(filePath)) as con:
            for t in tdf.all_tables:
                con.cursor().execute("SELECT * INTO [%s] FROM %s"%(t.replace("_", " "), t)).commit()
                con.cursor().execute("DROP TABLE %s"%t).commit()
        #shutil.copy(filePath, "spaces.mdb") #uncomment to make readonly test file as .mdb
        dat3 = tdf.mdb.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(dat, dat3))

    def testWeirdDiets(self):
        if not _can_mdb_unit_test:
            return
        filePath = os.path.join(_scratchDir, "weirdDiet.mdb")
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.mdb.write_file(dat2, filePath , allow_overwrite=True)
        mdbTicDat = tdf.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, mdbTicDat))


        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.mdb.write_file(dat3, makeCleanPath(filePath))
        with py.connect(_connection_str(filePath)) as con:
            con.cursor().execute("SELECT * INTO [nutrition quantities] FROM nutrition_quantities").commit()
            con.cursor().execute("DROP TABLE nutrition_quantities").commit()

        mdbTicDat2 = tdf3.mdb.create_tic_dat(filePath)
        self.assertTrue(tdf3._same_data(dat3, mdbTicDat2))
        with py.connect(_connection_str(filePath)) as con:
            con.cursor().execute("create table nutrition_quantities (boger int)").commit()

        self.assertTrue(self.firesException(lambda : tdf3.mdb.create_tic_dat(filePath)))

_scratchDir = TestMdb.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not _can_mdb_unit_test:
        print("!!!!!!!!!FAILING MDB UNIT TESTS DUE TO FAILURE TO LOAD MDB LIBRARIES!!!!!!!!")
    unittest.main()
