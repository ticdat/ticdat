import os
import unittest
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, failToDebugger, runSuite
from ticdat.testing.ticdattestutils import makeCleanPath, addNetflowForeignKeys, addDietForeignKeys
import shutil

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@failToDebugger
class TestSql(unittest.TestCase):
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testDiet(self):
        def doTheTests(tdf) :
            ticDat = tdf.FrozenTicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
            filePath = makeCleanPath(os.path.join(_scratchDir, "diet.db"))
            tdf.sql.write_db_data(ticDat, filePath)
            sqlTicDat = tdf.sql.create_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            def changeit() :
                sqlTicDat.categories["calories"]["minNutrition"]=12
            changeit()
            self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

            self.assertTrue(self.firesException(lambda : tdf.sql.write_db_data(ticDat, filePath)))
            tdf.sql.write_db_data(ticDat, filePath, allow_overwrite=True)
            sqlTicDat = tdf.sql.create_frozen_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            self.assertTrue(self.firesException(changeit))
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

            filePath = makeCleanPath(os.path.join(_scratchDir, "diet.sql"))
            tdf.sql.write_sql_file(ticDat, filePath)
            sqlTicDat = tdf.sql.create_tic_dat_from_sql(filePath)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            changeit()
            self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

            tdf.sql.write_sql_file(ticDat, filePath, include_schema=True)
            sqlTicDat = tdf.sql.create_frozen_tic_dat_from_sql(filePath, includes_schema=True)
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
            self.assertTrue(self.firesException(changeit))
            self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

        doTheTests(TicDatFactory(**dietSchema()))

        tdf = TicDatFactory(**dietSchema())
        self.assertFalse(tdf.foreign_keys)
        tdf.set_default_values(categories =  {'maxNutrition': float("inf"), 'minNutrition': 0.0},
                               foods =  {'cost': 0.0},
                               nutritionQuantities =  {'qty': 0.0})
        addDietForeignKeys(tdf)
        ordered = tdf.sql._ordered_tables()
        self.assertTrue(ordered.index("categories") < ordered.index("nutritionQuantities"))
        self.assertTrue(ordered.index("foods") < ordered.index("nutritionQuantities"))

        ticDat = tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
        origTicDat = tdf.copy_tic_dat(ticDat)
        self.assertTrue(tdf._same_data(ticDat, origTicDat))
        self.assertFalse(tdf.find_foreign_key_failures(ticDat))
        ticDat.nutritionQuantities['hot dog', 'boger'] = ticDat.nutritionQuantities['junk', 'protein'] = -12
        self.assertTrue(tdf.find_foreign_key_failures(ticDat) ==
                        {("nutritionQuantities", "categories"):[('hot dog', 'boger')],
                         ("nutritionQuantities", "foods"):[('junk', 'protein')]})
        self.assertFalse(tdf._same_data(ticDat, origTicDat))
        tdf.remove_foreign_keys_failures(ticDat)
        self.assertFalse(tdf.find_foreign_key_failures(ticDat))
        self.assertTrue(tdf._same_data(ticDat, origTicDat))

        doTheTests(tdf)


    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        ordered = tdf.sql._ordered_tables()
        self.assertTrue(ordered.index("nodes") < min(ordered.index(_) for _ in ("arcs", "cost", "inflow")))
        self.assertTrue(ordered.index("commodities") < min(ordered.index(_) for _ in ("cost", "inflow")))
        ticDat = tdf.FrozenTicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        filePath = os.path.join(_scratchDir, "netflow.sql")
        tdf.sql.write_db_data(ticDat, filePath)
        sqlTicDat = tdf.sql.create_frozen_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
        def changeIt() :
            sqlTicDat.inflow['Pencils', 'Boston']["quantity"] = 12
        self.assertTrue(self.firesException(changeIt))
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))

        sqlTicDat = tdf.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, sqlTicDat))
        self.assertFalse(self.firesException(changeIt))
        self.assertFalse(tdf._same_data(ticDat, sqlTicDat))

        pkHacked = netflowSchema()
        pkHacked["nodes"][0] = ["nimrod"]
        tdfHacked = TicDatFactory(**pkHacked)
        ticDatHacked = tdfHacked.TicDat(**{t : getattr(ticDat, t) for t in tdf.all_tables})
        tdfHacked.sql.write_db_data(ticDatHacked, makeCleanPath(filePath))
        self.assertTrue(self.firesException(lambda : tdfHacked.sql.write_db_data(ticDat, filePath)))
        tdfHacked.sql.write_db_data(ticDat, filePath, allow_overwrite =True)
        self.assertTrue("Unable to recognize field name in table nodes" in
                        self.firesException(lambda  :tdf.sql.create_tic_dat(filePath)))


    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
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
        schema5["a"][0], schema5["b"][0] =  (),  []
        schema6 = sillyMeSchema()
        schema6["d"] =  [["dField"],()]

        tdf2, tdf3, tdf4, tdf5, tdf6 = (TicDatFactory(**x) for x in (schema2, schema3, schema4, schema5, schema6))
        tdf5.set_generator_tables(("a","c"))
        filePath = os.path.join(_scratchDir, "silly.sql")
        tdf.sql.write_db_data(ticDat, filePath)

        ticDat2 = tdf2.sql.create_tic_dat(filePath)
        self.assertFalse(tdf._same_data(ticDat, ticDat2))

        ticDat3 = tdf3.sql.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat3))

        ticDat4 = tdf4.sql.create_tic_dat(filePath)
        for t in ["a","b"]:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.sql.create_tic_dat(filePath)
        self.assertTrue(tdf5._same_data(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

        self.assertTrue("table d" in self.firesException(lambda  : tdf6.sql.create_tic_dat(filePath)))

        ticDat.a["theboger"] = (1, None, 12)
        tdf.sql.write_db_data(ticDat, makeCleanPath(filePath))
        ticDatNone = tdf.sql.create_frozen_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)



_scratchDir = TestSql.__name__ + "_scratch"

def runTheTests(fastOnly=True) :
    td = TicDatFactory()
    if not hasattr(td, "sql") :
        print "!!!!!!!!!FAILING SQL UNIT TESTS DUE TO FAILURE TO LOAD SQL LIBRARIES!!!!!!!!"
        return
    makeCleanDir(_scratchDir)
    runSuite(TestSql, fastOnly=fastOnly)
    shutil.rmtree(_scratchDir)

# Run the tests.
if __name__ == "__main__":
    runTheTests()