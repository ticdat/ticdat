import os
import unittest
import ticdat._private.utils as utils
from ticdat.core import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema
import shutil


#@utils.failToDebugger
class TestCsv(unittest.TestCase):
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.FrozenTicDat(**{t:getattr(dietData(),t) for t in tdf.primaryKeyFields})
        dirPath = os.path.join(_scratchDir, "diet")
        tdf.csv.writeDirectory(ticDat,dirPath)
        csvTicDat = tdf.csv.createTicDat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))
        def change() :
            csvTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(firesException(change))
        self.assertFalse(tdf._sameData(ticDat, csvTicDat))

        self.assertTrue(self.firesException(lambda  :
            tdf.csv.writeDirectory(ticDat, dirPath, dialect="excel_t")).endswith("Invalid dialect excel_t"))

        tdf.csv.writeDirectory(ticDat, dirPath, dialect="excel-tab")
        self.assertTrue(self.firesException(lambda : tdf.csv.createFrozenTicDat(dirPath)))
        csvTicDat = tdf.csv.createFrozenTicDat(dirPath, dialect="excel-tab")
        self.assertTrue(firesException(change))
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primaryKeyFields})
        dirPath = os.path.join(_scratchDir, "netflow")
        tdf.csv.writeDirectory(ticDat, dirPath)
        csvTicDat = tdf.csv.createFrozenTicDat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

        ticDat.nodes[12] = {}
        tdf.csv.writeDirectory(ticDat, dirPath)
        csvTicDat = tdf.csv.createFrozenTicDat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

        # minor flaw - strings that are floatable get turned into floats when reading csvs
        del(ticDat.nodes[12])
        ticDat.nodes['12'] = {}
        tdf.csv.writeDirectory(ticDat, dirPath)
        csvTicDat = tdf.csv.createFrozenTicDat(dirPath)
        self.assertFalse(tdf._sameData(ticDat, csvTicDat))

    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())
        schema2 = sillyMeSchema()
        schema2["primaryKeyFields"]["b"] = ("bField2", "bField1", "bField3")
        schema3 = sillyMeSchema()
        schema3["dataFields"]["a"] = ("aData2", "aData3", "aData1")
        schema4 = sillyMeSchema()
        schema4["dataFields"]["a"] = ("aData1", "aData3")
        schema5 = sillyMeSchema()
        _tuple = lambda x : tuple(x) if utils.containerish(x) else (x,)
        for t in ("a", "b") :
            schema5["dataFields"][t] = _tuple(schema5["dataFields"][t]) + _tuple(schema5["primaryKeyFields"][t])
        schema5["primaryKeyFields"] = {"a" : (), "b" : []}
        schema5["generatorTables"] = ["a", "c"]


        tdf2, tdf3, tdf4, tdf5 = (TicDatFactory(**x) for x in (schema2, schema3, schema4, schema5))

        dirPath = os.path.join(_scratchDir, "silly")
        tdf.csv.writeDirectory(ticDat, dirPath)

        ticDat2 = tdf2.csv.createTicDat(dirPath)
        self.assertFalse(tdf._sameData(ticDat, ticDat2))

        ticDat3 = tdf3.csv.createTicDat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, ticDat3))

        ticDat4 = tdf4.csv.createTicDat(dirPath)
        for t in tdf.primaryKeyFields:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.csv.createTicDat(dirPath)
        self.assertTrue(tdf5._sameData(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))



_scratchDir = TestCsv.__name__ + "_scratch"

def runTheTests(fastOnly=True) :
    utils.makeCleanDir(_scratchDir)
    utils.runSuite(TestCsv, fastOnly=fastOnly)
    shutil.rmtree(_scratchDir)
# Run the tests.
if __name__ == "__main__":
    runTheTests()

