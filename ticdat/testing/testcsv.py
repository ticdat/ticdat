import os
import unittest
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, failToDebugger, makeCleanDir, runSuite
import shutil


#@failToDebugger
class TestCsv(unittest.TestCase):
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.FrozenTicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
        dirPath = os.path.join(_scratchDir, "diet")
        tdf.csv.write_directory(ticDat,dirPath)
        csvTicDat = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))
        def change() :
            csvTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(firesException(change))
        self.assertFalse(tdf._sameData(ticDat, csvTicDat))

        self.assertTrue(self.firesException(lambda  :
            tdf.csv.write_directory(ticDat, dirPath, dialect="excel_t")).endswith("Invalid dialect excel_t"))

        tdf.csv.write_directory(ticDat, dirPath, dialect="excel-tab", allow_overwrite=True)
        self.assertTrue(self.firesException(lambda : tdf.csv.create_frozen_tic_dat(dirPath)))
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath, dialect="excel-tab")
        self.assertTrue(firesException(change))
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        dirPath = os.path.join(_scratchDir, "netflow")
        tdf.csv.write_directory(ticDat, dirPath)
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath, headers_present=False)
        self.assertFalse(tdf._sameData(ticDat, csvTicDat))
        tdf.csv.write_directory(ticDat, dirPath, write_header=False,allow_overwrite=True)
        self.assertTrue(self.firesException(lambda  : tdf.csv.create_frozen_tic_dat(dirPath)))
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath, headers_present=False)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

        ticDat.nodes[12] = {}
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath)
        self.assertTrue(tdf._sameData(ticDat, csvTicDat))

        # minor flaw - strings that are floatable get turned into floats when reading csvs
        del(ticDat.nodes[12])
        ticDat.nodes['12'] = {}
        self.assertTrue(firesException(lambda : tdf.csv.write_directory(ticDat, dirPath)))
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_frozen_tic_dat(dirPath)
        self.assertFalse(tdf._sameData(ticDat, csvTicDat))

    def testSilly(self):
        def doTest(headersPresent) :
            tdf = TicDatFactory(**sillyMeSchema())
            ticDat = tdf.TicDat(**sillyMeData())
            schema2 = sillyMeSchema()
            schema2["primary_key_fields"]["b"] = ("bField2", "bField1", "bField3")
            schema3 = sillyMeSchema()
            schema3["data_fields"]["a"] = ("aData2", "aData3", "aData1")
            schema4 = sillyMeSchema()
            schema4["data_fields"]["a"] = ("aData1", "aData3")
            schema5 = sillyMeSchema()
            _tuple = lambda x : tuple(x) if utils.containerish(x) else (x,)
            for t in ("a", "b") :
                schema5["data_fields"][t] = _tuple(schema5["data_fields"][t]) + _tuple(schema5["primary_key_fields"][t])
            schema5["primary_key_fields"] = {"a" : (), "b" : []}
            schema5["generator_tables"] = ["a", "c"]
            schema5b = sillyMeSchema()
            for t in ("a", "b") :
                schema5b["data_fields"][t] = _tuple(schema5b["primary_key_fields"][t]) + _tuple(schema5b["data_fields"][t])
            schema5b["primary_key_fields"] = {"a" : (), "b" : []}
            schema5b["generator_tables"] = ["a", "c"]
            schema6 = sillyMeSchema()
            schema6["primary_key_fields"]["d"] = "dField"

            tdf2, tdf3, tdf4, tdf5, tdf5b, tdf6 = (TicDatFactory(**x) for x in
                            (schema2, schema3, schema4, schema5, schema5b, schema6))

            dirPath = makeCleanDir(os.path.join(_scratchDir, "silly"))
            tdf.csv.write_directory(ticDat, dirPath, write_header=headersPresent)

            ticDat2 = tdf2.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertFalse if headersPresent else self.assertTrue)(tdf._sameData(ticDat, ticDat2))

            ticDat3 = tdf3.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(tdf._sameData(ticDat, ticDat3))

            if headersPresent :
                ticDat4 = tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)
                for t in tdf.primary_key_fields:
                    for k,v in getattr(ticDat4, t).items() :
                        for _k, _v in v.items() :
                            self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                        if set(v) == set(getattr(ticDat, t)[k]) :
                            self.assertTrue(t == "b")
                        else :
                            self.assertTrue(t == "a")
            else :
                self.assertTrue(self.firesException(lambda :
                                    tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)))

            ticDat5 = tdf5.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(tdf5._sameData(tdf._keyless(ticDat), ticDat5))
            self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

            ticDat5b = tdf5b.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf5b._sameData(tdf._keyless(ticDat), ticDat5b))
            self.assertTrue(callable(ticDat5b.a) and callable(ticDat5b.c) and not callable(ticDat5b.b))


            ticDat6 = tdf6.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf._sameData(ticDat, ticDat6))
            self.assertTrue(firesException(lambda : tdf6._sameData(ticDat, ticDat6)))
            self.assertTrue(hasattr(ticDat6, "d") and utils.dictish(ticDat6.d))

        utils.doIt(doTest(x) for x in (True, False))

_scratchDir = TestCsv.__name__ + "_scratch"

def runTheTests(fastOnly=True) :
    td = TicDatFactory()
    if not hasattr(td, "csv") :
        print "!!!!!!!!!FAILING CSV UNIT TESTS DUE TO FAILURE TO LOAD CSV LIBRARIES!!!!!!!!"
        return
    makeCleanDir(_scratchDir)
    runSuite(TestCsv, fastOnly=fastOnly)
    shutil.rmtree(_scratchDir)
# Run the tests.
if __name__ == "__main__":
    runTheTests()

