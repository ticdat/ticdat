import os
import unittest
import ticdat._private.utils as utils
from ticdat.core import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema
import shutil

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@utils.failToDebugger
class TestXls(unittest.TestCase):
    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.FrozenTicDat(**{t:getattr(dietData(),t) for t in tdf.primaryKeyFields})
        filePath = os.path.join(_scratchDir, "diet.xls")
        tdf.xls.writeFile(ticDat, filePath)
        xlsTicDat = tdf.xls.createTicDat(filePath)
        self.assertTrue(tdf._sameData(ticDat, xlsTicDat))
        xlsTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(tdf._sameData(ticDat, xlsTicDat))

    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.FrozenTicDat(**{t:getattr(netflowData(),t) for t in tdf.primaryKeyFields})
        filePath = os.path.join(_scratchDir, "netflow.xls")
        tdf.xls.writeFile(ticDat, filePath)
        xlsTicDat = tdf.xls.createFrozenTicDat(filePath)
        self.assertTrue(tdf._sameData(ticDat, xlsTicDat))
        def changeIt() :
            xlsTicDat.inflow['Pencils', 'Boston']["quantity"] = 12
        self.assertTrue(self.firesException(changeIt))
        self.assertTrue(tdf._sameData(ticDat, xlsTicDat))

        xlsTicDat = tdf.xls.createTicDat(filePath)
        self.assertTrue(tdf._sameData(ticDat, xlsTicDat))
        self.assertFalse(self.firesException(changeIt))
        self.assertFalse(tdf._sameData(ticDat, xlsTicDat))

        pkHacked = netflowSchema()
        pkHacked["primaryKeyFields"]["nodes"] = "nimrod"
        tdfHacked = TicDatFactory(**pkHacked)
        tdfHacked.xls.writeFile(ticDat, filePath)
        self.assertTrue("nodes : name" in self.firesException(lambda  :tdf.xls.createTicDat(filePath)))

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

        filePath = os.path.join(_scratchDir, "silly.xls")
        tdf.xls.writeFile(ticDat, filePath)

        ticDat2 = tdf2.xls.createTicDat(filePath)
        self.assertFalse(tdf._sameData(ticDat, ticDat2))

        ticDat3 = tdf3.xls.createTicDat(filePath)
        self.assertTrue(tdf._sameData(ticDat, ticDat3))

        ticDat4 = tdf4.xls.createTicDat(filePath)
        for t in tdf.primaryKeyFields:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.xls.createTicDat(filePath)
        self.assertTrue(tdf5._sameData(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))


        import xlwt
        book = xlwt.Workbook()
        for t in tdf.allTables :
            sheet = book.add_sheet(t)
            for i,f in enumerate(tdf.primaryKeyFields.get(t, ()) + tdf.dataFields.get(t, ())) :
                sheet.write(0, i, f)
            for rowInd, row in enumerate( [(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)]) :
                for fieldInd, cellValue in enumerate(row):
                    sheet.write(rowInd+1, fieldInd, cellValue)
        if os.path.exists(filePath):
            os.remove(filePath)
        book.save(filePath)

        ticDatMan = tdf.xls.createFrozenTicDat(filePath)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[(1, 20, 30)]["bData"] == 40)

        ticDat.a["theboger"] = (1, None, 12)
        tdf.xls.writeFile(ticDat, filePath)
        ticDatNone = tdf.xls.createFrozenTicDat(filePath)
        # THIS IS A FLAW - but a minor one. None's are hard to represent. It is turning into the empty string here.
        # not sure how to handle this, but documenting for now.
        self.assertFalse(tdf._sameData(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == "")


_scratchDir = TestXls.__name__ + "_scratch"

def runTheTests(fastOnly=True) :
    utils.makeCleanDir(_scratchDir)
    utils.runSuite(TestXls, fastOnly=fastOnly)
    shutil.rmtree(_scratchDir)
# Run the tests.
if __name__ == "__main__":
    runTheTests()
