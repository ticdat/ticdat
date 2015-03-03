import sys
import unittest
import ticdat._private.utils as utils
from ticdat.core import TicDatFactory, goodTicDatObject
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema


#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@utils.failToDebugger
class TestUtils(unittest.TestCase):

    def testOne(self):
        def _cleanIt(x) :
            x.foods['macaroni'] = {"cost": 2.09}
            x.foods['milk'] = {"cost":0.89}
            return x
        dataObj = dietData()
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj,
                        ("categories", "foods", "nutritionQuantities")))
        tdf = TicDatFactory(**dietSchema())
        self.assertTrue(tdf.goodTicDatObject(dataObj))
        dataObj = _cleanIt(dataObj)
        self.assertTrue(goodTicDatObject(dataObj) and goodTicDatObject(dataObj,
                        ("categories", "foods", "nutritionQuantities")))

        msg = []
        dataObj.foods[("milk", "cookies")] = {"cost": float("inf")}
        dataObj.boger = object()
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj, badMessageHandler=msg.append))
        self.assertTrue({"foods : Inconsistent key lengths", "boger : Unexpected object."} == set(msg))
        self.assertTrue(goodTicDatObject(dataObj, ("categories", "nutritionQuantities")))

        dataObj = dietData()
        dataObj.categories["boger"] = {"cost":1}
        dataObj.categories["boger"] = {"cost":1}
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj, badMessageHandler=msg.append))
        self.assertTrue({'boger : Unexpected object.', 'foods : Inconsistent key lengths',
                         'categories : Inconsistent field name keys.',
                         'foods : At least one value is not a dict-like object'} == set(msg))
        self.assertTrue("categories cannot be treated as a ticDat table : Inconsistent data field name keys" in
            firesException(lambda : tdf.FrozenTicDat(**{t:getattr(dataObj,t) for t in tdf.primaryKeyFields})).message)

    def _assertSame(self, t1, t2, goodTicDatTable):

        _ass = lambda _t1, _t2 : utils.assertTicDatTablesSame(_t1, _t2,
                _goodTicDatTable=  goodTicDatTable,
                **({} if utils.DEBUG() else {"_assertTrue":self.assertTrue, "_assertFalse":self.assertFalse}))

        _ass(t1, t2)
        _ass(t2, t1)

    def testTwo(self):
        objOrig = dietData()
        staticFactory = TicDatFactory(**dietSchema())
        tables = set(staticFactory.primaryKeyFields)
        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        self.assertTrue(goodTicDatObject(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t),
                                    lambda _t : staticFactory.goodTicDatTable(_t, t))

    def testThree(self):
        objOrig = netflowData()
        staticFactory = TicDatFactory(**netflowSchema())
        goodTable = lambda t : lambda _t : staticFactory.goodTicDatTable(_t, t)
        tables = set(staticFactory.primaryKeyFields)
        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        self.assertTrue(goodTicDatObject(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t), goodTable(t))

        objOrig.commodities.append(12.3)
        objOrig.arcs[(1, 2)] = [12]
        self._assertSame(objOrig.nodes, ticDat.nodes, goodTable("nodes"))
        self._assertSame(objOrig.cost, ticDat.cost, goodTable("cost"))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.commodities, ticDat.commodities, goodTable("commodities")) ))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.arcs, ticDat.arcs, goodTable("arcs")) ))

        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue(ticDat.arcs[1, 2]["capacity"] == 12)
        self.assertTrue(12.3 in ticDat.commodities)

        objOrig.cost[5]=5

        self.assertTrue("cost cannot be treated as a ticDat table : Inconsistent key lengths" in
            firesException(lambda : staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})))

        objOrig = netflowData()
        def editMeBadly(t) :
            def rtn() :
                t.cost["hack"] = 12
            return rtn
        def editMeWell(t) :
            def rtn() :
                t.cost["hack", "my", "balls"] = 12.12
            return rtn
        self.assertTrue(all(firesException(editMeWell(t)) and firesException(editMeBadly(t)) for t in
                            (ticDat, staticFactory.FrozenTicDat())))

        def attributeMe(t) :
            def rtn() :
                t.boger="bogerwoger"
            return rtn

        self.assertTrue(firesException(attributeMe(ticDat)) and firesException(attributeMe(staticFactory.FrozenTicDat())))

        mutable = staticFactory.TicDat(**{t:getattr(objOrig,t) for t in tables})
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(mutable,t), goodTable(t))
        ### get this one to work !!!!!! no reason not to override both..... make sure sqlTicDat does it too!!!! ####
        self.assertTrue(firesException(editMeBadly(mutable)))
        self.assertFalse(firesException(editMeWell(mutable)) or firesException(attributeMe(mutable)))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.cost, mutable.cost, goodTable("cost")) ))

    def testFour(self):
        objOrig = sillyMeData()
        staticFactory = TicDatFactory(**sillyMeSchema())
        goodTable = lambda t : lambda _t : staticFactory.goodTicDatTable(_t, t)
        tables = set(staticFactory.primaryKeyFields)
        ticDat = staticFactory.FrozenTicDat(**objOrig)
        self.assertTrue(goodTicDatObject(ticDat))
        for t in tables :
            self._assertSame(objOrig[t], getattr(ticDat,t), goodTable(t))

        mutTicDat = staticFactory.TicDat()
        for k,v in ticDat.a.items() :
            mutTicDat.a[k] = v.values()
        for k,v in ticDat.b.items() :
            mutTicDat.b[k] = v.values()[0]
        for t in tables :
            self._assertSame(getattr(mutTicDat, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue("theboger" not in mutTicDat.a)
        mutTicDat.a["theboger"]["aData2"] =22
        self.assertTrue("theboger" in mutTicDat.a and mutTicDat.a["theboger"].values() == (0, 22, 0))

def runTheTests(fastOnly=True) :
    utils.runSuite(TestUtils, fastOnly=fastOnly)

# Run the tests.
if __name__ == "__main__":
    runTheTests()

