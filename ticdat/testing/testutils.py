import sys
import unittest
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, runSuite, failToDebugger
from ticdat.testing.ticdattestutils import assertTicDatTablesSame, DEBUG, addNetflowForeignKeys, addDietForeignKeys


#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@failToDebugger
class TestUtils(unittest.TestCase):
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return e.message
    def testOne(self):
        def _cleanIt(x) :
            x.foods['macaroni'] = {"cost": 2.09}
            x.foods['milk'] = {"cost":0.89}
            return x
        dataObj = dietData()
        tdf = TicDatFactory(**dietSchema())
        self.assertTrue(tdf.good_tic_dat_object(dataObj))
        dataObj2 = tdf.copy_tic_dat(dataObj)
        dataObj3 = tdf.copy_tic_dat(dataObj, freeze_it=True)
        dataObj4 = tdf.TicDat(**dataObj3.pickle_this())
        self.assertTrue(all (tdf._same_data(dataObj, x) and dataObj is not x for x in (dataObj2, dataObj3, dataObj4)))
        dataObj = _cleanIt(dataObj)
        self.assertTrue(tdf.good_tic_dat_object(dataObj))
        self.assertTrue(all (tdf._same_data(dataObj, x) and dataObj is not x for x in (dataObj2, dataObj3)))
        def hackit(x) :
            x.foods["macaroni"] = 100
        self.assertTrue(self.firesException(lambda :hackit(dataObj3)))
        hackit(dataObj2)
        self.assertTrue(not tdf._same_data(dataObj, dataObj2) and  tdf._same_data(dataObj, dataObj3))

        msg = []
        dataObj.foods[("milk", "cookies")] = {"cost": float("inf")}
        dataObj.boger = object()
        self.assertFalse(tdf.good_tic_dat_object(dataObj) or
                         tdf.good_tic_dat_object(dataObj, bad_message_handler =msg.append))
        self.assertTrue({"foods : Inconsistent key lengths"} == set(msg))
        self.assertTrue(all(tdf.good_tic_dat_table(getattr(dataObj, t), t)
                            for t in ("categories", "nutritionQuantities")))

        dataObj = dietData()
        dataObj.categories["boger"] = {"cost":1}
        dataObj.categories["boger"] = {"cost":1}
        self.assertFalse(tdf.good_tic_dat_object(dataObj) or
                         tdf.good_tic_dat_object(dataObj, bad_message_handler=msg.append))
        self.assertTrue({'foods : Inconsistent key lengths',
                         'categories : Inconsistent data field name keys.'} == set(msg))
        self.assertTrue("categories cannot be treated as a ticDat table : Inconsistent data field name keys" in
            firesException(lambda : tdf.FrozenTicDat(**{t:getattr(dataObj,t) for t in tdf.primary_key_fields})).message)

    def _assertSame(self, t1, t2, goodTicDatTable):

        if utils.dictish(t1) or utils.dictish(t2) :
            _ass = lambda _t1, _t2 : assertTicDatTablesSame(_t1, _t2,
                    _goodTicDatTable=  goodTicDatTable,
                    **({} if DEBUG() else {"_assertTrue":self.assertTrue, "_assertFalse":self.assertFalse}))

            _ass(t1, t2)
            _ass(t2, t1)
        else :
            setify = lambda t : set(t) if len(t) and not hasattr(t[0], "values") else {r.values() for r in t}
            self.assertTrue(setify(t1) == setify(t2))

    def testTwo(self):
        objOrig = dietData()
        staticFactory = TicDatFactory(**dietSchema())
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t),
                                    lambda _t : staticFactory.good_tic_dat_table(_t, t))

    def testThree(self):
        objOrig = netflowData()
        staticFactory = TicDatFactory(**netflowSchema())
        goodTable = lambda t : lambda _t : staticFactory.good_tic_dat_table(_t, t)
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
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

        self.assertTrue(firesException(editMeBadly(mutable)))
        self.assertFalse(firesException(editMeWell(mutable)) or firesException(attributeMe(mutable)))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.cost, mutable.cost, goodTable("cost")) ))

    def testFour(self):
        objOrig = sillyMeData()
        staticFactory = TicDatFactory(**sillyMeSchema())
        goodTable = lambda t : lambda _t : staticFactory.good_tic_dat_table(_t, t)
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.FrozenTicDat(**objOrig)
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
        for t in tables :
            self._assertSame(objOrig[t], getattr(ticDat,t), goodTable(t))
        pickedData = staticFactory.TicDat(**ticDat.pickle_this())
        self.assertTrue(staticFactory._same_data(ticDat, pickedData))
        mutTicDat = staticFactory.TicDat()
        for k,v in ticDat.a.items() :
            mutTicDat.a[k] = v.values()
        for k,v in ticDat.b.items() :
            mutTicDat.b[k] = v.values()[0]
        for r in ticDat.c:
            mutTicDat.c.append(r)
        for t in tables :
            self._assertSame(getattr(mutTicDat, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue("theboger" not in mutTicDat.a)
        mutTicDat.a["theboger"]["aData2"] =22
        self.assertTrue("theboger" in mutTicDat.a and mutTicDat.a["theboger"].values() == (0, 22, 0))

        newSchema = sillyMeSchema()
        newSchema["a"][1] += ("aData4",)
        newFactory = TicDatFactory(**newSchema)
        def makeNewTicDat() : return newFactory.TicDat(a=ticDat.a, b=ticDat.b, c=ticDat.c)
        newTicDat = makeNewTicDat()
        self.assertFalse(staticFactory.good_tic_dat_object(newTicDat))
        self.assertTrue(newFactory.good_tic_dat_object(ticDat))
        self.assertTrue(newFactory._same_data(makeNewTicDat(), newTicDat))
        newTicDat.a[ticDat.a.keys()[0]]["aData4"]=12
        self.assertFalse(newFactory._same_data(makeNewTicDat(), newTicDat))

    def testFive(self):
        def fksSame(fk1, fk2) :
            self.assertTrue(set(fk1.keys()) == set(fk2.keys()))
            def addCardinality(fks):
                for fk in fks:
                    if "cardinality" not in fk :
                        fk["cardinality"] = fk.get("cardinality", "many-to-one")
            flatten = lambda x: [_ for z in x for _ in z]
            addCardinality(flatten(fk1.values())), addCardinality(flatten(fk2.values()))
            for t, fks in fk1.items():
                for fk in fks :
                    self.assertTrue(fk in fk2[t])
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        fksSame(tdf.foreign_keys, dict(arcs =  ({'foreignTable': u'nodes','mappings': {u'source': u'name'}},
                            {'foreignTable': u'nodes', 'mappings': {u'destination': u'name'}}),
                            cost = ({'foreignTable': u'nodes','mappings': {u'source': u'name'}},
                            {'foreignTable': u'nodes', 'mappings': {u'destination': u'name'}},
                            {'foreignTable': u'commodities', 'mappings': {u'commodity': u'name'}}),
                            inflow = ({'foreignTable': u'commodities', 'mappings': {u'commodity': u'name'}},
                                      {'foreignTable': u'nodes', 'mappings': {u'node': u'name'}})))
        tdf.clear_foreign_keys("cost")
        fksSame(tdf.foreign_keys, dict(arcs =  ({'foreignTable': u'nodes','mappings': {u'source': u'name'}},
                            {'foreignTable': u'nodes', 'mappings': {u'destination': u'name'}}),
                            inflow = ({'foreignTable': u'commodities', 'mappings': {u'commodity': u'name'}},
                                      {'foreignTable': u'nodes', 'mappings': {u'node': u'name'}})))

        tdf = TicDatFactory(**dietSchema())
        self.assertFalse(tdf.foreign_keys)
        addDietForeignKeys(tdf)

        fksSame(tdf.foreign_keys, {"nutritionQuantities"  :
                            ({'foreignTable': u'categories', 'mappings': {u'category': u'name'}},
                            {'foreignTable': u'foods', 'mappings': {u'food': u'name'}})})
        tdf.TicDat()
        self.assertTrue(self.firesException(lambda  : tdf.clear_foreign_keys("nutritionQuantities")))
        self.assertTrue(tdf.foreign_keys)
        tdf = TicDatFactory(**dietSchema())
        addDietForeignKeys(tdf)
        tdf.clear_foreign_keys("nutritionQuantities")
        self.assertFalse(tdf.foreign_keys)

        tdf = TicDatFactory(parentTable = [["pk"],["pd1", "pd2", "pd3"]],
                            goodChild = [["gk"], ["gd1", "gd2"]],
                            badChild = [["bk1", "bk2"], ["bd"]],
                            appendageChild = [["ak"], ["ad1", "ad2"]],
                            appendageBadChild = [["bk1", "bk2"], []])
        tdf.add_foreign_key("goodChild", "parentTable", {"gd1" : "pk"})
        tdf.add_foreign_key("badChild", "parentTable", {"bk2" : "pk"})
        self.assertTrue("many-to-many" in self.firesException(lambda :
                tdf.add_foreign_key("badChild", "parentTable", {"bd" : "pd2"})))
        tdf.add_foreign_key("appendageChild", "parentTable", {"ak" : "pk"})
        tdf.add_foreign_key("appendageBadChild", "badChild", {"bk2" : "bk2", "bk1":"bk1"})
        fks = tdf.foreign_keys
        self.assertTrue(fks["goodChild"][0]["cardinality"] == "many-to-one")
        self.assertTrue(fks["badChild"][0]["cardinality"] == "many-to-one")
        self.assertTrue(fks["appendageChild"][0]["cardinality"] == "one-to-one")
        self.assertTrue(fks["appendageBadChild"][0]["cardinality"] == "one-to-one")

        tdf.clear_foreign_keys("appendageBadChild")
        self.assertTrue(tdf.foreign_keys and "appendageBadChild" not in tdf.foreign_keys)
        tdf.clear_foreign_keys()
        self.assertFalse(tdf.foreign_keys)


def runTheTests(fastOnly=True) :
    runSuite(TestUtils, fastOnly=fastOnly)

# Run the tests.
if __name__ == "__main__":
    runTheTests()

