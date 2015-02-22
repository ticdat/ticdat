import sys
import unittest
import ticdat._private.utils as utils
from ticdat.static import TicDatFactory, goodTicDatObject
import ticdat.static as static


_GRB_INFINITY = 1e+100

def _firesException(f) :
    try:
        f()
    except Exception as e:
        return e

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@utils.failToDebugger
class TestUtils(unittest.TestCase):

    # gurobi netflow problem - http://www.gurobi.com/documentation/6.0/example-tour/netflow_py
    def _netflowSchema(self):
        return {
            "primaryKeyFields" : {"commodities" : "name", "nodes":"name", "arcs" : ("source", "destination"),
                                  "cost" : ("commodity", "source", "destination"),
                                  "inflow" : ("commodity", "node")},
            "dataFields" : {"arcs" : "capacity", "cost" : "cost", "inflow" : "quantity"}
        }
    def _origNetflowTicDat(self) :
        class _(object) :
            pass

        dat = _() # simplest object with a __dict__

        # simplest possible copy

        dat.commodities = ['Pencils', 'Pens']
        dat.nodes = ['Detroit', 'Denver', 'Boston', 'New York', 'Seattle']

        dat.arcs = {
  ('Detroit', 'Boston'):   100,
  ('Detroit', 'New York'):  80,
  ('Detroit', 'Seattle'):  120,
  ('Denver',  'Boston'):   120,
  ('Denver',  'New York'): 120,
  ('Denver',  'Seattle'):  120 }

        dat.cost = {
  ('Pencils', 'Detroit', 'Boston'):   10,
  ('Pencils', 'Detroit', 'New York'): 20,
  ('Pencils', 'Detroit', 'Seattle'):  60,
  ('Pencils', 'Denver',  'Boston'):   40,
  ('Pencils', 'Denver',  'New York'): 40,
  ('Pencils', 'Denver',  'Seattle'):  30,
  ('Pens',    'Detroit', 'Boston'):   20,
  ('Pens',    'Detroit', 'New York'): 20,
  ('Pens',    'Detroit', 'Seattle'):  80,
  ('Pens',    'Denver',  'Boston'):   60,
  ('Pens',    'Denver',  'New York'): 70,
  ('Pens',    'Denver',  'Seattle'):  30 }

        dat.inflow = {
  ('Pencils', 'Detroit'):   50,
  ('Pencils', 'Denver'):    60,
  ('Pencils', 'Boston'):   -50,
  ('Pencils', 'New York'): -50,
  ('Pencils', 'Seattle'):  -10,
  ('Pens',    'Detroit'):   60,
  ('Pens',    'Denver'):    40,
  ('Pens',    'Boston'):   -40,
  ('Pens',    'New York'): -30,
  ('Pens',    'Seattle'):  -30 }

        return dat
    # gurobi diet problem - http://www.gurobi.com/documentation/6.0/example-tour/diet_py
    def _dietSchema(self):
        return {
            # deliberately mixing up singleton containers and strings for situations where one field is being listed
            "primaryKeyFields" : {"categories" : ("name",), "foods" : "name", "nutritionQuantities" : ("food", "category")},
            "dataFields" : {"categories" : ("minNutrition", "maxNutrition"),
                            "foods": "cost",
                            "nutritionQuantities" : ["qty"]}
        }
    def _origDietTicDat(self):
        # this is the gurobi diet data in ticDat format

        class _(object) :
            pass

        dat = _() # simplest object with a __dict__

        dat.categories = {
          'calories': {"minNutrition": 1800, "maxNutrition" : 2200},
          'protein':  {"minNutrition": 91,   "maxNutrition" : _GRB_INFINITY},
          'fat':      {"minNutrition": 0,    "maxNutrition" : 65},
          'sodium':   {"minNutrition": 0,    "maxNutrition" : 1779}}

        dat.foods = {
          'hamburger': {"cost": 2.49},
          'chicken':   {"cost": 2.89},
          'hot dog':   {"cost": 1.50},
          'fries':     {"cost": 1.89},
          'macaroni':  2.09,
          'pizza':     {"cost": 1.99},
          'salad':     {"cost": 2.49},
          'milk':      (0.89,),
          'ice cream': {"cost": 1.59}}

        dat.nutritionQuantities = {
          ('hamburger', 'calories'): {"qty" : 410},
          ('hamburger', 'protein'):  {"qty" : 24},
          ('hamburger', 'fat'):      {"qty" : 26},
          ('hamburger', 'sodium'):   {"qty" : 730},
          ('chicken',   'calories'): {"qty" : 420},
          ('chicken',   'protein'):  {"qty" : 32},
          ('chicken',   'fat'):      {"qty" : 10},
          ('chicken',   'sodium'):   {"qty" : 1190},
          ('hot dog',   'calories'): {"qty" : 560},
          ('hot dog',   'protein'):  {"qty" : 20},
          ('hot dog',   'fat'):      {"qty" : 32},
          ('hot dog',   'sodium'):   {"qty" : 1800},
          ('fries',     'calories'): {"qty" : 380},
          ('fries',     'protein'):  {"qty" : 4},
          ('fries',     'fat'):      {"qty" : 19},
          ('fries',     'sodium'):   {"qty" : 270},
          ('macaroni',  'calories'): {"qty" : 320},
          ('macaroni',  'protein'):  {"qty" : 12},
          ('macaroni',  'fat'):      {"qty" : 10},
          ('macaroni',  'sodium'):   {"qty" : 930},
          ('pizza',     'calories'): {"qty" : 320},
          ('pizza',     'protein'):  {"qty" : 15},
          ('pizza',     'fat'):      {"qty" : 12},
          ('pizza',     'sodium'):   {"qty" : 820},
          ('salad',     'calories'): {"qty" : 320},
          ('salad',     'protein'):  {"qty" : 31},
          ('salad',     'fat'):      {"qty" : 12},
          ('salad',     'sodium'):   {"qty" : 1230},
          ('milk',      'calories'): {"qty" : 100},
          ('milk',      'protein'):  {"qty" : 8},
          ('milk',      'fat'):      {"qty" : 2.5},
          ('milk',      'sodium'):   {"qty" : 125},
          ('ice cream', 'calories'): {"qty" : 330},
          ('ice cream', 'protein'):  {"qty" : 8},
          ('ice cream', 'fat'):      {"qty" : 10},
          ('ice cream', 'sodium'):   {"qty" : 180} }

        return dat

    def testOne(self):
        def _cleanIt(x) :
            x.foods['macaroni'] = {"cost": 2.09}
            x.foods['milk'] = {"cost":0.89}
            return x
        dataObj = self._origDietTicDat()
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj,
                        ("categories", "foods", "nutritionQuantities")))
        tdf = TicDatFactory(**self._dietSchema())
        self.assertTrue(tdf.goodTicDatObject(dataObj))
        dataObj = _cleanIt(dataObj)
        self.assertTrue(goodTicDatObject(dataObj) and goodTicDatObject(dataObj,
                        ("categories", "foods", "nutritionQuantities")))

        msg = []
        dataObj.foods[("milk", "cookies")] = {"cost": float("inf")}
        dataObj.boger = object()
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj, badMessageHandler=msg.append))
        self.assertTrue({"foods : Inconsistent key lengths", "boger : Not a dict-like object."} == set(msg))
        self.assertTrue(goodTicDatObject(dataObj, ("categories", "nutritionQuantities")))

        dataObj = self._origDietTicDat()
        dataObj.categories["boger"] = {"cost":1}
        dataObj.categories["boger"] = {"cost":1}
        self.assertFalse(goodTicDatObject(dataObj) or goodTicDatObject(dataObj, badMessageHandler=msg.append))
        self.assertTrue({'boger : Not a dict-like object.', 'foods : Inconsistent key lengths',
                         'categories : Inconsistent field name keys.',
                         'foods : At least one value is not a dict-like object'} == set(msg))
        self.assertTrue("categories cannot be treated as a ticDat table : Inconsistent data field name keys" in
            _firesException(lambda : tdf.FrozenTicDat(**{t:getattr(dataObj,t) for t in tdf.primaryKeyFields})).message)

    def _assertSame(self, t1, t2, goodTicDatTable):
        _ass = lambda _t1, _t2 : utils.assertTicDatTablesSame(_t1, _t2,
                _goodTicDatTable=  goodTicDatTable,
                _assertTrue=self.assertTrue, _assertFalse=self.assertFalse)
        _ass(t1, t2)
        _ass(t2, t1)

    def testTwo(self):
        objOrig = self._origDietTicDat()
        staticFactory = TicDatFactory(**self._dietSchema())
        tables = set(staticFactory.primaryKeyFields)
        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        self.assertTrue(goodTicDatObject(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t),
                                    lambda _t : staticFactory.goodTicDatTable(_t, t))

    def testThree(self):
        objOrig = self._origNetflowTicDat()
        staticFactory = TicDatFactory(**self._netflowSchema())
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
        self.assertTrue(_firesException(lambda : self._assertSame(
            objOrig.commodities, ticDat.commodities, goodTable("commodities")) ))
        self.assertTrue(_firesException(lambda : self._assertSame(
            objOrig.arcs, ticDat.arcs, goodTable("arcs")) ))

        ticDat = staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue(ticDat.arcs[1, 2]["capacity"] == 12)
        self.assertTrue(12.3 in ticDat.commodities)

        objOrig.cost[5]=5

        self.assertTrue("cost cannot be treated as a ticDat table : Inconsistent key lengths" in
            _firesException(lambda : staticFactory.FrozenTicDat(**{t:getattr(objOrig,t) for t in tables})))




def runTheTests(fastOnly=True) :
    utils.runSuite(TestUtils, fastOnly=fastOnly)

# Run the tests.
if __name__ == "__main__":
    runTheTests()

