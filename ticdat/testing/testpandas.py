import os
import ticdat.utils as utils
import sys
from ticdat.ticdatfactory import TicDatFactory, DataFrame
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData
from ticdat.testing.ticdattestutils import  netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger
from ticdat.testing.ticdattestutils import  makeCleanDir, addNetflowForeignKeys
import unittest


#@fail_to_debugger
class TestPandas(unittest.TestCase):
    canRun = False
    def testDiet(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dietSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        ticDat = tdf.copy_to_pandas(oldDat)
        for k in oldDat.foods:
            self.assertTrue(oldDat.foods[k]["cost"] == ticDat.foods.cost[k])
        for k in oldDat.categories:
            self.assertTrue(oldDat.categories[k]["minNutrition"] == ticDat.categories.minNutrition[k])
        for k1, k2 in oldDat.nutritionQuantities:
            self.assertTrue(oldDat.nutritionQuantities[k1,k2]["qty"] ==
                            ticDat.nutritionQuantities.qty[k1,k2])
        nut = ticDat.nutritionQuantities
        self.assertTrue(firesException(lambda : nut.qty.loc[:, "fatty"]))
        self.assertTrue(firesException(lambda : nut.qty.loc["chickeny", :]))
        self.assertFalse(firesException(lambda : nut.qty.sloc[:, "fatty"]))
        self.assertFalse(firesException(lambda : nut.qty.sloc["chickeny", :]))
        self.assertTrue(0 == sum(nut.qty.sloc[:, "fatty"]) == sum(nut.qty.sloc["chickeny", :]))
        self.assertTrue(sum(nut.qty.sloc[:, "fat"]) == sum(nut.qty.loc[:, "fat"]) ==
                        sum(r["qty"] for (f,c),r in oldDat.nutritionQuantities.items() if c == "fat"))
        self.assertTrue(sum(nut.qty.sloc["chicken",:]) == sum(nut.qty.loc["chicken",:]) ==
                        sum(r["qty"] for (f,c),r in oldDat.nutritionQuantities.items() if f == "chicken"))

        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))



    def testNetflow(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        addNetflowForeignKeys(tdf)
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        ticDat = tdf.copy_to_pandas(oldDat, ["arcs", "cost"])
        self.assertTrue(all(hasattr(ticDat, t) == (t in ["arcs", "cost"]) for t in tdf.all_tables))
        self.assertTrue(len(ticDat.arcs.capacity.sloc["Boston",:]) == len(oldDat.nodes["Boston"].arcs_source) == 0)
        self.assertTrue(len(ticDat.arcs.capacity.sloc[:,"Boston"]) == len(oldDat.nodes["Boston"].arcs_destination) == 2)
        self.assertTrue(all(ticDat.arcs.capacity.sloc[:,"Boston"][src] == r["capacity"]
                            for src, r in oldDat.nodes["Boston"].arcs_destination.items()))
        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=True)
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        # because we have single pk field tables, dropping the pk columns is probelmatic
        self.assertFalse(tdf._same_data(rebornTicDat, oldDat))

        # but with the default argument all is well
        ticDat = tdf.copy_to_pandas(oldDat)
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))
        self.assertTrue(set(ticDat.inflow.columns) == {"quantity"})
        self.assertTrue(set(ticDat.nodes.columns) == {"name"})



    def testSilly(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dict({"d" : [("dData1", "dData2", "dData3", "dData4"),[]],
                                    "e" : [["eData"],[]]}, **sillyMeSchema()))
        ticDat = tdf.copy_to_pandas(tdf.TicDat(**sillyMeData()))
        self.assertFalse(len(ticDat.d) + len(ticDat.e))
        oldDat = tdf.freeze_me(tdf.TicDat(**dict({"d" : {(1,2,3,4):{}, (1, "b","c","d"):{}, ("a", 2,"c","d"):{}},
                                                  "e" : {11:{},"boger":{}}},
                                **sillyMeData())))
        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=True)
        def checkTicDat():
            self.assertTrue(len(ticDat.d) ==3 and len(ticDat.e) == 2)
            self.assertTrue(set(ticDat.d.index.values) == {(1,2,3,4), (1, "b","c","d"), ("a", 2,"c","d")})
            self.assertTrue(set(ticDat.e.index.values) == {11,"boger"})
            self.assertTrue(len(ticDat.c) == len(oldDat.c) == 3)
            self.assertTrue(ticDat.c.loc[i] == oldDat.c[i] for i in range(3))
        checkTicDat()
        self.assertFalse(hasattr(ticDat.d, "dData1") or hasattr(ticDat.e, "eData"))

        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=False)
        checkTicDat()
        self.assertTrue(ticDat.e.loc[11].values[0] == 11)
        if sys.version_info[0] == 2:
            self.assertTrue(len(ticDat.d.dData1.sloc[1,:,:,:]) == 2)
        else : # very strange infrequent bug issue that I will investigate later
            self.assertTrue(len(ticDat.d.dData1.sloc[1]) == 2)

        ticDat = tdf.copy_to_pandas(oldDat)
        checkTicDat()
        if sys.version_info[0] == 2:
            self.assertTrue(len(ticDat.d.dData1.sloc[1,:,:,:]) == 2)
        else:
            self.assertTrue(len(ticDat.d.dData1.sloc[1]) == 2)
        self.assertTrue(ticDat.e.loc[11].values[0] == 11)
        self.assertTrue(set(ticDat.d.columns) == {"dData%s"%s for s in range(5)[1:]})

        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

        ticDat.b = ticDat.b.bData
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING PANDAS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestPandas.canRun = True
    unittest.main()

