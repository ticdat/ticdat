import os
import unittest
import ticdat.utils as utils
import shutil
from ticdat.ticdatfactory import TicDatFactory, pd
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData
from ticdat.testing.ticdattestutils import  netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, failToDebugger
from ticdat.testing.ticdattestutils import  makeCleanDir, runSuite

#@failToDebugger
class TestPandas(unittest.TestCase):

    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        ticDat = tdf.copy_to_pandas(oldDat)
        for k in oldDat.foods:
            self.assertTrue(oldDat.foods[k]["cost"] == ticDat.foods.cost[k])
        for k in oldDat.categories:
            self.assertTrue(oldDat.categories[k]["minNutrition"] == ticDat.categories.minNutrition[k])
        for k1, k2 in oldDat.nutritionQuantities:
            self.assertTrue(oldDat.nutritionQuantities[k1,k2]["qty"] ==
                            ticDat.nutritionQuantities.qty[k1,k2])


    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})

    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())



def runTheTests(fastOnly=True) :
    if not pd :
        print "!!!!!!!!!FAILING PANDAS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!"
        return
    runSuite(TestPandas, fastOnly=fastOnly)
# Run the tests.
if __name__ == "__main__":
    runTheTests()

