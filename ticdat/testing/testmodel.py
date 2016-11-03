from ticdat import TicDatFactory, Model, utils
from ticdat.model import cplex, gurobi, xpress
from ticdat.testing.ticdattestutils import dietSolver, fail_to_debugger, nearlySame, netflowSolver
import unittest

#@fail_to_debugger
class TestModel(unittest.TestCase):
    def _testDiet(self, modelType):
        sln, cost = dietSolver(modelType)
        self.assertTrue(sln)
        self.assertTrue(nearlySame(cost, 11.8289))
    def _testNetflow(self, modelType):
        sln, cost = netflowSolver(modelType)
        self.assertTrue(sln)
        self.assertTrue(nearlySame(cost, 5500.0))
    def testCplex(self):
        if utils.stringish(cplex) :
            print("\n\n\n!!!!!!!!!SKIPPING CPLEX UNIT TESTS DUE TO FAILURE TO LOAD CPLEX LIBRARIES!!!!!!!!")
            return
        self._testDiet("cplex")
        self._testNetflow("cplex")
    def testGurobi(self):
        if utils.stringish(gurobi) :
            print("\n\n\n!!!!!!!!!SKIPPING GUROBI UNIT TESTS DUE TO FAILURE TO LOAD GUROBI LIBRARIES!!!!!!!!")
            return
        self._testDiet("gurobi")
        self._testNetflow("gurobi")
    def testXpress(self):
        if utils.stringish(xpress) :
            print("\n\n\n!!!!!!!!!SKIPPING XPRESS UNIT TESTS DUE TO FAILURE TO LOAD XPRESS LIBRARIES!!!!!!!!")
            return
        self._testDiet("xpress")
        self._testNetflow("xpress")

_scratchDir = TestModel.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    unittest.main()