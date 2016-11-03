from ticdat import TicDatFactory, Model, utils
from ticdat.model import cplex
from ticdat.testing.ticdattestutils import dietSolver, fail_to_debugger, nearlySame
import unittest

#@fail_to_debugger
class TestCplex(unittest.TestCase):
    can_run = False
    def testDiet(self):
        sln, cost = dietSolver("cplex")
        self.assertTrue(sln)
        self.assertTrue(nearlySame(cost, 11.8289))

_scratchDir = TestCplex.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if utils.stringish(cplex) :
        print("!!!!!!!!!FAILING CPLEX UNIT TESTS DUE TO FAILURE TO LOAD CPLEX LIBRARIES!!!!!!!!")
    else:
        TestCplex.can_run = True
    unittest.main()