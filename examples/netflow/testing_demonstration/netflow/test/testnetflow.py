import os
import unittest
import inspect
import netflow
import math

def data_directory():
    rtn = os.path.join(os.path.dirname(os.path.abspath(inspect.getsourcefile(data_directory))), "data")
    assert os.path.isdir(rtn)
    return rtn

def _nearly_same(x1, x2):
    if max(abs(x1),abs(x2)) == 0:
        return True
    return (max(x1, x2) - min(x1,x2))/max(abs(x1), abs(x2)) < 0.0001

class TestNetflow(unittest.TestCase):
    def _compute_soln_cost(self, dat, soln):
        rtn = 0
        for (c, s, d),v in soln.flow.items():
            if v["quantity"] > 0:
                self.assertTrue((c, s, d) in dat.cost)
                rtn += v["quantity"] * dat.cost[c, s, d]["cost"]
        return rtn

    def test_simple_data(self):
        data_file = os.path.join(data_directory(), "simple_data.sql")
        dat = netflow.dataFactory.sql.create_tic_dat_from_sql(data_file)
        soln = netflow.solve(dat)
        self.assertTrue(_nearly_same(self._compute_soln_cost(dat, soln), 5500))

        # if I increase the cost of shipping Pens from Detroit to NY, then the model should do less of that.
        dat.cost['Pens', 'Detroit', 'New York']["cost"] *= 2
        soln2 = netflow.solve(dat)
        self.assertTrue(soln.flow['Pens', 'Detroit', 'New York']["quantity"] > 1.01 *
                        soln2.flow['Pens', 'Detroit', 'New York']["quantity"])
        # but also cost more in aggregate as a result
        self.assertTrue(self._compute_soln_cost(dat, soln2) > 1.01 * 5500)

# Run the tests.
if __name__ == "__main__":
    unittest.main()