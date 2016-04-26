import os
import unittest
import inspect


def data_directory():
    rtn = os.path.join(os.path.dirname(os.path.abspath(inspect.getsourcefile(data_directory))), "data")
    assert os.path.isdir(rtn)
    return rtn


class TestNetflow(unittest.TestCase):
    pass

# Run the tests.
if __name__ == "__main__":
    unittest.main()