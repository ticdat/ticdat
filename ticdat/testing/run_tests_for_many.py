#useful helper testing script

import ticdat.testing.ticdattestutils
run_suite = ticdat.testing.ticdattestutils._runSuite

from ticdat.testing.testcsv import TestCsv
from ticdat.testing.testxls import TestXls
from ticdat.testing.testpandas import TestPandas
from ticdat.testing.testutils import TestUtils
from ticdat.testing.testjson import TestJson
from ticdat.testing.testpandat_io import TestIO
import ticdat.testing.testpandat_utils
TestPandatUtils = ticdat.testing.testpandat_utils.TestUtils
from ticdat.testing.testsql import TestSql
from ticdat.testing.test_pgtd import TestPostres
from ticdat.testing.testmodel import TestModel

the_classes = [TestSql, TestPandatUtils, TestIO, TestJson, TestUtils, TestSql, TestPandatUtils, TestCsv,
               TestPandas, TestModel, TestXls, TestPostres,]

for c in the_classes:
    print(f"\n--------{c}")
    can_attr = [x for x in dir(c) if x.startswith("can")]
    assert len(can_attr) == 1 or c in [TestUtils, TestModel]
    if can_attr:
        setattr(c, can_attr[0], True)
    run_suite(c)

