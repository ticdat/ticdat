# This test needs a few things not in ticdat source to run - the kehaar package and the bernardo_slowby directory
# Pete knows how to track down both of these (bernardo_slowby.zip for the latter)
# This test is just for performance tracking.
try:
    import sqlalchemy as sa
except:
    sa = None
try:
    import testing.postgresql as testing_postgresql
except:
    testing_postgresql = None
try:
    import kehaar
except:
    kehaar = None
import math
from ticdat.testing.ticdattestutils import flagged_as_run_alone, fail_to_debugger, makeCleanDir
import ticdat.utils as utils
import unittest
import os
import inspect
from functools import wraps
import time
import shutil
from ticdat import PanDatFactory

def _codeFile() :
    return  os.path.realpath(os.path.abspath(inspect.getsourcefile(_codeFile)))
__codeFile = _codeFile()
def _codeDir():
    return os.path.dirname(__codeFile)

def _timeit(func, expected_time):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        exe_time = end - start
        print(f"**** {func.__name__} executed in {exe_time} expected time of {expected_time}")
        return result
    return wrapper

def _forced_field_types():
    rtn = {('products', 'Freight Class'): 'text', ('demand', 'Max Demand'): 'text'}
    for t, dvs in kehaar.input_schema.default_values.items():
        for f, dv in dvs.items():
            if isinstance(dv, str) and not kehaar.input_schema.data_types.get(t, {}).get(f, None):
                rtn[t, f] = "text"
    return rtn

#@fail_to_debugger
class TestSlowBernardo(unittest.TestCase):
    def setUp(self):
        try:
            self.postgresql = testing_postgresql.Postgresql()
            self.engine = sa.create_engine(self.postgresql.url())
            self.engine_fail = None
        except Exception as e:
            self.postgresql = self.engine = None
            self.engine_fail = e
        if self.engine_fail:
            print(f"!!!!Engine failed to load due to {self.engine_fail}")
        if self.engine:
            for test_schema in test_schemas:
                if utils.safe_apply(lambda: test_schema in sa.inspect(self.engine).get_schema_names())():
                    self.engine.execute(sa.schema.DropSchema(test_schema, cascade=True))
        makeCleanDir(_scratchDir)

    def tearDown(self):
        if self.postgresql:
            self.engine.dispose()
            self.postgresql.stop()
        shutil.rmtree(_scratchDir)

    def test_tdf(self):
        tdf = kehaar.input_schema
        dat = _timeit(tdf.csv.create_tic_dat, 30)(os.path.join(_codeDir(), "bernardo_slowby"))
        tdf.pgsql.write_schema(self.engine, test_schemas[0], include_ancillary_info=False,
                               forced_field_types=_forced_field_types())
        _timeit(tdf.pgsql.write_data, 30)(dat, self.engine, test_schemas[0], dsn=self.postgresql.dsn())
        _timeit(tdf.pgsql.create_tic_dat, 15)(self.engine, test_schemas[0])

    def test_tdf_2(self):
        tdf = kehaar.input_schema
        dat = _timeit(tdf.csv.create_tic_dat, 30)(os.path.join(_codeDir(), "bernardo_slowby"))
        _timeit(tdf.xls.write_file, 50)(dat, os.path.join(_scratchDir, "bernslow.xlsx"))
        _timeit(tdf.xls.create_tic_dat, 80)(os.path.join(_scratchDir, "bernslow.xlsx"))

    def test_pdf(self):
        pdf = kehaar.input_schema.clone(clone_factory=PanDatFactory)
        dat = _timeit(pdf.csv.create_pan_dat, 10)(os.path.join(_codeDir(), "bernardo_slowby"))
        pdf.pgsql.write_schema(self.engine, test_schemas[1], include_ancillary_info=False,
                               forced_field_types=_forced_field_types())
        _timeit(pdf.pgsql.write_data, 25)(dat, self.engine, test_schemas[1])
        _timeit(pdf.pgsql.create_pan_dat, 15)(self.engine, test_schemas[1])


test_schemas = [f"test_schema_{_}" for _ in range(5)]
_scratchDir = TestSlowBernardo.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    unittest.main()
