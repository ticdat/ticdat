import sys
import unittest
import ticdat.utils as utils
from ticdat import LogFile, Progress
from ticdat.ticdatfactory import TicDatFactory, _ForeignKey, _ForeignKeyMapping
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException, memo
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger
from ticdat.testing.ticdattestutils import spacesSchema, spacesData, clean_denormalization_errors, flagged_as_run_alone
import os
import itertools
import shutil

def create_inputset_mock(tdf, dat):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    class RtnObject(object):
        schema = {t:"not needed for mock object" for t in tdf.all_tables}
        def getTable(self, t):
            return getattr(temp_dat, t).reset_index(drop=True)
    return RtnObject

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestOpalytics(unittest.TestCase):

    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    # need to do some basic copying tests
    # DON'T FORGET TO ALSO TEST ERRORS AND DUPLICATES

# Run the tests.
if __name__ == "__main__":
    unittest.main()
