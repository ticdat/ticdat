import os
import ticdat.lingo as tlingo
import sys
from ticdat.ticdatfactory import TicDatFactory

import ticdat.utils as utils
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, addNetflowDataTypes, nearlySame
from ticdat.testing.ticdattestutils import  netflowSchema,sillyMeData, sillyMeSchema
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, get_testing_file_path
import unittest

#@fail_to_debugger
class TestLingo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_value = utils.development_deployed_environment
        utils.development_deployed_environment = True
    @classmethod
    def tearDownClass(cls):
        utils.development_deployed_environment = cls._original_value

    def testTryCreateSpace(self):
        def test_(schema_factory, data_factory):
            tdf = TicDatFactory(**schema_factory())
            dat = tdf.copy_tic_dat(data_factory())
            mapping = tlingo._try_create_space_case_mapping(tdf, dat)["mapping"]
            remapdat = tlingo._apply_space_case_mapping(tdf, dat, {v:k for k,v in mapping.items()})
            mapmapdat = tlingo._apply_space_case_mapping(tdf, remapdat, mapping)
            self.assertTrue(tdf._same_data(dat, mapmapdat))
            self.assertFalse(tdf._same_data(dat, remapdat))
        test_(dietSchema, dietData)
        test_(netflowSchema, netflowData)
        test_(sillyMeSchema, lambda : TicDatFactory(**sillyMeSchema()).TicDat(**sillyMeData()))

        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        dat.foods["ice_cream"] = dat.foods["ice cream"]
        dat.categories["ICE CREAM"] = {}
        dat.categories["fAt"] = dat.categories["fat"]
        failures = tlingo._try_create_space_case_mapping(tdf, dat)["failures"]
        self.assertTrue(failures == {'ICE_CREAM': ('ICE CREAM', 'ice cream', 'ice_cream'), 'FAT': ('fAt', 'fat')})




if __name__ == "__main__":
    unittest.main()
