import os
from ticdat.opl import create_opl_text, read_opl_text
import sys
from ticdat.ticdatfactory import TicDatFactory, DataFrame
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, spacesData, spacesSchema
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger, flagged_as_run_alone
from ticdat.testing.ticdattestutils import  makeCleanDir, addNetflowForeignKeys, clean_denormalization_errors
import unittest

#@fail_to_debugger
class TestOpl(unittest.TestCase):
    def testDiet(self):
        from ticdat.opl import create_opl_mod_text
        tdf = TicDatFactory(**dietSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        o = create_opl_mod_text(tdf)
        with open("o.mod","w") as f:
            f.write(o)
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertFalse(tdf._same_data(oldDat, newDat))
    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        o = create_opl_mod_text(tdf)
        with open("o.mod","w") as f:
            f.write(o)
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**sillyMeData()))
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

# Run the tests.
if __name__ == "__main__":
    unittest.main()

    diet.input_schema.schema()