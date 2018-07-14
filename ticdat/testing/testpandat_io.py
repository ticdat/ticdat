import unittest
from ticdat.pandatfactory import PanDatFactory
from ticdat.utils import DataFrame, numericish, ForeignKey, ForeignKeyMapping
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, spacesSchema
from ticdat.testing.ticdattestutils import netflowSchema, copy_to_pandas_with_reset, dietSchema, spacesData
from ticdat.testing.ticdattestutils import makeCleanDir, netflowData, dietData
from ticdat.ticdatfactory import TicDatFactory
import itertools
import shutil
import os

def _deep_anonymize(x)  :
    if not hasattr(x, "__contains__") or utils.stringish(x):
        return x
    if utils.dictish(x) :
        return {_deep_anonymize(k):_deep_anonymize(v) for k,v in x.items()}
    return list(map(_deep_anonymize,x))

def pan_dat_maker(schema, tic_dat):
    tdf = TicDatFactory(**schema)
    pdf = PanDatFactory(**schema)
    return pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, tic_dat))

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestIO(unittest.TestCase):
    can_run = False
    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_scratchDir)
    def testXlsSimple(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.xlsx")
        pdf.xls.write_file(panDat, filePath)
        xlsPanDat = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, xlsPanDat))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.xlsx")
        pdf.xls.write_file(panDat, filePath)
        panDat2 = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testXlsSpacey(self):
        if not self.can_run:
            return

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        ext = ".xlsx"
        filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
        pdf.xls.write_file(panDat, filePath, case_space_sheet_names=True)
        panDat2 = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
        pdf.xls.write_file(panDat, filePath, case_space_sheet_names=True)
        panDat2 = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testSqlSimple(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.db")
        pdf.sql.write_file(panDat, filePath)
        sqlPanDat = pdf.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, sqlPanDat))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.db")
        pdf.sql.write_file(panDat, filePath)
        panDat2 = pdf.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testSqlSpacey(self):
        if not self.can_run:
            return

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        ext = ".db"
        filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
        pdf.sql.write_file(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
        pdf.sql.write_file(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))


_scratchDir = TestIO.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING pandat IO UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestIO.can_run = True
    unittest.main()
