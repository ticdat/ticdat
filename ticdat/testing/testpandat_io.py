import unittest
from ticdat.pandatfactory import PanDatFactory
from ticdat.utils import DataFrame, numericish, ForeignKey, ForeignKeyMapping
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, spacesSchema, firesException
from ticdat.testing.ticdattestutils import netflowSchema, copy_to_pandas_with_reset, dietSchema, spacesData
from ticdat.testing.ticdattestutils import makeCleanDir, netflowData, dietData
from ticdat.ticdatfactory import TicDatFactory
import ticdat.pandatio as pandatio
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
    def firesException(self, f):
        e = firesException(f)
        if e:
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)
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

    def testDefaultAdd(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        xlsFilePath = os.path.join(_scratchDir, "diet_add.xlsx")
        pdf.xls.write_file(panDat, xlsFilePath)
        sqlFilePath = os.path.join(_scratchDir, "diet_add.sql")
        pdf.sql.write_file(panDat, sqlFilePath)
        csvDirPath = os.path.join(_scratchDir, "diet_add_csv")
        pdf.csv.write_directory(panDat, csvDirPath, case_space_table_names=True)

        pdf2 = PanDatFactory(**{k:[p,d] if k!="foods" else [p, list(d)+["extra"]] for k,(p,d) in dietSchema().items()})
        ex = self.firesException(lambda : pdf2.xls.create_pan_dat(xlsFilePath))
        self.assertTrue("missing" in ex and "extra" in ex)
        ex = self.firesException(lambda : pdf2.sql.create_pan_dat(sqlFilePath))
        self.assertTrue("missing" in ex and "extra" in ex)
        ex = self.firesException(lambda : pdf2.csv.create_pan_dat(csvDirPath))
        self.assertTrue("missing" in ex and "extra" in ex)

        panDat2 = pdf2.sql.create_pan_dat(sqlFilePath, fill_missing_fields=True)
        self.assertTrue(set(panDat2.foods["extra"]) == {0})
        panDat2.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        panDat2 = pdf2.xls.create_pan_dat(xlsFilePath, fill_missing_fields=True)
        self.assertTrue(set(panDat2.foods["extra"]) == {0})
        panDat2.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        panDat2 = pdf2.csv.create_pan_dat(csvDirPath, fill_missing_fields=True)
        self.assertTrue(set(panDat2.foods["extra"]) == {0})
        panDat2.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        pdf3 = PanDatFactory(**pdf2.schema())
        pdf3.set_default_value("foods", "extra", 13)
        panDat3 = pdf3.sql.create_pan_dat(sqlFilePath, fill_missing_fields=True)
        self.assertTrue(set(panDat3.foods["extra"]) == {13})
        panDat3.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat3))

        panDat3 = pdf3.xls.create_pan_dat(xlsFilePath, fill_missing_fields=True)
        self.assertTrue(set(panDat3.foods["extra"]) == {13})
        panDat3.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat3))

        panDat3 = pdf3.csv.create_pan_dat(csvDirPath, fill_missing_fields=True)
        self.assertTrue(set(panDat3.foods["extra"]) == {13})
        panDat3.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat3))

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
        self.assertTrue(pandatio.sql, "this unit test requires SQLite installed")

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**{
        "a_table" : {1 : [1, 2, "3"],
                     22.2 : (12, 0.12, "something"),
                     0.23 : (11, 12, "thirt")},
        "b_table" : {(1, 2, "foo") : 1, (1012.22, 4, "0012") : 12},
        "c_table" : (("this", 2, 3, 4), ("that", 102.212, 3, 5.5),
                      ("another",5, 12.5, 24) )
        })
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

    def testSqlSpaceyTwo(self):
        if not self.can_run:
            return
        self.assertTrue(pandatio.sql, "this unit test requires SQLite installed")

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**{
        "a_table" : {1 : [1, 2, "3"],
                     22.2 : (12, 0.12, "something"),
                     0.23 : (11, 12, "thirt")},
        "b_table" : {(1, 2, "foo") : 1, (1012.22, 4, "0012") : 12},
        "c_table" : (("this", 2, 3, 4), ("that", 102.212, 3, 5.5),
                      ("another",5, 12.5, 24) )
        })
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        ext = ".db"
        filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
        with pandatio.sql.connect(filePath) as con:
            pdf.sql.write_file(panDat, db_file_path=None, con=con, case_space_table_names=True)
        with pandatio.sql.connect(filePath) as con:
            panDat2 = pdf.sql.create_pan_dat(db_file_path=None, con=con)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
        with pandatio.sql.connect(filePath) as con:
            pdf.sql.write_file(panDat, db_file_path="", con=con, case_space_table_names=True)
        with pandatio.sql.connect(filePath) as con:
            panDat2 = pdf.sql.create_pan_dat(None, con)
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testCsvSimple(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "diet_csv")
        pdf.csv.write_directory(panDat, dirPath)
        panDat2 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "netflow_csv")
        pdf.csv.write_directory(panDat, dirPath)
        panDat2 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "diet_csv")
        pdf.csv.write_directory(panDat, dirPath, decimal=",")
        panDat2 = pdf.csv.create_pan_dat(dirPath)
        self.assertFalse(pdf._same_data(panDat, panDat2))
        panDat2 = pdf.csv.create_pan_dat(dirPath, decimal=",")
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testCsvSpacey(self):
        if not self.can_run:
            return
        self.assertTrue(pandatio.sql, "this unit test requires SQLite installed")

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**{
        "a_table" : {1 : [1, 2, "3"],
                     22.2 : (12, 0.12, "something"),
                     0.23 : (11, 12, "thirt")},
        "b_table" : {(1, 2, "foo") : 1, (1012.22, 4, "0012") : 12},
        "c_table" : (("this", 2, 3, 4), ("that", 102.212, 3, 5.5),
                      ("another",5, 12.5, 24) )
        })
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "spaces_2_csv")
        pdf.csv.write_directory(panDat, dirPath, case_space_table_names=True)
        panDat2 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "spaces_2_2_csv")
        pdf.csv.write_directory(panDat, dirPath, case_space_table_names=True, sep=":")
        panDat2 = pdf.csv.create_pan_dat(dirPath, sep=":")
        self.assertTrue(pdf._same_data(panDat, panDat2))

    def testJsonSimple(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.json")
        pdf.json.write_file(panDat, filePath)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.json")
        pdf.json.write_file(panDat, filePath)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.json")
        pdf.json.write_file(panDat, filePath, orient='columns')
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertFalse(pdf._same_data(panDat, panDat2))
        panDat2 = pdf.json.create_pan_dat(filePath, orient='columns')
        self.assertTrue(pdf._same_data(panDat, panDat2))

_scratchDir = TestIO.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING pandat IO UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestIO.can_run = True
    unittest.main()
