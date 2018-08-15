import unittest
from ticdat.pandatfactory import PanDatFactory
from ticdat.utils import DataFrame, numericish, ForeignKey, ForeignKeyMapping
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, spacesSchema, firesException
from ticdat.testing.ticdattestutils import netflowSchema, pan_dat_maker, dietSchema, spacesData
from ticdat.testing.ticdattestutils import makeCleanDir, netflowData, dietData, addDietForeignKeys
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, sillyMeDataTwoTables
from ticdat.ticdatfactory import TicDatFactory
import ticdat.pandatio as pandatio
import itertools
import shutil
import os
import json

def _deep_anonymize(x)  :
    if not hasattr(x, "__contains__") or utils.stringish(x):
        return x
    if utils.dictish(x) :
        return {_deep_anonymize(k):_deep_anonymize(v) for k,v in x.items()}
    return list(map(_deep_anonymize,x))

def hack_name(s):
    rtn = list(s)
    for i in range(len(s)):
        if s[i] == "_":
            rtn[i] = " "
        elif i%3==0:
            if s[i].lower() == s[i]:
                rtn[i] = s[i].upper()
            else:
                rtn[i] = s[i].lower()
    return "".join(rtn)

def create_inputset_mock(tdf, dat, hack_table_names=False, includeActiveEnabled = True):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    replaced_name = {t:hack_name(t) if hack_table_names else t for t in tdf.all_tables}
    original_name = {v:k for k,v in replaced_name.items()}
    if includeActiveEnabled:
        class RtnObject(object):
            schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
            def getTable(self, t, includeActive=False):
                rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
                if includeActive:
                    rtn["_active"] = True
                return rtn
    else:
        class RtnObject(object):
            schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
            def getTable(self, t):
                rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
                return rtn
    return RtnObject()

def create_inputset_mock_with_active_hack(tdf, dat, hack_table_names=False):
    tdf.good_tic_dat_object(dat)
    temp_dat = tdf.copy_to_pandas(dat, drop_pk_columns=False)
    replaced_name = {t:hack_name(t) if hack_table_names else t for t in tdf.all_tables}
    original_name = {v:k for k,v in replaced_name.items()}
    class RtnObject(object):
        schema = {replaced_name[t]:"not needed for mock object" for t in tdf.all_tables}
        def getTable(self, t, includeActive=False):
            rtn = getattr(temp_dat, original_name[t]).reset_index(drop=True)
            if "_active" in rtn.columns and not includeActive:
                rtn.drop("_active", axis=1, inplace=True)
            return rtn
    return RtnObject()

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
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        pdf2.xls.write_file(panDat, filePath)
        xlsPanDat = pdf2.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, xlsPanDat))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.xlsx")
        pdf.xls.write_file(panDat, filePath)
        panDat2 = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        xlsPanDat = pdf2.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, xlsPanDat))

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
        ex = self.firesException(lambda : pdf2.json.create_pan_dat(pdf.json.write_file(panDat, "")))
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

        panDat2 = pdf2.json.create_pan_dat(pdf.json.write_file(panDat, ""), fill_missing_fields=True)
        self.assertTrue(set(panDat2.foods["extra"]) == {0})
        panDat2.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))

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

        panDat3 = pdf3.json.create_pan_dat(pdf.json.write_file(panDat, ""), fill_missing_fields=True)
        self.assertTrue(set(panDat3.foods["extra"]) == {13})
        panDat3.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat3, epsilon=1e-5))


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
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        pdf2.sql.write_file(panDat, filePath)
        sqlPanDat = pdf2.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, sqlPanDat))


        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.db")
        pdf.sql.write_file(panDat, filePath)
        panDat2 = pdf.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        sqlPanDat = pdf2.sql.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, sqlPanDat))

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
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        panDat2 = pdf2.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf._same_data(panDat, panDat2))


        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        dirPath = os.path.join(_scratchDir, "netflow_csv")
        pdf.csv.write_directory(panDat, dirPath)
        panDat2 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        pdf2.csv.write_directory(panDat, dirPath)
        panDat2 = pdf2.csv.create_pan_dat(dirPath)
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
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        pdf2.json.write_file(panDat, filePath)
        panDat2 = pdf2.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "netflow.json")
        pdf.json.write_file(panDat, filePath)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file(panDat, ""))
        self.assertTrue(pdf._same_data(panDat, panDat3))
        dicted = json.loads(pdf.json.write_file(panDat, ""))
        panDat4 = pdf.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat4))
        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        panDat5 = pdf2.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat5))


        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.json")
        pdf.json.write_file(panDat, filePath, orient='columns', index=True)
        # the following doesn't generate a TicDatError, which is fine
        self.assertTrue(firesException(lambda : pdf.json.create_pan_dat(filePath)))
        panDat2 = pdf.json.create_pan_dat(filePath, orient='columns')
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file(panDat, "", orient='columns'), orient="columns")
        self.assertTrue(pdf._same_data(panDat, panDat3, epsilon=1e-5))
        dicted = json.loads(pdf.json.write_file(panDat, "", orient='columns'))
        panDat4 = pdf.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat4, epsilon=1e-5))

    def testJsonSpacey(self):
        if not self.can_run:
            return

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        ext = ".json"
        filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
        pdf.json.write_file(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file(panDat, "", case_space_table_names=True))
        self.assertTrue(pdf._same_data(panDat, panDat3))


        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
        pdf.json.write_file(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file(panDat, "", case_space_table_names=True))
        self.assertTrue(pdf._same_data(panDat, panDat3))

        dicted = json.loads(pdf.json.write_file(panDat, "", orient='columns'))
        panDat4 = pdf.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat4, epsilon=1e-5))


    def testMissingOpalyticsTable(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.copy_tic_dat(dietData()))
        inputset = create_inputset_mock(tdf, ticDat)

        pdf = PanDatFactory(**(dict(dietSchema(), missing_table=[["a"],["b"]])))
        panDat = pdf.opalytics.create_pan_dat(inputset)
        ticDat2 = pdf.copy_to_tic_dat(panDat)
        self.assertTrue(tdf._same_data(ticDat, ticDat2))
        self.assertFalse(ticDat2.missing_table)

    def testDietOpalytics(self):
        if not self.can_run:
            return
        for hack, raw_data, activeEnabled in list(itertools.product(*(([True, False],)*3))):
            tdf = TicDatFactory(**dietSchema())
            ticDat = tdf.freeze_me(tdf.copy_tic_dat(dietData()))
            inputset = create_inputset_mock(tdf, ticDat, hack, activeEnabled)

            pdf = PanDatFactory(**dietSchema())
            panDat = pdf.opalytics.create_pan_dat(inputset)
            self.assertFalse(pdf.find_duplicates(panDat))
            ticDat2 = pdf.copy_to_tic_dat(panDat)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))


            tdf2 = TicDatFactory(**{k:[pks, list(dfs) + ["dmy"]] for k,(pks, dfs) in tdf.schema().items()})
            _dat = tdf2.copy_tic_dat(ticDat)
            panDat = pdf.opalytics.create_pan_dat(create_inputset_mock(tdf2, _dat, hack))

            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

            pdf2 = PanDatFactory(**tdf2.schema())
            ex = self.firesException(lambda: pdf2.opalytics.create_pan_dat(inputset, raw_data=raw_data))
            self.assertTrue(all(_ in ex for _ in ["(table, field) pairs missing"] +
                                                  ["'%s', 'dmy'"%_ for _ in pdf2.all_tables]))

    def testDietCleaningOpalytics(self):
        sch = dietSchema()
        sch["categories"][-1].append("_active")
        tdf1 = TicDatFactory(**dietSchema())
        tdf2 = TicDatFactory(**sch)

        ticDat2 = tdf2.copy_tic_dat(dietData())
        for v in ticDat2.categories.values():
            v["_active"] = True
        ticDat2.categories["fat"]["_active"] = False
        ticDat1 = tdf1.copy_tic_dat(dietData())

        input_set = create_inputset_mock_with_active_hack(tdf2, ticDat2)
        pdf1 = PanDatFactory(**tdf1.schema())
        panDat = pdf1.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf1._same_data(pdf1.copy_to_tic_dat(panDat), ticDat1))

        panDatPurged = pdf1.opalytics.create_pan_dat(input_set)
        self.assertFalse(tdf1._same_data(pdf1.copy_to_tic_dat(panDatPurged), ticDat1))

        ticDat1.categories.pop("fat")
        tdf1.remove_foreign_key_failures(ticDat1)
        self.assertTrue(tdf1._same_data(pdf1.copy_to_tic_dat(panDatPurged), ticDat1))

    def testDietCleaningOpalyticsTwo(self):
        tdf = TicDatFactory(**dietSchema())
        addDietForeignKeys(tdf)
        tdf.set_data_type("categories", "maxNutrition", min=66, inclusive_max=True)
        ticDat = tdf.copy_tic_dat(dietData())

        input_set = create_inputset_mock(tdf, ticDat)
        pdf = PanDatFactory(**dietSchema())
        addDietForeignKeys(pdf)
        pdf.set_data_type("categories", "maxNutrition", min=66, inclusive_max=True)

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat) , ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.categories.pop("fat")
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))
        tdf.remove_foreign_key_failures(ticDat)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testDietCleaningOpalytisThree(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())

        pdf = PanDatFactory(**tdf.schema())
        pdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        addDietForeignKeys(pdf)

        input_set = create_inputset_mock(tdf, ticDat)

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat), ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.categories.pop("fat")
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))
        tdf.remove_foreign_key_failures(ticDat)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testDietCleaningOpalyticsFour(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        tdf.set_data_type("categories", "minNutrition", max=0, inclusive_max=True)
        addDietForeignKeys(tdf)
        ticDat = tdf.copy_tic_dat(dietData())

        input_set = create_inputset_mock(tdf, ticDat)

        pdf = PanDatFactory(**tdf.schema())
        pdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        pdf.set_data_type("categories", "minNutrition", max=0, inclusive_max=True)
        pdf.add_data_row_predicate("categories", lambda row : row["maxNutrition"] >= 66)
        addDietForeignKeys(pdf)

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat), ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.categories.pop("fat")
        ticDat.categories.pop("calories")
        ticDat.categories.pop("protein")

        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))
        tdf.remove_foreign_key_failures(ticDat)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testSillyCleaningOpalyticsOne(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.set_data_type("c", "cData4", number_allowed=False, strings_allowed=['d'])
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        pdf = PanDatFactory(**sillyMeSchema())
        pdf.set_data_type("c", "cData4", number_allowed=False, strings_allowed=['d'])

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat), ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testSillyCleaningOpalyticsTwo(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] == 'd')
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        pdf = PanDatFactory(**sillyMeSchema())
        pdf.add_data_row_predicate("c", lambda row : row["cData4"] == 'd')

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat), ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testSillyCleaningOpalyticsThree(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] != 4)
        tdf.add_data_row_predicate("c", lambda row : row["cData4"] != 24)
        ticDat = tdf.TicDat(**sillyMeData())

        input_set = create_inputset_mock(tdf, ticDat)

        pdf = PanDatFactory(**sillyMeSchema())
        pdf.add_data_row_predicate("c", lambda row : row["cData4"] != 4)
        pdf.add_data_row_predicate("c", lambda row : row["cData4"] != 24)

        panDat = pdf.opalytics.create_pan_dat(input_set, raw_data=True)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDat), ticDat))

        panDatPurged = pdf.opalytics.create_pan_dat(input_set, raw_data=False)
        self.assertFalse(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

        ticDat.c.pop()
        ticDat.c.pop(0)
        self.assertTrue(tdf._same_data(pdf.copy_to_tic_dat(panDatPurged), ticDat))

    def testSillyTwoTablesOpalytics(self):
        if not self.can_run:
            return
        for hack, raw_data in list(itertools.product(*(([True, False],)*2))):
            tdf = TicDatFactory(**sillyMeSchema())
            ticDat = tdf.TicDat(**sillyMeData())

            inputset = create_inputset_mock(tdf, ticDat, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

            ticDat = tdf.TicDat(**sillyMeDataTwoTables())
            inputset = create_inputset_mock(tdf, ticDat, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

    def testNetflowOpalytics(self):
        if not self.can_run:
            return
        for hack, raw_data in list(itertools.product(*(([True, False],)*2))):
            tdf = TicDatFactory(**netflowSchema())
            ticDat = tdf.copy_tic_dat(netflowData())
            inputset = create_inputset_mock(tdf, ticDat, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

            ticDat.nodes[12] = {}
            inputset = create_inputset_mock(tdf, ticDat, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

    def testSpacesOpalytics(self):
        if not self.can_run:
            return
        for hack, raw_data in list(itertools.product(*(([True, False],)*2))):
            tdf = TicDatFactory(**spacesSchema())
            ticDat = tdf.TicDat(**spacesData())
            inputset = create_inputset_mock(tdf, ticDat, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=raw_data)
            self.assertTrue(tdf._same_data(ticDat, pdf.copy_to_tic_dat(panDat)))

    def testDupsOpalytics(self):
        if not self.can_run:
            return
        for hack in [True, False]:
            tdf = TicDatFactory(one = [["a"],["b", "c"]],
                                two = [["a", "b"],["c"]],
                                three = [["a", "b", "c"],[]])
            tdf2 = TicDatFactory(**{t:[[],["a", "b", "c"]] for t in tdf.all_tables})
            td = tdf2.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                                for t in tdf.all_tables})
            inputset = create_inputset_mock(tdf2, td, hack)
            pdf = PanDatFactory(**tdf.schema())
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=True)
            self.assertTrue(all(len(getattr(panDat, t)) == 6 for t in tdf.all_tables))
            panDat = pdf.opalytics.create_pan_dat(inputset, raw_data=False)
            self.assertTrue(all(len(getattr(panDat, t)) < 6 for t in tdf.all_tables))
            td_1 = tdf.TicDat(**{t:[[1, 2, 1], [1, 2, 2], [2, 1, 3], [2, 2, 3], [1, 2, 2], ["new", 1, 2]]
                                for t in tdf.all_tables})
            td_2 = pdf.copy_to_tic_dat(panDat)
            self.assertTrue(all(set(getattr(td_1, t)) == set(getattr(td_2, t)) for t in tdf.all_tables))




_scratchDir = TestIO.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING pandat IO UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestIO.can_run = True
    unittest.main()
