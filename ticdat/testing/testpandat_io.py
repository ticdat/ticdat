import unittest
from ticdat.pandatfactory import PanDatFactory
from ticdat.utils import DataFrame, numericish, ForeignKey, ForeignKeyMapping
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, spacesSchema, firesException
from ticdat.testing.ticdattestutils import netflowSchema, pan_dat_maker, dietSchema, spacesData
from ticdat.testing.ticdattestutils import makeCleanDir, netflowData, dietData, addDietDataTypes
from ticdat.ticdatfactory import TicDatFactory
import ticdat.pandatio as pandatio
import shutil
import os
import json
try:
    import numpy
    import pandas as pd
except:
    numpy = pd = None
import math
try:
    import dateutil, dateutil.parser
except:
    dateutil = None
import datetime

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

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures (also hides a bunch of warnings)
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

        pdf_shrunk = PanDatFactory(**{k:v for k,v in dietSchema().items() if k != "nutritionQuantities"})
        self.assertTrue(len(pdf_shrunk.all_tables) == len(pdf.all_tables)-1)
        xlsPanDatShrunk = pdf_shrunk.xls.create_pan_dat(filePath)
        self.assertTrue(pdf_shrunk._same_data(panDat, xlsPanDatShrunk))
        filePathShrunk = os.path.join(_scratchDir, "diet_shrunk.xlsx")
        self.assertTrue(self.firesException(lambda: pdf.xls.create_pan_dat(filePathShrunk)))
        pdf_shrunk.xls.write_file(panDat, filePathShrunk)
        xlsPanDatShrunk = pdf.xls.create_pan_dat(filePathShrunk)
        self.assertTrue(pdf_shrunk._same_data(panDat, xlsPanDatShrunk))

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

    def testReadingJsonFromTDF(self):
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.TicDat(**{t: getattr(dietData(), t) for t in tdf.primary_key_fields})
        ticDat.categories["fat"]["minNutrition"] = -float("inf")  # violates integrity but better testing
        writePath = os.path.join(_scratchDir, "read_from_TDF.json")
        tdf.json.write_file(ticDat, writePath)
        dat2 = pdf.copy_to_tic_dat(pdf.json.create_pan_dat(writePath))
        self.assertTrue(tdf._same_data(ticDat, dat2, epsilon=1e-5))

    def testDietWithInfFlagging(self):
        diet_pdf = PanDatFactory(**dietSchema())
        addDietDataTypes(diet_pdf)
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_to_pandas(tdf.copy_tic_dat(dietData()), drop_pk_columns=False)
        diet_pdf.set_infinity_io_flag(999999999)
        core_path = os.path.join(_scratchDir, "diet_with_inf_flagging")
        diet_pdf.sql.write_file(dat, core_path+".db")
        diet_pdf.csv.write_directory(dat, core_path+"_csv")
        diet_pdf.json.write_file_pd(dat, core_path+".json")
        diet_pdf.json.write_file(dat, core_path+"2.json")
        diet_pdf.xls.write_file(dat, core_path+".xlsx")
        for attr, f in [["sql", core_path+".db"], ["csv", core_path+"_csv"], ["json", core_path+".json"],
                        ["json", core_path+"2.json"], ["xls", core_path+".xlsx"]]:
            dat_1 = getattr(diet_pdf, attr).create_pan_dat(f)
            self.assertTrue(diet_pdf._same_data(dat, dat_1, epsilon=1e-5))
            pdf = diet_pdf.clone()
            dat_1 = getattr(pdf, attr).create_pan_dat(f)
            self.assertTrue(pdf._same_data(dat, dat_1, epsilon=1e-5))
            pdf = PanDatFactory(**diet_pdf.schema())
            dat_1 = getattr(pdf, attr).create_pan_dat(f)
            self.assertFalse(pdf._same_data(dat, dat_1, epsilon=1e-5))
            protein = dat_1.categories["name"] == "protein"
            self.assertTrue(list(dat_1.categories[protein]["maxNutrition"])[0] == 999999999)
            dat_1.categories.loc[protein, "maxNutrition"] = float("inf")
            self.assertTrue(pdf._same_data(dat, dat_1, epsilon=1e-5))

    def test_missing_tables(self):
        core_path = os.path.join(_scratchDir, "missing_tables")
        pdf_1 = PanDatFactory(this = [["Something"],["Another"]])
        pdf_2 = PanDatFactory(**dict(pdf_1.schema(), that=[["What", "Ever"],[]]))
        dat = pdf_1.PanDat(this={"Something": ["a", "b", "c"], "Another": [2, 3, 5]})
        for attr, path, func in [["sql", core_path+".db", "write_file"],
                           ["csv", core_path+"_csv", "write_directory"],
                           ["json", core_path+".json", "write_file_pd"],
                           ["json", core_path + "2.json", "write_file"],
                           ["xls", core_path+".xlsx", "write_file"]]:
            getattr(getattr(pdf_1, attr), func)(dat, path)
            dat_1 = getattr(pdf_2, attr).create_pan_dat(path)
            self.assertTrue(pdf_1._same_data(dat, dat_1))

    def test_datetime(self):
        core_path = os.path.join(_scratchDir, "parameters")
        pdf = PanDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        pdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        pdf.add_parameter("p2", None, datetime=True, nullable=True)
        pdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        pdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)
        dat = TicDatFactory(**pdf.schema()).TicDat(
            table_with_stuffs =[[dateutil.parser.parse("July 11 1972"), None],
                                [datetime.datetime.now(), dateutil.parser.parse("Sept 11 2011")]],
        parameters = [["p1", "7/11/1911"], ["p2", None]]
        )
        dat = TicDatFactory(**pdf.schema()).copy_to_pandas(dat, drop_pk_columns=False)
        self.assertFalse(pdf.find_data_type_failures(dat) or pdf.find_data_row_failures(dat))

        for attr, path, func in [["sql", core_path+".db", "write_file"],
                           ["csv", core_path+"_csv", "write_directory"],
                           ["json", core_path+".json", "write_file_pd"],
                           ["json", core_path + "2.json", "write_file"],
                           ["xls", core_path+".xlsx", "write_file"]]:
            getattr(getattr(pdf, attr), func)(dat, path)
            # FutureWarning: Inferring datetime64[ns] from data containing strings is deprecated
            dat_1 = getattr(pdf, attr).create_pan_dat(path) # happens here, but only when "2.json" in path
            self.assertFalse(pdf._same_data(dat, dat_1))
            self.assertFalse(pdf.find_data_type_failures(dat_1) or pdf.find_data_row_failures(dat_1))
            dat_1 = pdf.copy_to_tic_dat(dat_1)
            self.assertTrue(set(dat_1.parameters) == {'p1', 'p2'})
            self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], (datetime.datetime, numpy.datetime64))
                            and not pd.isnull(dat_1.parameters["p1"]["b"]))
            self.assertTrue(pd.isnull(dat_1.parameters["p2"]["b"]))
            self.assertTrue(all(isinstance(_, (datetime.datetime, numpy.datetime64)) and not pd.isnull(_)
                                for _ in dat_1.table_with_stuffs))
            self.assertTrue(all(isinstance(_, (datetime.datetime, numpy.datetime64)) or _ is None
                                or utils.safe_apply(math.isnan)(_) for v in dat_1.table_with_stuffs.values()
                                for _ in v.values()))
            self.assertTrue({pd.isnull(_) for v in dat_1.table_with_stuffs.values() for _ in v.values()} ==
                            {True, False})

    def testDateTimeTwo(self):
        file = os.path.join(_scratchDir, "datetime_pd.xlsx")
        df = utils.pd.DataFrame({"a":list(map(utils.pd.Timestamp,
            ["June 13 1960 4:30PM", "Dec 11 1970 1AM", "Sept 11 2001 9:30AM"]))})
        df.to_excel(file, "Cool Runnings")
        pdf = PanDatFactory(cool_runnings = [["a"],[]])
        pdf.set_data_type("cool_runnings", "a", datetime=True)
        dat = pdf.xls.create_pan_dat(file)
        self.assertTrue(set(dat.cool_runnings["a"]) == set(df["a"]))

    def test_parameters(self):
        core_path = os.path.join(_scratchDir, "parameters")
        pdf = PanDatFactory(parameters=[["Key"], ["Value"]])
        pdf.add_parameter("Something", 100)
        pdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = TicDatFactory(**pdf.schema()).TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        dat = TicDatFactory(**pdf.schema()).copy_to_pandas(dat, drop_pk_columns=False)
        for attr, path, func in [["sql", core_path+".db", "write_file"],
                           ["csv", core_path+"_csv", "write_directory"],
                           ["json", core_path+".json", "write_file_pd"],
                           ["json", core_path + "2.json", "write_file"],
                           ["xls", core_path+".xlsx", "write_file"]]:
            getattr(getattr(pdf, attr), func)(dat, path)
            dat_1 = getattr(pdf, attr).create_pan_dat(path)
            self.assertTrue(pdf._same_data(dat, dat_1))
        core_path = os.path.join(_scratchDir, "parameters_two")
        dat = TicDatFactory(**pdf.schema()).TicDat(parameters = [["Something",float("inf")], ["Different", "05701"]])
        dat = TicDatFactory(**pdf.schema()).copy_to_pandas(dat, drop_pk_columns=False)
        for attr, path, func in [["sql", core_path+".db", "write_file"],
                           ["csv", core_path+"_csv", "write_directory"],
                           ["json", core_path+".json", "write_file_pd"],
                           ["json", core_path + "2.json", "write_file"],
                           ["xls", core_path+".xlsx", "write_file"]]:
            getattr(getattr(pdf, attr), func)(dat, path)
            dat_1 = getattr(pdf, attr).create_pan_dat(path)
            self.assertTrue(pdf._same_data(dat, dat_1))


    def testInfFlagging(self):
        pdf = PanDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            pdf.set_data_type("table", f, nullable=True)
        def make_dat(l):
            tdf = TicDatFactory(**pdf.schema())
            return tdf.copy_to_pandas(tdf.TicDat(table = l), drop_pk_columns=False)
        dat = make_dat([[None, 100], [200, 109], [0, 300], [300, None], [400, 0]])
        core_path = os.path.join(_scratchDir, "non_inf_flagging")
        for attr, path, func in [["sql", core_path+".db", "write_file"],
                                 ["csv", core_path+"_csv", "write_directory"],
                                 ["json", core_path+".json", "write_file_pd"],
                                 ["json", core_path + "2.json", "write_file"],
                                 ["xls", core_path+".xlsx", "write_file"]]:
            getattr(getattr(pdf, attr), func)(dat, path)
            dat_1 = getattr(pdf, attr).create_pan_dat(path)
            _ = PanDatFactory(table=[[], ["field one", "field two"]])
            self.assertTrue(_._same_data(dat, dat_1, nans_are_same_for_data_rows=True))

            pdf_ = PanDatFactory(table=[["field one"], ["field two"]])
            for f in ["field one", "field two"]:
                pdf_.set_data_type("table", f, max=float("inf"), inclusive_max=True)
            pdf_.set_infinity_io_flag(None)
            dat_inf = make_dat([[float("inf"), 100], [200, 109], [0, 300], [300, float("inf")], [400, 0]])
            dat_1 = getattr(pdf_, attr).create_pan_dat(path)
            self.assertTrue(pdf._same_data(dat_inf, dat_1))
            getattr(getattr(pdf_, attr), func)(dat, path)
            dat_1 = getattr(pdf_, attr).create_pan_dat(path)  #
            self.assertTrue(pdf._same_data(dat_inf, dat_1))
            pdf_ = PanDatFactory(table=[["field one"], ["field two"]])
            for f in ["field one", "field two"]:
                pdf_.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
            pdf_.set_infinity_io_flag(None)
            dat_1 = getattr(pdf_, attr).create_pan_dat(path)  #
            self.assertFalse(pdf._same_data(dat_inf, dat_1))
            dat_inf = make_dat([[float("-inf"), 100], [200, 109], [0, 300], [300, -float("inf")], [400, 0]])
            self.assertTrue(pdf._same_data(dat_inf, dat_1))

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
        ex = self.firesException(lambda : pdf2.json.create_pan_dat(pdf.json.write_file_pd(panDat, "")))
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

        panDat2 = pdf2.json.create_pan_dat(pdf.json.write_file_pd(panDat, ""), fill_missing_fields=True)
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

        panDat3 = pdf3.json.create_pan_dat(pdf.json.write_file_pd(panDat, ""), fill_missing_fields=True)
        self.assertTrue(set(panDat3.foods["extra"]) == {13})
        panDat3.foods.drop("extra", axis=1, inplace=True)
        self.assertTrue(pdf._same_data(panDat, panDat3, epsilon=1e-5))

        ex = []
        try:
            pdf3.json.create_pan_dat(pdf.json.write_file(panDat, ""), fill_missing_fields=True)
        except utils.TicDatError as e:
            ex.append(e)
        self.assertTrue(ex)

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

        pdf2 = PanDatFactory(**{k: v for k, v in dietSchema().items() if k != "nutritionQuantities"})
        panDat2 = pdf2.copy_pan_dat(panDat)
        dirPath = os.path.join(_scratchDir, "diet_missing_csv")
        pdf2.csv.write_directory(panDat2, dirPath, makeCleanDir(dirPath))
        panDat3 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf2._same_data(panDat2, panDat3))
        self.assertTrue(all(hasattr(panDat3, x) for x in pdf.all_tables))
        self.assertFalse(len(panDat3.nutritionQuantities))
        self.assertTrue(len(panDat3.categories) and len(panDat3.foods))

        pdf2 = PanDatFactory(**{k: v for k, v in dietSchema().items() if k == "categories"})
        panDat2 = pdf2.copy_pan_dat(panDat)
        pdf2.csv.write_directory(panDat2, dirPath, makeCleanDir(dirPath))
        panDat3 = pdf.csv.create_pan_dat(dirPath)
        self.assertTrue(pdf2._same_data(panDat2, panDat3))
        self.assertTrue(all(hasattr(panDat3, x) for x in pdf.all_tables))
        self.assertFalse(len(panDat3.nutritionQuantities) or len(panDat3.foods))
        self.assertTrue(len(panDat3.categories))

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
        for func in ["write_file", "write_file_pd"]:
            tdf = TicDatFactory(**dietSchema())
            pdf = PanDatFactory(**dietSchema())
            ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
            panDat = pan_dat_maker(dietSchema(), ticDat)
            filePath = os.path.join(_scratchDir, "diet.json")
            getattr(pdf.json, func)(panDat, filePath)
            panDat2 = pdf.json.create_pan_dat(filePath)
            self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
            pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables}) if func == "write_file_pd" else pdf
            getattr(pdf.json, func)(panDat, filePath)
            panDat2 = pdf2.json.create_pan_dat(filePath)
            self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))

            re_fielded_schema = {"categories" : (("name",),["maxNutrition", "minNutrition"]),
         "foods" :[["name"],[]],
         "nutritionQuantities" : (["food", "category"], ["qty"])}
            pdf3 = PanDatFactory(**re_fielded_schema)
            panDat3 = pdf3.json.create_pan_dat(filePath)
            for t, (pks, dfs) in re_fielded_schema.items():
                self.assertTrue(list(pks) + list(dfs) == list(getattr(panDat3, t).columns))

            tdf = TicDatFactory(**netflowSchema())
            pdf = PanDatFactory(**netflowSchema())
            ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
            panDat = pan_dat_maker(netflowSchema(), ticDat)
            filePath = os.path.join(_scratchDir, "netflow.json")
            getattr(pdf.json, func)(panDat, filePath)
            panDat2 = pdf.json.create_pan_dat(filePath)
            self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
            panDat3 = pdf.json.create_pan_dat(getattr(pdf.json, func)(panDat, ""))
            self.assertTrue(pdf._same_data(panDat, panDat3))
            dicted = json.loads(getattr(pdf.json, func)(panDat, ""))
            panDat4 = pdf.PanDat(**dicted)
            self.assertTrue(pdf._same_data(panDat, panDat4))
            pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables}) if func == "write_file_pd" else pdf
            panDat5 = pdf2.PanDat(**dicted)
            self.assertTrue(pdf._same_data(panDat, panDat5))


        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "diet.json")
        pdf.json.write_file_pd(panDat, filePath, orient='columns', index=True)
        # the following doesn't generate a TicDatError, which is fine
        self.assertTrue(firesException(lambda : pdf.json.create_pan_dat(filePath)))
        panDat2 = pdf.json.create_pan_dat(filePath, orient='columns')
        self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=1e-5))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file_pd(panDat, "", orient='columns'), orient="columns")
        self.assertTrue(pdf._same_data(panDat, panDat3, epsilon=1e-5))
        dicted = json.loads(pdf.json.write_file_pd(panDat, "", orient='columns'))
        panDat4 = pdf.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat4, epsilon=1e-5))

    def testJsonCross(self):
        if not self.can_run:
            return
        for func in ["write_file", "write_file_pd"]:
            tdf = TicDatFactory(**dietSchema())
            pdf = PanDatFactory(**dietSchema())
            ticDat = tdf.freeze_me(tdf.TicDat(**{t: getattr(dietData(),t) for t in tdf.primary_key_fields}))
            panDat = pan_dat_maker(dietSchema(), ticDat)
            filePath = os.path.join(_scratchDir, "diet_cross.json")
            getattr(pdf.json, func)(panDat, filePath)
            ticDat2 = tdf.json.create_tic_dat(filePath, from_pandas=True)
            self.assertTrue(tdf._same_data(ticDat, ticDat2, epsilon=0.0001))
            tdf.json.write_file(ticDat, filePath, allow_overwrite=True, to_pandas=True)
            panDat2 = pdf.json.create_pan_dat(filePath)
            self.assertTrue(pdf._same_data(panDat, panDat2, epsilon=0.0001))

    def testJsonSpacey(self):
        if not self.can_run:
            return

        tdf = TicDatFactory(**spacesSchema())
        pdf = PanDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        panDat = pan_dat_maker(spacesSchema(), ticDat)
        ext = ".json"
        filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
        pdf.json.write_file_pd(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file_pd(panDat, "", case_space_table_names=True))
        self.assertTrue(pdf._same_data(panDat, panDat3))


        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
        pdf.json.write_file_pd(panDat, filePath, case_space_table_names=True)
        panDat2 = pdf.json.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))
        panDat3 = pdf.json.create_pan_dat(pdf.json.write_file_pd(panDat, "", case_space_table_names=True))
        self.assertTrue(pdf._same_data(panDat, panDat3))

        dicted = json.loads(pdf.json.write_file_pd(panDat, "", orient='columns'))
        panDat4 = pdf.PanDat(**dicted)
        self.assertTrue(pdf._same_data(panDat, panDat4, epsilon=1e-5))

    def testIssue45(self):
        pdf = PanDatFactory(data=[["a"], ["b"]])
        tdf = TicDatFactory(**pdf.schema())
        dat_nums = tdf.copy_to_pandas(tdf.TicDat(data = [[1,2],[3,4], [22, 44]]), drop_pk_columns=False)
        dat_strs = tdf.copy_to_pandas(tdf.TicDat(data = [["1","2"],["3","4"], ["022", "0044"]]), drop_pk_columns=False)
        files = [os.path.join(_scratchDir, _) for _ in ["dat_nums.xlsx", "dat_strs.xlsx"]]
        pdf.xls.write_file(dat_nums, files[0])
        pdf.xls.write_file(dat_strs, files[1])
        dat_nums_2, dat_strs_2 = [pdf.xls.create_pan_dat(_) for _ in files]
        self.assertTrue(pdf._same_data(dat_nums, dat_nums_2))
        # this is pandas pushing things to be numeric
        self.assertFalse(pdf._same_data(dat_strs, dat_strs_2))
        self.assertTrue(pdf._same_data(dat_nums, dat_strs_2))

        pdf = PanDatFactory(data=[["a"], ["b"]])
        pdf.set_data_type("data", "a", number_allowed=False, strings_allowed='*')
        dat_mixed = tdf.copy_to_pandas(tdf.TicDat(data = [["1",2],["3",4], ["022", 44]]), drop_pk_columns=False)
        dat_nums_2, dat_strs_2 = [pdf.xls.create_pan_dat(_) for _ in files]
        self.assertFalse(pdf._same_data(dat_nums, dat_nums_2))
        self.assertFalse(pdf._same_data(dat_strs, dat_strs_2))
        self.assertFalse(pdf._same_data(dat_nums_2, dat_mixed))
        self.assertTrue(pdf._same_data(dat_strs_2, dat_mixed))

        pdf = PanDatFactory(data=[["a"], ["b"]])
        csv_dirs = [os.path.join(_scratchDir, _) for _ in ["dat_nums_csv", "dat_strs_csv"]]
        pdf.csv.write_directory(dat_nums, csv_dirs[0])
        pdf.csv.write_directory(dat_strs, csv_dirs[1])
        dat_nums_2, dat_strs_2 = [pdf.csv.create_pan_dat(_) for _ in csv_dirs]
        self.assertTrue(pdf._same_data(dat_nums, dat_nums_2))
        # this is pandas pushing things to be numeric
        self.assertFalse(pdf._same_data(dat_strs, dat_strs_2))
        self.assertTrue(pdf._same_data(dat_nums, dat_strs_2))
        pdf = PanDatFactory(data=[["a"], ["b"]])
        pdf.set_data_type("data", "a", number_allowed=False, strings_allowed='*')
        dat_nums_2, dat_strs_2 = [pdf.csv.create_pan_dat(_) for _ in csv_dirs]
        self.assertFalse(pdf._same_data(dat_nums, dat_nums_2))
        self.assertFalse(pdf._same_data(dat_strs, dat_strs_2))
        self.assertFalse(pdf._same_data(dat_nums_2, dat_strs_2))
        self.assertTrue(pdf._same_data(dat_strs_2, dat_mixed))

    def test_nullables(self):
        core_path = os.path.join(_scratchDir, "nullables")
        pdf = PanDatFactory(table_with_stuffs = [["field one"], ["field two"]])
        pdf.set_data_type("table_with_stuffs", "field one")
        pdf.set_data_type("table_with_stuffs", "field two", number_allowed=False, strings_allowed='*', nullable=True)
        dat = TicDatFactory(**pdf.schema()).TicDat(table_with_stuffs=[[101, "022"], [202, None], [303, "111"]])
        dat = TicDatFactory(**pdf.schema()).copy_to_pandas(dat, drop_pk_columns=False)
        self.assertFalse(pdf.find_data_type_failures(dat))

        for attr, path in [["csv", core_path+"_csv"], ["xls", core_path+".xlsx"], ["sql", core_path+".db"],
                           ["json", core_path+".json"]]:
            f_or_d = "directory" if attr == "csv" else "file"
            write_func, write_kwargs = utils._get_write_function_and_kwargs(pdf, path, f_or_d,
                                                                            case_space_table_names=False)
            write_func(dat, path, **write_kwargs)
            dat_1 = utils._get_dat_object(pdf, "create_pan_dat", path, f_or_d, False)
            self.assertTrue(pdf._same_data(dat, dat_1, nans_are_same_for_data_rows=True))

    def testLongName(self):
        prepend = "b"*20
        pdf = PanDatFactory(**{prepend*2+t:v for t,v in dietSchema().items()})
        self.assertTrue(self.firesException(lambda : pdf.xls._verify_differentiable_sheet_names()))

        pdf = PanDatFactory(**{prepend+t:v for t,v in dietSchema().items()})
        tdf = TicDatFactory(**pdf.schema())
        panDat = tdf.copy_to_pandas(tdf.TicDat(**{t:getattr(dietData(),t.replace(prepend, ""))
                                             for t in pdf.primary_key_fields}), reset_index=True)
        filePath = os.path.join(_scratchDir, "longname.xlsx")
        pdf.xls.write_file(panDat, filePath)
        panDat2 = pdf.xls.create_pan_dat(filePath)
        self.assertTrue(pdf._same_data(panDat, panDat2))

_scratchDir = TestIO.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING pandat IO UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestIO.can_run = True
    unittest.main()
