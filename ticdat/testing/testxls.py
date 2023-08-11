import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger
from ticdat.testing.ticdattestutils import spacesData, spacesSchema, memo, flagged_as_run_alone
from ticdat.testing.ticdattestutils import makeCleanPath, sillyMeDataTwoTables
import ticdat.xls as ticdat_xlsx
import shutil
import unittest
import datetime
try:
    import dateutil, dateutil.parser
except:
    dateutil=None

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestXls(unittest.TestCase):
    can_run = False

    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_scratchDir)
    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)
    def _test_generic_copy(self, ticDat, tdf, skip_tables=None):
        assert all(tdf.primary_key_fields.get(t) for t in tdf.all_tables)
        path = os.path.join(makeCleanDir(os.path.join(_scratchDir, "generic_copy")), "file.xlsx")
        replace_name  = lambda f : "name_" if f == "name" else f
        clean_tdf = TicDatFactory(**{t:[list(map(replace_name, pks)), dfs] for t,(pks, dfs)
                                     in tdf.schema().items()})

        temp_tdf = TicDatFactory(**{t:v if t in (skip_tables or []) else '*'
                                    for t,v in clean_tdf.schema().items()})
        temp_dat = temp_tdf.TicDat(**{t:getattr(ticDat, t) for t in (skip_tables or [])})
        for t in temp_tdf.generic_tables:
            setattr(temp_dat, t, getattr(clean_tdf.copy_to_pandas(ticDat, drop_pk_columns=False) ,t))

        temp_tdf.xls.write_file(temp_dat, path)
        self.assertFalse(temp_tdf.xls.find_duplicates(path))
        read_dat = temp_tdf.xls.create_tic_dat(path)
        generic_free_dat, _ = utils.create_generic_free(read_dat, temp_tdf)
        check_dat = clean_tdf.TicDat()

        for t in temp_tdf.generic_tables:
            for r in getattr(generic_free_dat, t):
                pks = clean_tdf.primary_key_fields[t]
                getattr(check_dat, t)[r[pks[0]] if len(pks) == 1 else tuple(r[_] for _ in pks)] = \
                    {df:r[df] for df in clean_tdf.data_fields.get(t, [])}
        for t in (skip_tables or []):
            for k,v in getattr(generic_free_dat, t).items():
                getattr(check_dat, t)[k] = v
        self.assertTrue(clean_tdf._same_data(check_dat, clean_tdf.copy_tic_dat(ticDat)))

    def testDiet(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["nutritionQuantities"])
        filePath = os.path.join(_scratchDir, "diet.xls")
        tdf.xls.write_file(ticDat, filePath)
        xlsTicDat = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))
        tdf.xls.write_file(ticDat, filePath+"x")
        self.assertTrue(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x")))
        self.assertFalse(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x", treat_inf_as_infinity=False)))
        xlsTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(tdf._same_data(ticDat, xlsTicDat))

        self.assertFalse(tdf.xls.find_duplicates(filePath))

        ex = self.firesException(lambda :
                                 tdf.xls.create_tic_dat(filePath, row_offsets={t:1 for t in tdf.all_tables}))
        self.assertTrue("field names could not be found" in ex)
        xlsTicDat = tdf.xls.create_tic_dat(filePath, row_offsets={t:1 for t in tdf.all_tables}, headers_present=False)
        self.assertTrue(tdf._same_data(xlsTicDat, ticDat))
        xlsTicDat = tdf.xls.create_tic_dat(filePath, row_offsets={t:2 for t in tdf.all_tables}, headers_present=False)
        self.assertFalse(tdf._same_data(xlsTicDat, ticDat))
        self.assertTrue(all(len(getattr(ticDat, t))-1 == len(getattr(xlsTicDat, t)) for t in tdf.all_tables))

    def testMissingTable(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**dietSchema())
        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k != "nutritionQuantities"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        filePath = makeCleanPath(os.path.join(_scratchDir, "diet_missing.xlsx"))
        tdf2.xls.write_file(ticDat2, filePath)
        ticDat3 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities)
        self.assertTrue(ticDat3.categories and ticDat3.foods)

        tdf2 = TicDatFactory(**{k:v for k,v in dietSchema().items() if k == "categories"})
        ticDat2 = tdf2.copy_tic_dat(dietData())
        filePath = makeCleanPath(os.path.join(_scratchDir, "diet_missing.xlsx"))
        tdf2.xls.write_file(ticDat2, filePath)
        ticDat3 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf2._same_data(ticDat2, ticDat3))
        self.assertTrue(all(hasattr(ticDat3, x) for x in tdf.all_tables))
        self.assertFalse(ticDat3.nutritionQuantities or ticDat3.foods)
        self.assertTrue(ticDat3.categories)


    def testNetflow(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["arcs", "nodes"])
        filePath = os.path.join(_scratchDir, "netflow.xls")
        tdf.xls.write_file(ticDat, filePath)
        xlsTicDat = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))
        tdf.xls.write_file(ticDat, filePath+"x")
        self.assertTrue(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x")))
        def changeIt() :
            xlsTicDat.inflow['Pencils', 'Boston']["quantity"] = 12
        self.assertTrue(self.firesException(changeIt))
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))

        xlsTicDat = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))
        self.assertFalse(self.firesException(changeIt))
        self.assertFalse(tdf._same_data(ticDat, xlsTicDat))

        self.assertFalse(tdf.xls.find_duplicates(filePath))

        pkHacked = netflowSchema()
        pkHacked["nodes"][0] = ["nimrod"]
        tdfHacked = TicDatFactory(**pkHacked)
        self.assertTrue(self.firesException(lambda : tdfHacked.xls.write_file(ticDat, filePath)))
        tdfHacked.xls.write_file(ticDat, filePath, allow_overwrite =True)
        self.assertTrue("nodes : name" in self.firesException(lambda  :tdf.xls.create_tic_dat(filePath)))

        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        ticDat.arcs["Detroit", "Boston"] = float("inf")
        ticDat.cost['Pencils', 'Detroit', 'Boston'] = -float("inf")
        tdf.xls.write_file(ticDat, makeCleanPath(filePath))
        xlsTicDat = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))
        tdf.xls.write_file(ticDat, filePath+"x", allow_overwrite=True)
        self.assertTrue(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x")))
        self.assertFalse(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x", treat_inf_as_infinity=False)))

    def testSillyTwoTables(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeDataTwoTables())
        filePath = os.path.join(_scratchDir, "sillyMeTwoTables.xls")
        tdf.xls.write_file(ticDat, filePath)
        xlsTicDat = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, xlsTicDat))

    def testSilly(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**sillyMeSchema())
        ticDat = tdf.TicDat(**sillyMeData())
        schema2 = sillyMeSchema()
        schema2["b"][0] = ("bField2", "bField1", "bField3")
        schema3 = sillyMeSchema()
        schema3["a"][1] = ("aData2", "aData3", "aData1")
        schema4 = sillyMeSchema()
        schema4["a"][1] = ("aData1", "aData3")
        schema5 = sillyMeSchema()
        _tuple = lambda x : tuple(x) if utils.containerish(x) else (x,)
        for t in ("a", "b") :
            schema5[t][1] = _tuple(schema5[t][1]) + _tuple(schema5[t][0])
        schema5["a"][0], schema5["b"][0] =  (),  []
        schema6 = sillyMeSchema()
        schema6["d"] =  [["dField"],()]

        tdf2, tdf3, tdf4, tdf5, tdf6 = (TicDatFactory(**x) for x in (schema2, schema3, schema4, schema5, schema6))
        tdf5.set_generator_tables(("a","c"))
        filePath = os.path.join(_scratchDir, "silly.xls")
        tdf.xls.write_file(ticDat, filePath)

        ticDat2 = tdf2.xls.create_tic_dat(filePath)
        self.assertFalse(tdf._same_data(ticDat, ticDat2))

        ticDat3 = tdf3.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat3))

        ticDat4 = tdf4.xls.create_tic_dat(filePath)
        for t in ["a","b"]:
            for k,v in getattr(ticDat4, t).items() :
                for _k, _v in v.items() :
                    self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                if set(v) == set(getattr(ticDat, t)[k]) :
                    self.assertTrue(t == "b")
                else :
                    self.assertTrue(t == "a")

        ticDat5 = tdf5.xls.create_tic_dat(filePath, treat_inf_as_infinity=False)
        self.assertTrue(tdf5._same_data(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

        ticDat6 = tdf6.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat6))
        self.assertTrue(firesException(lambda : tdf6._same_data(ticDat, ticDat6)))
        self.assertTrue(hasattr(ticDat6, "d") and utils.dictish(ticDat6.d))

        def writeData(data, write_header = "same"):
            assert filePath.endswith(".xls")
            assert not write_header or write_header in ("lower", "same", "duped")
            import xlwt
            book = xlwt.Workbook()
            for t in tdf.all_tables :
                sheet = book.add_sheet(t)
                if write_header:
                    all_fields = tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ())
                    for i,f in enumerate((2 if write_header == "duped" else 1) * all_fields) :
                        sheet.write(0, i, f.lower() if write_header == "lower" or i >= len(all_fields) else f)
                for rowInd, row in enumerate(data) :
                    for fieldInd, cellValue in enumerate((2 if write_header == "duped" else 1) * row):
                        sheet.write(rowInd+ (1 if write_header else 0), fieldInd, cellValue)
            if os.path.exists(filePath):
                os.remove(filePath)
            book.save(filePath)
            if write_header in ["lower", "same"]: # will use pandas to generate the xlsx file version
                file_path_x = filePath + "x"
                if os.path.exists(file_path_x):
                    os.remove(file_path_x)
                writer = utils.pd.ExcelWriter(file_path_x)
                for t, (pks, dfs) in tdf.schema().items():
                    fields = pks+dfs
                    if write_header == "lower":
                        fields = [_.lower() for _ in fields]
                    d = {f:[] for f in fields}
                    for row in data:
                        for f, c in zip(fields, row):
                            d[f].append(c)
                    utils.pd.DataFrame(d).to_excel(writer, t, index=False)
                writer.close()



        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)], write_header="duped")
        self.assertTrue(self.firesException(lambda : tdf.xls.create_tic_dat(filePath, freeze_it=True)))

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)])
        ticDatMan = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[1, 20, 30]["bData"] == 40)
        for f in [filePath, filePath+"x"]:
            rowCount = tdf.xls.find_duplicates(f)
            self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)], write_header="lower")
        ticDatMan = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[1, 20, 30]["bData"] == 40)
        for f in [filePath, filePath+"x"]:
            rowCount = tdf.xls.find_duplicates(f)
            self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)


        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)], write_header=False)
        self.assertTrue(self.firesException(lambda  : tdf.xls.create_tic_dat(filePath, freeze_it=True)))
        ticDatMan = tdf.xls.create_tic_dat(filePath, freeze_it=True, headers_present=False)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[1, 20, 30]["bData"] == 40)
        rowCount = tdf.xls.find_duplicates(filePath, headers_present=False)
        self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)

        ticDat.a["theboger"] = (1, None, 12)
        tdf.xls.write_file(ticDat, filePath, allow_overwrite=True)
        ticDatNone = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        # THIS IS A FLAW - but a minor one. None's are hard to represent. It is turning into the empty string here.
        # not sure how to handle this, but documenting for now.
        self.assertFalse(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == "")
        # the workaround for this flaw is to set the data type to be nullabe but not allow the empty string
        tdfwa = TicDatFactory(**sillyMeSchema())
        tdfwa.set_data_type("a", "aData2", nullable=True)
        ticDatNone = tdfwa.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)

        # checking the same thing with .xlsx - using openpyxl, None is indeed recovered even without tdfwa munging!
        tdf.xls.write_file(ticDat, filePath+"x", allow_overwrite=True)
        ticDatNone = tdf.xls.create_tic_dat(filePath+"x", freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)
        ticDatNone = tdfwa.xls.create_tic_dat(filePath+"x", freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40), (1,20,30,12)])
        for f in [filePath, filePath+"x"]:
            rowCount = tdf.xls.find_duplicates(f)
            self.assertTrue(set(rowCount) == {'a', 'b'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==3)
            self.assertTrue(set(rowCount["b"]) == {(1,20,30)} and rowCount["b"][1,20,30]==2)

    def testSpacey(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        def writeData(insert_spaces):
            import xlwt
            book = xlwt.Workbook()
            for t in tdf.all_tables :
                sheet = book.add_sheet(t.replace("_", " " if insert_spaces else "_"))
                for i,f in enumerate(tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ())) :
                    sheet.write(0, i, f)
                _t = getattr(ticDat, t)
                containerish = utils.containerish
                if utils.dictish(_t) :
                    for row_ind, (p_key, data) in enumerate(_t.items()) :
                        for field_ind, cell in enumerate( (p_key if containerish(p_key) else (p_key,)) +
                                            tuple(data[_f] for _f in tdf.data_fields.get(t, ()))):
                            sheet.write(row_ind+1, field_ind, cell)
                else :
                    for row_ind, data in enumerate(_t if containerish(_t) else _t()) :
                        for field_ind, cell in enumerate(tuple(data[_f] for _f in tdf.data_fields[t])) :
                            sheet.write(row_ind+1, field_ind, cell)
            if os.path.exists(filePath):
                os.remove(filePath)
            book.save(filePath)
        filePath = os.path.join(_scratchDir, "spaces.xls")
        writeData(insert_spaces=False)
        ticDat2 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat2))
        writeData(insert_spaces=True)
        ticDat3 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat3))

    def testSpacey2(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**spacesSchema())
        ticDat = tdf.TicDat(**spacesData())
        for ext in [".xls", ".xlsx"]:
            filePath = os.path.join(_scratchDir, "spaces_2%s" % ext)
            tdf.xls.write_file(ticDat, filePath, case_space_sheet_names=True)
            ticDat2 = tdf.xls.create_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))

        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        for ext in [".xls", ".xlsx"]:
            filePath = os.path.join(_scratchDir, "spaces_2_2%s" % ext)
            tdf.xls.write_file(ticDat, filePath, case_space_sheet_names=True)
            ticDat2 = tdf.xls.create_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))


    def testRowOffsets(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(boger = [[],["the", "big", "boger"]],
                            woger = [[], ["the", "real", "big", "woger"]])
        td = tdf.freeze_me(tdf.TicDat(boger = ([1, 2, 3], [12, 24, 36], tdf.data_fields["boger"], [100, 200, 400]),
                              woger = ([[1, 2, 3, 4]]*4) + [tdf.data_fields["woger"]] +
                                      ([[100, 200, 300, 400]]*5)))
        filePath = os.path.join(_scratchDir, "rowoff.xls")
        tdf.xls.write_file(td, filePath)

        td1= tdf.xls.create_tic_dat(filePath)
        td2 = tdf.xls.create_tic_dat(filePath, {"woger": 5})
        td3 = tdf.xls.create_tic_dat(filePath, {"woger":5, "boger":3})
        self.assertTrue(tdf._same_data(td, td1))
        tdCheck = tdf.TicDat(boger = td2.boger, woger = td.woger)
        self.assertTrue(tdf._same_data(td, tdCheck))
        self.assertTrue(all (td2.woger[i]["big"] == 300 for i in range(5)))
        self.assertTrue(all (td3.woger[i]["real"] == 200 for i in range(5)))
        self.assertTrue(td3.boger[0]["big"] == 200 and len(td3.boger) == 1)

    def testIntHandling(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(boger = [["the"],["big", "boger"]],
                            moger = [["the", "big"], ["boger"]],
                            woger = [[], ["the","big", "boger"]])
        for t in ["boger", "moger", "woger"]:
            tdf.set_data_type(t, "big", must_be_int=True)
        dat  = tdf.TicDat(boger = {1:[1.0, "t"], "b":[12, 11.1], 12.1:[14.0, 15.0]},
                          moger = {(1,1.0):"t", ("b", 12):11.1, (12.1,14.0):15.0},
                          woger = [(1,1.0,"t"),("b",12,11.1),(12.1,14.0,15.0)])
        filePath = os.path.join(_scratchDir, "intHandling.xls")
        tdf.xls.write_file(dat, filePath)
        dat2 = tdf.xls.create_tic_dat(filePath)

        tdf3 = TicDatFactory(boger = [["the"],["big", "boger"]],
                            moger = [["the", "big"], ["boger"]],
                            woger = [[], ["the","big", "boger"]])
        dat3 = tdf3.xls.create_tic_dat(filePath)
        self.assertFalse(any(map(tdf.find_data_type_failures, [dat, dat2, dat3])))
        self.assertTrue(all(tdf._same_data(dat, _) for _ in [dat2, dat3]))

        self.assertFalse(all(isinstance(r["big"], int) for r in list(dat.boger.values()) +
                            list(dat.woger)))
        self.assertTrue(all(isinstance(r["big"], int) for r in list(dat2.boger.values()) +
                            list(dat2.woger)))
        self.assertFalse(any(isinstance(r["big"], int) for r in list(dat3.boger.values()) +
                            list(dat3.woger)))
        self.assertTrue(all(isinstance(_.woger[1]["big"], int) for _ in [dat, dat2]))

        self.assertFalse(all(isinstance(k[-1], int) for k in dat.moger))
        self.assertTrue(any(isinstance(k[-1], int) for k in dat.moger))
        self.assertTrue(all(isinstance(k[-1], int) for k in dat2.moger))
        self.assertFalse(any(isinstance(k[-1], int) for k in dat3.moger))

    def testBiggie(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(boger = [["the"],["big", "boger"]],
                            moger = [["the", "big"], ["boger"]],
                            woger = [[], ["the","big", "boger"]])
        smalldat = tdf.TicDat(boger = {k:[(k+1)%10, (k+2)%5] for k in range(100)},
                              moger = {(k,(k+1)%10): (k+2)%5 for k in range(75)},
                              woger = [[k,(k+1)%10, (k+2)%5] for k in range(101)])
        filePath = os.path.join(_scratchDir, "smallBiggie.xls")
        tdf.xls.write_file(smalldat, filePath)
        smalldat2 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(smalldat, smalldat2))

        filePath = makeCleanPath(filePath + "x")
        tdf.xls.write_file(smalldat, filePath)
        smalldat2 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(smalldat, smalldat2))

        bigdat = tdf.TicDat(boger = {k:[(k+1)%10, (k+2)%5] for k in range(65537)},
                              moger = {(k,(k+1)%10): (k+2)%5 for k in range(75)},
                              woger = [[k,(k+1)%10, (k+2)%5] for k in range(65537)])
        filePath = os.path.join(_scratchDir, "bigBiggie.xls")
        self.assertTrue(firesException(lambda : tdf.xls.write_file(bigdat, filePath)))
        filePath = makeCleanPath(filePath + "x")
        tdf.xls.write_file(bigdat, filePath)
        bigdat2 = tdf.xls.create_tic_dat(filePath)
        # the following is just to GD slow
        #self.assertTrue(tdf._same_data(bigdat, bigdat2))
        self.assertTrue(all(len(getattr(bigdat, t)) == len(getattr(bigdat2, t)) for t in tdf.all_tables))

    def testLongName(self):
        prepend = "b"*20
        tdf = TicDatFactory(**{prepend*2+t:v for t,v in dietSchema().items()})
        self.assertTrue(self.firesException(lambda : tdf.xls._verify_differentiable_sheet_names()))

        tdf = TicDatFactory(**{prepend+t:v for t,v in dietSchema().items()})
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t.replace(prepend, ""))
                                             for t in tdf.primary_key_fields}))
        for ext in [".xls", ".xlsx"]:
            filePath = os.path.join(_scratchDir, f"longname{ext}")
            tdf.xls.write_file(ticDat, filePath)
            self.assertFalse(tdf.xls.find_duplicates(filePath))
            ticDat2 = tdf.xls.create_tic_dat(filePath)
            self.assertTrue(tdf._same_data(ticDat, ticDat2))

    def test_empty_text_none(self):
        # this is a naive data scientist who isn't using the parameters functionality
        filePath = os.path.join(_scratchDir, "empty.xls")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        dat_n = tdf.TicDat(parameters=[[None, 100], ["b", 10.01], ["three", 200], ["d", None]])
        dat_s = tdf.TicDat(parameters=[["", 100], ["b", 10.01], ["three", 200], ["d", ""]])
        def round_trip():
            tdf.xls.write_file(dat_n, filePath, allow_overwrite=True)
            return tdf.xls.create_tic_dat(filePath)
        dat2 = round_trip()
        self.assertTrue(tdf._same_data(dat_s, dat2) and not tdf._same_data(dat_n, dat2))
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.set_data_type("parameters", "Key", nullable=True)
        tdf.set_default_value("parameters", "Value", None) # this default alone will mess with number reading
        dat2 = round_trip()
        self.assertTrue(not tdf._same_data(dat_s, dat2) and tdf._same_data(dat_n, dat2))

        tdf = TicDatFactory(parameters='*')
        dat = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(dat.parameters.shape == (4, 2))

    def test_parameters(self):
        filePath = os.path.join(_scratchDir, "parameters.xlsx")
        tdf = TicDatFactory(parameters=[["Key"], ["Value"]])
        tdf.add_parameter("Something", 100)
        tdf.add_parameter("Different", 'boo', strings_allowed='*', number_allowed=False)
        dat = tdf.TicDat(parameters = [["Something",float("inf")], ["Different", "inf"]])
        tdf.xls.write_file(dat, filePath)
        dat_ = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(dat, dat_))

    def testDietWithInfFlagging(self):
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        tdf.set_infinity_io_flag(999999999)
        file_one = os.path.join(_scratchDir, "dietInfFlag.xls")
        file_two = os.path.join(_scratchDir, "dietInfFlag.xlsx")
        tdf.xls.write_file(dat, file_one)
        tdf.xls.write_file(dat, file_two)
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))
        tdf = tdf.clone()
        dat_1 = tdf.xls.create_tic_dat(file_one)
        self.assertTrue(tdf._same_data(dat, dat_1))
        tdf = TicDatFactory(**dietSchema())
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertFalse(tdf._same_data(dat, dat_1))
        self.assertFalse(tdf._same_data(dat, dat_2))
        self.assertTrue({_.categories["protein"]["maxNutrition"] for _ in [dat_1, dat_2]} == {999999999})
        for _ in [dat_1, dat_2]:
            _.categories["protein"]["maxNutrition"] = float("inf")
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))

    def testNulls(self):
        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, nullable=True)
        dat = tdf.TicDat(table = [[None, 100], [200, 109], [0, 300], [300, None], [400, 0]])
        file_one = os.path.join(_scratchDir, "boolDefaults.xls")
        file_two = os.path.join(_scratchDir, "boolDefaults.xlsx")
        tdf.xls.write_file(dat, file_one)
        tdf.xls.write_file(dat, file_two)
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat, dat_1))
        self.assertTrue(tdf._same_data(dat, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, max=float("inf"), inclusive_max=True)
        tdf.set_infinity_io_flag(None)
        dat_inf = tdf.TicDat(table = [[float("inf"), 100], [200, 109], [0, 300], [300, float("inf")], [400, 0]])
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)

        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))
        tdf.xls.write_file(dat_inf, file_one, allow_overwrite=True)
        tdf.xls.write_file(dat_inf, file_two, allow_overwrite=True)
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))

        tdf = TicDatFactory(table=[["field one"], ["field two"]])
        for f in ["field one", "field two"]:
            tdf.set_data_type("table", f, min=-float("inf"), inclusive_min=True)
        tdf.set_infinity_io_flag(None)
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertFalse(tdf._same_data(dat_inf, dat_1))
        self.assertFalse(tdf._same_data(dat_inf, dat_2))
        dat_inf = tdf.TicDat(table = [[float("-inf"), 100], [200, 109], [0, 300], [300, -float("inf")], [400, 0]])
        self.assertTrue(tdf._same_data(dat_inf, dat_1))
        self.assertTrue(tdf._same_data(dat_inf, dat_2))

    def testDateTime(self):
        tdf = TicDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        tdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        tdf.add_parameter("p2", None, datetime=True, nullable=True)
        tdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        tdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)

        dat = tdf.TicDat(table_with_stuffs = [["July 11 1972", None],
                                              [datetime.datetime.now(), dateutil.parser.parse("Sept 11 2011")]],
                         parameters = [["p1", "7/11/1911"], ["p2", None]])
        self.assertFalse(tdf.find_data_type_failures(dat) or tdf.find_data_row_failures(dat))

        file_one = os.path.join(_scratchDir, "datetime.xls")
        file_two = os.path.join(_scratchDir, "datetime.xlsx")
        tdf.xls.write_file(dat, file_one)
        tdf.xls.write_file(dat, file_two)
        dat_1 = tdf.xls.create_tic_dat(file_one)
        dat_2 = tdf.xls.create_tic_dat(file_two)
        self.assertTrue(tdf._same_data(dat_1, dat_2, nans_are_same_for_data_rows=True))
        self.assertFalse(tdf.find_data_type_failures(dat_1) or tdf.find_data_row_failures(dat_1))
        self.assertFalse(tdf._same_data(dat, dat_1, nans_are_same_for_data_rows=True))
        self.assertTrue(isinstance(dat_1.parameters["p1"]["b"], datetime.datetime))
        self.assertTrue(all(isinstance(_, datetime.datetime) for _ in dat_1.table_with_stuffs))
        self.assertTrue(all(isinstance(_, datetime.datetime) or _ is None for v in dat_1.table_with_stuffs.values()
                            for _ in v.values()))

    def testDateTimeTwo(self): # this is good test for datetime stuff
        file = os.path.join(_scratchDir, "datetime_pd.xlsx")
        df = utils.pd.DataFrame({"a":list(map(utils.pd.Timestamp,
            ["June 13 1960 4:30PM", "Dec 11 1970 1AM", "Sept 11 2001 9:30AM"]))})
        tdf = TicDatFactory(cool_runnings = [["a"],[]])
        tdf.set_data_type("cool_runnings", "a", datetime=True)
        df.to_excel(file, "Cool Runnings")
        dat = tdf.xls.create_tic_dat(file)
        self.assertTrue(set(dat.cool_runnings) == set(df["a"]))
        for x, y in zip(sorted(dat.cool_runnings), sorted(set(df["a"]))):
            delta = x-y
            self.assertTrue(abs(delta.total_seconds()) < 1e-4)

    def testIssue45(self):
        tdf = TicDatFactory(data=[["a"], ["b"]])
        dat_nums = tdf.TicDat(data = [[1,2],[3,4], [22, 44]])
        dat_strs = tdf.TicDat(data = [["1","2"],["3","4"], ["022", "0044"]])
        files = [os.path.join(_scratchDir, _) for _ in ["dat_nums.xlsx", "dat_strs.xlsx"]]
        tdf.xls.write_file(dat_nums, files[0])
        tdf.xls.write_file(dat_strs, files[1])
        dat_nums_2, dat_strs_2 = [tdf.xls.create_tic_dat(_) for _ in files]
        self.assertTrue(tdf._same_data(dat_nums, dat_nums_2))
        self.assertTrue(tdf._same_data(dat_strs, dat_strs_2))
        # my hands are sort of tied here, xlrd has a disturbing tendency to read data as floats when it reads numbers
        # that said, not sure I really need to do more, since these two tests pass.

    def testColumnsWithoutData(self):
        tdf = TicDatFactory(data=[["a"], ["b"]])
        for x in ["", "x"]:
            file = os.path.join(_scratchDir, "no_data.xls" + x)
            tdf.xls.write_file(tdf.TicDat(), file)
            dat = tdf.xls.create_tic_dat(file)
            self.assertFalse(dat._len_dict())

    def testEmptyWorkbooks(self):
        file = os.path.join(_scratchDir, "empty_wb.xlsx")
        book = ticdat_xlsx.xlsx.Workbook(file)
        book.add_worksheet("data")
        book.close()
        tdf = TicDatFactory(data=[["a"], ["b"]])
        ex = []
        try:
            tdf.xls.create_tic_dat(file)
        except utils.TicDatError as e:
            ex.append(e)
        self.assertTrue(ex)
        self.assertTrue("The following field names could not be found :" in str(ex[0]))

    def testEndingAllNones(self): # unfortunately, not really programmatically testing set_xlsx_trailing_empty_rows
        # due to underlying library weirdness. I did some manually testing originally. Hopefully not going to bit
        # on this one, its not mission critical functionality anyway.
        file_path = os.path.join(_scratchDir, "ending_all_nones.xlsx")
        tdf = TicDatFactory(data=[[],["a", "b"]])
        dat_write = tdf.TicDat(data=[["x", "v"], ["y", "k"], ["xx", None], [None, None], [None, None]])
        tdf.xls.write_file(dat_write, file_path)
        dat = tdf.TicDat(data=[["x", "v"], ["y", "k"], ["xx", None]])
        dat_2 = tdf.xls.create_tic_dat(file_path)
        self.assertTrue(tdf._same_data(dat, dat_2))
        tdf.set_xlsx_trailing_empty_rows("ignore")
        dat_2 = tdf.xls.create_tic_dat(file_path)
        # self.assertTrue(tdf._same_data(dat_2, dat_write)) # this is how it ought to work!
        self.assertTrue(tdf._same_data(dat_2, dat))  # this is whats happening!
        # to be more specific, here is the inner deal
        book = ticdat_xlsx.openpyxl.load_workbook(file_path, data_only=True)
        sheet = book["data"]
        self.assertTrue(sheet.max_row == 4) # 1 based indexing, the column row, three data rows (BUT NOT 5 DATA ROWS)



_scratchDir = TestXls.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING XLS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not ticdat_xlsx._can_unit_test :
        print("!!!!!!!!!FAILING XLS UNIT TESTS DUE TO FAILURE TO LOAD XLS LIBRARIES!!!!!!!!")
    else:
        TestXls.can_run = True
    unittest.main()
