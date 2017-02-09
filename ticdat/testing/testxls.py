import os
import ticdat.utils as utils
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger
from ticdat.testing.ticdattestutils import spacesData, spacesSchema, memo, flagged_as_run_alone
from ticdat.testing.ticdattestutils import makeCleanPath
from ticdat.xls import _can_unit_test
import shutil
import unittest

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
        path = os.path.join(makeCleanDir(os.path.join(_scratchDir, "generic_copy")), "file.xls")
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
        self.assertFalse(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x")))
        self.assertTrue(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x", treat_large_as_inf=True)))
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
        self.assertFalse(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x")))
        self.assertTrue(tdf._same_data(ticDat, tdf.xls.create_tic_dat(filePath+"x", treat_large_as_inf=True)))

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

        ticDat5 = tdf5.xls.create_tic_dat(filePath)
        self.assertTrue(tdf5._same_data(tdf._keyless(ticDat), ticDat5))
        self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

        ticDat6 = tdf6.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat6))
        self.assertTrue(firesException(lambda : tdf6._same_data(ticDat, ticDat6)))
        self.assertTrue(hasattr(ticDat6, "d") and utils.dictish(ticDat6.d))

        def writeData(data, write_header = "same"):
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

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)], write_header="duped")
        self.assertTrue(self.firesException(lambda : tdf.xls.create_tic_dat(filePath, freeze_it=True)))

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)])
        ticDatMan = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[1, 20, 30]["bData"] == 40)
        rowCount = tdf.xls.find_duplicates(filePath)
        self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)], write_header="lower")
        ticDatMan = tdf.xls.create_tic_dat(filePath, freeze_it=True)
        self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
        self.assertTrue(ticDatMan.b[1, 20, 30]["bData"] == 40)
        rowCount = tdf.xls.find_duplicates(filePath)
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

        # checking the same thing with .xlsx
        tdf.xls.write_file(ticDat, filePath+"x", allow_overwrite=True)
        ticDatNone = tdf.xls.create_tic_dat(filePath+"x", freeze_it=True)
        self.assertFalse(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == "")
        ticDatNone = tdfwa.xls.create_tic_dat(filePath+"x", freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, ticDatNone))
        self.assertTrue(ticDatNone.a["theboger"]["aData2"] == None)

        writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40), (1,20,30,12)])
        rowCount = tdf.xls.find_duplicates(filePath)
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
        filePath = os.path.join(_scratchDir, "longname.xls")
        tdf.xls.write_file(ticDat, filePath)
        self.assertFalse(tdf.xls.find_duplicates(filePath))
        ticDat2 = tdf.xls.create_tic_dat(filePath)
        self.assertTrue(tdf._same_data(ticDat, ticDat2))

_scratchDir = TestXls.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING XLS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not _can_unit_test :
        print("!!!!!!!!!FAILING XLS UNIT TESTS DUE TO FAILURE TO LOAD XLS LIBRARIES!!!!!!!!")
    else:
        TestXls.can_run = True
    unittest.main()
