import os

import ticdat.utils as utils
import shutil
from ticdat.ticdatfactory import TicDatFactory
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, dietSchemaWeirdCase
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, copyDataDietWeirdCase
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger
from ticdat.testing.ticdattestutils import makeCleanDir, dietSchemaWeirdCase2, copyDataDietWeirdCase2
import unittest
from ticdat.csvtd import _can_unit_test

#@fail_to_debugger
class TestCsv(unittest.TestCase):
    can_run = False

    @classmethod
    def setUpClass(cls):
        makeCleanDir(_scratchDir)
    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(_scratchDir)
    def firesException(self, f, troubleshoot=False):
        if troubleshoot:
            import ipdb
            ipdb.set_trace()
            f()
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)
    def _test_generic_copy(self, ticDat, tdf, skip_tables=None):
        assert all(tdf.primary_key_fields.get(t) for t in tdf.all_tables)
        path = makeCleanDir(os.path.join(_scratchDir, "generic_copy"))
        replace_name  = lambda f : "name_" if f == "name" else f
        clean_tdf = TicDatFactory(**{t:[list(map(replace_name, pks)), dfs]
                                     for t,(pks, dfs) in tdf.schema().items()})

        temp_tdf = TicDatFactory(**{t:v if t in (skip_tables or []) else '*'
                                    for t,v in clean_tdf.schema().items()})
        temp_dat = temp_tdf.TicDat(**{t:getattr(ticDat, t) for t in (skip_tables or [])})
        for t in temp_tdf.generic_tables:
            setattr(temp_dat, t, getattr(clean_tdf.copy_to_pandas(ticDat, drop_pk_columns=False) ,t))

        temp_tdf.csv.write_directory(temp_dat, path)
        self.assertFalse(temp_tdf.csv.find_duplicates(path))
        read_dat = temp_tdf.csv.create_tic_dat(path)
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
        dirPath = os.path.join(_scratchDir, "diet")
        tdf.csv.write_directory(ticDat,dirPath)
        self.assertFalse(tdf.csv.find_duplicates(dirPath))
        csvTicDat = tdf.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))
        def change() :
            csvTicDat.categories["calories"]["minNutrition"]=12
        self.assertFalse(firesException(change))
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))

        self.assertTrue(self.firesException(lambda  :
            tdf.csv.write_directory(ticDat, dirPath, dialect="excel_t")
                                    ).endswith("Invalid dialect excel_t"))

        tdf.csv.write_directory(ticDat, dirPath, dialect="excel-tab", allow_overwrite=True)
        self.assertTrue(self.firesException(lambda : tdf.csv.create_tic_dat(dirPath, freeze_it=True)))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True, dialect="excel-tab")
        self.assertTrue(firesException(change))
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        tdf2 = TicDatFactory(**dietSchemaWeirdCase())
        dat2 = copyDataDietWeirdCase(ticDat)
        tdf2.csv.write_directory(dat2, dirPath, allow_overwrite=True)
        csvTicDat2 = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat2))
        os.rename(os.path.join(dirPath, "nutritionquantities.csv"),
                  os.path.join(dirPath, "nutritionquantities.csv".upper()))
        csvTicDat2 = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat2))

        tdf3 = TicDatFactory(**dietSchemaWeirdCase2())
        dat3 = copyDataDietWeirdCase2(ticDat)
        tdf3.csv.write_directory(dat3, dirPath, allow_overwrite=True)
        os.rename(os.path.join(dirPath, "nutrition_quantities.csv"),
                  os.path.join(dirPath, "nutrition quantities.csv"))
        csvDat3 = tdf3.csv.create_tic_dat(dirPath)
        self.assertTrue(tdf3._same_data(dat3, csvDat3))
        shutil.copy(os.path.join(dirPath, "nutrition quantities.csv"),
                    os.path.join(dirPath, "nutrition_quantities.csv"))
        self.assertTrue(self.firesException(lambda : tdf3.csv.create_tic_dat(dirPath)))


    def testNetflow(self):
        if not self.can_run:
            return
        tdf = TicDatFactory(**netflowSchema())
        ticDat = tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields})
        self._test_generic_copy(ticDat, tdf)
        self._test_generic_copy(ticDat, tdf, ["arcs", "nodes"])
        dirPath = os.path.join(_scratchDir, "netflow")
        tdf.csv.write_directory(ticDat, dirPath)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertFalse(tdf.csv.find_duplicates(dirPath))
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it= True, headers_present=False)
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))
        tdf.csv.write_directory(ticDat, dirPath, write_header=False,allow_overwrite=True)
        self.assertTrue(self.firesException(lambda : tdf.csv.create_tic_dat(dirPath, freeze_it=True)))
        csvTicDat = tdf.csv.create_tic_dat(dirPath, headers_present=False, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        ticDat.nodes[12] = {}
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertTrue(tdf._same_data(ticDat, csvTicDat))

        # minor flaw - strings that are floatable get turned into floats when reading csvs
        del(ticDat.nodes[12])
        ticDat.nodes['12'] = {}
        self.assertTrue(firesException(lambda : tdf.csv.write_directory(ticDat, dirPath)))
        tdf.csv.write_directory(ticDat, dirPath, allow_overwrite=True)
        csvTicDat = tdf.csv.create_tic_dat(dirPath, freeze_it=True)
        self.assertFalse(tdf._same_data(ticDat, csvTicDat))

    def testSilly(self):
        if not self.can_run:
            return
        def doTest(headersPresent) :
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
            schema5["a"][0], schema5["b"][0] = (), []
            schema5b = sillyMeSchema()
            for t in ("a", "b") :
                schema5b[t][1] = _tuple(schema5b[t][0]) + _tuple(schema5b[t][1])
            schema5b["a"][0], schema5b["b"][0] = (), []
            schema6 = sillyMeSchema()
            schema6["d"] = [("dField",),[]]

            tdf2, tdf3, tdf4, tdf5, tdf5b, tdf6 = (TicDatFactory(**x) for x in
                            (schema2, schema3, schema4, schema5, schema5b, schema6))
            tdf5.set_generator_tables(["a", "c"])
            tdf5b.set_generator_tables(("a", "c"))


            dirPath = makeCleanDir(os.path.join(_scratchDir, "silly"))
            tdf.csv.write_directory(ticDat, dirPath, write_header=headersPresent)

            ticDat2 = tdf2.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertFalse if headersPresent else self.assertTrue)(tdf._same_data(ticDat, ticDat2))

            ticDat3 = tdf3.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(tdf._same_data(ticDat, ticDat3))

            if headersPresent :
                ticDat4 = tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)
                for t in ("a", "b") :
                    for k,v in getattr(ticDat4, t).items() :
                        for _k, _v in v.items() :
                            self.assertTrue(getattr(ticDat, t)[k][_k] == _v)
                        if set(v) == set(getattr(ticDat, t)[k]) :
                            self.assertTrue(t == "b")
                        else :
                            self.assertTrue(t == "a")
            else :
                self.assertTrue(self.firesException(lambda :
                                    tdf4.csv.create_tic_dat(dirPath, headers_present=headersPresent)))

            ticDat5 = tdf5.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            (self.assertTrue if headersPresent else self.assertFalse)(
                                                    tdf5._same_data(tdf._keyless(ticDat), ticDat5))
            self.assertTrue(callable(ticDat5.a) and callable(ticDat5.c) and not callable(ticDat5.b))

            ticDat5b = tdf5b.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf5b._same_data(tdf._keyless(ticDat), ticDat5b))
            self.assertTrue(callable(ticDat5b.a) and callable(ticDat5b.c) and not callable(ticDat5b.b))


            ticDat6 = tdf6.csv.create_tic_dat(dirPath, headers_present=headersPresent)
            self.assertTrue(tdf._same_data(ticDat, ticDat6))
            self.assertTrue(firesException(lambda : tdf6._same_data(ticDat, ticDat6)))
            self.assertTrue(hasattr(ticDat6, "d") and utils.dictish(ticDat6.d))
            allDataTdf = TicDatFactory(**{t:[[], tdf.primary_key_fields.get(t, ()) + tdf.data_fields.get(t, ())]
                             for t in tdf.all_tables})

            def writeData(data):
                td = allDataTdf.TicDat(a = data, b=data, c=data)
                allDataTdf.csv.write_directory(td, dirPath, allow_overwrite=True, write_header=headersPresent)

            writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)])
            ticDatMan = tdf.csv.create_tic_dat(dirPath, headers_present=headersPresent, freeze_it=True)
            self.assertTrue(len(ticDatMan.a) == 2 and len(ticDatMan.b) == 3)
            self.assertTrue(ticDatMan.b[(1, 20, 30)]["bData"] == 40)
            rowCount = tdf.csv.find_duplicates(dirPath, headers_present= headersPresent)
            self.assertTrue(set(rowCount) == {'a'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==2)


            writeData([(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40), (1,20,30,12)])
            rowCount = tdf.csv.find_duplicates(dirPath, headers_present=headersPresent)
            self.assertTrue(set(rowCount) == {'a', 'b'} and set(rowCount["a"]) == {1} and rowCount["a"][1]==3)
            self.assertTrue(set(rowCount["b"]) == {(1,20,30)} and rowCount["b"][1,20,30]==2)



        utils.do_it(doTest(x) for x in (True, False))

_scratchDir = TestCsv.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    td = TicDatFactory()
    if not utils.DataFrame :
        print("!!!!!!!!!FAILING CSV UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    elif not _can_unit_test :
        print("!!!!!!!!!FAILING CSV UNIT TESTS DUE TO FAILURE TO LOAD CSV LIBRARIES!!!!!!!!")
    else:
        TestCsv.can_run = True

    unittest.main()

