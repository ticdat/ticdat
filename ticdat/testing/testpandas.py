import os
import ticdat.utils as utils
import sys
from ticdat.ticdatfactory import TicDatFactory, DataFrame
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData
from ticdat.testing.ticdattestutils import  netflowSchema, firesException, spacesData, spacesSchema
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, fail_to_debugger, flagged_as_run_alone
from ticdat.testing.ticdattestutils import  makeCleanDir, addNetflowForeignKeys, clean_denormalization_errors
import unittest


#@fail_to_debugger
class TestPandas(unittest.TestCase):
    canRun = False

    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def _test_generic_free_copy(self, ticDat, tdf, skip_tables=None):
        assert all(tdf.primary_key_fields.get(t) for t in tdf.all_tables)
        replace_name  = lambda f : "name_" if f == "name" else f
        clean_tdf = TicDatFactory(**{t:[list(map(replace_name, pks)), dfs] for t,(pks, dfs) in tdf.schema().items()})

        temp_tdf = TicDatFactory(**{t:v if t in (skip_tables or []) else '*' for t,v in clean_tdf.schema().items()})
        temp_dat = temp_tdf.TicDat(**{t:getattr(ticDat, t) for t in (skip_tables or [])})
        for t in temp_tdf.generic_tables:
            setattr(temp_dat, t, getattr(clean_tdf.copy_to_pandas(ticDat, drop_pk_columns=False) ,t))
        generic_free_dat, _ = utils.create_generic_free(temp_dat, temp_tdf)
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

    def testDenormalizedErrors(self):
        if not self.canRun:
            return
        c = clean_denormalization_errors
        f = utils.find_denormalized_sub_table_failures
        tdf = TicDatFactory(**spacesSchema())
        dat = tdf.TicDat(**spacesData())
        p = lambda :tdf.copy_to_pandas(dat, drop_pk_columns=False).b_table
        self.assertFalse(f(p(),"b Field 1",("b Field 2", "b Field 3")))
        dat.b_table[2,2,3] = "boger"
        self.assertFalse(f(p(), "b Field 1",("b Field 2", "b Field 3")))
        chk = f(p(), "b Field 2",("b Field 1", "b Field 3"))
        self.assertTrue(c(chk) == {2: {'b Field 1': {1, 2}}})
        dat.b_table[2,2,4] = "boger"
        dat.b_table[1,'b','b'] = "boger"
        chk = f(p(), ["b Field 2"],("b Field 1", "b Field 3", "b Data"))
        self.assertTrue(c(chk) == c({2: {'b Field 3': (3, 4), 'b Data': (1, 'boger'), 'b Field 1': (1, 2)},
                                 'b': {'b Data': ('boger', 12), 'b Field 1': ('a', 1)}}))

        ex = self.firesException(lambda : f(p(), ["b Data"],"wtf"))
        self.assertTrue("wtf isn't a column" in ex)


        p = lambda :tdf.copy_to_pandas(dat, drop_pk_columns=False).c_table
        chk = f(p(), pk_fields=["c Data 1", "c Data 2"], data_fields=["c Data 3", "c Data 4"])
        self.assertTrue(c(chk) == {('a', 'b'): {'c Data 3': {'c', 12}, 'c Data 4': {24, 'd'}}})
        dat.c_table.append((1, 2, 3, 4))
        dat.c_table.append((1, 2, 1, 4))
        dat.c_table.append((1, 2, 1, 5))
        dat.c_table.append((1, 2, 3, 6))
        chk = f(p(), pk_fields=["c Data 1", "c Data 2"], data_fields=["c Data 3", "c Data 4"])
        self.assertTrue(c(chk) == {('a', 'b'): {'c Data 3': {'c', 12}, 'c Data 4': {24, 'd'}},
                                   (1,2):{'c Data 3':{3,1}, 'c Data 4':{4,5,6}}})

    def testDiet(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dietSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_free_copy(oldDat, tdf)
        self._test_generic_free_copy(oldDat, tdf, ["nutritionQuantities"])
        ticDat = tdf.copy_to_pandas(oldDat)
        for k in oldDat.foods:
            self.assertTrue(oldDat.foods[k]["cost"] == ticDat.foods.cost[k])
        for k in oldDat.categories:
            self.assertTrue(oldDat.categories[k]["minNutrition"] == ticDat.categories.minNutrition[k])
        for k1, k2 in oldDat.nutritionQuantities:
            self.assertTrue(oldDat.nutritionQuantities[k1,k2]["qty"] ==
                            ticDat.nutritionQuantities.qty[k1,k2])
        nut = ticDat.nutritionQuantities
        self.assertTrue(firesException(lambda : nut.qty.loc[:, "fatty"]))
        self.assertTrue(firesException(lambda : nut.qty.loc["chickeny", :]))
        self.assertFalse(firesException(lambda : nut.qty.sloc[:, "fatty"]))
        self.assertFalse(firesException(lambda : nut.qty.sloc["chickeny", :]))
        self.assertTrue(0 == sum(nut.qty.sloc[:, "fatty"]) == sum(nut.qty.sloc["chickeny", :]))
        self.assertTrue(sum(nut.qty.sloc[:, "fat"]) == sum(nut.qty.loc[:, "fat"]) ==
                        sum(r["qty"] for (f,c),r in oldDat.nutritionQuantities.items() if c == "fat"))
        self.assertTrue(sum(nut.qty.sloc["chicken",:]) == sum(nut.qty.loc["chicken",:]) ==
                        sum(r["qty"] for (f,c),r in oldDat.nutritionQuantities.items() if f == "chicken"))

        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

        tdf2 = TicDatFactory(**{t:'*' for t in tdf.all_tables})
        self.assertTrue(firesException(lambda : tdf2.set_data_type("nutritionQuantities", "qty")))
        genTicDat = tdf2.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})

        for k in oldDat.categories:
            self.assertTrue(oldDat.categories[k]["minNutrition"] == genTicDat.categories.minNutrition[k])
        for k1, k2 in oldDat.nutritionQuantities:
            self.assertTrue(oldDat.nutritionQuantities[k1,k2]["qty"] ==
                            genTicDat.nutritionQuantities.qty[k1,k2])
        self.assertFalse(tdf.good_tic_dat_object(genTicDat))
        self.assertTrue(tdf2.good_tic_dat_object(genTicDat))
        rebornTicDat = tdf.TicDat(**{t:getattr(genTicDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))
        rebornGenTicDat = tdf2.TicDat(**tdf2.as_dict(genTicDat))
        for t, pks in tdf.primary_key_fields.items():
            getattr(rebornGenTicDat, t).index.names = pks
        rebornTicDat = tdf.TicDat(**{t:getattr(rebornGenTicDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

        tdf3 = TicDatFactory(**dict(dietSchema(), **{"categories":'*'}))
        self.assertFalse(firesException(lambda : tdf3.set_data_type("nutritionQuantities", "qty")))
        mixTicDat = tdf3.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        for k in oldDat.categories:
            self.assertTrue(oldDat.categories[k]["minNutrition"] == mixTicDat.categories.minNutrition[k])
        for k1, k2 in oldDat.nutritionQuantities:
            self.assertTrue(oldDat.nutritionQuantities[k1,k2]["qty"] ==
                            mixTicDat.nutritionQuantities[k1,k2]["qty"])
        self.assertFalse(tdf2.good_tic_dat_object(mixTicDat))
        self.assertFalse(tdf3.good_tic_dat_object(genTicDat))
        self.assertTrue(tdf3.good_tic_dat_object(mixTicDat))
        rebornTicDat = tdf.TicDat(**{t:getattr(mixTicDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

    def testNetflow(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        addNetflowForeignKeys(tdf)
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        self._test_generic_free_copy(oldDat, tdf)
        self._test_generic_free_copy(oldDat, tdf, ["arcs", "nodes"])
        ticDat = tdf.copy_to_pandas(oldDat, ["arcs", "cost"])
        self.assertTrue(all(hasattr(ticDat, t) == (t in ["arcs", "cost"]) for t in tdf.all_tables))
        self.assertTrue(len(ticDat.arcs.capacity.sloc["Boston",:]) == len(oldDat.nodes["Boston"].arcs_source) == 0)
        self.assertTrue(len(ticDat.arcs.capacity.sloc[:,"Boston"]) == len(oldDat.nodes["Boston"].arcs_destination) == 2)
        self.assertTrue(all(ticDat.arcs.capacity.sloc[:,"Boston"][src] == r["capacity"]
                            for src, r in oldDat.nodes["Boston"].arcs_destination.items()))
        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=True)
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        # because we have single pk field tables, dropping the pk columns is probelmatic
        self.assertFalse(tdf._same_data(rebornTicDat, oldDat))

        # but with the default argument all is well
        ticDat = tdf.copy_to_pandas(oldDat)
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))
        self.assertTrue(set(ticDat.inflow.columns) == {"quantity"})
        self.assertTrue(set(ticDat.nodes.columns) == {"name"})



    def testSilly(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dict({"d" : [("dData1", "dData2", "dData3", "dData4"),[]],
                                    "e" : [["eData"],[]]}, **sillyMeSchema()))
        ticDat = tdf.copy_to_pandas(tdf.TicDat(**sillyMeData()))
        self.assertFalse(len(ticDat.d) + len(ticDat.e))
        oldDat = tdf.freeze_me(tdf.TicDat(**dict({"d" : {(1,2,3,4):{}, (1, "b","c","d"):{}, ("a", 2,"c","d"):{}},
                                                  "e" : {11:{},"boger":{}}},
                                **sillyMeData())))
        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=True)
        def checkTicDat():
            self.assertTrue(len(ticDat.d) ==3 and len(ticDat.e) == 2)
            self.assertTrue(set(ticDat.d.index.values) == {(1,2,3,4), (1, "b","c","d"), ("a", 2,"c","d")})
            self.assertTrue(set(ticDat.e.index.values) == {11,"boger"})
            self.assertTrue(len(ticDat.c) == len(oldDat.c) == 3)
            self.assertTrue(ticDat.c.loc[i] == oldDat.c[i] for i in range(3))
        checkTicDat()
        self.assertFalse(hasattr(ticDat.d, "dData1") or hasattr(ticDat.e, "eData"))

        ticDat = tdf.copy_to_pandas(oldDat, drop_pk_columns=False)
        checkTicDat()
        self.assertTrue(ticDat.e.loc[11].values[0] == 11)
        if sys.version_info[0] == 2:
            self.assertTrue(len(ticDat.d.dData1.sloc[1,:,:,:]) == 2)
        else : # very strange infrequent bug issue that I will investigate later
            self.assertTrue(len(ticDat.d.dData1.sloc[1]) == 2)

        ticDat = tdf.copy_to_pandas(oldDat)
        checkTicDat()
        if sys.version_info[0] == 2:
            self.assertTrue(len(ticDat.d.dData1.sloc[1,:,:,:]) == 2)
        else:
            self.assertTrue(len(ticDat.d.dData1.sloc[1]) == 2)
        self.assertTrue(ticDat.e.loc[11].values[0] == 11)
        self.assertTrue(set(ticDat.d.columns) == {"dData%s"%s for s in range(5)[1:]})

        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

        ticDat.b = ticDat.b.bData
        rebornTicDat = tdf.TicDat(**{t:getattr(ticDat, t) for t in tdf.all_tables})
        self.assertTrue(tdf._same_data(rebornTicDat, oldDat))

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING PANDAS UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestPandas.canRun = True
    unittest.main()

