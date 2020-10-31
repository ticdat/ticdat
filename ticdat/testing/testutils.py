import sys
import unittest
import ticdat.utils as utils
from ticdat import LogFile, Progress, PanDatFactory
from ticdat.ticdatfactory import TicDatFactory, ForeignKey, ForeignKeyMapping
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException, memo
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger, flagged_as_run_alone
from ticdat.testing.ticdattestutils import assertTicDatTablesSame, DEBUG, addNetflowForeignKeys, addDietForeignKeys
from ticdat.testing.ticdattestutils import spacesSchema, spacesData, clean_denormalization_errors, get_testing_file_path
import os
import itertools
import shutil
import json
try:
    import dateutil
except:
    dateutil = None
import datetime
from unittest.mock import patch

try:
    import testing.postgresql as testing_postgresql
except:
    testing_postgresql = None
try:
    import sqlalchemy as sa
except:
    sa = None

try:
    import pandas as pd
except:
    pd = None

def _deep_anonymize(x)  :
    if not hasattr(x, "__contains__") or utils.stringish(x):
        return x
    if utils.dictish(x) :
        return {_deep_anonymize(k):_deep_anonymize(v) for k,v in x.items()}
    return list(map(_deep_anonymize,x))

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestUtils(unittest.TestCase):
    def _testTdfReproduction(self, tdf):
        def _tdfs_same(tdf, tdf2):
            self.assertTrue(tdf.schema() == tdf2.schema())
            self.assertTrue(tdf.data_types == tdf2.data_types)
            self.assertTrue(tdf.default_values == tdf2.default_values)
            self.assertTrue(set(tdf.foreign_keys) == set(tdf2.foreign_keys))
        _tdfs_same(tdf, TicDatFactory.create_from_full_schema(tdf.schema(True)))
        _tdfs_same(tdf, TicDatFactory.create_from_full_schema(_deep_anonymize(tdf.schema(True))))

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

    def testXToMany(self):
        input_schema = TicDatFactory (roster = [["Name"],["Grade", "Arrival Inning", "Departure Inning",
                                                          "Min Innings Played", "Max Innings Played"]],
                                      positions = [["Position"],["Position Importance", "Position Group",
                                                                 "Consecutive Innings Only"]],
                                      innings = [["Inning"],["Inning Group"]],
                                      position_constraints = [["Position Group", "Inning Group", "Grade"],
                                                              ["Min Players", "Max Players"]])
        input_schema.add_foreign_key("position_constraints", "roster", ["Grade", "Grade"])
        input_schema.add_foreign_key("position_constraints", "positions", ["Position Group", "Position Group"])
        input_schema.add_foreign_key("position_constraints", "innings", ["Inning Group", "Inning Group"])

        self.assertTrue({fk.cardinality for fk in input_schema.foreign_keys} == {"many-to-many"})

        dat = input_schema.TicDat()
        for i,p in enumerate(["bob", "joe", "fred", "alice", "lisa", "joean", "ginny"]):
            dat.roster[p]["Grade"] = (i%3)+1
        dat.roster["dummy"]["Grade"]  = "whatevers"
        for i,p in enumerate(["pitcher", "catcher", "1b", "2b", "ss", "3b", "lf", "cf", "rf"]):
            dat.positions[p]["Position Group"] = "PG %s"%((i%4)+1)
        for i in range(1, 10):
            dat.innings[i]["Inning Group"] = "before stretch" if i < 7 else "after stretch"
        dat.innings[0] ={}
        for pg, ig, g in itertools.product(["PG %s"%i for i in range(1,5)], ["before stretch", "after stretch"],
                                           [1, 2, 3]):
            dat.position_constraints[pg, ig, g] = {}

        orig_dat = input_schema.copy_tic_dat(dat, freeze_it=True)
        self.assertFalse(input_schema.find_foreign_key_failures(orig_dat))

        dat.position_constraints["no", "no", "no"] = dat.position_constraints[1, 2, 3] = {}
        fk_fails = input_schema.find_foreign_key_failures(input_schema.copy_tic_dat(dat, freeze_it=True))
        fk_fails_2 = input_schema.find_foreign_key_failures(dat, verbosity="Low")

        self.assertTrue({(1, 'no'), (2, 'no'), ('no', )} ==
                        {tuple(sorted(_.native_values, key=str)) for _ in fk_fails.values()} ==
                        {tuple(sorted(_[0], key=str)) for _ in fk_fails_2.values()})
        input_schema.remove_foreign_key_failures(dat)
        self.assertTrue(input_schema._same_data(dat, orig_dat) and not input_schema.find_foreign_key_failures(dat))

        input_schema = TicDatFactory(table_one = [["One", "Two"], []],
                                     table_two = [["One"], ["Two"]])
        input_schema.add_foreign_key("table_two", "table_one", ["One", "One"])
        self.assertTrue({fk.cardinality for fk in input_schema.foreign_keys} == {"one-to-many"})


        dat = input_schema.TicDat(table_one = [[1,2], [3,4], [5,6], [7,8]], table_two = {1:2, 3:4, 5:6})
        ex = self.firesException(lambda : input_schema.obfusimplify(dat))
        self.assertTrue("complex foreign key" in str(ex))
        orig_dat = input_schema.copy_tic_dat(dat, freeze_it=True)
        self.assertFalse(input_schema.find_foreign_key_failures(orig_dat))
        dat.table_two[9]=10
        self.assertTrue(input_schema.find_foreign_key_failures(input_schema.copy_tic_dat(dat, freeze_it=True)))
        input_schema.remove_foreign_key_failures(dat)
        self.assertTrue(input_schema._same_data(dat, orig_dat) and not input_schema.find_foreign_key_failures(dat))


    def testXToManyTwo(self):
        input_schema = TicDatFactory (parent = [["F1", "F2"],["F3"]], child_one = [["F1", "F2", "F3"], []],
                                      child_two = [["F1", "F2"], ["F3"]], child_three = [[],["F1", "F2", "F3"]])
        for t in ["child_one", "child_two", "child_three"]:
            input_schema.add_foreign_key(t, "parent", [["F1"]*2, ["F2"]*2, ["F3"]*2])
        self.assertTrue({fk.cardinality for fk in input_schema.foreign_keys} == {"one-to-one", "many-to-one"})

        rows =[[1,2,3], [1,2.1,3], [4,5,6],[4,5.1,6],[7,8,9]]
        dat = input_schema.TicDat(parent = rows, child_one = rows, child_two = rows, child_three=rows)
        self.assertTrue(all(len(getattr(dat, t)) == 5 for t in input_schema.all_tables))
        orig_dat = input_schema.copy_tic_dat(dat, True)
        self.assertFalse(input_schema.find_foreign_key_failures(orig_dat))
        dat.child_one[1, 2, 4] = {}
        dat.child_two[1,2.2]=3
        dat.child_three.append([1,2,4])
        self.assertTrue(len(input_schema.find_foreign_key_failures(dat)) == 3)
        dat.child_three.pop() # because no PK cannot participate in remove foreign keys
        input_schema.remove_foreign_key_failures(dat)
        self.assertTrue(input_schema._same_data(dat, orig_dat) and not input_schema.find_foreign_key_failures(dat))

        input_schema = TicDatFactory (parent = [["F1", "F2"],["F3"]], child_one = [["F1", "F2", "F3"], []],
                                      child_two = [["F1", "F2"], ["F3"]], child_three = [[],["F1", "F2", "F3"]])
        for t in ["child_one", "child_two", "child_three"]:
            input_schema.add_foreign_key(t, "parent", [["F1"]*2, ["F3"]*2])
        dat = input_schema.TicDat(parent = rows, child_one = rows, child_two = rows, child_three=rows)
        self.assertTrue(all(len(getattr(dat, t)) == 5 for t in input_schema.all_tables))
        orig_dat = input_schema.copy_tic_dat(dat, True)
        self.assertFalse(input_schema.find_foreign_key_failures(orig_dat))
        dat.child_one[1, 2, 4] = {}
        dat.child_two[1,2.2]=4
        dat.child_three.append([1,2,4])
        self.assertTrue(len(input_schema.find_foreign_key_failures(dat)) == 3)
        dat.child_three.pop() # because no PK cannot participate in remove foreign keys
        input_schema.remove_foreign_key_failures(dat)
        self.assertTrue(input_schema._same_data(dat, orig_dat) and not input_schema.find_foreign_key_failures(dat))

    def testFindCaseSpaceDuplicates(self):
        test2 = TicDatFactory(table=[['PK 1','PK 2'],['DF 1','DF 2']])
        self.assertFalse(utils.find_case_space_duplicates(test2))
        test3 = TicDatFactory(table=[['PK 1', 'PK_1'], []])
        self.assertEqual(len(utils.find_case_space_duplicates(test3).keys()),1)
        test4 = TicDatFactory(table=[[], ['DF 1', 'df_1']])
        self.assertEqual(len(utils.find_case_space_duplicates(test4).keys()),1)
        test5 = TicDatFactory(table=[['is Dup'], ['is_Dup']])
        self.assertEqual(len(utils.find_case_space_duplicates(test5).keys()),1)
        test6 = TicDatFactory(table1=[['test'],[]], table2=[['test'],[]])
        self.assertFalse(utils.find_case_space_duplicates(test6))
        test7 = TicDatFactory(table1=[['dup 1', 'Dup_1'],[]], table2=[['Dup 2', 'Dup_2'],[]])
        self.assertEqual(len(utils.find_case_space_duplicates(test7).keys()),2)

    def testChangeFieldsWithReservedKeywords(self):
        input_schema = TicDatFactory(
            categories=[["Name"], ["Min Nutrition", "Max Nutrition"]],
            foods=[["Name"], ["Cost"]],
            nutrition_quantities=[["Food", "Category"], ["Quantity"]])
        input_schema.set_data_type("categories", "Min Nutrition", min=0, max=float("inf"),
                                   inclusive_min=True, inclusive_max=False)
        input_schema.set_data_type("categories", "Max Nutrition", min=0, max=float("inf"),
                                   inclusive_min=True, inclusive_max=True)
        input_schema.set_data_type("foods", "Cost", min=0, max=float("inf"),
                                   inclusive_min=True, inclusive_max=False)
        input_schema.set_data_type("nutrition_quantities", "Quantity", min=0, max=float("inf"),
                                   inclusive_min=True, inclusive_max=False)
        input_schema.add_data_row_predicate(
            "categories", predicate_name="Min Max Check",
            predicate=lambda row: row["Max Nutrition"] >= row["Min Nutrition"])
        input_schema.set_default_value("categories", "Max Nutrition", float("inf"))
        reserved_keywords = ["Words", "CPLEX", "key"]
        new_input_schema = utils.change_fields_with_reserved_keywords(input_schema, reserved_keywords)
        self.assertDictEqual(input_schema.data_types, new_input_schema.data_types)
        self.assertDictEqual(input_schema.default_values, new_input_schema.default_values)
        old_tdf = TicDatFactory(table=[['Key', 'PK 2'], ['cplex', 'DF 2']])
        old_schema = old_tdf.schema()
        new_tdf = utils.change_fields_with_reserved_keywords(old_tdf, reserved_keywords)
        new_schema = new_tdf.schema()
        self.assertTrue('_Key' in new_schema['table'][0])
        new_old_tdf = utils.change_fields_with_reserved_keywords(new_tdf, reserved_keywords,True)
        new_old_schema = new_old_tdf.schema()
        self.assertDictEqual(old_schema, new_old_schema)

    def testOne(self):
        def _cleanIt(x) :
            x.foods['macaroni'] = {"cost": 2.09}
            x.foods['milk'] = {"cost":0.89}
            return x
        dataObj = dietData()
        tdf = TicDatFactory(**dietSchema())
        self.assertTrue(tdf.good_tic_dat_object(dataObj, row_checking="generous"))
        dataObj2 = tdf.copy_tic_dat(dataObj)
        dataObj3 = tdf.copy_tic_dat(dataObj, freeze_it=True)
        dataObj4 = tdf.TicDat(**tdf.as_dict(dataObj3))
        self.assertTrue(all (tdf._same_data(dataObj, x) and dataObj is not x for x in (dataObj2, dataObj3, dataObj4)))
        dataObj = _cleanIt(dataObj)
        self.assertTrue(tdf.good_tic_dat_object(dataObj))
        self.assertTrue(all (tdf._same_data(dataObj, x) and dataObj is not x for x in (dataObj2, dataObj3)))
        def hackit(x) :
            x.foods["macaroni"] = 100
        self.assertTrue(self.firesException(lambda :hackit(dataObj3)))
        hackit(dataObj2)
        self.assertTrue(not tdf._same_data(dataObj, dataObj2) and  tdf._same_data(dataObj, dataObj3))

        msg = []
        dataObj.foods[("milk", "cookies")] = {"cost": float("inf")}
        dataObj.boger = object()
        self.assertFalse(tdf.good_tic_dat_object(dataObj, row_checking="generous") or
                         tdf.good_tic_dat_object(dataObj, bad_message_handler =msg.append, row_checking="generous"))
        self.assertTrue({"foods : Inconsistent key lengths"} == set(msg))
        self.assertTrue(all(tdf.good_tic_dat_table(getattr(dataObj, t), t)
                            for t in ("categories", "nutritionQuantities")))

        dataObj = dietData()
        dataObj.categories["boger"] = {"cost":1}
        dataObj.categories["boger"] = {"cost":1}
        self.assertFalse(tdf.good_tic_dat_object(dataObj, row_checking="generous") or
                         tdf.good_tic_dat_object(dataObj, row_checking="generous", bad_message_handler=msg.append))
        self.assertTrue({'foods : Inconsistent key lengths',
                         'categories : Inconsistent data field name keys.'} == set(msg))
        ex = str(firesException(lambda : tdf.freeze_me(tdf.TicDat(**{t:getattr(dataObj,t)
                                                                for t in tdf.primary_key_fields}))))
        self.assertTrue("categories cannot be treated as a ticDat table : Inconsistent data field name keys" in ex)

    def _assertSame(self, t1, t2, goodTicDatTable):

        if utils.dictish(t1) or utils.dictish(t2) :
            _ass = lambda _t1, _t2 : assertTicDatTablesSame(_t1, _t2,
                    _goodTicDatTable=  goodTicDatTable,
                    **({} if DEBUG() else {"_assertTrue":self.assertTrue, "_assertFalse":self.assertFalse}))

            _ass(t1, t2)
            _ass(t2, t1)
        else :
            setify = lambda t : set(t) if len(t) and not hasattr(t[0], "values") else {r.values() for r in t}
            self.assertTrue(setify(t1) == setify(t2))

    def testTwo(self):
        objOrig = dietData()
        staticFactory = TicDatFactory(**dietSchema())
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.freeze_me(staticFactory.TicDat(**{t:getattr(objOrig,t) for t in tables}))
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t),
                                    lambda _t : staticFactory.good_tic_dat_table(_t, t))

    def testThree(self):
        objOrig = netflowData()
        staticFactory = TicDatFactory(**netflowSchema())
        goodTable = lambda t : lambda _t : staticFactory.good_tic_dat_table(_t, t)
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.freeze_me(staticFactory.TicDat(**{t:getattr(objOrig,t) for t in tables}))
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t), goodTable(t))

        objOrig.commodities.append(12.3)
        objOrig.arcs[(1, 2)] = [12]
        self._assertSame(objOrig.nodes, ticDat.nodes, goodTable("nodes"))
        self._assertSame(objOrig.cost, ticDat.cost, goodTable("cost"))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.commodities, ticDat.commodities, goodTable("commodities")) ))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.arcs, ticDat.arcs, goodTable("arcs")) ))

        ticDat = staticFactory.freeze_me(staticFactory.TicDat(**{t:getattr(objOrig,t) for t in tables}))
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue(ticDat.arcs[1, 2]["capacity"] == 12)
        self.assertTrue(12.3 in ticDat.commodities)

        objOrig.cost[5]=5

        self.assertTrue("cost cannot be treated as a ticDat table : Inconsistent key lengths" in
            str(firesException(lambda : staticFactory.freeze_me(staticFactory.TicDat
                                    (**{t:getattr(objOrig,t) for t in tables})))))

        objOrig = netflowData()
        def editMeBadly(t) :
            def rtn() :
                t.cost["hack"] = 12
            return rtn
        def editMeWell(t) :
            def rtn() :
                t.cost["hack", "my", "balls"] = 12.12
            return rtn
        self.assertTrue(all(firesException(editMeWell(t)) and firesException(editMeBadly(t)) for t in
                            (ticDat, staticFactory.freeze_me(staticFactory.TicDat()))))

        def attributeMe(t) :
            def rtn() :
                t.boger="bogerwoger"
            return rtn

        self.assertTrue(firesException(attributeMe(ticDat)) and firesException(attributeMe(
                staticFactory.freeze_me(staticFactory.TicDat()))))

        mutable = staticFactory.TicDat(**{t:getattr(objOrig,t) for t in tables})
        for t in tables :
            self._assertSame(getattr(objOrig, t), getattr(mutable,t), goodTable(t))

        self.assertTrue(firesException(editMeBadly(mutable)))
        self.assertFalse(firesException(editMeWell(mutable)) or firesException(attributeMe(mutable)))
        self.assertTrue(firesException(lambda : self._assertSame(
            objOrig.cost, mutable.cost, goodTable("cost")) ))

    def testFour(self):
        objOrig = sillyMeData()
        staticFactory = TicDatFactory(**sillyMeSchema())
        goodTable = lambda t : lambda _t : staticFactory.good_tic_dat_table(_t, t)
        tables = set(staticFactory.primary_key_fields)
        ticDat = staticFactory.freeze_me(staticFactory.TicDat(**objOrig))
        self.assertTrue(staticFactory.good_tic_dat_object(ticDat))
        for t in tables :
            self._assertSame(objOrig[t], getattr(ticDat,t), goodTable(t))
        pickedData = staticFactory.TicDat(**staticFactory.as_dict(ticDat))
        self.assertTrue(staticFactory._same_data(ticDat, pickedData))
        mutTicDat = staticFactory.TicDat()
        for k,v in ticDat.a.items() :
            mutTicDat.a[k] = v.values()
        for k,v in ticDat.b.items() :
            mutTicDat.b[k] = v.values()[0]
        for r in ticDat.c:
            mutTicDat.c.append(r)
        for t in tables :
            self._assertSame(getattr(mutTicDat, t), getattr(ticDat,t), goodTable(t))

        self.assertTrue("theboger" not in mutTicDat.a)
        mutTicDat.a["theboger"]["aData2"] =22
        self.assertTrue("theboger" in mutTicDat.a and mutTicDat.a["theboger"].values() == (0, 22, 0))

        newSchema = sillyMeSchema()
        newSchema["a"][1] += ("aData4",)
        newFactory = TicDatFactory(**newSchema)
        def makeNewTicDat() : return newFactory.TicDat(a=ticDat.a, b=ticDat.b, c=ticDat.c)
        newTicDat = makeNewTicDat()
        self.assertFalse(staticFactory.good_tic_dat_object(newTicDat))
        self.assertTrue(newFactory.good_tic_dat_object(ticDat, row_checking="generous"))
        self.assertTrue(newFactory._same_data(makeNewTicDat(), newTicDat))
        newTicDat.a[list(ticDat.a)[0]]["aData4"]=12
        self.assertFalse(newFactory._same_data(makeNewTicDat(), newTicDat))

    def testFive(self):
        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        dat = tdf.freeze_me(tdf.TicDat(**{t : getattr(netflowData(), t) for t in tdf.all_tables}))
        obfudat = tdf.obfusimplify(dat, freeze_it=1)
        self.assertFalse(tdf._same_data(dat, obfudat.copy))
        for (s,d),r in obfudat.copy.arcs.items():
            self.assertFalse((s,d) in dat.arcs)
            self.assertTrue(dat.arcs[obfudat.renamings[s][1], obfudat.renamings[d][1]]["capacity"] == r["capacity"])
        obfudat = tdf.obfusimplify(dat, freeze_it=1, skip_tables=["commodities", "nodes"])
        self.assertTrue(tdf._same_data(obfudat.copy, dat))

        tdf = TicDatFactory(**netflowSchema())
        addNetflowForeignKeys(tdf)
        mone, one2one = "many-to-one",  "one-to-one"
        fk, fkm = ForeignKey, ForeignKeyMapping
        self.assertTrue(set(tdf.foreign_keys) ==  {fk("arcs", 'nodes', fkm('source',u'name'), mone),
                            fk("arcs", 'nodes', fkm('destination',u'name'), mone),
                            fk("cost", 'nodes', fkm('source',u'name'), mone),
                            fk("cost", 'nodes', fkm('destination',u'name'), mone),
                            fk("cost", 'commodities', fkm('commodity',u'name'), mone),
                            fk("inflow", 'commodities', fkm('commodity',u'name'), mone),
                            fk("inflow", 'nodes', fkm('node',u'name'), mone)})

        tdf.clear_foreign_keys("cost")
        self.assertTrue(set(tdf.foreign_keys) ==  {fk("arcs", 'nodes', fkm('source',u'name'), mone),
                            fk("arcs", 'nodes', fkm('destination',u'name'), mone),
                            fk("inflow", 'commodities', fkm('commodity',u'name'), mone),
                            fk("inflow", 'nodes', fkm('node',u'name'), mone)})

        tdf = TicDatFactory(**dietSchema())
        self.assertFalse(tdf.foreign_keys)
        addDietForeignKeys(tdf)

        self.assertTrue(set(tdf.foreign_keys) == {fk("nutritionQuantities", 'categories', fkm('category',u'name'), mone),
                                                  fk("nutritionQuantities", 'foods', fkm('food',u'name'), mone)})

        tdf.TicDat()
        self.assertTrue(self.firesException(lambda  : tdf.clear_foreign_keys("nutritionQuantities")))
        self.assertTrue(tdf.foreign_keys)
        tdf = TicDatFactory(**dietSchema())
        addDietForeignKeys(tdf)
        tdf.clear_foreign_keys("nutritionQuantities")
        self.assertFalse(tdf.foreign_keys)

        tdf = TicDatFactory(parentTable = [["pk"],["pd1", "pd2", "pd3"]],
                            goodChild = [["gk"], ["gd1", "gd2"]],
                            badChild = [["bk1", "bk2"], ["bd"]],
                            appendageChild = [["ak"], ["ad1", "ad2"]],
                            appendageBadChild = [["bk1", "bk2"], []])
        tdf.add_foreign_key("goodChild", "parentTable", fkm("gd1" , "pk"))
        tdf.add_foreign_key("badChild", "parentTable", ["bk2" , "pk"])
        self.assertFalse(self.firesException(lambda :
                tdf.add_foreign_key("badChild", "parentTable", ["bd", "pd2"])))
        tdf.add_foreign_key("appendageChild", "parentTable", ["ak", "pk"])
        tdf.add_foreign_key("appendageBadChild", "badChild", (("bk2", "bk2"), ("bk1","bk1")))
        fks = tdf.foreign_keys
        def _getfk_cards(t):
            return {_.cardinality for _ in fks if _.native_table == t}
        self.assertTrue(_getfk_cards("goodChild") == {"many-to-one"})
        self.assertTrue(_getfk_cards("appendageChild") == {"one-to-one"})
        self.assertTrue(_getfk_cards("appendageBadChild") == {"one-to-one"})
        self.assertTrue(_getfk_cards("badChild") == {"many-to-one", "many-to-many"})

        tdf.clear_foreign_keys("appendageBadChild")
        self.assertTrue(tdf.foreign_keys and "appendageBadChild" not in tdf.foreign_keys)
        tdf.clear_foreign_keys()
        self.assertFalse(tdf.foreign_keys)

    def testFiveB(self):
        tdf = TicDatFactory(parent = [["Boo"],["Field"]], other_parent = [["Bar"],["Field"]],
                            child = [["Boo", "Bar"], ["Field"]], more_child = [["Bar", "Boo"], ["Field"]])
        tdf.add_foreign_key("child", "parent", ["Boo"]*2)
        tdf.add_foreign_key("child", "other_parent", ["Bar"]*2)
        tdf.add_foreign_key("more_child", "child", [["Boo"]*2, ["Bar"]*2])

        tic_dat = tdf.TicDat(parent = [[1, 1], [2,2], [3,3]],
                             other_parent = [[4,4], [5,5]])
        for i, (p,op) in enumerate(itertools.product(tic_dat.parent, tic_dat.other_parent)):
            tic_dat.child[p,op] = i
            if i%3==1:
                tic_dat.more_child[op,p]=10*i
        tic_dat = tdf.copy_tic_dat(tic_dat, freeze_it=1)
        obfudat = tdf.obfusimplify(tic_dat, freeze_it=1)
        other_dat = obfudat.copy
        renamings = obfudat.renamings

        for t in tdf.all_tables:
            self.assertTrue(len(getattr(tic_dat, t)) == len(getattr(other_dat, t)))

        for k,r in other_dat.parent.items():
            self.assertTrue(tic_dat.parent[renamings[k][1]]["Field"] == r["Field"])

        for k,r in other_dat.other_parent.items():
            self.assertTrue(tic_dat.other_parent[renamings[k][1]]["Field"] == r["Field"])

        for (k1, k2),r in other_dat.child.items():
            self.assertTrue(tic_dat.child[renamings[k1][1], renamings[k2][1]]["Field"] == r["Field"])

        for (k1, k2),r in other_dat.more_child.items():
            self.assertTrue(tic_dat.more_child[renamings[k1][1], renamings[k2][1]]["Field"] == r["Field"])

    def testSix(self):
        for cloning in [True, False, "*"]:
            clone_me_maybe = lambda x : x.clone(tdf.all_tables if cloning == "*" else None) if cloning else x

            tdf = TicDatFactory(plants = [["name"], ["stuff", "otherstuff"]],
                                lines = [["name"], ["plant", "weird stuff"]],
                                line_descriptor = [["name"], ["booger"]],
                                products = [["name"],["gover"]],
                                production = [["line", "product"], ["min", "max"]],
                                pureTestingTable = [[], ["line", "plant", "product", "something"]],
                                extraProduction = [["line", "product"], ["extramin", "extramax"]],
                                weirdProduction = [["line1", "line2", "product"], ["weirdmin", "weirdmax"]])
            tdf.add_foreign_key("production", "lines", ("line", "name"))
            tdf.add_foreign_key("production", "products", ("product", "name"))
            tdf.add_foreign_key("lines", "plants", ("plant", "name"))
            tdf.add_foreign_key("line_descriptor", "lines", ("name", "name"))
            for f in set(tdf.data_fields["pureTestingTable"]).difference({"something"}):
                tdf.add_foreign_key("pureTestingTable", "%ss"%f, (f,"name"))
            tdf.add_foreign_key("extraProduction", "production", (("line", "line"), ("product","product")))
            tdf.add_foreign_key("weirdProduction", "production", (("line1", "line"), ("product","product")))
            tdf.add_foreign_key("weirdProduction", "extraProduction", (("line2","line"), ("product","product")))

            # note that the following line fails very inconsistently in Py3. Perhaps 1 out of 10 times, and never
            # from within ipython, so hard to debug. I suspect it might have something to do with cardinality of
            # foreign key reporting, which isn't super important. Lets just live with it for now.
            self._testTdfReproduction(tdf)

            tdf = clone_me_maybe(tdf)

            goodDat = tdf.TicDat()
            goodDat.plants["Cleveland"] = ["this", "that"]
            goodDat.plants["Newark"]["otherstuff"] =1
            goodDat.products["widgets"] = goodDat.products["gadgets"] = "shizzle"

            for i,p in enumerate(goodDat.plants):
                goodDat.lines[i]["plant"] = p

            for i,(pl, pd) in enumerate(itertools.product(goodDat.lines, goodDat.products)):
                goodDat.production[pl, pd] = {"min":1, "max":10+i}

            badDat1 = tdf.copy_tic_dat(goodDat)
            badDat1.production["notaline", "widgets"] = [0,1]
            badDat2 = tdf.copy_tic_dat(badDat1)

            fk, fkm = ForeignKey, ForeignKeyMapping
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) == tdf.find_foreign_key_failures(badDat2) ==
                            {fk('production', 'lines', fkm('line', 'name'), 'many-to-one'):
                                 (('notaline',), (('notaline', 'widgets'),))})
            self.assertTrue(tdf.find_foreign_key_failures(badDat1, verbosity="Low") ==
                            tdf.find_foreign_key_failures(badDat2, verbosity="Low") ==
                            {('production', 'lines', ('line', 'name')):
                                 (('notaline',), (('notaline', 'widgets'),))})
            badDat1.lines["notaline"]["plant"] = badDat2.lines["notaline"]["plant"] = "notnewark"
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) == tdf.find_foreign_key_failures(badDat2) ==
                            {fk('lines', 'plants', fkm('plant', 'name'), 'many-to-one'):
                                 (('notnewark',), ('notaline',))})
            tdf.remove_foreign_key_failures(badDat1, propagate=False)
            tdf.remove_foreign_key_failures(badDat2, propagate=True)
            self.assertTrue(tdf._same_data(badDat2, goodDat) and not tdf.find_foreign_key_failures(badDat2))
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) ==
                    {fk('production', 'lines', fkm('line', 'name'), 'many-to-one'):
                         (('notaline',), (('notaline', 'widgets'),))})

            tdf.remove_foreign_key_failures(badDat1, propagate=False)
            self.assertTrue(tdf._same_data(badDat1, goodDat) and not tdf.find_foreign_key_failures(badDat1))

            _ = len(goodDat.lines)
            for i,p in enumerate(list(goodDat.plants.keys()) + list(goodDat.plants.keys())):
                goodDat.lines[i+_]["plant"] = p
            for l in goodDat.lines:
                if i%2:
                    goodDat.line_descriptor[l] = i+10

            for i,(l,pl,pdct) in enumerate(sorted(itertools.product(goodDat.lines, goodDat.plants, goodDat.products))):
                goodDat.pureTestingTable.append((l,pl,pdct,i))
            self.assertFalse(tdf.find_foreign_key_failures(goodDat))
            badDat = tdf.copy_tic_dat(goodDat)
            badDat.pureTestingTable.append(("j", "u", "nk", "ay"))
            l = len(goodDat.pureTestingTable)
            self.assertTrue(tdf.find_foreign_key_failures(badDat) ==
             {fk('pureTestingTable', 'plants', fkm('plant', 'name'), 'many-to-one'): (('u',),(l,)),
              fk('pureTestingTable', 'products', fkm('product', 'name'), 'many-to-one'): (('nk',), (l,)),
              fk('pureTestingTable', 'lines', fkm('line', 'name'), 'many-to-one'): (('j',), (l,))})

            obfudat = tdf.obfusimplify(goodDat, {"plants": "P"}, freeze_it=True)
            self.assertTrue(all(len(getattr(obfudat.copy, t)) == len(getattr(goodDat, t))
                                for t in tdf.all_tables))
            for n in list(goodDat.plants) + list(goodDat.lines) + list(goodDat.products) :
                self.assertTrue(n in {_[1] for _ in obfudat.renamings.values()})
                self.assertFalse(n in obfudat.renamings)
            self.assertTrue(obfudat.copy.plants['P2']['otherstuff'] == 1)
            self.assertFalse(tdf._same_data(obfudat.copy, goodDat))
            for k,r in obfudat.copy.line_descriptor.items():
                i = r.values()[0] - 10
                self.assertTrue(i%2 and (goodDat.line_descriptor[i].values()[0] == i+10))

            obfudat2 = tdf.obfusimplify(goodDat, {"plants": "P", "lines" : "L", "products" :"PR"})
            self.assertTrue(tdf._same_data(obfudat.copy, obfudat2.copy))

            obfudat3 = tdf.obfusimplify(goodDat, skip_tables=["plants", "lines", "products"])
            self.assertTrue(tdf._same_data(obfudat3.copy, goodDat))

            obfudat4 = tdf.obfusimplify(goodDat, skip_tables=["lines", "products"])
            self.assertFalse(tdf._same_data(obfudat4.copy, goodDat))
            self.assertFalse(tdf._same_data(obfudat4.copy, obfudat.copy))

    def testSixB(self):
        tdf = TicDatFactory(pt1 = [["F1"],[]], pt2 = [["F2"],[]], pt3 = [["F1","F2"],[]],
                            pt4 = [["F1"],["F2"]], pt5 = [[],["F1","F2"]])
        for c in ["pt3", "pt4", "pt5"]:
            tdf.add_foreign_key(c, "pt1", ["F1", "F1"])
            tdf.add_foreign_key(c, "pt2", ["F2", "F2"])
        ticDat = tdf.TicDat(pt1=[1, 2, 3, 4], pt2=[5, 6, 7, 8])
        for f1, f2 in itertools.product(range(1,5), range(5,9)):
            ticDat.pt3[f1, f2] = {}
            ticDat.pt4[f1] = f2
            ticDat.pt5.append((f1, f2))
        origDat = tdf.copy_tic_dat(ticDat, freeze_it=True)
        self.assertFalse(tdf.find_foreign_key_failures(origDat))
        ticDat.pt3["no",6] = ticDat.pt3[1, "no"] = {}
        ticDat.pt4["no"] = 6
        ticDat.pt4["nono"]=6.01
        fails1 = tdf.find_foreign_key_failures(ticDat)
        self.assertTrue(fails1)
        tdf.remove_foreign_key_failures(ticDat)
        self.assertTrue(tdf._same_data(ticDat, origDat) and not tdf.find_foreign_key_failures(ticDat))

        orig_lens = {t:len(getattr(ticDat, t)) for t in tdf.all_tables}
        ticDat.pt3["no",6] = ticDat.pt3[1, "no"] = {}
        ticDat.pt4["no"] = 6
        ticDat.pt4["nono"]=6.01
        ticDat.pt5.append(("no",6))
        ticDat.pt5.append((1, "no"))
        fails2 = tdf.find_foreign_key_failures(ticDat)
        self.assertTrue(set(fails1) != set(fails2) and set(fails1).issubset(fails2))
        tdf.remove_foreign_key_failures(ticDat)
        self.assertFalse(tdf.find_foreign_key_failures(ticDat))
        self.assertTrue({t:len(getattr(ticDat, t)) for t in tdf.all_tables} == orig_lens)


    def testSeven(self):
        tdf = TicDatFactory(**dietSchema())
        def makeIt() :
            rtn = tdf.TicDat()
            rtn.foods["a"] = {}
            rtn.categories["1"] = {}
            rtn.categories["2"] = [0,1]
            self.assertTrue(rtn.categories["2"]["minNutrition"] == 0)
            self.assertTrue(rtn.categories["2"]["maxNutrition"] == 1)
            rtn.nutritionQuantities['junk',1] = {}
            return tdf.freeze_me(rtn)
        td = makeIt()
        self.assertTrue(td.foods["a"]["cost"]==0 and td.categories["1"].values() == (0,0) and
                        td.nutritionQuantities['junk',1]["qty"] == 0)
        tdf = TicDatFactory(**dietSchema())
        tdf.set_default_values(foods = {"cost":"dontcare"},nutritionQuantities = {"qty":100} )
        self._testTdfReproduction(tdf)
        td = makeIt()
        self.assertTrue(td.foods["a"]["cost"]=='dontcare' and td.categories["1"].values() == (0,0) and
                        td.nutritionQuantities['junk',1]["qty"] == 100)
        tdf = TicDatFactory(**dietSchema())
        tdf.set_default_value("categories", "minNutrition", 1)
        tdf.set_default_value("categories", "maxNutrition", 2)
        self._testTdfReproduction(tdf)
        td = makeIt()
        self.assertTrue(td.foods["a"]["cost"]==0 and td.categories["1"].values() == (1,2) and
                        td.nutritionQuantities['junk',1]["qty"] == 0)

    def testEight(self):
        for cloning in [True, False, "*"]:
            clone_me_maybe = lambda x : x.clone(tdf.all_tables if cloning == "*" else None) if cloning else x

            tdf = TicDatFactory(**dietSchema())
            def makeIt() :
                rtn = tdf.TicDat()
                rtn.foods["a"] = 12
                rtn.foods["b"] = None
                rtn.categories["1"] = {"maxNutrition":100, "minNutrition":40}
                rtn.categories["2"] = [10,20]
                for f, p in itertools.product(rtn.foods, rtn.categories):
                    rtn.nutritionQuantities[f,p] = 5
                rtn.nutritionQuantities['a', 2] = 12
                return tdf.freeze_me(rtn)
            dat = makeIt()
            self.assertFalse(tdf.find_data_type_failures(dat))

            tdf = TicDatFactory(**dietSchema())
            tdf.set_data_type("foods", "cost", nullable=False)
            tdf.set_data_type("nutritionQuantities", "qty", min=5, inclusive_min=False, max=12, inclusive_max=True)

            tdf.set_default_value("foods", "cost", 2)
            tdf = clone_me_maybe(tdf)
            self._testTdfReproduction(tdf)
            dat = makeIt()
            failed = tdf.find_data_type_failures(dat)
            self.assertTrue(set(failed) == {('foods', 'cost'), ('nutritionQuantities', 'qty')})
            self.assertTrue(set(failed['nutritionQuantities', 'qty'].pks) ==
                            {('b', '1'), ('a', '2'), ('a', '1'), ('b', '2')})
            self.assertTrue(failed['nutritionQuantities', 'qty'].bad_values == (5,))
            ex = self.firesException(lambda : tdf.replace_data_type_failures(tdf.copy_tic_dat(dat)))
            self.assertTrue(all(_ in ex for _ in ("replacement value", "nutritionQuantities", "qty")))
            fixedDat = tdf.replace_data_type_failures(tdf.copy_tic_dat(dat),
                                replacement_values={("nutritionQuantities", "qty"):5.001})
            self.assertFalse(tdf.find_data_type_failures(fixedDat) or tdf._same_data(fixedDat, dat))
            self.assertTrue(all(fixedDat.nutritionQuantities[pk]["qty"] == 5.001 for pk in
                                failed['nutritionQuantities', 'qty'].pks))
            self.assertTrue(fixedDat.foods["a"]["cost"] == 12 and fixedDat.foods["b"]["cost"] == 2 and
                            fixedDat.nutritionQuantities['a', 2]["qty"] == 12)

            tdf = TicDatFactory(**dietSchema())
            tdf.set_data_type("foods", "cost", nullable=False)
            tdf.set_data_type("nutritionQuantities", "qty", min=5, inclusive_min=False, max=12, inclusive_max=True)
            tdf = clone_me_maybe(tdf)
            fixedDat2 = tdf.replace_data_type_failures(tdf.copy_tic_dat(dat),
                                replacement_values={("nutritionQuantities", "qty"):5.001, ("foods", "cost") : 2})
            self.assertTrue(tdf._same_data(fixedDat, fixedDat2))

            tdf = TicDatFactory(**dietSchema())
            tdf.set_data_type("foods", "cost", nullable=True)
            tdf.set_data_type("nutritionQuantities", "qty",number_allowed=False)
            tdf = clone_me_maybe(tdf)
            failed = tdf.find_data_type_failures(dat)
            self.assertTrue(set(failed) == {('nutritionQuantities', 'qty')})
            self.assertTrue(set(failed['nutritionQuantities', 'qty'].pks) == set(dat.nutritionQuantities))
            ex = self.firesException(lambda : tdf.replace_data_type_failures(tdf.copy_tic_dat(dat)))
            self.assertTrue(all(_ in ex for _ in ("replacement value", "nutritionQuantities", "qty")))

            tdf = TicDatFactory(**dietSchema())
            tdf.set_data_type("foods", "cost")
            self._testTdfReproduction(tdf)
            fixedDat = tdf.replace_data_type_failures(tdf.copy_tic_dat(makeIt()))
            self.assertTrue(fixedDat.foods["a"]["cost"] == 12 and fixedDat.foods["b"]["cost"] == 0)

            tdf = TicDatFactory(**netflowSchema())
            addNetflowForeignKeys(tdf)
            tdf = clone_me_maybe(tdf)
            self._testTdfReproduction(tdf)
            dat = tdf.copy_tic_dat(netflowData(), freeze_it=1)
            self.assertFalse(hasattr(dat.nodes["Detroit"], "arcs_source"))

            tdf = TicDatFactory(**netflowSchema())
            addNetflowForeignKeys(tdf)
            tdf.enable_foreign_key_links()
            tdf = clone_me_maybe(tdf)
            dat = tdf.copy_tic_dat(netflowData(), freeze_it=1)
            self.assertTrue(hasattr(dat.nodes["Detroit"], "arcs_source"))

            tdf = clone_me_maybe(TicDatFactory(**netflowSchema()))
            def makeIt() :
                if not tdf.foreign_keys:
                    tdf.enable_foreign_key_links()
                    addNetflowForeignKeys(tdf)
                orig = netflowData()
                rtn = tdf.copy_tic_dat(orig)
                for n in rtn.nodes["Detroit"].arcs_source:
                    rtn.arcs["Detroit", n] = n
                self.assertTrue(all(len(getattr(rtn, t)) == len(getattr(orig, t)) for t in tdf.all_tables))
                return tdf.freeze_me(rtn)
            dat = makeIt()
            self.assertFalse(tdf.find_data_type_failures(dat))

            tdf = TicDatFactory(**netflowSchema())
            tdf.set_data_type("arcs", "capacity", strings_allowed="*")
            tdf = clone_me_maybe(tdf)
            dat = makeIt()
            self.assertFalse(tdf.find_data_type_failures(dat))

            tdf = TicDatFactory(**netflowSchema())
            tdf.set_data_type("arcs", "capacity", strings_allowed=["Boston", "Seattle", "lumberjack"])
            tdf = clone_me_maybe(tdf)
            dat = makeIt()
            failed = tdf.find_data_type_failures(dat)
            self.assertTrue(failed == {('arcs', 'capacity'):(("New York",), (("Detroit", "New York"),))})
            fixedDat = tdf.replace_data_type_failures(tdf.copy_tic_dat(makeIt()))
            netflowData_ = tdf.copy_tic_dat(netflowData())
            self.assertFalse(tdf.find_data_type_failures(fixedDat) or tdf._same_data(dat, netflowData_))
            fixedDat = tdf.copy_tic_dat(tdf.replace_data_type_failures(tdf.copy_tic_dat(makeIt()),
                                            {("arcs", "capacity"):80, ("cost","cost") :"imok"}))
            fixedDat.arcs["Detroit", "Boston"] = 100
            fixedDat.arcs["Detroit", "Seattle"] = 120
            self.assertTrue(tdf._same_data(fixedDat, netflowData_))

    def testNine(self):
        for schema in (dietSchema(), sillyMeSchema(), netflowSchema()) :
            tdf = TicDatFactory(**schema)
            tdf2 = TicDatFactory.create_from_full_schema(tdf.schema(True))
            self.assertTrue(tdf.schema() == tdf.schema(True)["tables_fields"] == tdf2.schema() ==
                            {k : list(map(list, v)) for k,v in schema.items()})

    def testTen(self):
        with LogFile(os.path.join(_scratchDir, "boger.txt")) as f:
            f.log_table("boger", [["a", "b", 12]] + [list(range(_))[-3:] for _ in list(range(16))[3:]])
        with LogFile(os.path.join(_scratchDir, "boger.txt")) as f:
            f.log_table("boger", [["a", "b", 12]] + [list(range(_))[-3:] for _ in list(range(12))[3:]])
        with LogFile(os.path.join(_scratchDir, "boger.txt")) as f:
            self.assertTrue(self.firesException(lambda : f.log_table("boger",
                    [["a", "b", 12]] + [list(range(_))[-3:] for _ in list(range(12))])))

    def testEleven(self):
        def do_checks(po):
            self.assertTrue(po.mip_progress("this", 1, 2))
            self.assertTrue(po.mip_progress("this", 2, 2))
            self.assertTrue(po.mip_progress("this", -2, -1))
            self.assertTrue(po.mip_progress("this", -2, -2))
            self.assertTrue(self.firesException(lambda : po.mip_progress("this", 2.1, 2)))
            self.assertTrue(self.firesException(lambda : po.mip_progress("this", -2, -2.1)))
            self.assertTrue(po.numerical_progress("boger", 1))
            self.assertTrue(self.firesException(lambda : po.numerical_progress("boger", "1")))
        do_checks(Progress())
        do_checks(Progress(quiet=True))

    def testTwelve(self):
        self.assertTrue(set(utils.all_underscore_replacements("boger")) == {"boger"})
        self.assertTrue(set(utils.all_underscore_replacements("the_boger")) ==
                        {"the_boger", "the boger"})
        self.assertTrue(set(utils.all_underscore_replacements("the_big_odd_hairy_boger")) ==
                        {"the_big_odd_hairy_boger", "the big_odd_hairy_boger", "the_big odd_hairy_boger",
                         "the_big_odd hairy_boger", "the_big_odd_hairy boger", "the big odd_hairy_boger",
                         "the_big odd hairy_boger", "the_big_odd hairy boger", "the big_odd hairy_boger",
                         "the_big odd_hairy boger", "the big_odd_hairy boger", "the_big odd hairy boger",
                         "the big_odd hairy boger", "the big odd_hairy boger", "the big odd hairy_boger",
                         "the big odd hairy boger"})
    def test13(self):
        # THIRTEEN! THIRTEEN!
        TicDatFactory(boger = [["fieldone", "fieldtwo"],["fieldthree","fieldfour"]])
        exceptions = [self.firesException(lambda :
                TicDatFactory(boger = [["fieldone", "fieldONE"],["fieldthree","fieldfour"]])),
                      self.firesException(lambda :
                TicDatFactory(boger = [["fieldone", "fieldtwo"],["fieldtwo","fieldfour"]])),
                      self.firesException(lambda :
                TicDatFactory(boger = [["fieldone", "fieldtwo"],["fieldTWO","fieldfour"]]))]
        self.assertTrue(all(exceptions))
        self.assertTrue(len(set(exceptions)) == 2)

        TicDatFactory(boger = [["a"],["b"]], moger = [["a"], ["b"]])
        self.assertTrue(self.firesException(lambda : TicDatFactory(boger = [["a"],["b"]], Boger = [["a"], ["b"]])))

    def testFourteen(self):
        slicer = utils.Slicer(([1,2],(x for x in (2,3)), [1, 12], [12.2, 2]))
        def dotests():
            self.assertTrue(set(slicer.slice('*', 2)) == {(1,2),(12.2,2)})
            self.assertTrue(set(slicer.slice(1, '*')) == {(1,12),(1,2)})
            self.assertTrue(slicer.slice(1,2) == [(1,2)])
            self.assertTrue(set(slicer.slice('*', '*')) == {(1,2),(2,3),(1, 12),(12.2, 2)})
        dotests()
        self.assertFalse(slicer._archived_slicings and slicer._gu)
        slicer._forceguout()
        dotests()
        self.assertTrue(len(slicer._archived_slicings) == 4 and not slicer._gu)

        slicer = utils.Slicer(set([tuple(range(_))[-5:] for _ in range(5,30)] +
                                  [(1, 2, 3) + tuple(range(_))[-2:] for _ in range(2,10)]))
        def dotests():
            self.assertTrue(len(slicer.slice(1, 2, '*', '*', '*'))== 8)
            for x in range(25):
                if x != 1:
                    self.assertTrue(len(slicer.slice(x, x+1, '*', '*', '*'))== 1)
                self.assertTrue(len(slicer.slice('*', '*', '*', '*', x)) ==
                                (2 if x in (4,6,7,8) else (0 if x == 0 else 1)))
            self.assertTrue(slicer.slice('no', '*', '*', '*', '*') == [])
        dotests()
        self.assertFalse(slicer._archived_slicings and slicer._gu)
        slicer._forceguout()
        dotests()
        self.assertTrue(len(slicer._archived_slicings) == 3 and not slicer._gu)

        self.assertTrue(self.firesException(
            lambda : utils.Slicer(([1,2], [2,3], [1, '*'], [12.2, 2]))))

        self.assertTrue(self.firesException(
            lambda : utils.Slicer(([1,2], [2,3], [1, 3, 3], [12.2, 2]))))

    def testFifteen(self):
        tdf = TicDatFactory(theTable = [["fieldOne"],["fieldTwo"]])
        for f in ["fieldOne", "fieldTwo"]:
            tdf.set_data_type("theTable", f, must_be_int=True)
        dat1 = tdf.TicDat()
        for d in [1, 2, 22]:
            dat1.theTable[d]=d
        dat2 = tdf.TicDat(theTable = dat1.theTable)
        for d in [22.2, "22"]:
            dat2.theTable[d] = d
        self.assertFalse(tdf.find_data_type_failures(dat1))
        fnd = tdf.find_data_type_failures(dat2)
        self.assertTrue(len(fnd) == 2 and len(set(fnd.values())) == 1)
        tdf.replace_data_type_failures(dat2)
        for k in dat2.theTable:
            self.assertTrue((dat2.theTable[k]["fieldTwo"] == 0) ==
                            (k in [22.2, "22"]))
        fnd = tdf.find_data_type_failures(dat2)
        fnd_values = list(fnd.values())
        self.assertTrue(len(fnd) == 1 and len(fnd_values[0].pks)==2)
        self.assertTrue(set(fnd_values[0].pks) == set(fnd_values[0].bad_values))

    def testSixteen(self):
        def makeTdfDat(add_integrality_rule = False):
            tdf = TicDatFactory(boger=[[],["foo", "goo"]])
            if add_integrality_rule:
                tdf.set_data_type("boger", "foo", must_be_int=True)
            dat = tdf.TicDat()
            dat.boger.append([1]*2)
            dat.boger.append([2.1]*2)
            dat.boger.append([1.1]*2)
            dat.boger.append([1.1]*2)
            dat.boger.append([2]*2)
            return tdf, dat
        tdf, dat = makeTdfDat()
        self.assertFalse(tdf.find_data_type_failures(dat))
        tdf, dat = makeTdfDat(add_integrality_rule=True)
        failures = tdf.find_data_type_failures(dat)
        self.assertTrue(len(failures) == 1)
        badValues, badPks = failures["boger", "foo"]
        self.assertTrue(badPks == (1,2,3))
        self.assertTrue(set(badValues) == {2.1, 1.1})
        tdf.replace_data_type_failures(dat)
        self.assertFalse(tdf.find_data_type_failures(dat))
        self.assertTrue({x["foo"] for x in dat.boger} == {0,1,2})
        self.assertTrue({x["goo"] for x in dat.boger} == {2.1,1.1,1,2})
        tdf, dat = makeTdfDat(add_integrality_rule=True)
        tdf.replace_data_type_failures(dat, {("boger","foo"):11})
        self.assertTrue({x["foo"] for x in dat.boger} == {11,1,2})
        self.assertTrue({x["goo"] for x in dat.boger} == {2.1,1.1,1,2})

    def testSeventeen(self):
         tdf = TicDatFactory(bo = [["a","b"],["c"]])
         dat = tdf.TicDat(bo = [[1, 2, 3], ["a", "b", "c"]])
         dat2 = tdf.TicDat(bo = [{"b":2, "a":1},{"a":"a","b":"b"}])
         self.assertTrue(set(dat.bo) == set(dat2.bo) == {(1,2), ("a","b")})
         self.assertTrue(dat.bo[1,2]["c"] == 3 and dat.bo["a","b"]["c"] == "c")
         self.assertTrue(dat2.bo[1,2]["c"] == 0 and dat2.bo["a","b"]["c"] == 0)
         fd = utils.find_duplicates_from_dict_ticdat
         self.assertFalse(fd(tdf, {"bo":[{"b":2, "a":1},{"a":"a","b":"b"}]}))
         self.assertTrue(set(fd(tdf, {"bo":[{"b":2}, {"a":1}, {"a":1, "b":0}]})["bo"]) == {(1,0)})
         self.assertTrue(set(fd(tdf, {"bo":[{"b":2}, {"a":1}, {"a":1, "b":0}, {"a":0, "b":2}]})["bo"]) ==
                         {(1,0),(0,2)})

         tdf = TicDatFactory(bo = [["c"],[]])
         dat = tdf.TicDat(bo = [1, "a"])
         self.assertTrue(set(dat.bo) == {"a",1})

    def testEighteen(self):
        # this test won't run properly if the -O flag is applied
        for cloning in [True, False, "*"]:
            clone_me_maybe = lambda x : x.clone(tdf.all_tables if cloning == "*" else None) if cloning else x
            tdf = TicDatFactory(**dietSchema())
            dat = tdf.TicDat()
            dat.foods["a"] = 12
            dat.foods["b"] = dat.foods["c"] = None
            dat.categories[1] = {"maxNutrition":100, "minNutrition":40}
            dat.categories[2] = [21,20]
            dat.categories[3] = [20,21]
            for f, p in itertools.product(dat.foods, dat.categories):
                dat.nutritionQuantities[f,p] = 6
            dat.nutritionQuantities['b', 3] = 5
            dat.nutritionQuantities['a', 2] = 12
            dat = tdf.freeze_me(dat)

            self.assertFalse(tdf.find_data_row_failures(dat))
            tdf = TicDatFactory(**dietSchema())
            tdf.add_data_row_predicate("categories", lambda row : row["minNutrition"] <= row["maxNutrition"],
                                       "minmax")
            tdf.add_data_row_predicate("nutritionQuantities",
                                       lambda row : row["category"] < 3 or row["qty"] % 2)
            def make_error_message_predicate(f, name):
                def error_message_predicate(row):
                    rtn = f(row)
                    if rtn:
                        return True
                    return f"{name} failed!"
                return error_message_predicate
            def make_tdf_2(_tdf):
                tdf2 = TicDatFactory(**_tdf.schema())
                for t, preds in tdf._data_row_predicates.items():
                    for p_name, rpi in preds.items():
                        tdf2.add_data_row_predicate(t, make_error_message_predicate(rpi.predicate, p_name),
                                                    predicate_name=p_name, predicate_failure_response="Error Message")
                return tdf2
            tdf2 = make_tdf_2(tdf)
            tdf = clone_me_maybe(tdf)
            tdf2 = clone_me_maybe(tdf2)
            failures = tdf.find_data_row_failures(dat)
            failures2 = tdf2.find_data_row_failures(dat)
            for f in [failures, failures2]:
                self.assertTrue(any(k for k in f if
                                    k.table == "nutritionQuantities" and k.predicate_name == 0))
                self.assertTrue(any(k for k in f if
                                    k.table == "categories" and k.predicate_name == "minmax"))
            self.assertTrue(failures["categories","minmax"] == (2,))
            self.assertTrue(list(map(tuple, failures2["categories","minmax"])) == [(2, 'minmax failed!')])
            self.assertTrue(set(failures["nutritionQuantities", 0]) == {("a",3), ("c",3)})
            self.assertTrue(set(map(tuple, failures2["nutritionQuantities", 0])) ==
                            {(('a', 3), '0 failed!'), (('c', 3), '0 failed!')})
            tdf.add_data_row_predicate("nutritionQuantities", predicate=None, predicate_name=0)
            self.assertTrue(set(tdf.find_data_row_failures(dat)) == {("categories","minmax")})
            for i in range(1,4):
                tdf.add_data_row_predicate("nutritionQuantities",
                    (lambda j : lambda row : row["category"] < 3 or row["qty"] % j)(i))
            tdf = clone_me_maybe(tdf)
            tdf2 = make_tdf_2(tdf)
            failures = tdf.find_data_row_failures(dat)
            failures2 = tdf2.find_data_row_failures(dat)
            self.assertTrue(failures["categories","minmax"] == (2,))
            self.assertTrue(list(map(tuple, failures2["categories","minmax"])) == [(2, 'minmax failed!')])
            self.assertTrue(set(failures["nutritionQuantities", 0]) == {("a",3), ("b",3), ("c",3)})
            self.assertTrue(set(failures["nutritionQuantities", 1]) ==
                            set(failures["nutritionQuantities", 2]) == {("a",3), ("c",3)})
            self.assertTrue({i: len(set(failures2["nutritionQuantities", i])) for i in range(3)} ==
                            {0: 3, 1: 2, 2: 2})
            dat = tdf.copy_tic_dat(dat)
            dat.nutritionQuantities['b', 3]['qty'] = None
            for _tdf in [tdf, tdf2]:
                ex = []
                try:
                    _tdf.find_data_row_failures(dat)
                except Exception as e:
                    ex[:] = [str(e.__class__)]
                self.assertTrue("TypeError" in ex[0])
            failures =  tdf.find_data_row_failures(dat, exception_handling="Handled as Failure")
            self.assertTrue(set(failures["nutritionQuantities", 0]) == set(failures["nutritionQuantities", 1]) ==
                            set(failures["nutritionQuantities", 2]) == {("a", 3), ("b", 3), ("c", 3)})
            failures2 = tdf2.find_data_row_failures(dat, exception_handling="Handled as Failure")
            for i in range(3):
                self.assertTrue(set(map(tuple, failures2["nutritionQuantities", i])) ==
                                {(('a', 3), f'{i} failed!'),
                                 (('b', 3),
                                  "Exception<unsupported operand type(s) for %: 'NoneType' and 'int'>"),
                                 (('c', 3), f'{i} failed!')}
                                )

            tdf = TicDatFactory(**spacesSchema())
            tdf.add_data_row_predicate("c_table",
                                       lambda row : len(list(filter(utils.numericish, row.values()))) >= 2,
                                       predicate_name= "two_nums")
            tdf.add_data_row_predicate("c_table",
                                       lambda row : all(map(utils.stringish, row.values())),
                                       predicate_name= "all_strings")
            tdf = clone_me_maybe(tdf)
            dat = tdf.TicDat(**spacesData())
            failures = tdf.find_data_row_failures(dat)
            self.assertTrue(failures["c_table", "two_nums"] == (1,))
            self.assertTrue(failures["c_table", "all_strings"] == (0,2))

        tdf = TicDatFactory(table=[[],["Field", "Error Message", "Error Message (1)"]])
        tdf.add_data_row_predicate("table", predicate=lambda row: f"Oops {row['Field']}" if row["Field"] > 1 else True,
                                   predicate_name="silly", predicate_failure_response="Error Message")
        dat = tdf.TicDat(table=[[2, "what", "now"], [1, "go", "go"]])
        fails = tdf.find_data_row_failures(dat)
        self.assertTrue(set(map(tuple, fails["table", 'silly'])) == {(0, 'Oops 2')})

    def testEighteenDotOne(self):
        tdf = TicDatFactory(**dietSchema())
        num_calls=[0]
        mess_it_up=[]
        def pre_processor(dat):
            num_calls[0] += 1
            if mess_it_up:
                dat.messing_it_up+=1
            return {t:len(getattr(dat, t)) for t in tdf.all_tables}
        tdf.add_data_row_predicate("foods", lambda row, y: y==12, predicate_kwargs_maker=lambda dat: {"y":12})
        tdf.add_data_row_predicate("categories", lambda row, nutritionQuantities, foods, categories:
                                   row["name"] == "fat" or categories == 4,
                                   predicate_name="catfat", predicate_kwargs_maker=pre_processor)
        tdf.add_data_row_predicate("foods", lambda row, nutritionQuantities, foods, categories:
                                   row["name"] == "pizza" or foods == 9,
                                   predicate_name= "foodza", predicate_kwargs_maker=pre_processor)
        def dummy_kwargs_maker(dat):
            if tdf.good_tic_dat_object(dat):
                return {"x":1}
        for t in tdf.all_tables:
            tdf.add_data_row_predicate(t, lambda row, x: x==1, predicate_name=f"dummy_{t}",
                                       predicate_kwargs_maker=dummy_kwargs_maker)
        dat = tdf.copy_tic_dat(dietData())
        self.assertFalse(tdf.find_data_row_failures(dat))
        self.assertTrue(num_calls[0] == 1)
        dat.foods.pop("pizza")
        dat.categories.pop("fat")
        fails = tdf.find_data_row_failures(dat)
        self.assertTrue(num_calls[0] == 2)
        self.assertTrue(set(map(tuple, fails)) == {('categories', 'catfat'), ('foods', 'foodza')})
        self.assertTrue(set(fails['categories', 'catfat']) == set(dietData().categories).difference(["fat"]))
        self.assertTrue(set(fails['foods', 'foodza']) == set(dietData().foods).difference(["pizza"]))

        mess_it_up.append(1)
        ex = []
        try:
            tdf.find_data_row_failures(dat)
        except Exception as e:
            ex[:] = [str(e.__class__)]
        self.assertTrue("AttributeError" in ex[0])
        fails = tdf.find_data_row_failures(dat, exception_handling="Handled as Failure")
        self.assertTrue(set(map(tuple, fails)) == {('categories', 'catfat'), ('foods', 'foodza')})
        self.assertTrue(num_calls[0] == 4)
        for v in fails.values():
            self.assertTrue(v.primary_key == '*' and "no attribute" in v.error_message)
        tdf = tdf.clone()
        fails = tdf.find_data_row_failures(dat, exception_handling="Handled as Failure")
        self.assertTrue(set(map(tuple, fails)) == {('categories', 'catfat'), ('foods', 'foodza')})
        mess_it_up=[]
        def fail_on_bad_name(row, bad_name):
            if row["name"] == bad_name:
                return f"{bad_name} is bad"
            return True
        tdf.add_data_row_predicate("foods", fail_on_bad_name, predicate_name="baddy",
                                   predicate_kwargs_maker=lambda dat: {"bad_name": sorted(dat.foods)[0]},
                                   predicate_failure_response="Error Message")
        fails = tdf.find_data_row_failures(tdf.copy_tic_dat(dietData()))
        self.assertTrue(set(map(tuple, fails)) == {('foods', 'baddy')})
        self.assertTrue(len(fails['foods', 'baddy']) == 1)
        self.assertTrue(fails['foods', 'baddy'][0].error_message == "chicken is bad")

    def testNineteen(self):
        dataObj = dietData()
        tdf = TicDatFactory(**dietSchema())
        self.assertTrue(tdf.good_tic_dat_object(dataObj, row_checking="generous"))
        dataObj2 = tdf.copy_tic_dat(dataObj)
        dataObj2.categories["calories"]["minNutrition"] *= 1.00001
        dataObj2.nutritionQuantities["salad", "sodium"]["qty"]  *= 2-1.00001

        self.assertFalse(tdf._same_data(dataObj, dataObj2))
        self.assertFalse(tdf._same_data(dataObj, dataObj2, pow(.00001, 3)))
        self.assertTrue(tdf._same_data(dataObj, dataObj2, pow(.00001, 0.333)))

    def testTwenty(self):
        def make_tdf():
            tdf = TicDatFactory(data_table = [["a"], ["b", "c"]],
                                parameters = [["a"], ["b"]])
            tdf.add_parameter("Something", 100, max=100, inclusive_max=True)
            tdf.add_parameter("Another thing", 5, must_be_int=True)
            tdf.add_parameter("Untyped thing", "whatever", enforce_type_rules=False)
            tdf.add_parameter("Last", 'boo', number_allowed=False, strings_allowed='*')
            return TicDatFactory.create_from_full_schema(tdf.schema(True))

        tdf = make_tdf()
        dat = tdf.TicDat(data_table = [[1, 2, 3], [4, 5, 6]],
                         parameters = [["Something", 100], ["Another thing", 200], ["Last", "goo"],
                                       ["Untyped thing", -float("inf")]])

        self.assertFalse(tdf.find_data_row_failures(dat))
        dat.parameters["Another thing"] = 200.1
        dat.parameters["Last"] = 100
        dat.parameters["Bad P"] = dat.parameters.pop("Untyped thing")
        self.assertTrue(set(tdf.find_data_row_failures(dat)) == {("parameters", "Good Name/Value Check")})
        self.assertTrue(set(next(iter(tdf.find_data_row_failures(dat).values()))) == {"Another thing", "Last", "Bad P"})

        dat.parameters["Untyped thingy"] = dat.parameters.pop("Bad P")
        tdf = make_tdf()
        tdf.add_parameter("Another thing", 5, max=100)
        tdf.add_data_row_predicate("parameters", lambda row: "thing" in row["a"],
                                   predicate_name="Good Name/Value Check")
        tdf.add_data_row_predicate("data_table", lambda row: row["a"] + row["b"] > row["c"], predicate_name="boo")
        fails = tdf.find_data_row_failures(dat)
        self.assertTrue({k:len(v) for k,v in fails.items()} ==
                        {("parameters", "Good Name/Value Check"): 1,
                         ("parameters", 'Good Name/Value Check_0'): 3, ('data_table', "boo"): 1})

        tdf = make_tdf()
        dat = tdf.TicDat(parameters = [["Something", 90], ["Last", "boo"]])
        self.assertTrue(tdf.create_full_parameters_dict(dat) ==
                        {"Something": 90, "Another thing": 5, "Last": "boo", "Untyped thing": "whatever"})

    def testTwentyOne(self):
        simple_sch = {'tables_fields': {'commodities': [['name'], []],
                      'inflow': [['commodity', 'node'], ['quantity']],
                      'nodes': [['name'], []],
                      'cost': [['commodity', 'source', 'destination'], ['cost']],
                      'arcs': [['source', 'destination'], ['capacity']]},
                     'foreign_keys': [['arcs', 'nodes', ['destination', 'name'], 'many-to-one'],
                      ['arcs', 'nodes', ['source', 'name'], 'many-to-one'],
                      ['cost', 'commodities', ['commodity', 'name'], 'many-to-one'],
                      ['cost', 'nodes', ['destination', 'name'], 'many-to-one'],
                      ['cost', 'nodes', ['source', 'name'], 'many-to-one'],
                      ['inflow', 'commodities', ['commodity', 'name'], 'many-to-one'],
                      ['inflow', 'nodes', ['node', 'name'], 'many-to-one']],
                     'default_values': {'arcs': {'capacity': 0},
                      'cost': {'cost': 0},
                      'inflow': {'quantity': 0}},
                     'data_types': {}}
        for _class in [TicDatFactory, PanDatFactory]:
            tdf = _class.create_from_full_schema(simple_sch)
            new_sch = tdf.schema(include_ancillary_info=True)
            self.assertFalse(new_sch.pop("parameters"))
            self.assertTrue(new_sch.pop("infinity_io_flag") == "N/A")
            new_sch = json.loads(json.dumps(new_sch))
            new_sch["foreign_keys"] = sorted(new_sch["foreign_keys"])
            self.assertTrue(new_sch == simple_sch)

    def testTwentyTwo(self):
        tdf = TicDatFactory(table_with_stuffs = [["field one"], ["field two"]],
                            parameters = [["a"],["b"]])
        tdf.set_data_type("table_with_stuffs", "field one", datetime=True)
        tdf.set_data_type("table_with_stuffs", "field two", datetime=True, nullable=True)
        tdf.add_parameter("p1", "Dec 15 1970", datetime=True)
        tdf.add_parameter("p2", None, datetime=True, nullable=True)
        dat = tdf.TicDat(table_with_stuffs = [["July 11 1972", None],
                                              [datetime.datetime.now(), dateutil.parser.parse("Sept 11 2011")]],
                         parameters = [["p1", "7/11/1911"], ["p2", None]])
        self.assertFalse(tdf.find_data_type_failures(dat) or tdf.find_data_row_failures(dat))
        dat.table_with_stuffs["Nov 22 1963"] = dat.table_with_stuffs["Nov 222 1963"] = datetime.datetime.now()
        dat.parameters["p2"] = 100
        self.assertTrue(set(map(len, [tdf.find_data_type_failures(dat), tdf.find_data_row_failures(dat)])) == {1})

        all_params = tdf.create_full_parameters_dict(tdf.TicDat())
        pdf = PanDatFactory.create_from_full_schema(tdf.schema(include_ancillary_info=True))
        all_params_2 = pdf.create_full_parameters_dict(pdf.PanDat())
        self.assertTrue(all_params == all_params_2 and len(all_params) == 2)
        self.assertTrue(all_params["p1"] ==  dateutil.parser.parse("Dec 15 1970") and utils.pd.isnull(all_params["p2"]))

    def testTwentyThree(self):
        tdf = TicDatFactory(**dietSchema())
        def makeIt() :
            rtn = tdf.TicDat()
            rtn.foods["a"] = 12
            rtn.foods["b"] = None
            rtn.foods[None] = 101
            rtn.categories["1"] = {"maxNutrition":100, "minNutrition":40}
            rtn.categories["2"] = [10,20]
            for f, p in itertools.product(rtn.foods, rtn.categories):
                rtn.nutritionQuantities[f,p] = 5
            rtn.nutritionQuantities['a', 2] = 12
            return tdf.freeze_me(rtn)
        dat = makeIt()
        self.assertTrue({tuple(k):tuple(v.bad_values) for k, v in tdf.find_data_type_failures(dat).items()} ==
                        {('foods', 'name'): (None,), ('nutritionQuantities', 'food'): (None,)})
        tdf = TicDatFactory(**dietSchema())
        tdf.set_data_type("foods", "name", nullable=True, strings_allowed='*')
        tdf.set_data_type("nutritionQuantities", "food", nullable=True, strings_allowed='*')
        self.assertFalse(tdf.find_data_type_failures(dat))
        tdf.set_data_type("foods", "cost", nullable=False)
        self.assertTrue({tuple(k):tuple(v.bad_values) for k, v in tdf.find_data_type_failures(dat).items()} ==
                        {('foods', 'cost'): (None,)})

    def testTwentyFour(self):
        tdf = TicDatFactory(data =[[], ["field one", "field two"]])
        tdf.set_data_type("data", "field one")
        dat = tdf.TicDat(data=[["a", "a"], ["b", "b"]])
        tdf.replace_data_type_failures(dat)
        tdf.replace_data_type_failures(dat) # coverage
        self.assertTrue(tdf._same_data(dat, tdf.TicDat(data=[[0, "a"], [0, "b"]])))

    def testTwentyFive(self):
        core_path = os.path.join(_scratchDir, "more_coverage")
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.freeze_me(tdf.TicDat(**{t: getattr(dietData(), t) for t in tdf.primary_key_fields}))
        for attr, path in [["csv", core_path+"_csv"], ["xls", core_path+".xlsx"], ["sql", core_path+".sql"],
                           ["json", core_path+".json"]]:
            f_or_d = "directory" if attr == "csv" else "file"
            write_func, write_kwargs = utils._get_write_function_and_kwargs(tdf, path, f_or_d,
                                                                            case_space_table_names=False)
            write_func(dat, path, **write_kwargs)
            dat_1 = utils._get_dat_object(tdf, "create_tic_dat", path, f_or_d, False)
            self.assertTrue(tdf._same_data(dat, dat_1))


    def testTwentySix(self):
        data_path = os.path.join(_scratchDir, "custom_module")
        makeCleanDir(data_path)
        module_path = get_testing_file_path("funky.py")
        import ticdat.testing.funky as funky
        weirdo_hacks_needed = ["solve", "an_action", "another_action"]
        for w in weirdo_hacks_needed:
            _w = getattr(funky, w)
            _w.__module__ = "weirdo_temp_junky_thing_for_hacking"
        sys.modules[funky.solve.__module__] = funky
        dat = funky.input_schema.TicDat(table=[['c'], ['d']])
        funky.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"))
        test_args_one = [module_path, "-i", os.path.join(data_path, "input.json"), "-o",
                         os.path.join(data_path, "output.json")]
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        sln = funky.solution_schema.json.create_tic_dat(os.path.join(data_path, "output.json"))
        self.assertTrue(set(sln.table) == set(dat.table))
        test_args_two = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk", "-a", "an_action"]
        with patch.object(sys, 'argv', test_args_two):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        dat = funky.input_schema.json.create_tic_dat(os.path.join(data_path, "input.json"))
        self.assertTrue(set(sln.table).union({'a'}) == set(dat.table))
        with patch.object(sys, 'argv', test_args_one + ["-a", "another_action"]):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        dat = funky.input_schema.json.create_tic_dat(os.path.join(data_path, "input.json"))
        sln = funky.solution_schema.json.create_tic_dat(os.path.join(data_path, "output.json"))
        self.assertTrue(set(dat.table) == {'a', 'c', 'd', 'e'})
        self.assertTrue(set(sln.table) == {'c', 'd', 'e'})
        sys.modules.pop(funky.solve.__module__)

    def testTwentySeven(self):
        # this test will fail without the EnframeOfflineHandler being present. Note that EnframeOfflineHandler
        # has its own unit tests, this mainly exercises the command line
        postgresql = testing_postgresql.Postgresql()
        engine = sa.create_engine(postgresql.url())
        data_path = os.path.join(_scratchDir, "custom_module_two")
        makeCleanDir(data_path)
        module_path = get_testing_file_path("funky.py")
        import ticdat.testing.funky as funky
        for w in ["solve", "an_action", "another_action"]:
            _w = getattr(funky, w)
            _w.__module__ = "weirdo_temp_thing_for_hacking"
        sys.modules[funky.solve.__module__] = funky
        dat = funky.input_schema.TicDat(table=[['c'], ['d']])

        def make_the_json(solve_type, scenario_name="", master_schema=""):
            d = {"postgres_url": postgresql.url(), "solve_type": solve_type, "scenario_name": scenario_name,
                 "master_schema": master_schema}
            rtn = os.path.join(data_path, "ticdat_enframe.json")
            with open(rtn, "w") as f:
                json.dump(d, f, indent=2)
            return rtn
        funky.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"))
        e_json = make_the_json("Copy Input to Postgres and Solve")
        test_args_one = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "crappola", "-e", e_json]
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        solution_schema = TicDatFactory(s_table=[['field'], []])
        sln = solution_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue(set(sln.s_table) == set(dat.table) == {'c', 'd'})
        test_args_two = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk", "-a", "an_action",
                         "-e", e_json]
        with patch.object(sys, 'argv', test_args_two):
             utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        dat = funky.solution_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue(set(sln.s_table).union({'a'}) == set(dat.table))
        funky.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"), allow_overwrite=True)
        make_the_json("Copy Input To Postgres") # side effects the path
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        make_the_json("Proxy Enframe Solve") # side effects the path
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        sln = solution_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue(set(sln.s_table) == {'a', 'c', 'd'})
        test_args_three = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk",
                           "-a", "another_action", "-e", e_json]
        with patch.object(sys, 'argv', test_args_three):
            utils.standard_main(funky.input_schema, funky.solution_schema, funky.solve)
        sln = solution_schema.pgsql.create_tic_dat(engine, "scenario_1")
        dat = funky.solution_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue(set(sln.s_table) == set(dat.table) == {'a', 'c', 'd', 'e'})
        engine.dispose()
        postgresql.stop()
        sys.modules.pop(funky.solve.__module__)

    def testTwentyEight(self):
        sch = """
        {"tables_fields": {"categories": [["Name"], ["Min Nutrition", "Max Nutrition"]], 
                           "foods": [["Name"], ["Cost"]], 
                           "nutrition_quantities": [["Food", "Category"], ["Quantity"]]}, 
         "foreign_keys": [["nutrition_quantities", "foods", ["Food", "Name"]], 
                          ["nutrition_quantities", "categories", ["Category", "Name"]]], 
         "default_values": {"categories": {"Min Nutrition": 0, "Max Nutrition": Infinity}, 
                            "foods": {"Cost": 0}, "nutrition_quantities": {"Quantity": 0}}, 
         "data_types": {"categories": {"Min Nutrition": [true, true, false, 0, Infinity, false, [], false, false], 
                                       "Max Nutrition": [true, true, true, 0, Infinity, false, [], false, false]}, 
         "foods": {"Cost": [true, true, false, 0, Infinity, false, [], false, false]}, 
         "nutrition_quantities": {"Quantity": [true, true, false, 0, Infinity, false, [], false, false]}}, 
         "parameters": {}, "infinity_io_flag": "N/A"}
        """
        for factory in (TicDatFactory, PanDatFactory):
            tdf = factory.create_from_full_schema(json.loads(sch))
            tdf.add_data_row_predicate("categories", predicate_name="Min Max Check",
                predicate=lambda row : row["Max Nutrition"] >= row["Min Nutrition"])
            tdf = tdf.clone(table_restrictions=["categories", "nutrition_quantities"])
            self.assertTrue(list(tdf._data_row_predicates) == ["categories"])
            self.assertTrue(list(tdf._data_row_predicates['categories']) == ["Min Max Check"])
            small_sch = json.loads(json.dumps(tdf.schema(include_ancillary_info=True)))
            inf = float("inf")
            self.assertTrue(small_sch ==
                {'tables_fields': {'nutrition_quantities': [['Food', 'Category'],['Quantity']],
                                    'categories': [['Name'], ['Min Nutrition', 'Max Nutrition']]},
                 'foreign_keys': [['nutrition_quantities','categories', ['Category', 'Name'], 'many-to-one']],
                 'default_values': {'nutrition_quantities': {'Quantity': 0},
                                    'categories': {'Min Nutrition': 0, 'Max Nutrition': inf}},
                 'data_types': {'categories': {'Min Nutrition': [True, True, False, 0, inf, False, [], False, False],
                                                'Max Nutrition': [True, True, True, 0, inf, False, [], False, False]},
                                 'nutrition_quantities': {'Quantity': [True, True, False, 0, inf, False, [], False,
                                                                       False]}},
                 'parameters': {}, 'infinity_io_flag': 'N/A'})

    def testTwentyNine(self):
        data_path = os.path.join(_scratchDir, "custom_module_three")
        makeCleanDir(data_path)
        module_path = get_testing_file_path("funky_diet.py")
        import ticdat.testing.funky_diet as funky_diet
        funky_diet.solve.__module__ = "weirdo_temp_junky_thing_for_hacking"
        sys.modules[funky_diet.solve.__module__] = funky_diet
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        d = json.loads(tdf.json.write_file(dat, "", verbose=False))
        d["nutrition_quantities"] = d.pop("nutritionQuantities")
        dat = funky_diet.input_schema.TicDat(**d)
        dat.stupid_table["ju", "nk"] = 10
        funky_diet.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"))
        test_args_one = [module_path, "-i", os.path.join(data_path, "input.json"), "-o",
                         os.path.join(data_path, "output.json")]
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        def read_sln():
            f = os.path.join(data_path, "output.json")
            with open(f, "r") as _f:
                d = json.load(_f)
                self.assertTrue(set(d) == {'parameters', 'buy_food', 'consume_nutrition'})
            rtn = funky_diet.solution_schema.json.create_tic_dat(f)
            self.assertFalse(rtn.weird_table)
            return rtn
        sln = read_sln()
        self.assertTrue({t:len(getattr(sln, t)) for t in funky_diet.solution_schema.all_tables} ==
                        {"buy_food": 3, "consume_nutrition": 4, "weird_table": 0, "parameters": 1})

        dat.nutrition_quantities["pizza", "junk"] = {}
        dat.categories["weirdness"] = dat.categories["wokeness"] = [100, 20]
        funky_diet.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"), allow_overwrite=True)
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        sln = read_sln()
        self.assertTrue({t:len(getattr(sln, t)) for t in funky_diet.solution_schema.all_tables} ==
                        {"buy_food": 3, "consume_nutrition": 4, "weird_table": 0, "parameters": 3})
        self.assertTrue(sln.parameters["find_foreign_key_failures"]["Value"] == 1)
        self.assertTrue(sln.parameters["find_data_row_failures"]["Value"] == 2)

        dat = funky_diet.input_schema.TicDat(**d)
        funky_diet.input_schema.json.write_file(dat, os.path.join(data_path, "pizza.json"))
        with patch.object(sys, 'argv', [module_path, "-i", os.path.join(data_path, "pizza.json"), "-o", "trash",
                                        "-a", "remove_the_pizza"]):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        with open(os.path.join(data_path, "pizza.json"), "r") as _f:
            d = json.load(_f)
        self.assertTrue({k: len(v) for k, v in d.items()} == {"foods": 8, "nutrition_quantities": 32})

        with patch.object(sys, 'argv', [module_path, "-i", "junk", "-o", os.path.join(data_path, "output.json"),
                                        "-a", "checks_the_unit_test_result"]):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)

        with patch.object(sys, 'argv', [module_path, "-i", os.path.join(data_path, "input.json"),
                                        "-o", os.path.join(data_path, "output_csvs")]):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        self.assertTrue(os.path.exists(os.path.join(data_path, "output_csvs", "consume_nutrition.csv")))
        with patch.object(sys, 'argv', [module_path, "-i", os.path.join(data_path, "input.json"),
                                        "-o", os.path.join(data_path, "output_csvs_2")]):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve,
                                case_space_table_names=True)
        self.assertTrue(os.path.exists(os.path.join(data_path, "output_csvs_2", "Consume Nutrition.csv")))
        sys.modules.pop(funky_diet.solve.__module__)

    def testThirty(self):
        # this test will fail without the EnframeOfflineHandler being present. Note that EnframeOfflineHandler
        # has its own unit tests, this mainly exercises the command line
        postgresql = testing_postgresql.Postgresql()
        engine = sa.create_engine(postgresql.url())
        data_path = os.path.join(_scratchDir, "custom_module_four")
        makeCleanDir(data_path)
        module_path = get_testing_file_path("funky_diet.py")

        import ticdat.testing.funky_diet as funky_diet
        for w in ["solve", "remove_the_pizza", "checks_the_unit_test_result", "a_solvish_act"]:
            _w = getattr(funky_diet, w)
            _w.__module__ = "weirdo_temp_junky_thing_for_hacking"
        sys.modules[funky_diet.solve.__module__] = funky_diet
        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        d = json.loads(tdf.json.write_file(dat, "", verbose=False))
        d["nutrition_quantities"] = d.pop("nutritionQuantities")
        dat = funky_diet.input_schema.TicDat(**d)
        clean_dat = funky_diet.input_schema.copy_tic_dat(dat)
        clean_dat.stupid_table['a', 'b'] = 2
        dat.stupid_table["ju", "nk"] = 10
        funky_diet.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"))
        def make_the_json(solve_type, scenario_name="", master_schema=""):
            d = {"postgres_url": postgresql.url(), "solve_type": solve_type, "scenario_name": scenario_name,
                 "master_schema": master_schema}
            rtn = os.path.join(data_path, "ticdat_enframe.json")
            with open(rtn, "w") as f:
                json.dump(d, f, indent=2)
            return rtn
        e_json = make_the_json("Copy Input to Postgres and Solve")
        test_args_one = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk", "-e", e_json]
        with patch.object(sys, 'argv', test_args_one):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        full_schema = TicDatFactory(**{"s_"+k: v for k,v in funky_diet.solution_schema.schema().items()})
        self.assertTrue(set(full_schema.pgsql.check_tables_fields(engine, "scenario_1")) == {'s_weird_table'})
        sln = full_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue({t: len(getattr(sln, "s_"+t)) for t in funky_diet.solution_schema.all_tables} ==
                        {"buy_food": 3, "consume_nutrition": 4, "weird_table": 0, "parameters": 1})

        dat.nutrition_quantities["pizza", "junk"] = {}
        dat.categories["weirdness"] = dat.categories["wokeness"] = [100, 20]
        funky_diet.input_schema.json.write_file(dat, os.path.join(data_path, "input.json"), allow_overwrite=True)
        test_args_two = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk", "-e", e_json,
                         "-a", "a_solvish_act"]
        with patch.object(sys, 'argv', test_args_two):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        sln = full_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue({t:len(getattr(sln, "s_"+t)) for t in funky_diet.solution_schema.all_tables} ==
                        {"buy_food": 3, "consume_nutrition": 4, "weird_table": 0, "parameters": 3})
        self.assertTrue(sln.s_parameters["find_foreign_key_failures"]["Value"] == 1)
        self.assertTrue(sln.s_parameters["find_data_row_failures"]["Value"] == 2)

        e_json = make_the_json("Proxy Enframe Solve")
        test_args_three = [module_path, "-i", "junk", "-o", "also_junk", "-e", e_json,
                         "-a", "checks_the_unit_test_result"]
        with patch.object(sys, 'argv', test_args_three):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)

        test_args_four = [module_path, "-i", "junk", "-o", "also_junk", "-e", e_json,
                         "-a", "remove_the_pizza"]
        with patch.object(sys, 'argv', test_args_four):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        dat = funky_diet.input_schema.pgsql.create_tic_dat(engine, "scenario_1")
        self.assertTrue({t: len(getattr(dat, t)) for t in funky_diet.input_schema.all_tables} ==
                        {"foods": 8, "nutrition_quantities": 32, "categories":6, "stupid_table": 0})

        engine.dispose()
        postgresql.stop()

        postgresql = testing_postgresql.Postgresql()
        engine = sa.create_engine(postgresql.url())
        funky_diet.input_schema.json.write_file(clean_dat, os.path.join(data_path, "input.json"), allow_overwrite=True)
        e_json = make_the_json("Copy Input to Postgres", master_schema="the_master", scenario_name="the_scenario")
        test_args_five = [module_path, "-i", os.path.join(data_path, "input.json"), "-o", "junk", "-e", e_json]
        with patch.object(sys, 'argv', test_args_five):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        e_json = make_the_json("proxy EnFramE solve", master_schema="the_master", scenario_name="the_scenario")
        test_args_six = [module_path, "-i", "junk", "-o", "alsojunk", "-e", e_json]
        with patch.object(sys, 'argv', test_args_six):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)

        self.assertTrue([('hamburger', 1, 'the_scenario'), ('ice cream', 1, 'the_scenario'),
                         ('milk', 1, 'the_scenario')] ==
                        sorted(_[:3] for _ in engine.execute("Select * from the_master.s_buy_food")))

        self.assertTrue(sorted(engine.execute("Select * from the_master.foods")) ==
                        [('chicken', 1, 'the_scenario', 2.89), ('fries', 1, 'the_scenario', 1.89),
                         ('hamburger', 1, 'the_scenario', 2.49), ('hot dog', 1, 'the_scenario', 1.5),
                         ('ice cream', 1, 'the_scenario', 1.59), ('macaroni', 1, 'the_scenario', 2.09),
                         ('milk', 1, 'the_scenario', 0.89), ('pizza', 1, 'the_scenario', 1.99),
                         ('salad', 1, 'the_scenario', 2.49)])

        test_args_seven = [module_path, "-i", "junk", "-o", "alsojunk", "-e", e_json, "-a", "remove_the_pizza"]
        with patch.object(sys, 'argv', test_args_seven):
            utils.standard_main(funky_diet.input_schema, funky_diet.solution_schema, funky_diet.solve)
        self.assertTrue(set(_[0] for _ in engine.execute("Select * from the_master.foods")) ==
                        set(_[0] for _ in engine.execute("Select * from scenario_1.foods")) ==
                        {'hamburger', 'ice cream', 'salad', 'milk', 'macaroni', 'fries', 'chicken', 'hot dog'})

        engine.dispose()
        postgresql.stop()
        sys.modules.pop(funky_diet.solve.__module__)

    def testThirtyOne(self):
        slicer  = utils.Slicer(itertools.product([1, 2], [(1, 2, 3), (2, 3, 4)], [4, 5, 6]))
        self.assertTrue(set(slicer.slice('*', '*', 5)) ==
                        {(1, (2, 3, 4), 5), (2, (2, 3, 4), 5), (1, (1, 2, 3), 5), (2, (1, 2, 3), 5)})
        self.assertTrue(set(slicer.slice('*', (2, 3, 4), 5)) =={(1, (2, 3, 4), 5), (2, (2, 3, 4), 5)})

    def test_good_ticdat_object_strict(self):
        small_sch = TicDatFactory(** {
     "categories" : (("name",),["maxNutrition"]),
     "foods" :[["name"],("cost",)],
     "nutritionQuantities" : (["food", "category"], [])
    })
        full_sch = TicDatFactory(**dietSchema())
        small_dat = small_sch.TicDat(categories = [["boo", 100], ["woo", 200]],
                                     foods = [["this", 10], ["that", 100], ["theother", 12]])
        for f,c in itertools.product(small_dat.categories, small_dat.foods):
            small_dat.nutritionQuantities[f, c] = {}
        # we allow missing fields in the copy from object (this facilitates construction)
        fuller_copy = full_sch.copy_tic_dat(small_dat)
        ex = []
        try: # we don't allow extra field in the copy from object (perhaps we should, but easy enough to munge away)
            small_sch.copy_tic_dat(fuller_copy)
        except Exception as _:
            ex.append(str(_))
        self.assertTrue("Inconsistent data field name keys" in ex[0])
        self.assertTrue(full_sch.good_tic_dat_object(small_dat, row_checking="generous"))
        self.assertFalse(full_sch.good_tic_dat_object(small_dat))
        df = pd.DataFrame({"food":["boo", "woo"], "category": ["this", "that"]})
        df.set_index(["food", "category"], inplace=True, drop=False)
        self.assertTrue(small_sch.good_tic_dat_table(df, "nutritionQuantities"))
        self.assertFalse(small_sch.good_tic_dat_table(df, "nutritionQuantities", row_checking="strict"))
        by_rows = [{"food": "boo", "category": "woo"}]
        self.assertTrue(small_sch.good_tic_dat_table(by_rows, "nutritionQuantities"))
        self.assertFalse(small_sch.good_tic_dat_table(by_rows, "nutritionQuantities", row_checking="strict"))
        by_rows = {"boo":1, "foo": 2}
        self.assertTrue(small_sch.good_tic_dat_table(by_rows, "foods"))
        self.assertFalse(small_sch.good_tic_dat_table(by_rows, "foods", row_checking="strict"))

    def test_data_type_max_failures(self):
        tdf = TicDatFactory(table_one = [["Field"], []], table_two = [[], ["Field"]])
        for t in ["table_one", "table_two"]:
            tdf.set_data_type(t, "Field")
        dat = tdf.TicDat(table_one=[[_] for _ in range(1,11)] + [[-_] for _ in range(1,11)],
                         table_two=[[10.1]]*10 + [[-2]]*10)
        errs = tdf.find_data_type_failures(dat)
        self.assertTrue(len(errs) == 2 and all(len(_.pks) == 10 for _ in errs.values()))
        errs = tdf.find_data_type_failures(dat, 11)
        self.assertTrue(len(errs) == 2)
        self.assertTrue(any(len(_.pks) == 10 for _ in errs.values()) and any(len(_.pks) == 1 for _ in errs.values()))
        errs = tdf.find_data_type_failures(dat, 10)
        self.assertTrue(len(errs) == 1 and all(len(_.pks) == 10 for _ in errs.values()))
        errs = tdf.find_data_type_failures(dat, 9)
        self.assertTrue(len(errs) == 1 and all(len(_.pks) == 9 for _ in errs.values()))

    def test_data_row_max_failures(self):
        tdf = TicDatFactory(table_one = [["Field"], []], table_two = [[], ["Field"]])
        for t in ["table_one", "table_two"]:
            tdf.set_data_type(t, "Field")
        for table, dts in tdf.data_types.items():
            for field, dt in dts.items():
                if table == "table_one":
                    tdf.add_data_row_predicate(table, lambda row: dt.valid_data(row["Field"]))
                else:
                    tdf.add_data_row_predicate(table, lambda row: True if not dt.valid_data(row["Field"]) else "Oops",
                                               predicate_failure_response="Error Message")
        dat = tdf.TicDat(table_one=[[_] for _ in range(1,11)] + [[-_] for _ in range(1,11)],
                         table_two=[[10.1]]*10 + [[-2]]*10)
        errs = tdf.find_data_row_failures(dat)
        self.assertTrue(len(errs) == 2 and all(len(_) == 10 for _ in errs.values()))
        errs = tdf.find_data_row_failures(dat, max_failures=11)
        self.assertTrue(len(errs) == 2 and set(map(len, errs.values())) == {10, 1})
        errs = tdf.find_data_row_failures(dat, max_failures=10)
        self.assertTrue(len(errs) == 1 and all(len(_) == 10 for _ in errs.values()))
        errs = tdf.find_data_row_failures(dat, max_failures=9)
        self.assertTrue(len(errs) == 1 and all(len(_) == 9 for _ in errs.values()))

    def test_fk_max_failures(self):
        tdf = TicDatFactory(**dietSchema())
        addDietForeignKeys(tdf)
        dat = tdf.TicDat(nutritionQuantities=[[f"food_{_}", f"cat_{_}", 10] for _ in range(10)])
        errs = tdf.find_foreign_key_failures(dat)
        self.assertTrue(len(errs) == 2 and all(len(_.native_pks) == 10 for _ in errs.values()))
        errs = tdf.find_foreign_key_failures(dat, max_failures=11)
        self.assertTrue(len(errs) == 2 and set(map(len, [_.native_pks for _ in errs.values()])) == {10, 1})
        errs = tdf.find_foreign_key_failures(dat, max_failures=10)
        self.assertTrue(len(errs) == 1 and all(len(_.native_pks) == 10 for _ in errs.values()))
        errs = tdf.find_foreign_key_failures(dat, max_failures=9)
        self.assertTrue(len(errs) == 1 and all(len(_.native_pks) == 9 for _ in errs.values()))

_scratchDir = TestUtils.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    unittest.main()
