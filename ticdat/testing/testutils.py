import sys
import unittest
import ticdat.utils as utils
from ticdat import LogFile, Progress
from ticdat.ticdatfactory import TicDatFactory, _ForeignKey, _ForeignKeyMapping
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, netflowSchema, firesException, memo
from ticdat.testing.ticdattestutils import sillyMeData, sillyMeSchema, makeCleanDir, fail_to_debugger, flagged_as_run_alone
from ticdat.testing.ticdattestutils import assertTicDatTablesSame, DEBUG, addNetflowForeignKeys, addDietForeignKeys
from ticdat.testing.ticdattestutils import spacesSchema, spacesData, clean_denormalization_errors
import os
import itertools
import shutil

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
            self.assertTrue(set(tdf.foreign_keys) == set(tdf2.foreign_keys))
            self.assertTrue(tdf.data_types == tdf2.data_types)
            self.assertTrue(tdf.default_values == tdf2.default_values)
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

    def testDenormalizedErrors(self):
        c = clean_denormalization_errors
        tdf = TicDatFactory(**spacesSchema())
        dat = tdf.TicDat(**spacesData())
        self.assertFalse(tdf.find_denormalized_sub_table_failures(dat, "b_table", "b Field 1",
                                                                  ("b Field 2", "b Field 3")))
        dat.b_table[2,2,3] = "boger"
        self.assertFalse(tdf.find_denormalized_sub_table_failures(dat, "b_table", "b Field 1",
                                                                  ("b Field 2", "b Field 3")))
        chk = tdf.find_denormalized_sub_table_failures(dat, "b_table", "b Field 2",
                                                                  ("b Field 1", "b Field 3"))
        self.assertTrue(c(chk) == {2: {'b Field 1': {1, 2}}})
        dat.b_table[2,2,4] = "boger"
        dat.b_table[1,'b','b'] = "boger"
        chk = tdf.find_denormalized_sub_table_failures(dat, "b_table", ["b Field 2"],
                                                            ("b Field 1", "b Field 3", "b Data"))
        self.assertTrue(c(chk) == c({2: {'b Field 3': (3, 4), 'b Data': (1, 'boger'), 'b Field 1': (1, 2)},
                                 'b': {'b Data': ('boger', 12), 'b Field 1': ('a', 1)}}))

        ex = self.firesException(lambda : tdf.find_denormalized_sub_table_failures(dat, "b_table", ["b Data"],"wtf"))
        self.assertTrue("wtf isn't a key" in ex)


        chk = utils.find_denormalized_sub_table_failures(tuple(map(dict, dat.c_table)),
                        pk_fields=["c Data 1", "c Data 2"], data_fields=["c Data 3", "c Data 4"])
        self.assertTrue(c(chk) == {('a', 'b'): {'c Data 3': {'c', 12}, 'c Data 4': {24, 'd'}}})
        dat.c_table.append((1, 2, 3, 4))
        dat.c_table.append((1, 2, 1, 4))
        dat.c_table.append((1, 2, 1, 5))
        dat.c_table.append((1, 2, 3, 6))
        chk = utils.find_denormalized_sub_table_failures(dat.c_table,
                        pk_fields=["c Data 1", "c Data 2"], data_fields=["c Data 3", "c Data 4"])
        self.assertTrue(c(chk) == {('a', 'b'): {'c Data 3': {'c', 12}, 'c Data 4': {24, 'd'}},
            (1,2):{'c Data 3':{3,1}, 'c Data 4':{4,5,6}}})

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
        self.assertTrue(tdf.good_tic_dat_object(dataObj))
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
        self.assertFalse(tdf.good_tic_dat_object(dataObj) or
                         tdf.good_tic_dat_object(dataObj, bad_message_handler =msg.append))
        self.assertTrue({"foods : Inconsistent key lengths"} == set(msg))
        self.assertTrue(all(tdf.good_tic_dat_table(getattr(dataObj, t), t)
                            for t in ("categories", "nutritionQuantities")))

        dataObj = dietData()
        dataObj.categories["boger"] = {"cost":1}
        dataObj.categories["boger"] = {"cost":1}
        self.assertFalse(tdf.good_tic_dat_object(dataObj) or
                         tdf.good_tic_dat_object(dataObj, bad_message_handler=msg.append))
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
        self.assertTrue(newFactory.good_tic_dat_object(ticDat))
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
        fk, fkm = _ForeignKey, _ForeignKeyMapping
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
        self.assertTrue("X-to-many" in self.firesException(lambda :
                tdf.add_foreign_key("badChild", "parentTable", ["bd", "pd2"])))
        tdf.add_foreign_key("appendageChild", "parentTable", ["ak", "pk"])
        tdf.add_foreign_key("appendageBadChild", "badChild", (("bk2", "bk2"), ("bk1","bk1")))
        fks = tdf.foreign_keys
        _getfk = lambda t : next(_ for _ in fks if _.native_table == t)
        self.assertTrue(_getfk("goodChild").cardinality == "many-to-one")
        self.assertTrue(_getfk("badChild").cardinality == "many-to-one")
        self.assertTrue(_getfk("appendageChild").cardinality == "one-to-one")
        self.assertTrue(_getfk("appendageBadChild").cardinality == "one-to-one")

        tdf.clear_foreign_keys("appendageBadChild")
        self.assertTrue(tdf.foreign_keys and "appendageBadChild" not in tdf.foreign_keys)
        tdf.clear_foreign_keys()
        self.assertFalse(tdf.foreign_keys)

    def testSix(self):
        for cloning in [True, False]:
            clone_me_maybe = lambda x : x.clone() if cloning else x

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

            fk, fkm = _ForeignKey, _ForeignKeyMapping
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) == tdf.find_foreign_key_failures(badDat2) ==
                            {fk('production', 'lines', fkm('line', 'name'), 'many-to-one'):
                                 (('notaline',), (('notaline', 'widgets'),))})
            badDat1.lines["notaline"]["plant"] = badDat2.lines["notaline"]["plant"] = "notnewark"
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) == tdf.find_foreign_key_failures(badDat2) ==
                            {fk('lines', 'plants', fkm('plant', 'name'), 'many-to-one'):
                                 (('notnewark',), ('notaline',))})
            tdf.remove_foreign_keys_failures(badDat1, propagate=False)
            tdf.remove_foreign_keys_failures(badDat2, propagate=True)
            self.assertTrue(tdf._same_data(badDat2, goodDat) and not tdf.find_foreign_key_failures(badDat2))
            self.assertTrue(tdf.find_foreign_key_failures(badDat1) ==
                    {fk('production', 'lines', fkm('line', 'name'), 'many-to-one'):
                         (('notaline',), (('notaline', 'widgets'),))})

            tdf.remove_foreign_keys_failures(badDat1, propagate=False)
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
        for cloning in [True, False]:
            clone_me_maybe = lambda x : x.clone() if cloning else x

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
            self.assertTrue(self.firesException(lambda : po.mip_progress("this", 2.1, 2)))
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
        self.assertTrue(badPks is None)
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
        for cloning in [True, False]:
            clone_me_maybe = lambda x : x.clone() if cloning else x
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
            tdf = clone_me_maybe(tdf)
            failures = tdf.find_data_row_failures(dat)
            self.assertTrue(any(k for k in failures if
                                k.table == "nutritionQuantities" and k.predicate_name == 0))
            self.assertTrue(any(k for k in failures if
                                k.table == "categories" and k.predicate_name == "minmax"))
            self.assertTrue(failures["categories","minmax"] == (2,))
            self.assertTrue(set(failures["nutritionQuantities", 0]) == {("a",3), ("c",3)})
            tdf.add_data_row_predicate("nutritionQuantities", predicate=None, predicate_name=0)
            self.assertTrue(set(tdf.find_data_row_failures(dat)) == {("categories","minmax")})
            for i in range(1,4):
                tdf.add_data_row_predicate("nutritionQuantities",
                    (lambda j : lambda row : row["category"] < 3 or row["qty"] % j)(i))
            tdf = clone_me_maybe(tdf)
            failures = tdf.find_data_row_failures(dat)
            self.assertTrue(failures["categories","minmax"] == (2,))
            self.assertTrue(set(failures["nutritionQuantities", 0]) == {("a",3), ("b",3), ("c",3)})
            self.assertTrue(set(failures["nutritionQuantities", 1]) ==
                            set(failures["nutritionQuantities", 2]) == {("a",3), ("c",3)})

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




_scratchDir = TestUtils.__name__ + "_scratch"


# Run the tests.
if __name__ == "__main__":
    unittest.main()
