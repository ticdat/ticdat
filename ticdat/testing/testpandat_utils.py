import unittest
from ticdat.pandatfactory import PanDatFactory
from ticdat.utils import DataFrame, numericish, ForeignKey, ForeignKeyMapping
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, netflowPandasData
from ticdat.testing.ticdattestutils import netflowSchema, copy_to_pandas_with_reset, dietSchema, netflowData
from ticdat.testing.ticdattestutils import addNetflowForeignKeys, sillyMeSchema, dietData, pan_dat_maker
from ticdat.ticdatfactory import TicDatFactory
import itertools
from math import isnan

def _deep_anonymize(x)  :
    if not hasattr(x, "__contains__") or utils.stringish(x):
        return x
    if utils.dictish(x) :
        return {_deep_anonymize(k):_deep_anonymize(v) for k,v in x.items()}
    return list(map(_deep_anonymize,x))

#uncomment decorator to drop into debugger for assertTrue, assertFalse failures
#@fail_to_debugger
class TestUtils(unittest.TestCase):
    canRun = False

    def testSimple(self):
        if not self.canRun:
            return
        pdf = PanDatFactory(**netflowSchema())
        _dat = netflowPandasData()
        dat = pdf.PanDat(**{t:getattr(_dat, t) for t in pdf.all_tables})
        self.assertTrue(pdf.good_pan_dat_object(dat))

        dat2 = pdf.copy_pan_dat(dat)
        self.assertTrue(pdf._same_data(dat, dat2))
        self.assertTrue(pdf.good_pan_dat_object(dat2))
        delattr(dat2, "nodes")
        msg = []
        self.assertFalse(pdf.good_pan_dat_object(dat2, msg.append))
        self.assertTrue(msg[-1] == "nodes not an attribute.")

        dat3 = pdf.copy_pan_dat(dat)
        dat3.cost.drop("commodity", axis=1, inplace=True)
        self.assertFalse(pdf.good_pan_dat_object(dat3, msg.append))
        self.assertTrue("The following are (table, field) pairs missing from the data" in msg[-1])

        dat4 = pdf.copy_pan_dat(dat)
        dat4.cost["cost"] += 1
        self.assertFalse(pdf._same_data(dat, dat4))

        pdf2 = PanDatFactory(**{t:'*' for t in pdf.all_tables})
        dat5 = pdf2.copy_pan_dat(dat)
        self.assertTrue(pdf._same_data(dat, dat5))
        self.assertTrue(pdf2._same_data(dat, dat5))
        dat.commodities = dat.commodities.append(dat.commodities[dat.commodities["name"] == "Pencils"])
        dat.arcs = dat.arcs.append(dat.arcs[dat.arcs["destination"] == "Boston"])
        self.assertFalse(pdf2._same_data(dat, dat5))
        self.assertFalse(pdf._same_data(dat, dat5))

    def testDataTypes(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())

        ticdat = tdf.TicDat()
        ticdat.foods["a"] = 12
        ticdat.foods["b"] = None
        ticdat.categories["1"] = {"maxNutrition":100, "minNutrition":40}
        ticdat.categories["2"] = [10,20]
        for f, p in itertools.product(ticdat.foods, ticdat.categories):
            ticdat.nutritionQuantities[f,p] = 5
        ticdat.nutritionQuantities['a', 2] = 12

        pandat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticdat))

        self.assertFalse(pdf.find_data_type_failures(pandat))

        pdf = PanDatFactory(**dietSchema())
        pdf.set_data_type("foods", "cost", nullable=False)
        pdf.set_data_type("nutritionQuantities", "qty", min=5, inclusive_min=False, max=12, inclusive_max=True)
        failed = pdf.find_data_type_failures(pandat)
        self.assertTrue(set(failed) == {('foods', 'cost'), ('nutritionQuantities', 'qty')})
        self.assertTrue(set(failed['foods', 'cost']["name"]) == {'b'})
        self.assertTrue(set({(v["food"], v["category"])
                             for v in failed['nutritionQuantities', 'qty'].T.to_dict().values()}) ==
                            {('b', '1'), ('a', '2'), ('a', '1'), ('b', '2')})

        failed = pdf.find_data_type_failures(pandat, as_table=False)
        self.assertTrue(4 == failed['nutritionQuantities', 'qty'].value_counts()[True])

        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        addNetflowForeignKeys(tdf)
        pdf = PanDatFactory(**netflowSchema())
        ticdat = tdf.copy_tic_dat(netflowData())
        for n in ticdat.nodes["Detroit"].arcs_source:
            ticdat.arcs["Detroit", n] = n
        pandat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticdat))
        self.assertFalse(pdf.find_data_type_failures(pandat))

        pdf = PanDatFactory(**netflowSchema())
        pdf.set_data_type("arcs", "capacity", strings_allowed="*")
        self.assertFalse(pdf.find_data_type_failures(pandat))

        pdf = PanDatFactory(**netflowSchema())
        pdf.set_data_type("arcs", "capacity", strings_allowed=["Boston", "Seattle", "lumberjack"])
        failed = pdf.find_data_type_failures(pandat)
        self.assertTrue(set(failed) == {('arcs', 'capacity')})
        self.assertTrue(set({(v["source"], v["destination"])
                             for v in failed['arcs', 'capacity'].T.to_dict().values()}) == {("Detroit", "New York")})

    def testDataTypes_two(self):
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**tdf.schema())
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
            return tdf.copy_to_pandas(rtn, drop_pk_columns=False)
        dat = makeIt()
        errs = pdf.find_data_type_failures(dat)
        self.assertTrue(len(errs) == 2)
        from pandas import isnull
        def noneify(iter_of_tuples):
            return {tuple(None if isnull(_) else _ for _ in tuple_) for tuple_ in iter_of_tuples}
        self.assertTrue(noneify(errs['nutritionQuantities', 'food'].itertuples(index=False)) ==
                        {(None, "1", 5), (None, "2", 5)})
        self.assertTrue(noneify(errs['foods', 'name'].itertuples(index=False)) == {(None, 101)})
        pdf = PanDatFactory(**tdf.schema())
        pdf.set_data_type("foods", "name", nullable=True, strings_allowed='*')
        pdf.set_data_type("nutritionQuantities", "food", nullable=True, strings_allowed='*')
        self.assertFalse(pdf.find_data_type_failures(dat))
        pdf.set_data_type("foods", "cost", nullable=False)
        errs = pdf.find_data_type_failures(dat)
        self.assertTrue(len(errs) == 1)
        self.assertTrue(noneify(errs['foods', 'cost'].itertuples(index=False)) == {('b', None)})

    def testDataPredicates(self):
        if not self.canRun:
            return
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())

        ticdat = tdf.TicDat()
        ticdat.foods["a"] = 12
        ticdat.foods["b"] = None
        ticdat.categories["1"] = {"maxNutrition":100, "minNutrition":40}
        ticdat.categories["2"] = [21,20]
        for f, p in itertools.product(ticdat.foods, ticdat.categories):
            ticdat.nutritionQuantities[f,p] = 5
        ticdat.nutritionQuantities['a', 2] = 12

        pandat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticdat))
        self.assertFalse(pdf.find_duplicates(pandat))

        self.assertFalse(pdf.find_data_row_failures(pandat))


        def perform_predicate_checks(sch):
            pdf = PanDatFactory(**sch)
            pdf.add_data_row_predicate("foods", lambda row: numericish(row["cost"]) and not isnan(row["cost"]), "cost")
            good_qty = lambda qty :  numericish(qty) and 5 < qty <= 12
            pdf.add_data_row_predicate("nutritionQuantities", lambda row: good_qty(row["qty"]), "qty")
            pdf.add_data_row_predicate("categories",
                                       lambda row: all(map(numericish, [row["minNutrition"], row["maxNutrition"]]))
                                                   and row["maxNutrition"] >= row["minNutrition"],
                                       "minmax")
            failed = pdf.find_data_row_failures(pandat)
            self.assertTrue(set(failed) == {('foods', 'cost'), ('nutritionQuantities', 'qty'), ('categories', 'minmax')})
            self.assertTrue(set(failed['foods', 'cost']["name"]) == {'b'})
            self.assertTrue(set({(v["food"], v["category"])
                                 for v in failed['nutritionQuantities', 'qty'].T.to_dict().values()}) ==
                                {('b', '1'), ('a', '2'), ('a', '1'), ('b', '2')})
            self.assertTrue(set(failed['categories', 'minmax']["name"]) == {'2'})
            failed = pdf.find_data_row_failures(pandat, as_table=False)
            self.assertTrue(4 == failed['nutritionQuantities', 'qty'].value_counts()[True])
        perform_predicate_checks(dietSchema())
        perform_predicate_checks({t:'*' for t in dietSchema()})

        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        addNetflowForeignKeys(tdf)
        pdf = PanDatFactory(**netflowSchema())
        ticdat = tdf.copy_tic_dat(netflowData())
        for n in ticdat.nodes["Detroit"].arcs_source:
            ticdat.arcs["Detroit", n] = n
        pandat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticdat))
        self.assertFalse(pdf.find_duplicates(pandat))
        self.assertFalse(pdf.find_data_row_failures(pandat))

        pdf = PanDatFactory(**netflowSchema())
        pdf.add_data_row_predicate("arcs", lambda row: True, "capacity")
        self.assertFalse(pdf.find_data_row_failures(pandat))

        pdf = PanDatFactory(**netflowSchema())
        good_capacity = lambda capacity: numericish(capacity) or capacity in ["Boston", "Seattle", "lumberjack"]
        pdf.add_data_row_predicate("arcs", lambda row: good_capacity(row["capacity"]), "capacity")
        failed = pdf.find_data_row_failures(pandat)
        self.assertTrue(set(failed) == {('arcs', 'capacity')})
        self.assertTrue(set({(v["source"], v["destination"])
                             for v in failed['arcs', 'capacity'].T.to_dict().values()}) == {("Detroit", "New York")})

    def testXToMany(self):
        input_schema = PanDatFactory (roster = [["Name"],["Grade", "Arrival Inning", "Departure Inning",
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

        tdf = TicDatFactory(**input_schema.schema())
        dat = tdf.TicDat()
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

        orig_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertFalse(input_schema.find_foreign_key_failures(orig_pan_dat))

        dat.position_constraints["no", "no", "no"] = dat.position_constraints[1, 2, 3] = {}
        new_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertFalse(input_schema._same_data(orig_pan_dat, new_pan_dat))
        fk_fails = input_schema.find_foreign_key_failures(new_pan_dat)
        fk_fails_2 = input_schema.find_foreign_key_failures(new_pan_dat, verbosity="Low")
        fk_fails_3 = input_schema.find_foreign_key_failures(new_pan_dat, verbosity="Low", as_table=False)
        self.assertTrue({tuple(k)[:2] + (tuple(k[2]),): len(v) for k,v in fk_fails.items()} ==
                        {k:len(v) for k,v in fk_fails_2.items()} ==
                        {k:v.count(True) for k,v in fk_fails_3.items()} ==
                        {('position_constraints', 'innings', ("Inning Group", "Inning Group")): 2,
                         ('position_constraints', 'positions', ("Position Group", "Position Group")): 2,
                         ('position_constraints', 'roster', ("Grade", "Grade")): 1})
        input_schema.remove_foreign_key_failures(new_pan_dat)
        self.assertFalse(input_schema.find_foreign_key_failures(new_pan_dat))
        self.assertTrue(input_schema._same_data(orig_pan_dat, new_pan_dat))

        input_schema = PanDatFactory(table_one=[["One", "Two"], []],
                                     table_two=[["One"], ["Two"]])
        input_schema.add_foreign_key("table_two", "table_one", ["One", "One"])
        self.assertTrue({fk.cardinality for fk in input_schema.foreign_keys} == {"one-to-many"})

        tdf = TicDatFactory(**input_schema.schema())
        dat = tdf.TicDat(table_one = [[1,2], [3,4], [5,6], [7,8]], table_two = {1:2, 3:4, 5:6})

        orig_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertFalse(input_schema.find_foreign_key_failures(orig_pan_dat))
        dat.table_two[9]=10
        new_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        fk_fails = input_schema.find_foreign_key_failures(new_pan_dat)
        self.assertTrue({tuple(k)[:2]:len(v) for k,v in fk_fails.items()} == {('table_two', 'table_one'): 1})
        input_schema.remove_foreign_key_failures(new_pan_dat)
        self.assertFalse(input_schema.find_foreign_key_failures(new_pan_dat))
        self.assertTrue(input_schema._same_data(orig_pan_dat, new_pan_dat))


    def testXToManyTwo(self):
        input_schema = PanDatFactory (parent = [["F1", "F2"],["F3"]], child_one = [["F1", "F2", "F3"], []],
                                      child_two = [["F1", "F2"], ["F3"]], child_three = [[],["F1", "F2", "F3"]])
        for t in ["child_one", "child_two", "child_three"]:
            input_schema.add_foreign_key(t, "parent", [["F1"]*2, ["F2"]*2, ["F3"]*2])
        self.assertTrue({fk.cardinality for fk in input_schema.foreign_keys} == {"one-to-one", "many-to-one"})

        rows =[[1,2,3], [1,2.1,3], [4,5,6],[4,5.1,6],[7,8,9]]
        tdf = TicDatFactory(**input_schema.schema())
        dat = tdf.TicDat(parent = rows, child_one = rows, child_two = rows, child_three=rows)
        self.assertTrue(all(len(getattr(dat, t)) == 5 for t in input_schema.all_tables))
        orig_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertFalse(input_schema.find_foreign_key_failures(orig_pan_dat))
        dat.child_one[1, 2, 4] = {}
        dat.child_two[1,2.2]=3
        dat.child_three.append([1,2,4])
        new_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        fk_fails = input_schema.find_foreign_key_failures(new_pan_dat)
        self.assertTrue(len(fk_fails) == 3)
        input_schema.remove_foreign_key_failures(new_pan_dat)
        self.assertFalse(input_schema.find_foreign_key_failures(new_pan_dat))
        self.assertTrue(input_schema._same_data(orig_pan_dat, new_pan_dat))

        input_schema = PanDatFactory (parent = [["F1", "F2"],["F3"]], child_one = [["F1", "F2", "F3"], []],
                                      child_two = [["F1", "F2"], ["F3"]], child_three = [[],["F1", "F2", "F3"]])
        for t in ["child_one", "child_two", "child_three"]:
            input_schema.add_foreign_key(t, "parent", [["F1"]*2, ["F3"]*2])
        tdf = TicDatFactory(**input_schema.schema())
        dat = tdf.TicDat(parent=rows, child_one=rows, child_two=rows, child_three=rows)
        self.assertTrue(all(len(getattr(dat, t)) == 5 for t in input_schema.all_tables))
        orig_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertFalse(input_schema.find_foreign_key_failures(orig_pan_dat))
        dat.child_one[1, 2, 4] = {}
        dat.child_two[1,2.2]=4
        dat.child_three.append([1,2,4])
        new_pan_dat = input_schema.copy_pan_dat(copy_to_pandas_with_reset(tdf, dat))
        self.assertTrue(len(input_schema.find_foreign_key_failures(new_pan_dat)) == 3)
        input_schema.remove_foreign_key_failures(new_pan_dat)
        self.assertFalse(input_schema.find_foreign_key_failures(new_pan_dat))
        self.assertTrue(input_schema._same_data(orig_pan_dat, new_pan_dat))

    def _testPdfReproduction(self, pdf):
        def _tdfs_same(pdf, pdf2):
            self.assertTrue(pdf.schema() == pdf2.schema())
            self.assertTrue(set(pdf.foreign_keys) == set(pdf2.foreign_keys))
            self.assertTrue(pdf.data_types == pdf2.data_types)
            self.assertTrue(pdf.default_values == pdf2.default_values)
        _tdfs_same(pdf, TicDatFactory.create_from_full_schema(pdf.schema(True)))
        _tdfs_same(pdf, TicDatFactory.create_from_full_schema(_deep_anonymize(pdf.schema(True))))

    def testBasicFKs(self):
        for cloning in [True, False]:
            clone_me_maybe = lambda x : x.clone() if cloning else x

            pdf = PanDatFactory(plants = [["name"], ["stuff", "otherstuff"]],
                                lines = [["name"], ["plant", "weird stuff"]],
                                line_descriptor = [["name"], ["booger"]],
                                products = [["name"],["gover"]],
                                production = [["line", "product"], ["min", "max"]],
                                pureTestingTable = [[], ["line", "plant", "product", "something"]],
                                extraProduction = [["line", "product"], ["extramin", "extramax"]],
                                weirdProduction = [["line1", "line2", "product"], ["weirdmin", "weirdmax"]])
            pdf.add_foreign_key("production", "lines", ("line", "name"))
            pdf.add_foreign_key("production", "products", ("product", "name"))
            pdf.add_foreign_key("lines", "plants", ("plant", "name"))
            pdf.add_foreign_key("line_descriptor", "lines", ("name", "name"))
            for f in set(pdf.data_fields["pureTestingTable"]).difference({"something"}):
                pdf.add_foreign_key("pureTestingTable", "%ss"%f, (f,"name"))
            pdf.add_foreign_key("extraProduction", "production", (("line", "line"), ("product","product")))
            pdf.add_foreign_key("weirdProduction", "production", (("line1", "line"), ("product","product")))
            pdf.add_foreign_key("weirdProduction", "extraProduction", (("line2","line"), ("product","product")))
            self._testPdfReproduction(pdf)
            pdf = clone_me_maybe(pdf)

            tdf = TicDatFactory(**pdf.schema())
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


            def pan_dat_(_):
                rtn = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, _))
                self.assertFalse(pdf.find_duplicates(rtn))
                return rtn
            fk, fkm = ForeignKey, ForeignKeyMapping
            fk_fails1 = pdf.find_foreign_key_failures(pan_dat_(badDat1))
            fk_fails2 = pdf.find_foreign_key_failures(pan_dat_(badDat2))

            self.assertTrue(set(fk_fails1) == set(fk_fails2) ==
                            {fk('production', 'lines', fkm('line', 'name'), 'many-to-one')})
            self.assertTrue(set(pdf.find_foreign_key_failures(pan_dat_(badDat1), verbosity="Low")) ==
                            set(pdf.find_foreign_key_failures(pan_dat_(badDat2), verbosity="Low")) ==
                             {('production', 'lines', ('line', 'name'))})
            for row_fails in [next(iter(_.values())) for _ in [fk_fails1, fk_fails2]]:
                self.assertTrue(set(row_fails["line"]) == {"notaline"} and set(row_fails["product"]) == {"widgets"})

            badDat1.lines["notaline"]["plant"] = badDat2.lines["notaline"]["plant"] = "notnewark"
            fk_fails1 = pdf.find_foreign_key_failures(pan_dat_(badDat1))
            fk_fails2 = pdf.find_foreign_key_failures(pan_dat_(badDat2))
            self.assertTrue(set(fk_fails1) == set(fk_fails2) ==
                            {fk('lines', 'plants', fkm('plant', 'name'), 'many-to-one')})
            for row_fails in [next(iter(_.values())) for _ in [fk_fails1, fk_fails2]]:
                self.assertTrue(set(row_fails["name"]) == {"notaline"} and set(row_fails["plant"]) == {"notnewark"})


            for bad in [badDat1, badDat2]:
                bad_pan = pdf.remove_foreign_key_failures(pan_dat_(bad))
                self.assertFalse(pdf.find_foreign_key_failures(bad_pan))
                self.assertTrue(pdf._same_data(bad_pan, pan_dat_(goodDat)))


            _ = len(goodDat.lines)
            for i,p in enumerate(list(goodDat.plants.keys()) + list(goodDat.plants.keys())):
                goodDat.lines[i+_]["plant"] = p
            for l in goodDat.lines:
                if i%2:
                    goodDat.line_descriptor[l] = i+10

            for i,(l,pl,pdct) in enumerate(sorted(itertools.product(goodDat.lines, goodDat.plants, goodDat.products))):
                goodDat.pureTestingTable.append((l,pl,pdct,i))
            self.assertFalse(pdf.find_foreign_key_failures(pan_dat_(goodDat)))
            badDat = tdf.copy_tic_dat(goodDat)
            badDat.pureTestingTable.append(("j", "u", "nk", "ay"))
            fk_fails = pdf.find_foreign_key_failures(pan_dat_(badDat))
            self.assertTrue(set(fk_fails) ==
                {fk('pureTestingTable', 'plants', fkm('plant', 'name'), 'many-to-one'),
                 fk('pureTestingTable', 'products', fkm('product', 'name'), 'many-to-one'),
                 fk('pureTestingTable', 'lines', fkm('line', 'name'), 'many-to-one')})

            for df in fk_fails.values():
                df = df.T
                c = df.columns[0]
                self.assertTrue({'ay', 'j', 'nk', 'u'} == set(df[c]))

    def testAdditionalFKs(self):
        pdf = PanDatFactory(pt1 = [["F1"],[]], pt2 = [["F2"],[]], pt3 = [["F1","F2"],[]],
                            pt4 = [["F1"],["F2"]], pt5 = [[],["F1","F2"]])
        for c in ["pt3", "pt4", "pt5"]:
            pdf.add_foreign_key(c, "pt1", ["F1", "F1"])
            pdf.add_foreign_key(c, "pt2", ["F2", "F2"])
        tdf = TicDatFactory(**pdf.schema())
        def pan_dat_(_):
            rtn =  pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, _))
            self.assertFalse(pdf.find_duplicates(rtn))
            return rtn
        ticDat = tdf.TicDat(pt1=[1, 2, 3, 4], pt2=[5, 6, 7, 8])
        for f1, f2 in itertools.product(range(1,5), range(5,9)):
            ticDat.pt3[f1, f2] = {}
            ticDat.pt4[f1] = f2
            ticDat.pt5.append((f1, f2))
        origDat = tdf.copy_tic_dat(ticDat, freeze_it=True)
        self.assertFalse(pdf.find_foreign_key_failures(pan_dat_(origDat)))
        ticDat.pt3["no",6] = ticDat.pt3[1, "no"] = {}
        ticDat.pt4["no"] = 6
        ticDat.pt4["nono"]=6.01
        panDat = pan_dat_(ticDat)
        fails1 = pdf.find_foreign_key_failures(panDat)
        self.assertTrue(fails1)
        pdf.remove_foreign_key_failures(panDat)
        self.assertFalse(pdf.find_foreign_key_failures(panDat))
        self.assertTrue(pdf._same_data(panDat, pan_dat_(origDat)))

        orig_lens = {t:len(getattr(origDat, t)) for t in tdf.all_tables}
        ticDat.pt3["no",6] = ticDat.pt3[1, "no"] = {}
        ticDat.pt4["no"] = 6
        ticDat.pt4["nono"]=6.01
        ticDat.pt5.append(("no",6))
        ticDat.pt5.append((1, "no"))
        panDat = pan_dat_(ticDat)
        fails2 = pdf.find_foreign_key_failures(panDat)
        self.assertTrue(set(fails1) != set(fails2) and set(fails1).issubset(fails2))
        pdf.remove_foreign_key_failures(panDat)
        self.assertFalse(pdf.find_foreign_key_failures(panDat))
        self.assertTrue({t:len(getattr(panDat, t)) for t in tdf.all_tables} == orig_lens)

    def testFindDups(self):
        pdf = PanDatFactory(**sillyMeSchema())
        tdf = TicDatFactory(**{k:[[],list(pkfs)+list(dfs)] for k, (pkfs, dfs) in sillyMeSchema().items()})
        rows = [(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40)]
        ticDat = tdf.TicDat(**{t:rows for t in tdf.all_tables})
        panDat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticDat))
        dups = pdf.find_duplicates(panDat)
        self.assertTrue(set(dups) == {'a'} and set(dups['a']['aField']) == {1})
        dups = pdf.find_duplicates(panDat, as_table=False, keep=False)
        self.assertTrue(set(dups) == {'a'} and dups['a'].value_counts()[True] == 2)
        dups = pdf.find_duplicates(panDat, as_table=False)
        self.assertTrue(set(dups) == {'a'} and dups['a'].value_counts()[True] == 1)
        rows = [(1, 2, 3, 4), (1, 20, 30, 40), (10, 20, 30, 40), (1, 2, 3, 40)]
        ticDat = tdf.TicDat(**{t:rows for t in tdf.all_tables})
        panDat = pdf.copy_pan_dat(copy_to_pandas_with_reset(tdf, ticDat))
        dups = pdf.find_duplicates(panDat, keep=False)
        self.assertTrue(set(dups) == {'a', 'b'} and set(dups['a']['aField']) == {1})
        dups = pdf.find_duplicates(panDat, as_table=False, keep=False)
        self.assertTrue({k:v.value_counts()[True] for k,v in dups.items()} == {'a':3, 'b':2})

    def testDictConstructions(self):
        tdf = TicDatFactory(**dietSchema())
        pdf = PanDatFactory(**dietSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(dietSchema(), ticDat)
        panDat2 = pdf.PanDat(**{t:getattr(panDat, t).to_dict() for t in pdf.all_tables})
        panDat3 = pdf.PanDat(**{t:getattr(panDat, t).to_dict(orient="list") for t in pdf.all_tables})
        panDat3_1 = pdf.PanDat(**{t:list(map(list, getattr(panDat, t).itertuples(index=False)))
                                  for t in pdf.all_tables})

        self.assertTrue(all(pdf._same_data(panDat, _) for _ in [panDat2, panDat3, panDat3_1]))
        panDat.foods["extra"] = 12
        panDat4 = pdf.PanDat(**{t:getattr(panDat, t).to_dict(orient="list") for t in pdf.all_tables})
        self.assertTrue(pdf._same_data(panDat, panDat4))
        self.assertTrue(set(panDat4.foods["extra"]) == {12})

        tdf = TicDatFactory(**netflowSchema())
        pdf = PanDatFactory(**netflowSchema())
        ticDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        panDat = pan_dat_maker(netflowSchema(), ticDat)
        panDat2 = pdf.PanDat(**{t:getattr(panDat, t).to_dict() for t in pdf.all_tables})
        panDat3 = pdf.PanDat(**{t:getattr(panDat, t).to_dict(orient="records") for t in pdf.all_tables})
        self.assertTrue(all(pdf._same_data(panDat, _) for _ in [panDat2, panDat3]))
        panDat.cost["extra"] = "boger"
        panDat4 = pdf.PanDat(**{t:getattr(panDat, t).to_dict(orient="list") for t in pdf.all_tables})
        self.assertTrue(pdf._same_data(panDat, panDat4))
        self.assertTrue(set(panDat4.cost["extra"]) == {"boger"})

    def testParametersTest(self):
        def make_pdf():
            pdf = PanDatFactory(data_table = [["a"], ["b", "c"]],
                                parameters = [["a"], ["b"]])
            pdf.add_parameter("Something", 100, max=100, inclusive_max=True)
            pdf.add_parameter("Another thing", 5, must_be_int=True)
            pdf.add_parameter("Untyped thing", "whatever", enforce_type_rules=False)
            pdf.add_parameter("Last", 'boo', number_allowed=False, strings_allowed='*')
            return PanDatFactory.create_from_full_schema(pdf.schema(True))

        pdf = make_pdf()

        dat = pdf.PanDat(data_table = DataFrame({"a":[1, 4], "b":[2, 5], "c":[3, 6]}),
                         parameters = DataFrame({"a": ["Something", "Another thing", "Last", "Untyped thing"],
                                                 "b":[100, 200, "goo", -float("inf")]}))

        self.assertFalse(pdf.find_data_row_failures(dat))
        dat.parameters = DataFrame({"a": ["Something", "Another thing", "Bad P", "Last"],
                                    "b": [100, 200.1, -float("inf"), 100]})
        self.assertTrue(set(pdf.find_data_row_failures(dat)) == {("parameters", "Good Name/Value Check")})
        self.assertTrue(set(next(iter(pdf.find_data_row_failures(dat).values()))["a"])
                        == {"Another thing", "Last", "Bad P"})

        dat.parameters = DataFrame({"a": ["Something", "Another thing", "Untyped thingy", "Last"],
                                    "b": [100, 200.1, -float("inf"), 100]})
        pdf = make_pdf()
        pdf.add_parameter("Another thing", 5, max=100)
        pdf.add_data_row_predicate("parameters", lambda row: "thing" in row["a"],
                                   predicate_name="Good Name/Value Check")
        pdf.add_data_row_predicate("data_table", lambda row: row["a"] + row["b"] > row["c"], predicate_name="boo")
        fails = pdf.find_data_row_failures(dat)
        self.assertTrue({k:len(v) for k,v in fails.items()} ==
                        {("parameters", "Good Name/Value Check"): 1,
                         ("parameters", 'Good Name/Value Check_0'): 3, ('data_table', "boo"): 1})

        pdf = make_pdf()
        dat = pdf.PanDat(parameters = DataFrame({"a": ["Something", "Last"], "b": [90, "boo"]}))
        self.assertTrue(pdf.create_full_parameters_dict(dat) ==
                        {"Something": 90, "Another thing": 5, "Last": "boo", "Untyped thing": "whatever"})

    def testVariousCoverages(self):
        pdf = PanDatFactory(**dietSchema())
        _d = dict(categories={"minNutrition": 0, "maxNutrition": float("inf")},
                               foods={"cost": 0}, nutritionQuantities={"qty": 0})
        pdf.set_default_values(**_d)
        self.assertTrue(pdf._default_values == _d)
        pdf = PanDatFactory(**netflowSchema())
        addNetflowForeignKeys(pdf)
        pdf.clear_foreign_keys("arcs")
        self.assertTrue({_[0] for _ in pdf._foreign_keys} == {"cost", "inflow"})

        pdf.add_data_row_predicate("arcs", lambda row: True)
        pdf.add_data_row_predicate("arcs", lambda row: True, "dummy")
        pdf.add_data_row_predicate("arcs", None, 0)
        pdf = pdf.clone()
        self.assertTrue(set(pdf._data_row_predicates["arcs"]) == {"dummy"})

# Run the tests.
if __name__ == "__main__":
    if not DataFrame :
        print("!!!!!!!!!FAILING pandat utils UNIT TESTS DUE TO FAILURE TO LOAD PANDAS LIBRARIES!!!!!!!!")
    else:
        TestUtils.canRun = True
    unittest.main()
