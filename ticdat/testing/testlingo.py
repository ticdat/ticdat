import os
import ticdat.lingo as tlingo
import sys
from ticdat.ticdatfactory import TicDatFactory

import ticdat.utils as utils
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, addNetflowDataTypes, nearlySame
from ticdat.testing.ticdattestutils import  netflowSchema,sillyMeData, sillyMeSchema
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, get_testing_file_path
import unittest

#@fail_to_debugger
class TestLingo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_value = utils.development_deployed_environment
        utils.development_deployed_environment = True
    @classmethod
    def tearDownClass(cls):
        utils.development_deployed_environment = cls._original_value

    def testDiet_runlingoRequired(self):
        self.assertTrue(tlingo._can_run_lingo_run_tests)
        diet_schema = {"categories": (("Name",), ["Min Nutrition", "Max Nutrition"]),
                       "foods": [["Name"], ("Cost",)],
                       "nutrition_quantities": (["Food", "Category"], ["Quantity"])
                       }
        in_tdf = TicDatFactory(**diet_schema)
        solution_variables = TicDatFactory(buy=[["Food"], ["Quantity"]])
        makeDat = lambda: in_tdf.TicDat(
            categories={'calories': [1800, 2200],
                        'protein': [91, float("inf")],
                        'fat': [0, 65],
                        'sodium': [0, 1779]},

            foods={'hamburger': 2.49,
                   'chicken': 2.89,
                   'hot dog': 1.50,
                   'fries': 1.89,
                   'macaroni': 2.09,
                   'pizza': 1.99,
                   'salad': 2.49,
                   'milk': 0.89,
                   'ice cream': 1.59},
            nutrition_quantities=[('hamburger', 'calories', 410),
                                 ('hamburger', 'protein', 24),
                                 ('hamburger', 'fat', 26),
                                 ('hamburger', 'sodium', 730),
                                 ('chicken', 'calories', 420),
                                 ('chicken', 'protein', 32),
                                 ('chicken', 'fat', 10),
                                 ('chicken', 'sodium', 1190),
                                 ('hot dog', 'calories', 560),
                                 ('hot dog', 'protein', 20),
                                 ('hot dog', 'fat', 32),
                                 ('hot dog', 'sodium', 1800),
                                 ('fries', 'calories', 380),
                                 ('fries', 'protein', 4),
                                 ('fries', 'fat', 19),
                                 ('fries', 'sodium', 270),
                                 ('macaroni', 'calories', 320),
                                 ('macaroni', 'protein', 12),
                                 ('macaroni', 'fat', 10),
                                 ('macaroni', 'sodium', 930),
                                 ('pizza', 'calories', 320),
                                 ('pizza', 'protein', 15),
                                 ('pizza', 'fat', 12),
                                 ('pizza', 'sodium', 820),
                                 ('salad', 'calories', 320),
                                 ('salad', 'protein', 31),
                                 ('salad', 'fat', 12),
                                 ('salad', 'sodium', 1230),
                                 ('milk', 'calories', 100),
                                 ('milk', 'protein', 8),
                                 ('milk', 'fat', 2.5),
                                 ('milk', 'sodium', 125),
                                 ('ice cream', 'calories', 330),
                                 ('ice cream', 'protein', 8),
                                 ('ice cream', 'fat', 10),
                                 ('ice cream', 'sodium', 180)])
        in_tdf.add_foreign_key("nutrition_quantities", "foods", ['Food', 'Name'])
        in_tdf.add_foreign_key("nutrition_quantities", "categories", ['Category', 'Name'])
        lingo_soln = tlingo.lingo_run(get_testing_file_path("sample_diet.lng"), in_tdf, makeDat(), solution_variables)
        self.assertTrue(nearlySame(lingo_soln.buy["hamburger"]["Quantity"], 0.6045, epsilon=0.0001))
        # lingo_run should not complete when there is an infeasible solution
        dat = makeDat()
        dat.categories["calories"]["Min Nutrition"] = dat.categories["calories"]["Max Nutrition"] + 1
        try:
            lingo_soln = tlingo.lingo_run(get_testing_file_path("sample_diet.lng"), in_tdf, dat, solution_variables)
            self.assertTrue(False)
        except:
            self.assertTrue(True)

    def testNetflow_runlingoRequired(self):
        self.assertTrue(tlingo._can_run_lingo_run_tests)
        in_tdf = TicDatFactory(**netflowSchema())
        in_tdf.add_foreign_key("arcs", "nodes", ['source', 'name'])
        in_tdf.add_foreign_key("arcs", "nodes", ['destination', 'name'])
        in_tdf.add_foreign_key("cost", "nodes", ['source', 'name'])
        in_tdf.add_foreign_key("cost", "nodes", ['destination', 'name'])
        in_tdf.add_foreign_key("cost", "commodities", ['commodity', 'name'])
        in_tdf.add_foreign_key("inflow", "commodities", ['commodity', 'name'])
        in_tdf.add_foreign_key("inflow", "nodes", ['node', 'name'])
        solution_variables = TicDatFactory(flow=[["Commodity", "Source", "Destination"], ["quantity"]])
        dat = in_tdf.TicDat(**{t: getattr(netflowData(), t) for t in in_tdf.primary_key_fields})
        lingo_soln = tlingo.lingo_run("sample_netflow.lng", in_tdf, dat, solution_variables)
        self.assertTrue(nearlySame(lingo_soln.flow["Pens", "Detroit", "New York"]["quantity"], 30))

    def testSortedTables(self):
        test1 = TicDatFactory(table3=[["PK3","FK1", "FK2"],["Val D"]],
                               table2=[["PK2"],["Val A", "Val B"]],
                               table1=[["PK1"],["Val C"]])
        test1.add_foreign_key("table3", "table1", ["FK1", "PK1"])
        test1.add_foreign_key("table3", "table2", ["FK2", "PK2"])
        self.assertTrue(tlingo._sorted_tables(test1)[-1] == 'table3')

        # Uncomment when Ticdat supports many-many foreign key relationships
        # test2 = TicDatFactory(table3=[["FK1", "FK2", "PK3"], ["Val C"]],
        #                       table2=[["PK2", "FK1"], ["Val B"]],
        #                       table1=[["PK1"], ["Val A"]])
        # test2.add_foreign_key("table3", "table1", ["FK1", "PK1"])
        # test2.add_foreign_key("table2", "table1", ["FK1", "PK1"])
        # test2.add_foreign_key("table3", "table2", ["FK2", "PK2"])
        #
        # self.assertTrue(tlingo._sorted_tables(test2) == ['table1', 'table2', 'table3'])
        # test3 = TicDatFactory(table3=[["PK2", "FK1"],["Val B"]],
        #                       table2=[["PK1", "FK3"],["Val C"]],
        #                       table4=[["FK1", "FK2", "FK3"],["Val A"]],
        #                       table1=[["PK3"],[]])
        # test3.add_foreign_key("table4", "table2", ["FK1", "PK1"])
        # test3.add_foreign_key("table4", "table3", ["FK2", "PK2"])
        # test3.add_foreign_key("table4", "table1", ["FK3", "PK3"])
        # test3.add_foreign_key("table3", "table2", ["FK1", "PK1"])
        # test3.add_foreign_key("table2", "table1", ["FK3", "PK3"])
        # self.assertTrue(tlingo._sorted_tables(test3) == ['table1', 'table2', 'table3', 'table4'])

    def testTryCreateSpace(self):
        def test_(schema_factory, data_factory):
            tdf = TicDatFactory(**schema_factory())
            dat = tdf.copy_tic_dat(data_factory())
            mapping = tlingo._try_create_space_case_mapping(tdf, dat)["mapping"]
            remapdat = tlingo._apply_space_case_mapping(tdf, dat, {v:k for k,v in mapping.items()})
            mapmapdat = tlingo._apply_space_case_mapping(tdf, remapdat, mapping)
            self.assertTrue(tdf._same_data(dat, mapmapdat))
            self.assertFalse(tdf._same_data(dat, remapdat))
        test_(dietSchema, dietData)
        test_(netflowSchema, netflowData)
        test_(sillyMeSchema, lambda : TicDatFactory(**sillyMeSchema()).TicDat(**sillyMeData()))

        tdf = TicDatFactory(**dietSchema())
        dat = tdf.copy_tic_dat(dietData())
        dat.foods["ice_cream"] = dat.foods["ice cream"]
        dat.categories["ICE CREAM"] = {}
        dat.categories["fAt"] = dat.categories["fat"]
        failures = tlingo._try_create_space_case_mapping(tdf, dat)["failures"]
        self.assertTrue(failures == {'ICE_CREAM': ('ICE CREAM', 'ice cream', 'ice_cream'), 'FAT': ('fAt', 'fat')})




if __name__ == "__main__":
    unittest.main()
