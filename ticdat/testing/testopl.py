import os
from ticdat.opl import create_opl_text, read_opl_text, opl_run,create_opl_mod_text
from ticdat.opl import _can_run_oplrun_tests, pattern_finder, _find_case_space_duplicates
from ticdat.opl import _fix_fields_with_opl_keywords, _unfix_fields_with_opl_keywords
import sys
from ticdat.ticdatfactory import TicDatFactory
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, addNetflowDataTypes, nearlySame
from ticdat.testing.ticdattestutils import  netflowSchema,sillyMeData, sillyMeSchema
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, get_testing_file_path
import unittest

#@fail_to_debugger
class TestOpl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_value = utils.development_deployed_environment
        utils.development_deployed_environment = True
    @classmethod
    def tearDownClass(cls):
        utils.development_deployed_environment = cls._original_value
    def testDiet_oplrunRequired(self):
        self.assertTrue(_can_run_oplrun_tests)
        diet_schema = {"categories" : (("Name",),["Min Nutrition", "Max Nutrition"]),
                       "foods" :[["Name"],("Cost",)],
                       "nutritionQuantities" : (["Food", "Category"], ["Qty"])
                      }
        in_tdf = TicDatFactory(**diet_schema)
        soln_tdf = TicDatFactory(
            parameters=[["Parameter Name"], ["Parameter Value"]],
            buy_food=[["Food"], ["Qty"]],consume_nutrition=[["Category"], ["Qty"]])
        makeDat = lambda : in_tdf.TicDat(
            categories = {'calories': [1800, 2200],
                          'protein':  [91,   float("inf")],
                          'fat':      [0, 65],
                          'sodium':   [0, 1779]},

            foods = {'hamburger': 2.49,
                      'chicken':   2.89,
                      'hot dog':   1.50,
                      'fries':     1.89,
                      'macaroni':  2.09,
                      'pizza':     1.99,
                      'salad':     2.49,
                      'milk':      0.89,
                      'ice cream': 1.59},
            nutritionQuantities= [('hamburger', 'calories', 410),
                                  ('hamburger', 'protein', 24),
                                  ('hamburger', 'fat', 26),
                                  ('hamburger', 'sodium', 730),
                                  ('chicken',   'calories', 420),
                                  ('chicken',   'protein', 32),
                                  ('chicken',   'fat', 10),
                                  ('chicken',   'sodium', 1190),
                                  ('hot dog',   'calories', 560),
                                  ('hot dog',   'protein', 20),
                                  ('hot dog',   'fat', 32),
                                  ('hot dog',   'sodium', 1800),
                                  ('fries',     'calories', 380),
                                  ('fries',     'protein', 4),
                                  ('fries',     'fat', 19),
                                  ('fries',     'sodium', 270),
                                  ('macaroni',  'calories', 320),
                                  ('macaroni',  'protein', 12),
                                  ('macaroni',  'fat', 10),
                                  ('macaroni',  'sodium', 930),
                                  ('pizza',     'calories', 320),
                                  ('pizza',     'protein', 15),
                                  ('pizza',     'fat', 12),
                                  ('pizza',     'sodium', 820),
                                  ('salad',     'calories', 320),
                                  ('salad',     'protein', 31),
                                  ('salad',     'fat', 12),
                                  ('salad',     'sodium', 1230),
                                  ('milk',      'calories', 100),
                                  ('milk',      'protein', 8),
                                  ('milk',      'fat', 2.5),
                                  ('milk',      'sodium', 125),
                                  ('ice cream', 'calories', 330),
                                  ('ice cream', 'protein', 8),
                                  ('ice cream', 'fat', 10),
                                  ('ice cream', 'sodium', 180) ] )
        opl_soln = opl_run(get_testing_file_path("sample_diet.mod"), in_tdf, makeDat(), soln_tdf)
        self.assertTrue(nearlySame(opl_soln.parameters["Total Cost"]["Parameter Value"], 11.829, epsilon=0.0001))
        self.assertTrue(nearlySame(opl_soln.consume_nutrition["protein"]["Qty"], 91, epsilon=0.0001))
        # opl_run should not complete when there is an infeasible solution
        dat = makeDat()
        dat.categories["calories"]["Min Nutrition"] = dat.categories["calories"]["Max Nutrition"]+1
        try:
            opl_soln = opl_run(get_testing_file_path("sample_diet.mod"), in_tdf, dat, soln_tdf)
            self.assertTrue(False)
        except:
            self.assertTrue(True)
    def testNetflow_oplrunRequired(self):
        self.assertTrue(_can_run_oplrun_tests)
        in_tdf = TicDatFactory(**netflowSchema())
        in_tdf.enable_foreign_key_links()
        soln_tdf = TicDatFactory(flow=[["source", "destination", "commodity"], ["quantity"]],
                                 parameters=[["paramKey"], ["value"]])
        dat = in_tdf.TicDat(**{t: getattr(netflowData(), t) for t in in_tdf.primary_key_fields})
        opl_soln = opl_run("sample_netflow.mod", in_tdf, dat, soln_tdf)
        self.assertTrue(nearlySame(opl_soln.parameters["Total Cost"]["value"],5500))
        self.assertTrue(nearlySame(opl_soln.flow["Pens", "Detroit", "New York"]["quantity"], 30))
    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertFalse(tdf._same_data(oldDat, newDat))
        oldDat.categories["protein"]["maxNutrition"]=12 # Remove infinity from the data
        changedDatStr = create_opl_text(tdf, oldDat)
        changedDat = read_opl_text(tdf, changedDatStr)
        self.assertTrue(tdf._same_data(oldDat,changedDat))
        tdf.opl_prepend = "pre_"
        origStr, changedDatStr = changedDatStr, create_opl_text(tdf, oldDat)
        changedDat = read_opl_text(tdf, changedDatStr)
        self.assertTrue(tdf._same_data(oldDat,changedDat))
        self.assertFalse(origStr == changedDatStr)

    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
        tdf.opl_prepend = "stuff"
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**sillyMeData()))
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
        tdf.opl_prepend = "ooooo"
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

    def testPatternFinder(self):
        testpattern = 'parameters={'
        test1 = 'be sk={ipped}=\tme.\nLP86\n parameters={<"test" 0001>}\n\n<<< post process\n\n\n<<< done\n\n'
        self.assertTrue(25 == pattern_finder(test1, testpattern))
        test2 = 'e sk={ipped}=\tme.\nLP86\n parameters=  {<"test" 0001>}\n\n<<< post process\n\n\n<<< done\n\n'
        self.assertTrue(24 == pattern_finder(test2, testpattern))
        test3 = 'parameters={'
        self.assertTrue(0 == pattern_finder(test3, testpattern))
        test4 = 'be sk={ipped}=\tme.\nLP  pa r      par              amete\nrs=  {      <"test" 0001>}\n\n<ss\done\n\n'
        self.assertTrue(33 == pattern_finder(test4, testpattern))

        testpattern = 'notfound'
        test5 = 'this text should just be sk={ipped}=\tme.\nLP86\npanot foudndrs=  {<"test" 0001>}\n\n<<< post proc'
        self.assertTrue(False == pattern_finder(test5, testpattern))
        test6 = 'found not'
        self.assertTrue(False == pattern_finder(test6, testpattern))
        test7 = 'dnuofton'
        self.assertTrue(False == pattern_finder(test7, testpattern))

        testpattern = '>}'
        test8 = 'be sk={ipped}=\tme.\nLP86\n parameters=  {<"test" 0001\n>      }\n\n<<< posprocess\n\n\n<<< done\n\n'
        self.assertTrue(59 == pattern_finder(test8, testpattern, True))
        test9 = '01>\n\t}\n\n<<< post process\n\n\n<<< done\n\n'
        self.assertTrue(5 == pattern_finder(test9, testpattern, True))
        test10 = '>}>}>}>}>}>}>}>}>}>}>}>}>}>}>}>}>}>}>         } >tadah!'
        self.assertTrue(46 == pattern_finder(test10, testpattern, True))
        test11 = '>}'
        self.assertTrue(1 == pattern_finder(test11, testpattern, True))
        # Run the tests.
    def testFindCaseSpaceDuplicates(self):
        test2 = TicDatFactory(table=[['PK 1','PK 2'],['DF 1','DF 2']])
        self.assertFalse(_find_case_space_duplicates(test2))
        test3 = TicDatFactory(table=[['PK 1', 'PK_1'], []])
        self.assertEqual(len(_find_case_space_duplicates(test3).keys()),1)
        test4 = TicDatFactory(table=[[], ['DF 1', 'df_1']])
        self.assertEqual(len(_find_case_space_duplicates(test4).keys()),1)
        test5 = TicDatFactory(table=[['is Dup'], ['is_Dup']])
        self.assertEqual(len(_find_case_space_duplicates(test5).keys()),1)
        test6 = TicDatFactory(table1=[['test'],[]], table2=[['test'],[]])
        self.assertFalse(_find_case_space_duplicates(test6))
        test7 = TicDatFactory(table1=[['dup 1', 'Dup_1'],[]], table2=[['Dup 2', 'Dup_2'],[]])
        self.assertEqual(len(_find_case_space_duplicates(test7).keys()),2)
    def testChangeFieldsWithOplKeywords(self):
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
        new_input_schema = _fix_fields_with_opl_keywords(input_schema)
        self.assertDictEqual(input_schema.data_types, new_input_schema.data_types)
        self.assertDictEqual(input_schema.default_values, new_input_schema.default_values)
        old_tdf = TicDatFactory(table=[['Key', 'PK 2'], ['cplex', 'DF 2']])
        old_schema = old_tdf.schema()
        new_tdf = _fix_fields_with_opl_keywords(old_tdf)
        new_schema = new_tdf.schema()
        self.assertTrue('_Key' in new_schema['table'][0])
        new_old_tdf = _unfix_fields_with_opl_keywords(new_tdf)
        new_old_schema = new_old_tdf.schema()
        self.assertDictEqual(old_schema,new_old_schema)

if __name__ == "__main__":
    unittest.main()