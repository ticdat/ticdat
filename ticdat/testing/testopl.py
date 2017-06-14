import os
from ticdat.opl import create_opl_text, read_opl_text, opl_run,create_opl_mod_text
from ticdat.opl import _can_run_oplrun_tests
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
        return
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
        return
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

    def testOther(self):
        tdf = TicDatFactory(
            table1=[["String_Field"], []],
            table2=[["String_Field", "Num_PK"], ["Num_Field_1","Num_Field_2"]])
        data = {
            "table1": {
                "test1": [],
                "test2": [],
            },
            "table2": {
                ("test1",1): [2,3],
                ("test2",2): [3,4]
            }
        }
        oldDat = tdf.freeze_me(tdf.TicDat(**data))
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
        tdf.opl_prepend = "_"
        oldDatStr = create_opl_text(tdf, oldDat)
        newDat = read_opl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

    def testCreateModText(self):
        tdf = TicDatFactory(
            table1=[["string_pk", "num_pk"], ["num_field1","string_field2"]])
        tdf.set_data_type("table1", "num_pk", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        tdf.set_data_type("table1", "string_field2", number_allowed=False, strings_allowed='*')
        modStr = create_opl_mod_text(tdf)
        self.assertTrue("key string string_pk;" in modStr)
        self.assertTrue("key float num_pk;" in modStr)
        self.assertTrue("float num_field1;" in modStr)
        self.assertTrue("string string_field2;" in modStr)

    def testReadModText(self):
        tdf1 = TicDatFactory( test_1 = [["sf1"],["sf2","nf1","nf2"]] )
        tdf1.set_data_type("test_1", "sf2", number_allowed=False, strings_allowed='*')
        test_str = 'test_1 =  {<"s1" "s2" 1 2> <"s3" "s4" 0 0>}'
        test_dat = read_opl_text(tdf1,test_str,False)
        self.assertTrue(test_dat.test_1["s1"]["sf2"] == "s2")
        self.assertTrue(test_dat.test_1["s1"]["nf2"] == 2)
        self.assertTrue(test_dat.test_1["s2"]["nf1"] == 0)

        tdf2 = TicDatFactory( test_2 = [["sf1"],[]] )
        test_str = 'test_2 =  {<"s3">}'
        test_dat = read_opl_text(tdf2,test_str,False)
        self.assertTrue(list(test_dat.test_2.keys())[0] == "s3")

        tdf3 = TicDatFactory( test_3 = [["nf1"],[]] )
        tdf3.set_data_type("test_3", "nf1", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        test_str = 'test_3 =  {<6> <5>}'
        test_dat = read_opl_text(tdf3,test_str,False)
        self.assertTrue(6 in test_dat.test_3.keys())
        self.assertTrue(5 in test_dat.test_3.keys())
        self.assertTrue(len(test_dat.test_3.keys()) == 2)

        tdf4 = TicDatFactory( test_4 = [["nf1"],["nf2","nf3","nf4"]] )
        tdf4.set_data_type("test_4", "nf1", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        tdf4.set_data_type("test_4", "nf2", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        tdf4.set_data_type("test_4", "nf3", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        tdf4.set_data_type("test_4", "nf4", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        test_str = 'test_4 =  {<7 0 809 9>}'
        test_dat = read_opl_text(tdf4,test_str,False)
        self.assertTrue(7 in test_dat.test_4.keys())
        self.assertTrue(len(test_dat.test_4[7]) == 3)
        self.assertTrue(test_dat.test_4[7]["nf3"] == 809)

        tdf5 = TicDatFactory( test_5 = [["sf1"],["sf2"]] )
        tdf5.set_data_type("test_5", "sf2", number_allowed=False, strings_allowed='*')
        test_str = 'test_5 =  {<"s4" "s5">}'
        test_dat = read_opl_text(tdf5,test_str,False)
        self.assertTrue("s4" in test_dat.test_5.keys())
        self.assertTrue(test_dat.test_5["s4"]["sf2"] == "s5")

        tdf6 = TicDatFactory( test_6 = [["nf1"],["sf1"]] )
        tdf6.set_data_type("test_6", "nf1", min=0, max=float("inf"), inclusive_min=True, inclusive_max=False)
        tdf6.set_data_type("test_6", "sf1", number_allowed=False, strings_allowed='*')
        test_str = 'test_6 =  {<0 "s6">}'
        test_dat = read_opl_text(tdf6,test_str,False)
        self.assertTrue(0 in test_dat.test_6.keys())
        self.assertTrue(test_dat.test_6[0.0]['sf1'] == "s6")


if __name__ == "__main__":
    unittest.main()