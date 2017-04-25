import os
from ticdat.ampl import create_ampl_text, read_ampl_text, ampl_run,create_ampl_mod_text
from ticdat.ampl import _can_run_ampl_run_tests
from ticdat.ampl import _fix_fields_with_ampl_keywords, _unfix_fields_with_ampl_keywords
import sys
from ticdat.ticdatfactory import TicDatFactory
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import dietData, dietSchema, netflowData, addNetflowDataTypes, nearlySame
from ticdat.testing.ticdattestutils import  netflowSchema,sillyMeData, sillyMeSchema
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone, get_testing_file_path
import unittest

#@fail_to_debugger
class TestAmpl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_value = utils.development_deployed_environment
        utils.development_deployed_environment = True
    @classmethod
    def tearDownClass(cls):
        utils.development_deployed_environment = cls._original_value
    def testDiet_ampl_runRequired(self):
        self.assertTrue(_can_run_ampl_run_tests)
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
        ampl_soln = ampl_run(get_testing_file_path("sample_diet_ampl.mod"), in_tdf, makeDat(), soln_tdf)
        self.assertTrue(nearlySame(ampl_soln.parameters["Total Cost"]["Parameter Value"], 11.829, epsilon=0.0001))
        self.assertTrue(nearlySame(ampl_soln.consume_nutrition["protein"]["Qty"], 91, epsilon=0.0001))
        # ampl_run should not complete when there is an infeasible solution
        # I don't actually know what it should do
        dat = makeDat()
        dat.categories["calories"]["Min Nutrition"] = dat.categories["calories"]["Max Nutrition"]+1
        try:
            ampl_soln = ampl_run(get_testing_file_path("sample_diet.mod"), in_tdf, dat, soln_tdf)
            self.assertTrue(False)
        except:
            self.assertTrue(True)
    def tesSomething_amplrunRequired(self):
        # Not sure what the second example will be yet, they might have netflow
        self.assertTrue(_can_run_ampl_run_tests)
        in_tdf = TicDatFactory(**netflowSchema())
        in_tdf.enable_foreign_key_links()
        soln_tdf = TicDatFactory(flow=[["source", "destination", "commodity"], ["quantity"]],
                                 parameters=[["paramKey"], ["value"]])
        dat = in_tdf.TicDat(**{t: getattr(netflowData(), t) for t in in_tdf.primary_key_fields})
        ampl_soln = ampl_run("sample_netflow.mod", in_tdf, dat, soln_tdf)
        self.assertTrue(nearlySame(ampl_soln.parameters["Total Cost"]["value"],5500))
        self.assertTrue(nearlySame(ampl_soln.flow["Pens", "Detroit", "New York"]["quantity"], 30))
    def testDiet(self):
        tdf = TicDatFactory(**dietSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.TicDat(**{t:getattr(dietData(),t) for t in tdf.primary_key_fields})
        oldDatStr = create_ampl_text(tdf, oldDat)
        newDat = read_ampl_text(tdf, oldDatStr)
        self.assertFalse(tdf._same_data(oldDat, newDat))
        oldDat.categories["protein"]["maxNutrition"]=12 # Remove infinity from the data
        changedDatStr = create_ampl_text(tdf, oldDat)
        changedDat = read_ampl_text(tdf, changedDatStr)
        self.assertTrue(tdf._same_data(oldDat,changedDat))
        tdf.ampl_prepend = "pre_"
        origStr, changedDatStr = changedDatStr, create_ampl_text(tdf, oldDat)
        changedDat = read_ampl_text(tdf, changedDatStr)
        self.assertTrue(tdf._same_data(oldDat,changedDat))
        self.assertFalse(origStr == changedDatStr)

    def testNetflow(self):
        tdf = TicDatFactory(**netflowSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**{t:getattr(netflowData(),t) for t in tdf.primary_key_fields}))
        oldDatStr = create_ampl_text(tdf, oldDat)
        newDat = read_ampl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
        tdf.ampl_prepend = "stuff"
        oldDatStr = create_ampl_text(tdf, oldDat)
        newDat = read_ampl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

    def testSilly(self):
        tdf = TicDatFactory(**sillyMeSchema())
        tdf.enable_foreign_key_links()
        oldDat = tdf.freeze_me(tdf.TicDat(**sillyMeData()))
        oldDatStr = create_ampl_text(tdf, oldDat)
        newDat = read_ampl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))
        tdf.ampl_prepend = "ooooo"
        oldDatStr = create_ampl_text(tdf, oldDat)
        newDat = read_ampl_text(tdf, oldDatStr)
        self.assertTrue(tdf._same_data(oldDat, newDat))

if __name__ == "__main__":
    unittest.main()