import os
import ticdat.utils as utils

__codeFile = []
def _codeFile() :
    if __codeFile:
        return __codeFile[0]
    import inspect
    __codeFile[:]=[os.path.abspath(inspect.getsourcefile(_codeFile))]
    return _codeFile()

def _codeDir():
    return os.path.dirname(_codeFile())


def firesException(f) :
    try:
        f()
    except Exception as e:
        return e

# gurobi netflow problem - http://www.gurobi.com/documentation/6.0/example-tour/netflow_py
def netflowSchema():
    return {
        "primaryKeyFields" : {"commodities" : "name", "nodes":"name", "arcs" : ("source", "destination"),
                              "cost" : ("commodity", "source", "destination"),
                              "inflow" : ("commodity", "node")},
        "dataFields" : {"arcs" : "capacity", "cost" : "cost", "inflow" : "quantity"}
    }
def netflowData() :
    class _(object) :
        pass

    dat = _() # simplest object with a __dict__

    # simplest possible copy

    dat.commodities = ['Pencils', 'Pens']
    dat.nodes = ['Detroit', 'Denver', 'Boston', 'New York', 'Seattle']

    dat.arcs = {
('Detroit', 'Boston'):   100,
('Detroit', 'New York'):  80,
('Detroit', 'Seattle'):  120,
('Denver',  'Boston'):   120,
('Denver',  'New York'): 120,
('Denver',  'Seattle'):  120 }

    dat.cost = {
('Pencils', 'Detroit', 'Boston'):   10,
('Pencils', 'Detroit', 'New York'): 20,
('Pencils', 'Detroit', 'Seattle'):  60,
('Pencils', 'Denver',  'Boston'):   40,
('Pencils', 'Denver',  'New York'): 40,
('Pencils', 'Denver',  'Seattle'):  30,
('Pens',    'Detroit', 'Boston'):   20,
('Pens',    'Detroit', 'New York'): 20,
('Pens',    'Detroit', 'Seattle'):  80,
('Pens',    'Denver',  'Boston'):   60,
('Pens',    'Denver',  'New York'): 70,
('Pens',    'Denver',  'Seattle'):  30 }

    dat.inflow = {
('Pencils', 'Detroit'):   50,
('Pencils', 'Denver'):    60,
('Pencils', 'Boston'):   -50,
('Pencils', 'New York'): -50,
('Pencils', 'Seattle'):  -10,
('Pens',    'Detroit'):   60,
('Pens',    'Denver'):    40,
('Pens',    'Boston'):   -40,
('Pens',    'New York'): -30,
('Pens',    'Seattle'):  -30 }

    return dat

# gurobi diet problem - http://www.gurobi.com/documentation/6.0/example-tour/diet_py
def dietSchema():
    return {
        # deliberately mixing up singleton containers and strings for situations where one field is being listed
        "primaryKeyFields" : {"categories" : ("name",), "foods" : "name", "nutritionQuantities" : ("food", "category")},
        "dataFields" : {"categories" : ("minNutrition", "maxNutrition"),
                        "foods": "cost",
                        "nutritionQuantities" : ["qty"]}
    }
def dietData():
    # this is the gurobi diet data in ticDat format

    class _(object) :
        pass

    dat = _() # simplest object with a __dict__

    dat.categories = {
      'calories': {"minNutrition": 1800, "maxNutrition" : 2200},
      'protein':  {"minNutrition": 91,   "maxNutrition" : float("inf")},
      'fat':      {"minNutrition": 0,    "maxNutrition" : 65},
      'sodium':   {"minNutrition": 0,    "maxNutrition" : 1779}}

    # deliberately goofing on it a little bit
    dat.foods = {
      'hamburger': {"cost": 2.49},
      'chicken':   {"cost": 2.89},
      'hot dog':   {"cost": 1.50},
      'fries':     {"cost": 1.89},
      'macaroni':  2.09,
      'pizza':     {"cost": 1.99},
      'salad':     {"cost": 2.49},
      'milk':      (0.89,),
      'ice cream': {"cost": 1.59}}

    dat.nutritionQuantities = {
      ('hamburger', 'calories'): {"qty" : 410},
      ('hamburger', 'protein'):  {"qty" : 24},
      ('hamburger', 'fat'):      {"qty" : 26},
      ('hamburger', 'sodium'):   {"qty" : 730},
      ('chicken',   'calories'): {"qty" : 420},
      ('chicken',   'protein'):  {"qty" : 32},
      ('chicken',   'fat'):      {"qty" : 10},
      ('chicken',   'sodium'):   {"qty" : 1190},
      ('hot dog',   'calories'): {"qty" : 560},
      ('hot dog',   'protein'):  {"qty" : 20},
      ('hot dog',   'fat'):      {"qty" : 32},
      ('hot dog',   'sodium'):   {"qty" : 1800},
      ('fries',     'calories'): {"qty" : 380},
      ('fries',     'protein'):  {"qty" : 4},
      ('fries',     'fat'):      {"qty" : 19},
      ('fries',     'sodium'):   {"qty" : 270},
      ('macaroni',  'calories'): {"qty" : 320},
      ('macaroni',  'protein'):  {"qty" : 12},
      ('macaroni',  'fat'):      {"qty" : 10},
      ('macaroni',  'sodium'):   {"qty" : 930},
      ('pizza',     'calories'): {"qty" : 320},
      ('pizza',     'protein'):  {"qty" : 15},
      ('pizza',     'fat'):      {"qty" : 12},
      ('pizza',     'sodium'):   {"qty" : 820},
      ('salad',     'calories'): {"qty" : 320},
      ('salad',     'protein'):  {"qty" : 31},
      ('salad',     'fat'):      {"qty" : 12},
      ('salad',     'sodium'):   {"qty" : 1230},
      ('milk',      'calories'): {"qty" : 100},
      ('milk',      'protein'):  {"qty" : 8},
      ('milk',      'fat'):      {"qty" : 2.5},
      ('milk',      'sodium'):   {"qty" : 125},
      ('ice cream', 'calories'): {"qty" : 330},
      ('ice cream', 'protein'):  {"qty" : 8},
      ('ice cream', 'fat'):      {"qty" : 10},
      ('ice cream', 'sodium'):   {"qty" : 180} }

    return dat

def sillyMeSchema() :
    return {
        "primaryKeyFields" : { "a" : ("aField",), "b" : ("bField1", "bField2", "bField3") },
        "dataFields" : { "a" : ("aData1", "aData2", "aData3"), "b" : "bData",
                         "c" : ("cData1", "cData2", "cData3", "cData4")}
    }

def sillyMeData() :
    return {
        "a" : {1 : (1, 2, 3), "b" : ("b", "d", 12), 0.23 : (11, 12, "thirt")},
        "b" : {(1, 2, 3) : 1, ("a", "b", "b") : 12},
        "c" : ((1, 2, 3, 4), ("a", "b", "c", "d"), ("a", "b", 12, 24) )
    }