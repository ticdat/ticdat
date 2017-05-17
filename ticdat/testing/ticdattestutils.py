import os,sys
import unittest
from shutil import rmtree, copy
import itertools
import fnmatch
from ticdat.utils import dictish, containerish, verify
import unittest
from ticdat import TicDatFactory, Model, Slicer

__codeFile = []
def _codeFile() :
    if __codeFile:
        return __codeFile[0]
    import inspect
    __codeFile[:]=[os.path.abspath(inspect.getsourcefile(_codeFile))]
    return _codeFile()

def _codeDir():
    return os.path.dirname(_codeFile())

def get_testing_file_path(base_name):
    rtn = os.path.join(_codeDir(), base_name)
    assert os.path.exists(rtn)
    return rtn

def configure_blank_accdb():
    verify(os.path.isfile("blank.accdb"),
           "You need a blank.accdb file in your current directory.")
    mdb_dir = os.path.abspath(os.path.join(_codeDir(), ".."))
    v_str = "Contact ticdat support at ticdat@opalytics.com"
    verify(os.path.isdir(mdb_dir), "%s is strangely not a directory. %s"%(mdb_dir, v_str))
    verify(os.path.isfile(os.path.join(mdb_dir, "mdb.py")), "mdb.py is missing. %s"%v_str)
    copy("blank.accdb", mdb_dir)

def configure_oplrun_path():
    if sys.platform in ['win32']:
        oplrun_name = os.path.abspath('oplrun.exe')
    else:
        oplrun_name = os.path.abspath('oplrun')
    verify(os.path.isfile(oplrun_name), "You need to be in the directory containing oplrun")
    opl_dir = os.path.abspath(os.path.join(_codeDir(), ".."))
    v_str = "Contact ticdat support at ticdat@opalytics.com"
    verify(os.path.isdir(opl_dir), "%s is strangely not a directory. %s"%(opl_dir, v_str))
    verify(os.path.isfile(os.path.join(opl_dir,"opl.py")), "opl.py is missing. %s"%v_str)
    oplrun_path = os.path.abspath(oplrun_name)
    with open(os.path.join(opl_dir, "oplrun_path.txt"), "w") as f:
        f.write(oplrun_path)

def configure_runlingo_path():
    if sys.platform in ['win32']:
        runlingo_name = os.path.abspath('runlingo.exe')
    else:
        runlingo_name = os.path.abspath('runlingo')
    verify(os.path.isfile(runlingo_name), "You need to be in the directory containing runlingo")
    lingo_dir = os.path.abspath(os.path.join(_codeDir(), ".."))
    v_str = "Contact ticdat support at ticdat@opalytics.com"
    verify(os.path.isdir(lingo_dir), "%s is strangely not a directory. %s"%(lingo_dir, v_str))
    verify(os.path.isfile(os.path.join(lingo_dir,"lingo.py")), "opl.py is missing. %s"%v_str)
    runlingo_path = os.path.abspath(runlingo_name)
    with open(os.path.join(lingo_dir, "runlingo_path.txt"), "w") as f:
        f.write(runlingo_path)

_debug = []
def _asserting() :
    _debug.append(())
    return _debug
assert _asserting()

def DEBUG() :
    return bool(_debug)

def firesException(f) :
    try:
        f()
    except Exception as e:
        return e

_memo = []
def memo(*args):
    rtn = list(_memo)
    _memo[:] = args
    return rtn[0] if len(rtn) == 1 else rtn

def clean_denormalization_errors(chk):
    return {k:{_k:set(_v) for _k,_v in v.items()} for k,v in chk.items()}

def fail_to_debugger(cls) :
    """
    decorator to allow a unittest class to enter the debugger if a unit test fails
    :param cls: a unittest class
    :return: cls decorated so as to fail to the ipdb debugger
    CAVEATS : Will side effect the unittest module by redirecting the main function!
              This routine is intended for **temporary** decorating of a unittest class
              for debugging/troubleshooting.
    """
    def _failToDebugger(x) :
        if not (x) :
            import ipdb; ipdb.set_trace()
            assert(x)
    cls.assertTrue = lambda s,x : _failToDebugger(x)
    cls.assertFalse = lambda s,x : _failToDebugger(not x)
    cls.failToDebugger = True
    unittest.main = lambda : _runSuite(cls)
    return cls

def flagged_as_run_alone(f):
    """
    a decorator to flag a unittest test function to be the sole test run for
    a fail_to_debugger decorated class
    :param f: a unittest test function
    :return: the same function decorated for fail_to_debugger
    """
    f.runAlone = True
    return f

def _runSuite(cls):
    _rtn = [getattr(cls, x) for x in dir(cls)
           if x.startswith("test")]
    assert all(callable(x) for x in _rtn)

    runalones = [x for x in _rtn if  hasattr(x, "runAlone")]
    assert len(runalones) <= 1, "you specified more than one to runAlone!"
    if runalones:
        _rtn = [runalones[0].__name__]
    else:
        _rtn = [x.__name__ for x in _rtn ]

    suite = unittest.TestSuite()
    for x in _rtn :
        suite.addTest(cls(x))
    if "failToDebugger" in dir(cls) and cls.failToDebugger :
        print("!!! Debugging suite for " + str(cls) + " !!!\n")
        suite.debug()
        print("!!! Debugged suite for " + str(cls) + " !!!\n")
    else :
        unittest.TextTestRunner().run(suite)

def unixWildCardDir(dirPath, unixStyleStr, fullPath = True, collapseSingleton = True, allowLinks = False) :
    assert os.path.isdir(dirPath)
    rtn = [x for x in os.listdir(dirPath) if fnmatch.fnmatch(x, unixStyleStr)]
    if (fullPath) :
        rtn = [os.path.abspath(os.path.join(dirPath, x)) for x in rtn]
    if (not allowLinks) :
        rtn = [x for x in rtn if not os.path.islink(x) ]
    if ( (len(rtn) == 1)  and collapseSingleton):
        return rtn[0]
    return rtn

def findAllUnixWildCard(directory, unixStyleStr, fullPath = True, recursive=True, allowLinks = False, skippedDirs = None):
    _skippedDirs = skippedDirs or [] # Py does scary stuff with mutable default arguments
    assert(os.path.isdir(directory))
    assert (not _skippedDirs)  or (recursive), "skipping directories makes sense only for recursive"
    rtn = unixWildCardDir(directory, unixStyleStr, fullPath = fullPath,
                          collapseSingleton = False, allowLinks = allowLinks)
    dirList=os.listdir(directory)

    for fname in dirList:
        fname = os.path.join(directory,  fname)
        if ( (os.path.isdir(fname)) and recursive and fname not in _skippedDirs) :
            rtn = rtn + findAllUnixWildCard(fname, unixStyleStr, recursive = True,
                                            allowLinks = allowLinks, skippedDirs = _skippedDirs)
    return rtn

def findAllFiles(path, extensions) :
    assert os.path.isdir(path)
    return list(itertools.chain(*[findAllUnixWildCard(path, "*" + x) for x in extensions]))

def makeCleanDir(path) :
    assert not os.path.exists(path) or os.path.isdir(path)
    rmtree(path, ignore_errors = True)
    os.mkdir(path)
    return path
def makeCleanPath(path) :
    if os.path.exists(path) :
        if os.path.isdir(path) :
            makeCleanDir(path)
        else :
            os.remove(path)
    return path

#assert is magic. It  can't be passed around directly. Lambda encapsulation lets us pass around a proxy. Yay lambda
def assertTrue(x) :
    assert (x)

def assertFalse(x) :
    assert (not x)

def spacesSchema() :
    return {"a_table" : [("a Field",),("a Data 1", "a Data 2", "a Data 3") ],
            "b_table" : [("b Field 1", "b Field 2", "b Field 3"), ["b Data"]],
            "c_table" : [[],("c Data 1", "c Data 2", "c Data 3", "c Data 4")]}

def spacesData() :
    return {
        "a_table" : {1 : {"a Data 3":3, "a Data 2":2, "a Data 1":1},
                     "b" : ("b", "d", 12), 0.23 : (11, 12, "thirt")},
        "b_table" : {(1, 2, 3) : 1, ("a", "b", "b") : 12},
        "c_table" : ((1, 2, 3, 4),
                      {"c Data 4":"d", "c Data 2":"b", "c Data 3":"c", "c Data 1":"a"},
                      ("a", "b", 12, 24) )
    }

def assertTicDatTablesSame(t1, t2, _goodTicDatTable,
                           _assertTrue = assertTrue, _assertFalse = assertFalse) :
    _assertTrue(set(t1) == set(t2))
    _assertTrue(_goodTicDatTable(t1) and _goodTicDatTable(t2))
    if not dictish(t1) and not dictish(t2) :
        return
    if dictish(t1) != dictish(t2) and dictish(t2) :
        t1,t2 = t2,t1
    if not dictish(t2) :
        _assertTrue(all(containerish(x) and len(x) == 0 for x in t1.values()))
        return
    for k1,v1 in t1.items() :
        v2 = t2[k1]
        if dictish(v1) != dictish(v2) and dictish(v2) :
            v2, v1 = v1, v2
        if dictish(v1) and dictish(v2) :
            _assertTrue(set(v1) == set(v2))
            for _k1 in v1 :
                _assertTrue(v1[_k1] == v2[_k1])
        elif dictish(v1) and containerish(v2) :
            _assertTrue(sorted(map(str, v1.values())) == sorted(map(str, v2)))
        elif dictish(v1) :
            _assertTrue(len(v1) == 1 and v1.values()[0] == v2)
        else :
            if containerish(v1) != containerish(v2) and containerish(v2) :
                v2, v1 = v1, v2
            if containerish(v1) and containerish(v2) :
                _assertTrue(len(v1) == len(v2))
                _assertTrue(all(v1[x] == v2[x] for x in range(len(v1))))
            elif containerish(v1) :
                _assertTrue(len(v1) == 1 and v1[0] == v2)
            else :
                _assertTrue(v1 == v2)

def deepFlatten(x) :
    # does a FULL recursive flatten.
    # this works for 2.7, will need to be replaced for 3
    # make sure replaced version works equally well with tuples as lists
    import compiler
    return tuple(compiler.ast.flatten(x))

def shallowFlatten(x) :
    return tuple(itertools.chain(*x))

# gurobi netflow problem - http://www.gurobi.com/documentation/6.0/example-tour/netflow_py
def netflowSchema():
    return {
        "commodities" : [["name"], []],
        "nodes": [["name"],[]],
        "arcs" : [("source", "destination"),["capacity"]],
        "cost" : [("commodity", "source", "destination"),["cost"]],
        "inflow" :[["commodity", "node"], ["quantity"]],
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

def addNetflowForeignKeys(tdf) :
    tdf.add_foreign_key("arcs", "nodes", [u'source', u'name'])
    tdf.add_foreign_key("arcs", "nodes", (u'destination', u'name'))
    tdf.add_foreign_key("cost", "nodes", (u'source', u'name'))
    tdf.add_foreign_key("cost", "nodes", [u'destination', u'name'])
    tdf.add_foreign_key("cost", "commodities", (u'commodity', u'name'))
    tdf.add_foreign_key("inflow", "commodities", (u'commodity', u'name'))
    tdf.add_foreign_key("inflow", "nodes", [u'node', u'name'])

def addNetflowDataTypes(tdf):
    tdf.set_data_type("arcs", "capacity")
    tdf.set_data_type("cost", "cost")
    tdf.set_data_type("inflow", "quantity", min=-float("inf"),
                              inclusive_min=False)

# gurobi diet problem - http://www.gurobi.com/documentation/6.0/example-tour/diet_py
def dietSchema():
    return {
     "categories" : (("name",),["minNutrition", "maxNutrition"]),
     "foods" :[["name"],("cost",)],
     "nutritionQuantities" : (["food", "category"], ["qty"])
    }

def dietSchemaWeirdCase():
    return {
     "cateGories" : (("name",),["miNnutrition", "maXnutrition"]),
     "foodS" :[["name"],("COST",)],
     "nutritionquantities" : (["food", "category"], ["qtY"])
    }
def copyDataDietWeirdCase(dat):
    tdf = TicDatFactory(**dietSchemaWeirdCase())
    rtn = tdf.TicDat()
    for c,r in dat.categories.items():
        rtn.cateGories[c]["miNnutrition"] = r["minNutrition"]
        rtn.cateGories[c]["maXnutrition"] = r["maxNutrition"]
    for f,r in dat.foods.items():
        rtn.foodS[f] = r["cost"]
    for (f,c),r in dat.nutritionQuantities.items():
        rtn.nutritionquantities[f,c] = r["qty"]
    return rtn
def dietSchemaWeirdCase2():
    rtn = dietSchemaWeirdCase()
    rtn["nutrition_quantities"] = rtn["nutritionquantities"]
    del(rtn["nutritionquantities"])
    return rtn
def copyDataDietWeirdCase2(dat):
    tdf = TicDatFactory(**dietSchemaWeirdCase2())
    tmp = copyDataDietWeirdCase(dat)
    rtn = tdf.TicDat(cateGories = tmp.cateGories, foodS = tmp.foodS)
    for (f,c),r in tmp.nutritionquantities.items():
        rtn.nutrition_quantities[f,c] = r
    return rtn

def addDietForeignKeys(tdf) :
    tdf.add_foreign_key("nutritionQuantities", 'categories',[u'category', u'name'])
    tdf.add_foreign_key("nutritionQuantities", 'foods', (u'food', u'name'))

def addDietDataTypes(tdf):
    for table, fields in tdf.data_fields.items():
        for field in fields:
            tdf.set_data_type(table, field)
    # We override the default data type for maxNutrition which can accept infinity
    tdf.set_data_type("categories", "maxNutrition", max=float("inf"),
                              inclusive_max=True)

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

def dietSolver(modelType):
    tdf = TicDatFactory(**dietSchema())
    addDietForeignKeys(tdf)
    addDietDataTypes(tdf)

    dat = tdf.copy_tic_dat(dietData())
    assert not tdf.find_data_type_failures(dat) and not tdf.find_foreign_key_failures(dat)

    mdl = Model(modelType, "diet")

    nutrition = {}
    for c,n in dat.categories.items() :
        nutrition[c] = mdl.add_var(lb=n["minNutrition"], ub=n["maxNutrition"], name=c)

    # Create decision variables for the foods to buy
    buy = {}
    for f in dat.foods:
        buy[f] = mdl.add_var(name=f)

     # Nutrition constraints
    for c in dat.categories:
        mdl.add_constraint(mdl.sum(dat.nutritionQuantities[f,c]["qty"] * buy[f]
                             for f in dat.foods)
                           == nutrition[c],
                           name = c)

    mdl.set_objective(mdl.sum(buy[f] * c["cost"] for f,c in dat.foods.items()))

    if mdl.optimize():
        solutionFactory = TicDatFactory(
                parameters = [[],["totalCost"]],
                buyFood = [["food"],["qty"]],
                consumeNutrition = [["category"],["qty"]])
        sln = solutionFactory.TicDat()
        for f,x in buy.items():
            if mdl.get_solution_value(x) > 0.0001:
                sln.buyFood[f] = mdl.get_solution_value(x)
        for c,x in nutrition.items():
            sln.consumeNutrition[c] = mdl.get_solution_value(x)
        return sln, sum(dat.foods[f]["cost"] * r["qty"] for f,r in sln.buyFood.items())


def netflowSolver(modelType):
    tdf = TicDatFactory(**netflowSchema())
    addNetflowForeignKeys(tdf)
    addNetflowDataTypes(tdf)

    dat = tdf.copy_tic_dat(netflowData())
    assert not tdf.find_data_type_failures(dat) and not tdf.find_foreign_key_failures(dat)

    mdl = Model(modelType, "netflow")

    flow = {}
    for h, i, j in dat.cost:
        if (i,j) in dat.arcs:
            flow[h,i,j] = mdl.add_var(name='flow_%s_%s_%s' % (h, i, j))

    flowslice = Slicer(flow)

    for i_,j_ in dat.arcs:
        mdl.add_constraint(mdl.sum(flow[h,i,j] for h,i,j in flowslice.slice('*',i_, j_))
                     <= dat.arcs[i_,j_]["capacity"],
                     name = 'cap_%s_%s' % (i_, j_))

    for h_,j_ in set(k for k,v in dat.inflow.items() if abs(v["quantity"]) > 0).union(
            {(h,i) for h,i,j in flow}, {(h,j) for h,i,j in flow}) :
        mdl.add_constraint(
          mdl.sum(flow[h,i,j] for h,i,j in flowslice.slice(h_,'*',j_)) +
              dat.inflow.get((h_,j_), {"quantity":0})["quantity"] ==
          mdl.sum(flow[h,i,j] for h,i,j in flowslice.slice(h_, j_, '*')),
                   name = 'node_%s_%s' % (h_, j_))

    mdl.set_objective(mdl.sum(flow * dat.cost[h, i, j]["cost"] for (h, i, j),flow in flow.items()))
    if mdl.optimize():
        solutionFactory = TicDatFactory(
                flow = [["commodity", "source", "destination"], ["quantity"]])
        if mdl.optimize():
            rtn = solutionFactory.TicDat()
            for (h, i, j),var in flow.items():
                if mdl.get_solution_value(var) > 0:
                    rtn.flow[h,i,j] = mdl.get_solution_value(var)
            return rtn, sum(dat.cost[h,i,j]["cost"] * r["quantity"] for (h,i,j),r in rtn.flow.items())

def sillyMeSchema() :
    return {"a" : [("aField",),("aData1", "aData2", "aData3") ],
            "b" : [("bField1", "bField2", "bField3"), ["bData"]],
            "c" : [[],("cData1", "cData2", "cData3", "cData4")]}


def sillyMeData() :
    return {
        "a" : {1 : (1, 2, 3), "b" : ("b", "d", 12), 0.23 : (11, 12, "thirt")},
        "b" : {(1, 2, 3) : 1, ("a", "b", "b") : 12},
        "c" : ((1, 2, 3, 4), ("a", "b", "c", "d"), ("a", "b", 12, 24) )
    }

EPSILON = 1e-05

def perError(x1, x2) :
    x1 = float(x1)
    x2 = float(x2)
    if (x1 < 0) and (x2 <  0) :
        return perError(-x1, -x2)
    if (x1 == float("inf")) :
        return 0 if (x2 == float("inf")) else x1
    SMALL_NOT_ZERO = 1e-10
    assert(EPSILON>SMALL_NOT_ZERO)
    abs1 = abs(x1)
    abs2 = abs(x2)
    # is it safe to divide by the bigger absolute value
    if (max(abs1, abs2) > SMALL_NOT_ZERO) :
        rtn = ((max(x1, x2) - min(x1, x2)) / max(abs1, abs2))
        return rtn
    return 0

def _nearlySame(x1, x2, epsilon) :
    return perError(x1, x2) < epsilon

def nearlySame(*args, **kwargs) :
    assert not kwargs or kwargs.keys() == ["epsilon"]
    epsilon = kwargs.get("epsilon", EPSILON)
    if len(args) < 2 :
        return True
    return all (_nearlySame(x[0], x[1], epsilon) for x in
                itertools.combinations(args, 2))

