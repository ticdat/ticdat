# on OSX operating system I used
# %env PATH = PATH:/Users/myusername/ampl/ampl
# from within ipython to find the AMPL executable. This was the directory created by
# ampl.macosx64.tgz (which I had placed in an ampl directory and then opened to create
# another ampl directory).

# these are the unit tests for the newer style AMPL-ticdat code (I.e. using PanDatFactory).

import os
import sys
from ticdat import PanDatFactory, TicDatFactory
import ticdat.utils as utils
from ticdat.testing.ticdattestutils import nearlySame, firesException, fail_to_debugger, pan_dat_maker
from ticdat.testing.ticdattestutils import flagged_as_run_alone
import unittest
from itertools import product
from collections import defaultdict
from ticdat.pandatfactory import pd
try:
    import amplpy
except:
    amplpy = None


# using the diet/netlfow flavors consistent with the examples instead of the older ones in
# ticdat.testing.ticdattestutils

def _pan_dat_maker_from_dict(schema, tic_dat_dict):
    tdf = TicDatFactory(**schema)
    tic_dat = tdf.TicDat(**tic_dat_dict)
    return pan_dat_maker(schema, tic_dat)

# the diet_mod is hacked from the example code to exercise more amplpy behaviors
_diet_mod = """
    set CAT;
    set FOOD;

    param cost {FOOD};

    param n_min {CAT} >= 0;
    param n_max {i in CAT} >= n_min[i];

    param amt {FOOD, CAT} >= 0;
    param other_amt {FOOD, CAT} >= 0;

    var Buy {j in FOOD} >= 0;
    var Consume {i in CAT } >= n_min [i], <= n_max [i];

    minimize Total_Cost:  sum {j in FOOD} cost[j] * Buy[j];

    subject to Diet {i in CAT}:
       Consume[i] =  sum {j in FOOD} (amt[j,i] + other_amt[j,i]) * Buy[j];
    """
_diet_input_pdf = PanDatFactory (
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods  = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity", "Other Quantity"]])
_diet_pan_dat_from_dict = lambda pd_dict : _pan_dat_maker_from_dict(_diet_input_pdf.schema(), pd_dict)
_diet_dat = _pan_dat_maker_from_dict(_diet_input_pdf.schema(),
{'categories': {
  u'calories': {'Max Nutrition': 2200.0, 'Min Nutrition': 1800},
  u'fat': {'Max Nutrition': 65.0, 'Min Nutrition': 0},
  u'protein': {'Max Nutrition': float('inf'), 'Min Nutrition': 91},
  u'sodium': {'Max Nutrition': 1779.0, 'Min Nutrition': 0}},
 'foods': {u'chicken': {'Cost': 2.89},
  u'fries': {'Cost': 1.89},
  u'hamburger': {'Cost': 2.49},
  u'hot dog': {'Cost': 1.5},
  u'ice cream': {'Cost': 1.59},
  u'macaroni': {'Cost': 2.09},
  u'milk': {'Cost': 0.89},
  u'pizza': {'Cost': 1.99},
  u'salad': {'Cost': 2.49}}, 'nutrition_quantities': {
  (u'chicken', u'calories'): {'Quantity': 420},
  (u'chicken', u'fat'): {'Quantity': 10},
  (u'chicken', u'protein'): {'Quantity': 32},
  (u'chicken', u'sodium'): {'Quantity': 1190},
  (u'fries', u'calories'): {'Quantity': 380},
  (u'fries', u'fat'): {'Quantity': 19},
  (u'fries', u'protein'): {'Quantity': 4},
  (u'fries', u'sodium'): {'Quantity': 270},
  (u'hamburger', u'calories'): {'Quantity': 410},
  (u'hamburger', u'fat'): {'Quantity': 26},
  (u'hamburger', u'protein'): {'Quantity': 24},
  (u'hamburger', u'sodium'): {'Quantity': 730},
  (u'hot dog', u'calories'): {'Quantity': 560},
  (u'hot dog', u'fat'): {'Quantity': 32},
  (u'hot dog', u'protein'): {'Quantity': 20},
  (u'hot dog', u'sodium'): {'Quantity': 1800},
  (u'ice cream', u'calories'): {'Quantity': 330},
  (u'ice cream', u'fat'): {'Quantity': 10},
  (u'ice cream', u'protein'): {'Quantity': 8},
  (u'ice cream', u'sodium'): {'Quantity': 180},
  (u'macaroni', u'calories'): {'Quantity': 320},
  (u'macaroni', u'fat'): {'Quantity': 10},
  (u'macaroni', u'protein'): {'Quantity': 12},
  (u'macaroni', u'sodium'): {'Quantity': 930},
  (u'milk', u'calories'): {'Quantity': 100},
  (u'milk', u'fat'): {'Quantity': 2.5},
  (u'milk', u'protein'): {'Quantity': 8},
  (u'milk', u'sodium'): {'Quantity': 125},
  (u'pizza', u'calories'): {'Quantity': 320},
  (u'pizza', u'fat'): {'Quantity': 12},
  (u'pizza', u'protein'): {'Quantity': 15},
  (u'pizza', u'sodium'): {'Quantity': 820},
  (u'salad', u'calories'): {'Quantity': 320},
  (u'salad', u'fat'): {'Quantity': 12},
  (u'salad', u'protein'): {'Quantity': 31},
  (u'salad', u'sodium'): {'Quantity': 1230}}})
_diet_sln_pdf = PanDatFactory(
    parameters = [["Key"],["Value"]],
    buy_food = [["Food"],["Quantity"]],
    consume_nutrition = [["Category"],["Quantity"]])
_diet_sln_pandat = _pan_dat_maker_from_dict(_diet_sln_pdf.schema(),
{'buy_food': {
  u'hamburger': {'Quantity': 0.604513888889},
  u'ice cream': {'Quantity': 2.59131944444},
  u'milk': {'Quantity': 6.97013888889}},
 'consume_nutrition': {
  u'calories': {'Quantity': 1800},
  u'fat': {'Quantity': 59.0559027778},
  u'protein': {'Quantity': 91},
  u'sodium': {'Quantity': 1779}},
 'parameters': {u'Total Cost': {'Value': 11.8288611111}}})

_netflow_mod = """
    set NODES;
    set ARCS within {i in NODES, j in NODES: i <> j};
    set COMMODITIES;
    param capacity {ARCS} >= 0;
    param cost {COMMODITIES,ARCS} > 0;
    param inflow {COMMODITIES,NODES};
    var Flow {COMMODITIES,ARCS} >= 0;
    minimize TotalCost:
       sum {h in COMMODITIES, (i,j) in ARCS} cost[h,i,j] * Flow[h,i,j];
    subject to Capacity {(i,j) in ARCS}:
       sum {h in COMMODITIES} Flow[h,i,j] <= capacity[i,j];
    subject to Conservation {h in COMMODITIES, j in NODES}:
       sum {(i,j) in ARCS} Flow[h,i,j] + inflow[h,j] = sum {(j,i) in ARCS} Flow[h,j,i];
"""
_netflow_input_pdf = PanDatFactory(
    commodities=[["Name"], []],
    nodes=[["Name"], []],
    arcs=[["Source", "Destination"], ["Capacity"]],
    cost=[["Commodity", "Source", "Destination"], ["Cost"]],
    inflow=[["Commodity", "Node"], ["Quantity"]]
)
_netflow_dat = _pan_dat_maker_from_dict(_netflow_input_pdf.schema(),
{'arcs': {(u'Denver', u'Boston'): {'Capacity': 120.0},
  (u'Denver', u'New York'): {'Capacity': 120.0},
  (u'Denver', u'Seattle'): {'Capacity': 120.0},
  (u'Detroit', u'Boston'): {'Capacity': 100.0},
  (u'Detroit', u'New York'): {'Capacity': 80.0},
  (u'Detroit', u'Seattle'): {'Capacity': 120.0}},
 'commodities': {u'Pencils': {}, u'Pens': {}},
 'cost': {(u'Pencils', u'Denver', u'Boston'): {'Cost': 40},
  (u'Pencils', u'Denver', u'New York'): {'Cost': 40},
  (u'Pencils', u'Denver', u'Seattle'): {'Cost': 30},
  (u'Pencils', u'Detroit', u'Boston'): {'Cost': 10},
  (u'Pencils', u'Detroit', u'New York'): {'Cost': 20},
  (u'Pencils', u'Detroit', u'Seattle'): {'Cost': 60},
  (u'Pens', u'Denver', u'Boston'): {'Cost': 60},
  (u'Pens', u'Denver', u'New York'): {'Cost': 70},
  (u'Pens', u'Denver', u'Seattle'): {'Cost': 30},
  (u'Pens', u'Detroit', u'Boston'): {'Cost': 20},
  (u'Pens', u'Detroit', u'New York'): {'Cost': 20},
  (u'Pens', u'Detroit', u'Seattle'): {'Cost': 80}},
 'inflow': {(u'Pencils', u'Boston'): {'Quantity': -50},
  (u'Pencils', u'Denver'): {'Quantity': 60},
  (u'Pencils', u'Detroit'): {'Quantity': 50},
  (u'Pencils', u'New York'): {'Quantity': -50},
  (u'Pencils', u'Seattle'): {'Quantity': -10},
  (u'Pens', u'Boston'): {'Quantity': -40},
  (u'Pens', u'Denver'): {'Quantity': 40},
  (u'Pens', u'Detroit'): {'Quantity': 60},
  (u'Pens', u'New York'): {'Quantity': -30},
  (u'Pens', u'Seattle'): {'Quantity': -30}},
 'nodes': {u'Boston': {},
  u'Denver': {},
  u'Detroit': {},
  u'New York': {},
  u'Seattle': {}}})


_netflow_sln_pdf = PanDatFactory(
        flow = [["Commodity", "Source", "Destination"], ["Quantity"]],
        parameters = [["Key"],["Value"]])
_netflow_sln_pandat = _pan_dat_maker_from_dict(_netflow_sln_pdf.schema(),
{'flow': {
  (u'Pencils', u'Denver', u'New York'): {'Quantity': 50},
  (u'Pencils', u'Denver', u'Seattle'): {'Quantity': 10},
  (u'Pencils', u'Detroit', u'Boston'): {'Quantity': 50},
  (u'Pens', u'Denver', u'Boston'): {'Quantity': 10},
  (u'Pens', u'Denver', u'Seattle'): {'Quantity': 30},
  (u'Pens', u'Detroit', u'Boston'): {'Quantity': 30},
  (u'Pens', u'Detroit', u'New York'): {'Quantity': 30}},
 'parameters': {u'Total Cost': {'Value': 5500}}}
)

_metro_input_pdf = PanDatFactory(
    parameters=[["Parameter"], ["Value"]],
    load_amounts=[["Amount"],[]],
    number_of_one_way_trips=[["Number"],[]],
    amount_leftover=[["Amount"], []])
_metro_dat = _pan_dat_maker_from_dict(_metro_input_pdf.schema(),
{
  "amount_leftover": [[0.0],[0.25],[2],[3],[4],[1.25],[1.0],[8.5],[1.75],[0.75],[1.5],[0.5]],
  "load_amounts": [[2.25],[1], [3], [5], [4.5], [40], [10], [20]],
  "number_of_one_way_trips": [[2], [4], [6], [8], [10], [12], [14], [16], [18], [20]],
  "parameters": [["Amount Leftover Constraint", "Equality"]]}
)
_metro_solution_pdf = PanDatFactory(
    load_amount_details=[["Number One Way Trips", "Amount Leftover", "Load Amount"],
                           ["Number Of Visits"]],
    load_amount_summary=[["Number One Way Trips", "Amount Leftover"],["Number Of Visits"]])

def _metro_solve(dat, excluded_tables=
                 frozenset(_metro_input_pdf.all_tables).difference({"load_amounts"})):
    input_schema = _metro_input_pdf
    AMPL = amplpy.AMPL
    default_parameters = {"One Way Price": 2.25, "Amount Leftover Constraint": "Upper Bound"}
    assert input_schema.good_pan_dat_object(dat)
    full_parameters = dict(default_parameters)
    for k,v in dat.parameters.itertuples(index=False):
        full_parameters[k] = v

    ampl_dat = input_schema.copy_to_ampl(dat, excluded_tables=excluded_tables)
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    param amount_leftover_lb >= 0;
    param amount_leftover_ub >= amount_leftover_lb;
    param one_way_price >= 0;
    param number_trips >= 0;
    set LOAD_AMTS;
    var Num_Visits {LOAD_AMTS} integer >= 0;
    var Amt_Leftover >= amount_leftover_lb, <= amount_leftover_ub;
    minimize Total_Visits:
       sum {la in LOAD_AMTS} Num_Visits[la];
    subj to Set_Amt_Leftover:
       Amt_Leftover = sum {la in LOAD_AMTS} la * Num_Visits[la] - one_way_price * number_trips;""")
    input_schema.set_ampl_data(ampl_dat, ampl, {"load_amounts": "LOAD_AMTS"})

    load_amount_details = pd.DataFrame(columns=['Number One Way Trips', 'Amount Leftover', 'Load Amount',
                                             'Number Of Visits'])
    # solve a distinct MIP for each pair of (# of one-way-trips, amount leftover)
    for number_trips, amount_leftover in product(list(dat.number_of_one_way_trips["Number"]),
                                                 list(dat.amount_leftover["Amount"])):

        ampl.param['amount_leftover_lb'] = amount_leftover \
            if full_parameters["Amount Leftover Constraint"] == "Equality" else 0
        ampl.param['amount_leftover_ub'] = amount_leftover
        ampl.param['number_trips'] = number_trips
        ampl.param['one_way_price'] = full_parameters["One Way Price"]

        ampl.solve()
        if ampl.getValue("solve_result") != "infeasible":
            df = ampl.getVariable("Num_Visits").getValues().toPandas().reset_index()
            df.rename(columns = {df.columns[0]: "Load Amount", df.columns[1]: "Number Of Visits"}, inplace=True)
            df["Number One Way Trips"] = number_trips
            df["Amount Leftover"] = amount_leftover
            load_amount_details = load_amount_details.append(df[df["Number Of Visits"] > 0])

    load_amount_details.sort_values(by=["Number One Way Trips", "Amount Leftover", "Load Amount"], inplace=True)
    load_amount_summary = pd.DataFrame(columns=['Number One Way Trips', 'Amount Leftover', 'Number Of Visits'])
    if len(load_amount_details):
        cols = ["Number One Way Trips", "Amount Leftover"]
        load_amount_summary = load_amount_details.set_index(cols, drop=False).groupby(level=cols).aggregate(
                                                  {"Number Of Visits":sum}).reset_index().sort_values(by=cols)
    sln = _metro_solution_pdf.PanDat(load_amount_details=load_amount_details,
                                     load_amount_summary=load_amount_summary)

    return sln

#@fail_to_debugger
class TestAmpl(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._original_value = utils.development_deployed_environment
        utils.development_deployed_environment = True
    @classmethod
    def tearDownClass(cls):
        utils.development_deployed_environment = cls._original_value

    def firesException(self, f):
        e = firesException(f)
        if e :
            self.assertTrue("TicDatError" in e.__class__.__name__)
            return str(e)

    def test_metro_amplpy(self):
        def feas(sln, dat):
            one_way_price = 2.25
            self.assertTrue("One Way Price" not in dat.parameters)
            sub_totals = defaultdict(int)
            price_needed = {k:one_way_price*k[0] for k in sln.load_amount_summary}
            for k,v in sln.load_amount_details.items():
                price_needed[k[:2]] -= k[-1] * v.values()[0]
                sub_totals[k[:2]] += v.values()[0]
            self.assertTrue(sub_totals == {k:v.values()[0] for k,v in sln.load_amount_summary.items()})
            if "Amount Leftover Constraint" in dat.parameters and\
                dat.parameters["Amount Leftover Constraint"]["Value"] == "Equality":
                self.assertTrue(all(nearlySame(k[1], -v) for k,v in price_needed.items()))
            else:
                self.assertTrue(all(k[1] >= -v and v<=0 for k,v in price_needed.items()))

        sln = _metro_solution_pdf.copy_to_tic_dat(_metro_solve(_metro_dat))
        feas(sln, _metro_input_pdf.copy_to_tic_dat(_metro_dat))
        self.assertTrue({k:v.values()[0] for k,v in sln.load_amount_summary.items()} == {(2, 0.0): 1, (2, 0.5): 1,
(2, 0.75): 2, (2, 1.0): 2, (2, 1.5): 2, (2, 1.75): 3, (2, 2): 3, (2, 3): 2, (2, 4): 3, (2, 8.5): 2, (4, 0.0): 2,
(4, 0.25): 4, (4, 0.5): 2, (4, 0.75): 3, (4, 1.0): 1, (4, 1.25): 3, (4, 1.5): 3, (4, 1.75): 4, (4, 2): 2, (4, 3): 3,
(4, 4): 2, (4, 8.5): 3, (6, 0.0): 3, (6, 0.25): 5, (6, 0.5): 3, (6, 0.75): 4, (6, 1.0): 2, (6, 1.25): 4,
(6, 1.5): 2, (6, 1.75): 3, (6, 2): 3, (6, 3): 4, (6, 4): 3, (6, 8.5): 3, (8, 0.0): 3, (8, 0.25): 4, (8, 0.5): 4,
(8, 0.75): 5, (8, 1.0): 3, (8, 1.25): 5, (8, 1.5): 3, (8, 1.75): 4, (8, 2): 1, (8, 3): 2, (8, 4): 3, (8, 8.5): 4,
(10, 0.0): 4, (10, 0.25): 5,(10, 0.5): 2, (10, 0.75): 3, (10, 1.0): 4, (10, 1.25): 6, (10, 1.5): 3, (10, 1.75): 4,
(10, 2): 2, (10, 3): 3, (10, 4): 4, (10, 8.5): 3, (12, 0.0): 4, (12, 0.25): 3, (12, 0.5): 3, (12, 0.75): 4,
(12, 1.0): 3, (12, 1.25): 4, (12, 1.5): 4, (12, 1.75): 5, (12, 2): 3, (12, 3): 2, (12, 4): 3, (12, 8.5): 4,
(14, 0.0): 5, (14, 0.25): 4, (14, 0.5): 4, (14, 0.75): 3, (14, 1.0): 4, (14, 1.25): 5, (14, 1.5): 3, (14, 1.75): 4,
(14, 2): 4, (14, 3): 3, (14, 4): 4, (14, 8.5): 1, (16, 0.0): 4, (16, 0.25): 5, (16, 0.5): 5, (16, 0.75): 4,
(16, 1.0): 5, (16, 1.25): 4, (16, 1.5): 4, (16, 1.75): 5, (16, 2): 4, (16, 3): 4, (16, 4): 1, (16, 8.5): 2,
(18, 0.0): 5, (18, 0.25): 6, (18, 0.5): 2, (18, 0.75): 5, (18, 1.0): 6, (18, 1.25): 5, (18, 1.5): 3, (18, 1.75): 2,
(18, 2): 5, (18, 3): 5, (18, 4): 2, (18, 8.5): 3, (20, 0.0): 2, (20, 0.25): 3, (20, 0.5): 3, (20, 0.75): 6,
(20, 1.0): 3, (20, 1.25): 4, (20, 1.5): 4, (20, 1.75): 3, (20, 2): 4, (20, 3): 3, (20, 4): 3, (20, 8.5): 4})


        dat = _metro_input_pdf.copy_to_tic_dat(_metro_dat)
        dat.parameters.pop("Amount Leftover Constraint")
        dat = pan_dat_maker(_metro_input_pdf.schema(), dat)

        sln = _metro_solution_pdf.copy_to_tic_dat(_metro_solve(dat))
        feas(sln, dat)
        self.assertTrue({k:v.values()[0] for k,v in sln.load_amount_summary.items()} == {
 (2, 0.0): 1, (2, 0.25): 1, (2, 0.5): 1,
 (2, 0.75): 1, (2, 1.0): 1, (2, 1.25): 1, (2, 1.5): 1, (2, 1.75): 1, (2, 2): 1, (2, 3): 1, (2, 4): 1, (2, 8.5): 1,
 (4, 0.0): 2, (4, 0.25): 2, (4, 0.5): 2, (4, 0.75): 2, (4, 1.0): 1, (4, 1.25): 1, (4, 1.5): 1, (4, 1.75): 1, (4, 2): 1,
 (4, 3): 1, (4, 4): 1, (4, 8.5): 1, (6, 0.0): 3, (6, 0.25): 3, (6, 0.5): 3, (6, 0.75): 3, (6, 1.0): 2, (6, 1.25): 2,
 (6, 1.5): 2, (6, 1.75): 2, (6, 2): 2, (6, 3): 2, (6, 4): 2, (6, 8.5): 1, (8, 0.0): 3, (8, 0.25): 3, (8, 0.5): 3,
 (8, 0.75): 3, (8, 1.0): 3, (8, 1.25): 3, (8, 1.5): 3, (8, 1.75): 3, (8, 2): 1, (8, 3): 1, (8, 4): 1, (8, 8.5): 1,
 (10, 0.0): 4, (10, 0.25): 4, (10, 0.5): 2, (10, 0.75): 2, (10, 1.0): 2, (10, 1.25): 2, (10, 1.5): 2, (10, 1.75): 2,
 (10, 2): 2, (10, 3): 2, (10, 4): 2, (10, 8.5): 2, (12, 0.0): 4, (12, 0.25): 3, (12, 0.5): 3, (12, 0.75): 3,
 (12, 1.0): 3, (12, 1.25): 3, (12, 1.5): 3, (12, 1.75): 3, (12, 2): 3, (12, 3): 2, (12, 4): 2, (12, 8.5): 2,
 (14, 0.0): 5, (14, 0.25): 4, (14, 0.5): 4, (14, 0.75): 3, (14, 1.0): 3, (14, 1.25): 3, (14, 1.5): 3, (14, 1.75): 3,
 (14, 2): 3, (14, 3): 3, (14, 4): 3, (14, 8.5): 1, (16, 0.0): 4, (16, 0.25): 4, (16, 0.5): 4, (16, 0.75): 4,
 (16, 1.0): 4, (16, 1.25): 4, (16, 1.5): 4, (16, 1.75): 4, (16, 2): 4, (16, 3): 4, (16, 4): 1, (16, 8.5): 1,
 (18, 0.0): 5, (18, 0.25): 5, (18, 0.5): 2, (18, 0.75): 2, (18, 1.0): 2, (18, 1.25): 2, (18, 1.5): 2, (18, 1.75): 2,
 (18, 2): 2, (18, 3): 2, (18, 4): 2, (18, 8.5): 2, (20, 0.0): 2, (20, 0.25): 2, (20, 0.5): 2, (20, 0.75): 2,
 (20, 1.0): 2, (20, 1.25): 2, (20, 1.5): 2, (20, 1.75): 2, (20, 2): 2, (20, 3): 2, (20, 4): 2, (20, 8.5): 2})

        ex = self.firesException(lambda : _metro_solve(dat, excluded_tables=[]))
        self.assertTrue(any(_ in str(ex) for _ in set(_metro_input_pdf.all_tables).difference({"load_amounts"})))

    def test_diet_amplpy(self):
        dat = _diet_input_pdf.copy_to_ampl(_diet_dat, field_renamings={("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "n_min", ("categories", "Max Nutrition"): "n_max",
                ("nutrition_quantities", "Quantity"): "amt",
                ("nutrition_quantities", "Other Quantity"): "other_amt"})
        self.assertTrue({"n_min", "n_max"}.issubset(dat.categories.toPandas().columns))
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_diet_mod)
        _diet_input_pdf.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})
        ampl.solve()

        sln = _diet_sln_pdf.copy_from_ampl_variables(
            {("buy_food", "Quantity"):ampl.getVariable("Buy"),
            ("consume_nutrition", "Quantity"):ampl.getVariable("Consume")})
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('Total_Cost').value()]

        _missing_field_pdf = PanDatFactory(**{t:[pks, (["Max Nutrition"] if t == "categories" else dfs)]
                                              for t,(pks, dfs) in _diet_input_pdf.schema().items()})
        dat = _missing_field_pdf.copy_to_ampl(_diet_dat, field_renamings={("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "n_min", ("categories", "Max Nutrition"): "n_max",
                ("nutrition_quantities", "Quantity"): "amt",
                ("nutrition_quantities", "Other Quantity"): "other_amt"})
        self.assertTrue({"n_min", "n_max"}.issubset(dat.categories.toPandas().columns))
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_diet_mod)
        _diet_input_pdf.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})
        ampl.solve()
        sln_2 = _diet_sln_pdf.copy_from_ampl_variables(
            {("buy_food", "Quantity"):ampl.getVariable("Buy"),
            ("consume_nutrition", "Quantity"):ampl.getVariable("Consume")})
        sln_2.parameters.loc[0] = ['Total Cost', ampl.getObjective('Total_Cost').value()]
        self.assertTrue(_diet_sln_pdf._same_data(sln, sln_2))

        diet_dat_two = _diet_input_pdf.copy_to_tic_dat(_diet_dat)
        for r in diet_dat_two.nutrition_quantities.values():
            r["Quantity"], r["Other Quantity"] = [0.5 * r["Quantity"]] * 2
        diet_dat_two = pan_dat_maker(_diet_input_pdf.schema(), diet_dat_two)

        dat = _diet_input_pdf.copy_to_ampl(diet_dat_two, field_renamings={("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "n_min", ("categories", "Max Nutrition"): "n_max",
                ("nutrition_quantities", "Quantity"): "amt",
                ("nutrition_quantities", "Other Quantity"): "other_amt"})
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_diet_mod)
        _diet_input_pdf.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})
        ampl.solve()
        self.assertTrue("solved" == ampl.getValue("solve_result"))

        sln = _diet_sln_pdf.copy_from_ampl_variables(
            {("buy_food", "Quantity"):ampl.getVariable("Buy"),
            ("consume_nutrition", "Quantity"):ampl.getVariable("Consume")})
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('Total_Cost').value()]

        self.assertTrue(_diet_sln_pdf._same_data(sln, _diet_sln_pandat, epsilon=1e-5))

        dat = _diet_input_pdf.copy_to_ampl(_diet_dat, {("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "", ("categories", "Max Nutrition"): "n_max"},
                ["nutrition_quantities"])
        self.assertFalse(hasattr(dat, "nutrition_quantities"))
        self.assertTrue({"n_min", "n_max"}.intersection(dat.categories.toPandas().columns) == {"n_max"})

        sln_tdf_2 = PanDatFactory(buy_food = [["Food"],["Quantity"]],
                                   consume_nutrition = [["Category"],[]])
        sln_tdf_2.set_default_value("buy_food", "Quantity", 1)
        sln_2 = sln_tdf_2.copy_from_ampl_variables(
            {("buy_food", False):ampl.getVariable("Buy"),
             ("consume_nutrition",False):(ampl.getVariable("Consume"), lambda x : x < 100)})
        self.assertTrue(set(sln_2.buy_food["Quantity"]) == {1})
        self.assertTrue(set(sln_2.buy_food["Food"]) == set(sln.buy_food["Food"]))
        self.assertTrue(len(sln_2.consume_nutrition)>0)
        self.assertTrue(set(sln_2.consume_nutrition["Category"]) ==
                        set(sln.consume_nutrition[sln.consume_nutrition["Quantity"] < 100]["Category"]))

        diet_dat_two = _diet_input_pdf.copy_to_tic_dat(_diet_dat)
        diet_dat_two.categories["calories"] = [0,200]
        diet_dat_two = pan_dat_maker(_diet_input_pdf.schema(), diet_dat_two)
        dat = _diet_input_pdf.copy_to_ampl(diet_dat_two, field_renamings={("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "n_min", ("categories", "Max Nutrition"): "n_max",
                ("nutrition_quantities", "Quantity"): "amt",
                ("nutrition_quantities", "Other Quantity"): "other_amt"})
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_diet_mod)
        _diet_input_pdf.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})
        ampl.solve()
        self.assertTrue("infeasible" == ampl.getValue("solve_result"))

        diet_dat_two = _diet_input_pdf.copy_to_tic_dat(_diet_dat)
        for v in diet_dat_two.categories.values():
            v["Max Nutrition"] = float("inf")
        diet_dat_two.foods["hamburger"] = -1
        diet_dat_two = pan_dat_maker(_diet_input_pdf.schema(), diet_dat_two)
        dat = _diet_input_pdf.copy_to_ampl(diet_dat_two, field_renamings={("foods", "Cost"): "cost",
                ("categories", "Min Nutrition"): "n_min", ("categories", "Max Nutrition"): "n_max",
                ("nutrition_quantities", "Quantity"): "amt",
                ("nutrition_quantities", "Other Quantity"): "other_amt"})
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_diet_mod)
        _diet_input_pdf.set_ampl_data(dat, ampl, {"categories": "CAT", "foods": "FOOD"})
        ampl.solve()
        self.assertTrue("unbounded" == ampl.getValue("solve_result"))

    def test_netflow_amplpy(self):
        dat = _netflow_input_pdf.copy_to_ampl(_netflow_dat, field_renamings={("arcs", "Capacity"): "capacity",
            ("cost", "Cost"): "cost", ("inflow", "Quantity"): "inflow"})
        ampl = amplpy.AMPL()
        ampl.setOption('solver', 'gurobi')
        ampl.eval(_netflow_mod)
        _netflow_input_pdf.set_ampl_data(dat, ampl, {"nodes": "NODES", "arcs": "ARCS",
                                           "commodities": "COMMODITIES"})
        ampl.solve()

        sln = _netflow_sln_pdf.copy_from_ampl_variables(
            {('flow' ,'Quantity'):ampl.getVariable("Flow")})
        sln.parameters.loc[0] = ['Total Cost', ampl.getObjective('TotalCost').value()]

        self.assertTrue(_netflow_sln_pdf._same_data(sln, _netflow_sln_pandat))

        sln2 = _netflow_sln_pdf.copy_from_ampl_variables(
            {('flow' ,'Quantity'):(ampl.getVariable("Flow"), lambda v : v>30)})
        sln3 = _netflow_sln_pdf.copy_from_ampl_variables(
            {('flow' ,'Quantity'):(ampl.getVariable("Flow"), lambda v : 0<v<=30)})
        sln2.parameters.loc[0] = ['Total Cost', ampl.getObjective('TotalCost').value()]
        sln3.parameters.loc[0] = ['Total Cost', ampl.getObjective('TotalCost').value()]

        self.assertTrue(len(sln2.flow) and len(sln3.flow))
        self.assertFalse(_netflow_sln_pdf._same_data(sln, sln2))
        self.assertFalse(_netflow_sln_pdf._same_data(sln, sln2))
        sln2.flow = sln2.flow.append(sln3.flow)
        self.assertTrue(_netflow_sln_pdf._same_data(sln, sln2))

if __name__ == "__main__":
    if not amplpy:
        print("!!!testpandat_ampl.py is going to fail because amplpy is not installed!!!")
    if not pd:
        print("!!!testpandat_ampl.py is going to fail because pandas is not installed!!!")

    unittest.main()