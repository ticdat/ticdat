from diet_simple_package.schemas import input_schema, solution_schema

try: # if you don't have gurobipy installed, the code will still load and then fail on solve
    import gurobipy as gu
except:
    gu = None
# ------------------------ create a solve function --------------------------------

def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    if gu is None: # even if you don't have gurobipy installed, you can still import this file for other uses
        print("*****\ngurobipy needs to be installed for this example code to solve!\n*****\n")
    mdl = gu.Model("diet")

    nutrition = {c:mdl.addVar(lb=n["Min Nutrition"], ub=n["Max Nutrition"], name=c)
                for c,n in dat.categories.items()}

    # Create decision variables for the foods to buy
    buy = {f:mdl.addVar(name=f) for f in dat.foods}

     # Nutrition constraints
    for c in dat.categories:
        mdl.addConstr(gu.quicksum(dat.nutrition_quantities[f,c]["Quantity"] * buy[f]
                      for f in dat.foods) == nutrition[c],
                      name = c)

    mdl.setObjective(gu.quicksum(buy[f] * c["Cost"] for f,c in dat.foods.items()),
                     sense=gu.GRB.MINIMIZE)
    mdl.optimize()

    if mdl.status == gu.GRB.OPTIMAL:
        sln = solution_schema.TicDat()
        for f,x in buy.items():
            if x.x > 0:
                sln.buy_food[f] = x.x
        for c,x in nutrition.items():
            sln.consume_nutrition[c] = x.x
        sln.parameters['Total Cost'] = sum(dat.foods[f]["Cost"] * r["Quantity"]
                                           for f,r in sln.buy_food.items())
        return sln
# ---------------------------------------------------------------------------------