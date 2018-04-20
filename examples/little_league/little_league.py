# this version of the file uses gurobipy
#
# 450 objective value

# !!!!!!!! NB !!!!!!!!!!!!!!
# see r["Consecutive Innings Only"] == "True" line for last remaining outstanding issue

# OK now can clean up code and json data to exploit x-to-many fks which are new!

import gurobipy as gu
from ticdat import TicDatFactory,  standard_main, gurobi_env, Slicer
from ticdat.utils import verify
from math import floor
from itertools import product, combinations

input_schema = TicDatFactory (
    grades = [["Name"],[]],
    roster = [["Name"],["Grade", "Arrival Inning", "Departure Inning", "Min Innings Played", "Max Innings Played"]],
    position_groups = [["Name"],[]],
    positions = [["Position"],["Position Importance", "Position Group", "Consecutive Innings Only"]],
    player_ratings = [["Name", "Position Group"], ["Rating"]],
    inning_groups = [["Name"],[]],
    innings = [["Inning"],["Inning Group"]],
    position_constraints = [["Position Group", "Inning Group", "Grade"],["Min Players", "Max Players"]],
    parameters = [["Key"],["Value"]])

input_schema.add_foreign_key("roster", "grades", ["Grade", "Name"])
input_schema.set_data_type("roster", "Min Innings Played", min=0, max=9, inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("roster", "Max Innings Played", min=0, max=9, inclusive_min=False, inclusive_max=True)
input_schema.add_data_row_predicate(
    "roster", predicate_name="Roster Min Max Check",
    predicate=lambda row : row["Max Innings Played"] >= row["Min Innings Played"])
input_schema.add_foreign_key("roster", "innings", ["Arrival Inning", "Inning"])
input_schema.add_foreign_key("roster", "innings", ["Departure Inning", "Inning"])

input_schema.set_data_type("positions", "Position Importance",min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("positions", "Consecutive Innings Only", strings_allowed=['True', 'False'],
                           number_allowed=False)
input_schema.add_foreign_key("positions", "position_groups", ["Position Group", "Name"])

input_schema.add_foreign_key("player_ratings", "roster", ["Name", "Name"])
input_schema.add_foreign_key("player_ratings", "position_groups", ["Position Group", "Name"])
input_schema.set_data_type("player_ratings", "Rating", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

# since we're going to be sorting innings need them to be numeric
input_schema.set_data_type("innings", "Inning", min=1, max=9, inclusive_min=True, inclusive_max=True)
input_schema.add_foreign_key("innings", "inning_groups", ["Inning Group", "Name"])

input_schema.add_foreign_key("position_constraints", "position_groups", ["Position Group", "Name"])
input_schema.add_foreign_key("position_constraints", "inning_groups", ["Inning Group", "Name"])
input_schema.add_foreign_key("position_constraints", "grades", ["Grade", "Name"])
input_schema.set_data_type("position_constraints", "Min Players", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("position_constraints", "Max Players", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=True)
input_schema.add_data_row_predicate(
    "position_constraints", predicate_name="Position Min Max Check",
    predicate=lambda row : row["Max Players"] >= row["Min Players"])

default_parameters = {"Balanced Playing Time": "True", "Limit Outfield Play": "True",
                      "Max Consecutive Bench": 9, "Outfield Group Label": "Outfield",
                      "Bench Roster Label":"Bench"}
def _good_parameter_key_value(key, value):
    if key == "Max Consecutive Bench":
        try:
            return (1 <= value <= 9) and floor(value) == value
        except:
            return False
    if key in ["Balanced Playing Time", "Limit Outfield Play"]:
        return value  in ["True", "False"]
    if key in ["Outfield Group Label", "Bench Roster Label"]:
        return True

assert all(_good_parameter_key_value(k,v) for k,v in default_parameters.items())

input_schema.set_data_type("parameters", "Key", number_allowed=False,
                           strings_allowed=default_parameters)
input_schema.add_data_row_predicate("parameters", predicate_name="Good Parameter Value for Key",
    predicate=lambda row : _good_parameter_key_value(row["Key"], row["Value"]))


solution_schema = TicDatFactory(
    lineup = [["Inning", "Position"],["Name"]])

def solve(dat):
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # use default parameters, unless they are overridden by user-supplied parameters
    full_parameters = dict(default_parameters, **{k: v["Value"] for k,v in dat.parameters.items()})

    bench = full_parameters["Bench Roster Label"]
    verify(bench in dat.positions, "%s needs to be one of the positions"%bench)

    verify(len(dat.roster) >= len(dat.positions) - 1, "Model is infeasible due to shortage of players!")

    mdl = gu.Model("little_league", env=gurobi_env())

    lineup = {(i,p,pl):mdl.addVar(vtype = gu.GRB.BINARY, name="lineup_%s_%s_%s"%(i, p, pl))
              for i, p, pl in product(dat.innings, dat.positions, dat.roster)}
    l_slicer = Slicer(lineup)
    # 1 player assigned to each active position per inning
    for i,p in product(dat.innings, dat.positions):
        if p != bench:
            mdl.addConstr(gu.quicksum(lineup[k] for k in l_slicer.slice(i, p, '*')) == 1,
                          name = "all_positions_filled_per_inning_%s_%s"%(i,p))
    # each player must be assigned to one position (including the bench) per inning
    for i,pl in product(dat.innings, dat.roster):
        mdl.addConstr(gu.quicksum(lineup[k] for k in l_slicer.slice(i, '*', pl)) == 1,
                      name = "at_most_one_position_per_inning_%s_%s"%(i,pl))

    grade_slice = Slicer([(pl, r["Grade"]) for pl, r in dat.roster.items()])
    position_slice = Slicer([(p, r["Position Group"]) for p, r in dat.positions.items()])
    innings_slice = Slicer([(i, r["Inning Group"]) for i,r in dat.innings.items()])
    # Position_Constraints satisfied
    for (pg, ig, g), r in dat.position_constraints.items():
        total_players = gu.quicksum(lineup[i[0], p[0], pl[0]] for i, p, pl in
                                    product(innings_slice.slice('*', ig), position_slice.slice('*', pg),
                                            grade_slice.slice('*', g)))
        mdl.addConstr(total_players >= r["Min Players"], name="min_players_%s_%s_%s"%(pg, ig, g))
        mdl.addConstr(total_players <= r["Max Players"], name="max_players_%s_%s_%s"%(pg, ig, g))

    # Enforce consecutive innings constraints
    sorted_innings = list(sorted(dat.innings))
    for p,r in dat.positions.items():
        if r["Consecutive Innings Only"] == "True":
            for pl in dat.roster:
                for pos1, pos2 in combinations(range(len(sorted_innings)), 2):
                    if pos2 < pos1:
                        pos1, pos2 = pos2, pos1
                    if pos2-pos1 > 1:
                        pass
                        # need Derek's determination of what it means

    # Balanced Playing time = a min playing time for each player
    if full_parameters["Balanced Playing Time"] == "True":
        for pl, r in dat.roster.items():
            mdl.addConstr(gu.quicksum(lineup[k] for k in l_slicer.slice('*', '*', pl)) >=
                          floor((r["Departure Inning"] - r["Arrival Inning"] + 1) /
                                 float(len(dat.positions) * len(dat.innings))),
                          name= "balanced_pt_%s"%pl)

    if full_parameters["Limit Outfield Play"] == "True":
        of = full_parameters["Outfield Group Label"]
        for pl in dat.roster:
            mdl.addConstr(gu.quicksum(lineup[i, p[0], pl] for p in position_slice.slice('*', of)
                                      for i in dat.innings) <=
                          gu.quicksum(lineup[i, p, pl] for i,p in product(dat.innings, dat.positions))*0.5 + 0.5,
                          name= "limit_OF_play_%s"%pl)

    max_bench = full_parameters["Max Consecutive Bench"]
    for pos,i in enumerate(sorted_innings):
        if max_bench+1+pos <= len(sorted_innings):
            for pl in dat.roster:
                mdl.addConstr(gu.quicksum(lineup[k] for i_ in sorted_innings[pos:pos+max_bench+1]
                                          for k in l_slicer.slice(i_, '*', pl)) >= 1,
                              name = "max_consecutive_bench_%s_%s"%(i, pl))

    mdl.setObjective(gu.quicksum(dat.positions[p]["Position Importance"] * dat.player_ratings[pl, pg]["Rating"] * v
                                 for (i, p, pl), v in lineup.items() for pg in [dat.positions[p]["Position Group"]]
                                 if (pl, pg) in dat.player_ratings),
                     sense=gu.GRB.MAXIMIZE)

    mdl.optimize()

    if mdl.status == gu.GRB.OPTIMAL:
        sln = solution_schema.TicDat()
        for (i, p, pl), v in lineup.items():
            if abs(v.x - 1) < 0.0001:
                sln.lineup[i,p] = pl
        return sln


if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
