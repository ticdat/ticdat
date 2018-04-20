# this version of the file uses gurobipy

# 2360.0 obj value

import gurobipy as gu
from ticdat import TicDatFactory,  standard_main, gurobi_env, Slicer
from math import floor
from itertools import product, combinations

input_schema = TicDatFactory (
    grades = [["Name"],[]],
    roster = [["Name"],["Grade"]],
    position_groups = [["Name"],[]],
    positions = [["Position"],["Position Importance", "Position Group"]],
    player_ratings = [["Name", "Position Group"], ["Rating"]],
    inning_groups = [["Name"],[]],
    innings = [["Inning"],["Inning Group"]],
    position_constraints = [["Position Group", "Inning Group", "Grade"],["Min Players", "Max Players"]])

input_schema.add_foreign_key("roster", "grades", ["Grade", "Name"])

input_schema.set_data_type("positions", "Position Importance",min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.add_foreign_key("positions", "position_groups", ["Position Group", "Name"])

input_schema.add_foreign_key("player_ratings", "roster", ["Name", "Name"])
input_schema.add_foreign_key("player_ratings", "position_groups", ["Position Group", "Name"])
input_schema.set_data_type("player_ratings", "Rating", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

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

solution_schema = TicDatFactory(
    lineup = [["Inning", "Position"],["Name"]])

def solve(dat):
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    if len(dat.roster) < len(dat.positions):
        print("\n****\nModel is infeasible due to shortage of players!\n****\n")

    mdl = gu.Model("simpler little_league", env=gurobi_env())

    lineup = {(i,p,pl):mdl.addVar(vtype = gu.GRB.BINARY, name="lineup_%s_%s_%s"%(i, p, pl))
              for i, p, pl in product(dat.innings, dat.positions, dat.roster)}
    l_slicer = Slicer(lineup)
    # 1 player assigned to each position per inning
    for i,p in product(dat.innings, dat.positions):
        mdl.addConstr(gu.quicksum(lineup[k] for k in l_slicer.slice(i, p, '*')) == 1,
                      name = "all_positions_filled_per_inning_%s_%s"%(i,p))
    # each player can play at most one position per inning
    for i,pl in product(dat.innings, dat.roster):
        mdl.addConstr(gu.quicksum(lineup[k] for k in l_slicer.slice(i, '*', pl)) <= 1,
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
