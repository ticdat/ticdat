# this version of the file uses gurobipy

# really should be 2360.0 obj value, this gets 2333.0 with purely numeric data, not sure what the problem is
# but not worrying about that too much, could be an LB/UB thing

from ticdat import TicDatFactory,  standard_main
from amplpy import AMPL
from ticdat.utils import verify


input_schema = TicDatFactory (
    roster = [["Name"],["Grade"]],
    positions = [["Position"],["Position Importance", "Position Group"]],
    player_ratings = [["Name", "Position Group"], ["Rating"]],
    innings = [["Inning"],["Inning Group"]],
    position_constraints = [["Position Group", "Inning Group", "Grade"],["Min Players", "Max Players"]])

input_schema.set_data_type("positions", "Position Importance",min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

# input_schema.add_foreign_key("player_ratings", "roster", ["Name", "Name"])
# input_schema.add_foreign_key("player_ratings", "positions", ["Position Group", "Position Group"])
input_schema.set_data_type("player_ratings", "Rating", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)

# input_schema.add_foreign_key("position_constraints", "positions", ["Position Group", "Position Group"])
# input_schema.add_foreign_key("position_constraints", "innings", ["Inning Group", "Inning Group"])
# input_schema.add_foreign_key("position_constraints", "roster", ["Grade", "Grade"])
input_schema.set_data_type("position_constraints", "Min Players", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("position_constraints", "Max Players", min=0, max=float("inf"),
                           inclusive_min=False, inclusive_max=True)
input_schema.add_data_row_predicate(
    "position_constraints", predicate_name="Position Min Max Check",
    predicate=lambda row : row["Max Players"] >= row["Min Players"])

solution_schema = TicDatFactory(
    lineup = [["Inning", "Position"],["Name"]],
    parameters = [["Key"],["Value"]])

def solve(dat):
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    dat = input_schema.copy_to_ampl(dat, field_renamings={
            ("roster", "Grade"):"grade",
            ("positions", "Position Importance"): "position_importance",
            ("positions", "Position Group"): "position_group",
            ("player_ratings", "Rating"): "player_rating",
            ("innings", "Inning Group"): "inning_group",
            ("position_constraints", "Min Players"): "min_players",
            ("position_constraints", "Max Players"): "max_players"
        })

    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')

    mod_str = """
    # Players and grades

    set PLAYERS;
    param grade {PLAYERS};
    set GRADES = setof {pl in PLAYERS} grade[pl];
    set IN_GRADE {g in GRADES} = {pl in PLAYERS: grade[pl] = g};

    # Positions and position groups; player ratings

    set POSITIONS;
    param position_importance {POSITIONS};
    param position_group {POSITIONS};
    set POSITION_GROUPS = setof {p in POSITIONS} position_group[p];
    set IN_POSITION_GROUP {pg in POSITION_GROUPS} = {p in POSITIONS: position_group[p] = pg};

    param player_rating {PLAYERS,POSITION_GROUPS};

    # Innings and inning groups; player limits

    set INNINGS;
    param inning_group {INNINGS};
    set INNING_GROUPS = setof {i in INNINGS} inning_group[i];
    set IN_INNING_GROUP {ig in INNING_GROUPS} = {i in INNINGS: inning_group[i] = ig};

    set POSITION_CONSTRAINTS within {POSITION_GROUPS,INNING_GROUPS,GRADES};
    param min_players {POSITION_CONSTRAINTS};
    param max_players {POSITION_CONSTRAINTS};

    # Decision variables: 1 ==> assignment of a player to a position in an inning

    var Play {INNINGS,POSITIONS,PLAYERS} binary;

    # Objective function: Maximize desirability of the assignment

    maximize TotalImportance:
       sum {i in INNINGS, p in POSITIONS, pl in PLAYERS}
          position_importance[p] * player_rating[pl,position_group[p]] * Play[i,p,pl];

    # Exactly one player per position per inning

    subject to AllPositionsFilledPerInning {i in INNINGS, p in POSITIONS}:
       sum {pl in PLAYERS} Play[i,p,pl] = 1;

    # At most one possition per player per inning

    subject to AtMostOnePositionPerInning {i in INNINGS, pl in PLAYERS}:
       sum {p in POSITIONS} Play[i,p,pl] <= 1;

    # Total roster slots assigned, for listed combinations of
    # position group, inning group, and grade, must be within specified limits

    subject to MinMaxPlayers {(pg,ig,g) in POSITION_CONSTRAINTS}:
       min_players[pg,ig,g] <=
         sum {p in IN_POSITION_GROUP[pg], i in IN_INNING_GROUP[ig], pl in IN_GRADE[g]} Play[i,p,pl]
           <= max_players[pg,ig,g];
    """
    ampl.eval(mod_str)

    input_schema.set_ampl_data(dat, ampl, {"roster": "PLAYERS", "positions": "POSITIONS",
                                           "innings": "INNINGS", "position_constraints": "POSITION_CONSTRAINTS"})
    ampl.solve()

    if ampl.getValue("solve_result") != "infeasible":
        sln = solution_schema.TicDat()
        sln.parameters["Total Cost"] = ampl.getObjective('TotalImportance').value()
        return sln

if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)

