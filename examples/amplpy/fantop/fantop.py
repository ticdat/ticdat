#
# Author: Pete Cacioppi Opalytics.com https://lnkd.in/bUhfyNn
#
# Solves a fantasy football drafting problem. Tries to maximize the weighted
# expected points of a draft, while obeying min/max restrictions for different
# positions (to include a maximum-flex-players constraint).
#
# Pre-computes the expected draft position of each player, so as to prevent
# creating a draft plan based on unrealistic expectations of player availability
# at each round.
#
# The current draft standing can be filled in as you go in the drafted table.
# A user can thus re-optimize for each of his draft picks.
#
# Uses the ticdat package to simplify file IO and provide command line functionality.
# Can read from .csv, JSON, Excel or SQLite files. Self validates the input data
# before solving to prevent strange errors or garbage-in, garbage-out problems.

from ticdat import PanDatFactory, standard_main
from amplpy import AMPL

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory(
 parameters = [["Parameter"],["Value"]],
 players = [['Player Name'],
            ['Position', 'Average Draft Position', 'Expected Points', 'Draft Status']],
 roster_requirements = [['Position'],
                       ['Min Num Starters', 'Max Num Starters', 'Min Num Reserve', 'Max Num Reserve',
                        'Flex Status']],
 my_draft_positions = [['Draft Position'],[]]
)

# add foreign key constraints (optional, but helps with preventing garbage-in, garbage-out)
input_schema.add_foreign_key("players", "roster_requirements", ['Position', 'Position'])

# set data types (optional, but helps with preventing garbage-in, garbage-out)
input_schema.set_data_type("parameters", "Parameter", number_allowed = False,
                          strings_allowed = ["Starter Weight", "Reserve Weight",
                                             "Maximum Number of Flex Starters"])
input_schema.set_data_type("parameters", "Value", min=0, max=float("inf"),
                          inclusive_min = True, inclusive_max = False)
input_schema.set_data_type("players", "Average Draft Position", min=0, max=float("inf"),
                          inclusive_min = False, inclusive_max = False)
input_schema.set_data_type("players", "Expected Points", min=-float("inf"), max=float("inf"),
                          inclusive_min = False, inclusive_max = False)
input_schema.set_data_type("players", "Draft Status",
                          strings_allowed = ["Un-drafted", "Drafted By Me", "Drafted By Someone Else"])
for fld in ("Min Num Starters",  "Min Num Reserve", "Max Num Reserve"):
    input_schema.set_data_type("roster_requirements", fld, min=0, max=float("inf"),
                          inclusive_min = True, inclusive_max = False, must_be_int = True)
input_schema.set_data_type("roster_requirements", "Max Num Starters", min=0, max=float("inf"),
                      inclusive_min = False, inclusive_max = True, must_be_int = True)
input_schema.set_data_type("roster_requirements", "Flex Status", number_allowed = False,
                          strings_allowed = ["Flex Eligible", "Flex Ineligible"])
input_schema.set_data_type("my_draft_positions", "Draft Position", min=0, max=float("inf"),
                          inclusive_min = False, inclusive_max = False, must_be_int = True)
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(
    parameters = [["Parameter"],["Value"]],
    my_draft = [['Player Name'], ['Draft Position', 'Position', 'Planned Or Actual',
                                  'Starter Or Reserve']])
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat):
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    parameters = {k:v for k,v in dat.parameters.itertuples(index=False)}

    # compute the Expected Draft Position column
    # for our purposes, its fine to assume all those drafted by someone else are drafted
    # prior to any players drafted by me
    dat.players["_temp_sort_column"] = dat.players.apply(axis=1, func=lambda row:
                                                    {"Un-drafted": row["Average Draft Position"],
                                                     "Drafted By Me": -1, "Drafted By Someone Else": -2}
                                                    [row["Draft Status"]])
    dat.players.sort_values(by="_temp_sort_column", inplace=True)
    dat.players.reset_index(drop=True, inplace=True) # get rid of the index that has become scrambled
    dat.players.reset_index(drop=False, inplace=True) # turn the sequential index into a column
    dat.players["Expected Draft Position"] = dat.players["index"] + 1
    dat.players.drop(["index", "_temp_sort_column"], inplace=True, axis=1)
    assert list(dat.players["Expected Draft Position"]) == list(range(1, len(dat.players)+1))

    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    set PLAYERS;
    param draft_status{PLAYERS} symbolic;
    param position{PLAYERS} symbolic;
    param expected_draft_position{PLAYERS} >=1;
    param expected_points{PLAYERS} >= 0;
    set DRAFTABLE_PLAYERS within PLAYERS = {p in PLAYERS : draft_status[p] <> 'Drafted By Someone Else'};
    var Starters {DRAFTABLE_PLAYERS} binary;
    var Reserves {DRAFTABLE_PLAYERS} binary;
    """)
    ampl.eval("""
    set MY_DRAFT_POSITIONS ordered;
    """)
    ampl.eval("""
    subject to Already_Drafted_By_Me {p in PLAYERS: draft_status[p] = 'Drafted By Me'}:
        Starters[p] + Reserves[p] = 1;
    subject to Cant_Draft_Twice {p in PLAYERS: draft_status[p] = 'Un-drafted'}:
        Starters[p] + Reserves[p] <= 1;
    """)
    ampl.eval("""
    subject to At_Most_X_Can_Be_Ahead_Of_Y {d in MY_DRAFT_POSITIONS}:
        sum{p in DRAFTABLE_PLAYERS: expected_draft_position[p] < d}(Starters[p] + Reserves[p]) <=
        ord(d, MY_DRAFT_POSITIONS) - 1;
    """)
    ampl.eval("""
    var My_Draft_Size >= card({p in PLAYERS: draft_status[p] = 'Drafted By Me'}),
                      <= card(MY_DRAFT_POSITIONS);
    subject to Set_My_Draft_Size:
    sum{p in PLAYERS: draft_status[p] <> 'Drafted By Someone Else'}(Starters[p] + Reserves[p]) =
        My_Draft_Size;
    """)
    ampl.eval("""
    set POSITIONS;
    param min_number_starters{POSITIONS} >= 0;
    param max_number_starters{p in POSITIONS} >= min_number_starters[p];
    param min_number_reserve{POSITIONS} >= 0;
    param max_number_reserve{p in POSITIONS} >= min_number_reserve[p];
    param flex_status{POSITIONS} symbolic;
    """)
    ampl.eval("""
    subject to Min_Number_Starters{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Starters[pl] >= min_number_starters[p];
    subject to Max_Number_Starters{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Starters[pl] <= max_number_starters[p];
    subject to Min_Number_Reserve{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Reserves[pl]>= min_number_reserve[p];
    subject to Max_Number_Reserve{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Reserves[pl] <= max_number_reserve[p];
    """)
    ampl.eval("""
    param max_number_of_flex_starters>=0;
    subject to Max_Number_Flex_Starters:
        sum{p in DRAFTABLE_PLAYERS: flex_status[position[p]] = 'Flex Eligible'}Starters[p]
        <= max_number_of_flex_starters;
    """)
    ampl.eval("""
    param starter_weight >=0;
    param reserve_weight >= 0;
    maximize Total_Expected_Points:
       sum{p in DRAFTABLE_PLAYERS}(expected_points[p] *
                                  (starter_weight * Starters[p] + reserve_weight * Reserves[p]));
    """)
    ampl_dat = input_schema.copy_to_ampl(dat,
        excluded_tables={"parameters"},
        field_renamings={("players", "Expected Draft Position"): "expected_draft_position",
                         ("players", "Position"): "position",
                         ("players", 'Average Draft Position'): "",
                         ("players", 'Expected Points'): "expected_points",
                         ("players", 'Draft Status'): "draft_status",
                         ("roster_requirements", 'Min Num Starters'): 'min_number_starters',
                         ("roster_requirements", 'Max Num Starters'): 'max_number_starters',
                         ("roster_requirements", 'Min Num Reserve'): 'min_number_reserve',
                         ("roster_requirements", 'Max Num Reserve'): 'max_number_reserve',
                         ("roster_requirements", 'Flex Status'): 'flex_status',
                         })
    input_schema.set_ampl_data(ampl_dat, ampl, {"players":"PLAYERS", "my_draft_positions":"MY_DRAFT_POSITIONS",
                                                "roster_requirements": "POSITIONS"})
    ampl.param['max_number_of_flex_starters'] = parameters.get('Maximum Number of Flex Starters',
                                                len(dat.my_draft_positions))
    ampl.param['starter_weight'] = parameters.get('Starter Weight', 1.)
    ampl.param['reserve_weight'] = parameters.get('Reserve Weight', 1.)

    # solve and recover solutions next

    m = gu.Model('fantop', env=gurobi_env())
    my_starters = {player_name:m.addVar(vtype=gu.GRB.BINARY, name="starter_%s"%player_name)
                  for player_name in can_be_drafted_by_me}
    my_reserves = {player_name:m.addVar(vtype=gu.GRB.BINARY, name="reserve_%s"%player_name)
                  for player_name in can_be_drafted_by_me}


    for player_name in can_be_drafted_by_me:
        if player_name in already_drafted_by_me:
            m.addConstr(my_starters[player_name] + my_reserves[player_name] == 1,
                        name="already_drafted_%s"%player_name)
        else:
            m.addConstr(my_starters[player_name] + my_reserves[player_name] <= 1,
                        name="cant_draft_twice_%s"%player_name)

    for i,draft_position in enumerate(sorted(dat.my_draft_positions)):
        m.addConstr(gu.quicksum(my_starters[player_name] + my_reserves[player_name]
                                for player_name in can_be_drafted_by_me
                                if expected_draft_position[player_name] < draft_position) <= i,
                    name = "at_most_%s_can_be_ahead_of_%s"%(i,draft_position))

    my_draft_size = gu.quicksum(my_starters[player_name] + my_reserves[player_name]
                                for player_name in can_be_drafted_by_me)
    m.addConstr(my_draft_size >= len(already_drafted_by_me) + 1,
                name = "need_to_extend_by_at_least_one")
    m.addConstr(my_draft_size <= len(dat.my_draft_positions), name = "cant_exceed_draft_total")

    for position, row in dat.roster_requirements.items():
        players = {player_name for player_name in can_be_drafted_by_me
                   if dat.players[player_name]["Position"] == position}
        starters = gu.quicksum(my_starters[player_name] for player_name in players)
        reserves = gu.quicksum(my_reserves[player_name] for player_name in players)
        m.addConstr(starters >= row["Min Num Starters"], name = "min_starters_%s"%position)
        m.addConstr(starters <= row["Max Num Starters"], name = "max_starters_%s"%position)
        m.addConstr(reserves >= row["Min Num Reserve"], name = "min_reserve_%s"%position)
        m.addConstr(reserves <= row["Max Num Reserve"], name = "max_reserve_%s"%position)

    if "Maximum Number of Flex Starters" in dat.parameters:
        players = {player_name for player_name in can_be_drafted_by_me if
                   dat.roster_requirements[dat.players[player_name]["Position"]]["Flex Status"] == "Flex Eligible"}
        m.addConstr(gu.quicksum(my_starters[player_name] for player_name in players)
                    <= dat.parameters["Maximum Number of Flex Starters"]["Value"],
                    name = "max_flex")

    starter_weight = dat.parameters["Starter Weight"]["Value"] if "Starter Weight" in dat.parameters else 1
    reserve_weight = dat.parameters["Reserve Weight"]["Value"] if "Reserve Weight" in dat.parameters else 1
    m.setObjective(gu.quicksum(dat.players[player_name]["Expected Points"] *
                               (my_starters[player_name] * starter_weight + my_reserves[player_name] * reserve_weight)
                               for player_name in can_be_drafted_by_me),
                   sense=gu.GRB.MAXIMIZE)

    m.optimize()

    if m.status != gu.GRB.OPTIMAL:
        print("No draft at all is possible!")
        return

    sln = solution_schema.TicDat()
    def almostone(x):
        return abs(x.x-1) < 0.0001
    picked = sorted([player_name for player_name in can_be_drafted_by_me
                     if almostone(my_starters[player_name]) or almostone(my_reserves[player_name])],
                    key=lambda _p: expected_draft_position[_p])
    assert len(picked) <= len(dat.my_draft_positions)
    if len(picked) < len(dat.my_draft_positions):
        print("Your model is over-constrained, and thus only a partial draft was possible")

    draft_yield = 0
    for player_name, draft_position in zip(picked, sorted(dat.my_draft_positions)):
        draft_yield += dat.players[player_name]["Expected Points"] * \
                       (starter_weight if almostone(my_starters[player_name]) else reserve_weight)
        assert draft_position <= expected_draft_position[player_name]
        sln.my_draft[player_name]["Draft Position"] = draft_position
        sln.my_draft[player_name]["Position"] = dat.players[player_name]["Position"]
        sln.my_draft[player_name]["Planned Or Actual"] = "Actual" if player_name in already_drafted_by_me else "Planned"
        sln.my_draft[player_name]["Starter Or Reserve"] = \
            "Starter" if almostone(my_starters[player_name]) else "Reserve"
    sln.parameters["Total Yield"] = draft_yield
    sln.parameters["Feasible"] = len(sln.my_draft) == len(dat.my_draft_positions)

    return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------