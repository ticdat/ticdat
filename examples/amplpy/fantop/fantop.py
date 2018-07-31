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
    def temp_sort_function(row):
        return {"Un-drafted": row["Average Draft Position"],
                "Drafted By Me": -1, "Drafted By Someone Else": -2}[row["Draft Status"]]
    dat.players["_temp_sort_column"] = dat.players.apply(axis=1, func=temp_sort_function)
    dat.players.sort_values(by="_temp_sort_column", inplace=True)
    dat.players.reset_index(drop=True, inplace=True) # get rid of the index that has become scrambled
    dat.players.reset_index(drop=False, inplace=True) # turn the sequential index into a column
    dat.players["Expected Draft Position"] = dat.players["index"] + 1
    dat.players.drop(["index", "_temp_sort_column"], inplace=True, axis=1)
    assert list(dat.players["Expected Draft Position"]) == list(range(1, len(dat.players)+1))

    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')
    ampl.eval("""
    param max_number_of_flex_starters>=0;
    param starter_weight >=0;
    param reserve_weight >= 0;

    set PLAYERS;
    param draft_status{PLAYERS} symbolic;
    param position{PLAYERS} symbolic;
    param expected_draft_position{PLAYERS} >=1;
    param expected_points{PLAYERS} >= 0;
    set DRAFTABLE_PLAYERS within PLAYERS = {p in PLAYERS : draft_status[p] <> 'Drafted By Someone Else'};

    set MY_DRAFT_POSITIONS ordered;

    set POSITIONS;
    param min_number_starters{POSITIONS} >= 0;
    param max_number_starters{p in POSITIONS} >= min_number_starters[p];
    param min_number_reserve{POSITIONS} >= 0;
    param max_number_reserve{p in POSITIONS} >= min_number_reserve[p];
    param flex_status{POSITIONS} symbolic;

    var Starters {DRAFTABLE_PLAYERS} binary;
    var Reserves {DRAFTABLE_PLAYERS} binary;

    subject to Already_Drafted_By_Me {p in PLAYERS: draft_status[p] = 'Drafted By Me'}:
        Starters[p] + Reserves[p] = 1;
    subject to Cant_Draft_Twice {p in PLAYERS: draft_status[p] = 'Un-drafted'}:
        Starters[p] + Reserves[p] <= 1;

    subject to At_Most_X_Can_Be_Ahead_Of_Y {d in MY_DRAFT_POSITIONS}:
        sum{p in DRAFTABLE_PLAYERS: expected_draft_position[p] < d}(Starters[p] + Reserves[p]) <=
        ord(d, MY_DRAFT_POSITIONS) - 1;

    var My_Draft_Size >= card({p in PLAYERS: draft_status[p] = 'Drafted By Me'}),
                      <= card(MY_DRAFT_POSITIONS);
    subject to Set_My_Draft_Size:
        sum{p in PLAYERS: draft_status[p] <> 'Drafted By Someone Else'}(Starters[p] + Reserves[p]) =
            My_Draft_Size;

    subject to Min_Number_Starters{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Starters[pl] >= min_number_starters[p];
    subject to Max_Number_Starters{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Starters[pl] <= max_number_starters[p];
    subject to Min_Number_Reserve{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Reserves[pl]>= min_number_reserve[p];
    subject to Max_Number_Reserve{p in POSITIONS}:
        sum{pl in DRAFTABLE_PLAYERS: position[pl] = p}Reserves[pl] <= max_number_reserve[p];

    subject to Max_Number_Flex_Starters:
        sum{p in DRAFTABLE_PLAYERS: flex_status[position[p]] = 'Flex Eligible'}Starters[p]
        <= max_number_of_flex_starters;

    maximize Total_Yield:
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
    ampl.solve()
    if ampl.getValue("solve_result") == "infeasible":
        print("No draft at all is possible!")
        return

    def selected_players(df, starter_or_reserve):
        assert len(df.columns) == 1
        # only capture those rows where the solution variable is nearly 1
        df = df[df.apply(lambda row: abs(row[df.columns[0]] - 1) < 0.0001, axis=1)]
        df = df.join(dat.players.set_index('Player Name'))
        df.reset_index(inplace=True)
        df.rename(columns={df.columns[0]: "Player Name"}, inplace=True)
        df["Planned Or Actual"] = df.apply(lambda row: "Planned" if row["Draft Status"] == "Un-drafted" else "Actual",
                                           axis=1)
        df["Starter Or Reserve"] = starter_or_reserve
        return df[["Player Name", "Position", "Planned Or Actual", "Starter Or Reserve", "Expected Draft Position"]]

    starters = selected_players(ampl.getVariable("Starters").getValues().toPandas(), "Starter")
    reserves = selected_players(ampl.getVariable("Reserves").getValues().toPandas(), "Reserve")
    my_draft = starters.append(reserves)
    my_draft = my_draft.sort_values(by="Expected Draft Position").drop("Expected Draft Position", axis=1)
    my_draft.reset_index(drop=True, inplace=True) # now its index is sorted by Expected Draft Position

    sorted_draft_positions = dat.my_draft_positions.sort_values(by='Draft Position').reset_index(drop=True)
    if len(my_draft) < len(sorted_draft_positions):
        print("Your model is over-constrained, and thus only a partial draft was possible")
    # my_draft and sorted_draft_positions both have sequential index values. join will use these by default
    sln = solution_schema.PanDat(my_draft=my_draft.join(sorted_draft_positions))

    sln.parameters.loc[0] = ["Total Yield", ampl.getObjective('Total_Yield').value()]
    sln.parameters.loc[1] = ["Draft Performed", "Complete" if len(sln.my_draft) == len(dat.my_draft_positions)
                             else "Partial"]

    return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------