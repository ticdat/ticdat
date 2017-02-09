from ticdat import TicDatFactory, Model, utils
from ticdat.model import cplex, gurobi, xpress
from ticdat.testing.ticdattestutils import dietSolver, nearlySame, netflowSolver
from ticdat.testing.ticdattestutils import fail_to_debugger, flagged_as_run_alone
import unittest
import os
import inspect

def _codeFile() :
    return  os.path.realpath(os.path.abspath(inspect.getsourcefile(_codeFile)))
__codeFile = _codeFile()
def _codeDir():
    return os.path.dirname(__codeFile)

# issue 1104 deals with Jenkins / OCP support of Model and the three horseman of MIPocalypse

#@fail_to_debugger
class TestModel(unittest.TestCase):
    def _testDiet(self, modelType):
        sln, cost = dietSolver(modelType)
        self.assertTrue(sln)
        self.assertTrue(nearlySame(cost, 11.8289))
    def _testNetflow(self, modelType):
        sln, cost = netflowSolver(modelType)
        self.assertTrue(sln)
        self.assertTrue(nearlySame(cost, 5500.0))
    def _testFantop(self, modelType):
        sln, draft_yield = _testFantop(modelType, "sample_data.sql")
        self.assertTrue(sln and nearlySame(draft_yield, 2988.61))
        sln, draft_yield = _testFantop(modelType, "sample_tweaked_most_importants.sql")
        self.assertTrue(sln and nearlySame(draft_yield, 2947.677))
        sln, draft_yield = _testFantop(modelType, "flex_constraint.sql")
        self.assertTrue(sln and nearlySame(draft_yield, 2952.252))
    def _testParameters(self, modelType):
        mdl = Model(modelType, "parameters")
        mdl.set_parameters(MIP_Gap =  0.01)
    def testCplex(self):
        self.assertFalse(utils.stringish(cplex))
        self._testDiet("cplex")
        self._testNetflow("cplex")
        self._testFantop("cplex")
        self._testParameters("cplex")
    def testGurobi(self):
        self.assertFalse(utils.stringish(gurobi))
        self._testDiet("gurobi")
        self._testNetflow("gurobi")
        self._testFantop("gurobi")
        self._testParameters("gurobi")
    def testXpress(self):
        self.assertFalse(utils.stringish(xpress))
        self._testDiet("xpress")
        self._testNetflow("xpress")
        self._testFantop("xpress")
        self._testParameters("xpress") # not yet working

def _testFantop(modelType, sqlFile):
    dataFactory = TicDatFactory (
     parameters = [["Key"],["Value"]],
     players = [['Player Name'],
                ['Position', 'Average Draft Position', 'Expected Points', 'Draft Status']],
     roster_requirements = [['Position'],
                           ['Min Num Starters', 'Max Num Starters', 'Min Num Reserve', 'Max Num Reserve',
                            'Flex Status']],
     my_draft_positions = [['Draft Position'],[]]
    )

    # add foreign key constraints (optional, but helps with preventing garbage-in, garbage-out)
    dataFactory.add_foreign_key("players", "roster_requirements", ['Position', 'Position'])

    # set data types (optional, but helps with preventing garbage-in, garbage-out)
    dataFactory.set_data_type("parameters", "Key", number_allowed = False,
                              strings_allowed = ["Starter Weight", "Reserve Weight",
                                                 "Maximum Number of Flex Starters"])
    dataFactory.set_data_type("parameters", "Value", min=0, max=float("inf"),
                              inclusive_min = True, inclusive_max = False)
    dataFactory.set_data_type("players", "Average Draft Position", min=0, max=float("inf"),
                              inclusive_min = False, inclusive_max = False)
    dataFactory.set_data_type("players", "Expected Points", min=-float("inf"), max=float("inf"),
                              inclusive_min = False, inclusive_max = False)
    dataFactory.set_data_type("players", "Draft Status",
                              strings_allowed = ["Un-drafted", "Drafted By Me", "Drafted By Someone Else"])
    for fld in ("Min Num Starters",  "Min Num Reserve", "Max Num Reserve"):
        dataFactory.set_data_type("roster_requirements", fld, min=0, max=float("inf"),
                              inclusive_min = True, inclusive_max = False, must_be_int = True)
    dataFactory.set_data_type("roster_requirements", "Max Num Starters", min=0, max=float("inf"),
                          inclusive_min = False, inclusive_max = True, must_be_int = True)
    dataFactory.set_data_type("roster_requirements", "Flex Status", number_allowed = False,
                              strings_allowed = ["Flex Eligible", "Flex Ineligible"])
    dataFactory.set_data_type("my_draft_positions", "Draft Position", min=0, max=float("inf"),
                              inclusive_min = False, inclusive_max = False, must_be_int = True)

    solutionFactory = TicDatFactory(
            my_draft = [['Player Name'], ['Draft Position', 'Position', 'Planned Or Actual',
                                         'Starter Or Reserve']])

    dat = dataFactory.sql.create_tic_dat_from_sql(os.path.join(_codeDir(), sqlFile), freeze_it=True)

    assert dataFactory.good_tic_dat_object(dat)
    assert not dataFactory.find_foreign_key_failures(dat)
    assert not dataFactory.find_data_type_failures(dat)


    expected_draft_position = {}
    # for our purposes, its fine to assume all those drafted by someone else are drafted
    # prior to any players drafted by me
    for player_name in sorted(dat.players,
                              key=lambda _p: {"Un-drafted":dat.players[_p]["Average Draft Position"],
                                              "Drafted By Me":-1,
                                              "Drafted By Someone Else":-2}[dat.players[_p]["Draft Status"]]):
        expected_draft_position[player_name] = len(expected_draft_position) + 1
    assert max(expected_draft_position.values()) == len(set(expected_draft_position.values())) == len(dat.players)
    assert min(expected_draft_position.values()) == 1

    already_drafted_by_me = {player_name for player_name,row in dat.players.items() if
                            row["Draft Status"] == "Drafted By Me"}
    can_be_drafted_by_me = {player_name for player_name,row in dat.players.items() if
                            row["Draft Status"] != "Drafted By Someone Else"}

    m = Model(modelType, 'fantop')
    my_starters = {player_name:m.add_var(type="binary",name="starter_%s"%player_name)
                  for player_name in can_be_drafted_by_me}
    my_reserves = {player_name:m.add_var(type="binary",name="reserve_%s"%player_name)
                  for player_name in can_be_drafted_by_me}


    for player_name in can_be_drafted_by_me:
        if player_name in already_drafted_by_me:
            m.add_constraint(my_starters[player_name] + my_reserves[player_name] == 1,
                             name="already_drafted_%s"%player_name)
        else:
            m.add_constraint(my_starters[player_name] + my_reserves[player_name] <= 1,
                             name="cant_draft_twice_%s"%player_name)

    for i,draft_position in enumerate(sorted(dat.my_draft_positions)):
        m.add_constraint(m.sum(my_starters[player_name] + my_reserves[player_name]
                                for player_name in can_be_drafted_by_me
                                if expected_draft_position[player_name] < draft_position) <= i,
                    name = "at_most_%s_can_be_ahead_of_%s"%(i,draft_position))

    my_draft_size = m.sum(my_starters[player_name] + my_reserves[player_name]
                          for player_name in can_be_drafted_by_me)
    m.add_constraint(my_draft_size >= len(already_drafted_by_me) + 1,
                     name = "need_to_extend_by_at_least_one")
    m.add_constraint(my_draft_size <= len(dat.my_draft_positions), name = "cant_exceed_draft_total")

    for position, row in dat.roster_requirements.items():
        players = {player_name for player_name in can_be_drafted_by_me
                   if dat.players[player_name]["Position"] == position}
        starters = m.sum(my_starters[player_name] for player_name in players)
        reserves = m.sum(my_reserves[player_name] for player_name in players)
        m.add_constraint(starters >= row["Min Num Starters"], name = "min_starters_%s"%position)
        m.add_constraint(starters <= row["Max Num Starters"], name = "max_starters_%s"%position)
        m.add_constraint(reserves >= row["Min Num Reserve"], name = "min_reserve_%s"%position)
        m.add_constraint(reserves <= row["Max Num Reserve"], name = "max_reserve_%s"%position)

    if "Maximum Number of Flex Starters" in dat.parameters:
        players = {player_name for player_name in can_be_drafted_by_me if
                   dat.roster_requirements[dat.players[player_name]["Position"]]["Flex Status"] == "Flex Eligible"}
        m.add_constraint(m.sum(my_starters[player_name] for player_name in players)
                    <= dat.parameters["Maximum Number of Flex Starters"]["Value"],
                    name = "max_flex")

    starter_weight = dat.parameters["Starter Weight"]["Value"] if "Starter Weight" in dat.parameters else 1
    reserve_weight = dat.parameters["Reserve Weight"]["Value"] if "Reserve Weight" in dat.parameters else 1
    m.set_objective(m.sum(dat.players[player_name]["Expected Points"] *
                               (my_starters[player_name] * starter_weight + my_reserves[player_name] * reserve_weight)
                               for player_name in can_be_drafted_by_me),
                   sense="maximize")

    if not m.optimize():
        return

    sln = solutionFactory.TicDat()
    def almostone(x):
        return abs(m.get_solution_value(x) -1) < 0.0001
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
    return sln, draft_yield


_scratchDir = TestModel.__name__ + "_scratch"

# Run the tests.
if __name__ == "__main__":
    unittest.main()