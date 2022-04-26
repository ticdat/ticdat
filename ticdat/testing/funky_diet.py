from ticdat import TicDatFactory
input_schema = TicDatFactory (
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods  = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity"]],
    stupid_table = [["Fo", "Cat"],["Junk"]])
input_schema.add_foreign_key("nutrition_quantities", "foods", ["Food", "Name"])
input_schema.add_foreign_key("stupid_table", "nutrition_quantities", [["Fo", "Food"],["Cat", "Category"]])
input_schema.add_foreign_key("nutrition_quantities", "categories",
                            ["Category", "Name"])
input_schema.set_data_type("categories", "Min Nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("categories", "Max Nutrition", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("foods", "Cost", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.set_data_type("nutrition_quantities", "Quantity", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=False)
input_schema.add_data_row_predicate(
    "categories", predicate_name="Min Max Check",
    predicate=lambda row : row["Max Nutrition"] >= row["Min Nutrition"])
input_schema.set_default_value("categories", "Max Nutrition", float("inf"))

solution_schema = TicDatFactory(
    parameters = [["Parameter"],["Value"]],
    buy_food = [["Food"],["Quantity"]],
    consume_nutrition = [["Category"],["Quantity"]],
    weird_table = [["This","That"],["Thother"]])

hard_coded_solution_dict = {
  # there is a deliberate flaw in the data  - we are consuming 91.9 not 91.0 units of protein
  "buy_food": [
    [
      "hamburger",
      0.6045138888888888
    ],
    [
      "milk",
      6.970138888888889
    ],
    [
      "ice cream",
      2.5913194444444447
    ]
  ],
  "consume_nutrition": [
    [
      "protein",
      91.9 # a real solution with standard input data would be 91.0 not 91.9
    ],
    [
      "calories",
      1800.0
    ],
    [
      "fat",
      59.055902777777774
    ],
    [
      "sodium",
      1779.0
    ]
  ],
  "parameters": [
    [
      "Total Cost",
      11.828861111111111
    ]
  ]
}

def solve(dat):
    assert input_schema.good_tic_dat_object(dat)
    smaller_sch = input_schema.clone(table_restrictions=["foods", "nutrition_quantities", "categories"])
    assert smaller_sch.good_tic_dat_object(dat)
    lens = {"foods": 9, "nutrition_quantities": 36, "categories": 4}
    for t in smaller_sch.all_tables:
        assert abs(len(getattr(dat, t)) - lens[t]) <= 2

    rtn = solution_schema.TicDat(**hard_coded_solution_dict)
    for f in ["find_foreign_key_failures", "find_data_type_failures", "find_data_row_failures"]:
        fails = getattr(smaller_sch, f)(dat)
        if fails:
            rtn.parameters[f] = sum(map(len, fails.values())) if f == "find_data_row_failures" else len(fails)
    return rtn

def a_solvish_act(dat):
    rtn = solve(dat)
    if rtn:
        return {"sln":rtn}

def remove_the_pizza(dat):
    assert not input_schema.good_tic_dat_object(dat)
    smaller_sch = input_schema.clone(table_restrictions=["foods", "nutrition_quantities"])
    dat.foods.pop("pizza", None)
    smaller_sch.remove_foreign_key_failures(dat)
    return dat

def checks_the_unit_test_result(sln):
    assert not any(hasattr(sln, _) for _ in set(solution_schema.all_tables).difference(["parameters"]))
    assert len(sln.parameters) == 3
    assert sln.parameters["find_foreign_key_failures"]["Value"] == 1
    assert sln.parameters["find_data_row_failures"]["Value"] == 2
    return None

