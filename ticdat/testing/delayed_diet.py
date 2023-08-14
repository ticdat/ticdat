# dspotconnect simple app and unit test helper
from ticdat import TicDatFactory
from ticdat.utils import verify
import time

input_schema = TicDatFactory (
    parameters = [["Key"], ["Value"]],
    categories = [["Name"],["Min Nutrition", "Max Nutrition"]],
    foods  = [["Name"],["Cost"]],
    nutrition_quantities = [["Food", "Category"], ["Quantity"]])
input_schema.add_parameter("Numerical Progress Delay", 2, min=0, max=3000)
input_schema.add_parameter("MIP Progress Delay", 3, min=0, max=3000)
input_schema.add_parameter("Initial LB", 90, min=0, max=100, inclusive_min=False, inclusive_max=False)
input_schema.add_parameter("Initial Done", 1, min=0, max=100, inclusive_min=False, inclusive_max=False)
input_schema.add_parameter("Progress Factor", 0.3, min=0, max=1, inclusive_min=False, inclusive_max=False)
input_schema.add_parameter("Thread Sleep", 0.3, min=0, max=10, inclusive_min=False, inclusive_max=False)
# For testing in the GUI, progress factor of 0.02 and thread sleep of 1 work better. Then have initial done and/or
# initial LB of 1, with delay of 60. Use a higher initial value and a smaller delay as needed.


input_schema.add_foreign_key("nutrition_quantities", "foods", ["Food", "Name"])
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
    consume_nutrition = [["Category"],["Quantity"]])

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

def solve(dat, progress, diagnostic_log):
    diagnostic_log.write("*****\ndelayed_diet app\n*****\n")
    start_0 = time.time()
    lens = {"foods": 9, "nutrition_quantities": 36, "categories": 4}
    for t in lens:
        verify(abs(len(getattr(dat, t)) - lens[t]) <= 2, f"check failed for {t}")
    all_parameters = input_schema.create_full_parameters_dict(dat)
    start = time.time()
    done = all_parameters["Initial Done"]
    while time.time()-start < all_parameters["Numerical Progress Delay"]:
        if not progress.numerical_progress("pre-MIP progression", done):
            return
        done += (100-done) * all_parameters["Progress Factor"]
        print(f"progress sleep {done}")
        time.sleep(all_parameters["Thread Sleep"])

    start = time.time()
    lb = all_parameters["Initial LB"]
    while time.time()-start < all_parameters["MIP Progress Delay"]:
        lb += (100-lb) * all_parameters["Progress Factor"]
        if not progress.mip_progress("core MIP progression", lb, 100):
            break
        print(f"mip sleep {lb}")
        time.sleep(all_parameters["Thread Sleep"])

    # following line is a prep for when issue #1 is addressed (i.e. API can recognize bugs in the code)
    verify("garbage" not in dat.categories, "there should be no garbage")
    rtn = solution_schema.TicDat(**hard_coded_solution_dict)
    rtn.parameters["Lower Bound"] = lb
    rtn.parameters["Upper Bound"] = 100
    run_time = "{0:.2f}".format(time.time() - start_0)
    diagnostic_log.write(f"*****\nRun time {run_time}\n*****\n")

    return rtn

# no command line interface. The solve is complex anyway.