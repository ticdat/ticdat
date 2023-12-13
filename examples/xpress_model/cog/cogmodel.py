#
# This example demonstrates a script that  pre-diagnoses infeasibility conditions and
# records them in a log file. It also keeps track of the MIP progress, and allows for the user
# to terminate the solve prior to achieving the "a priori" goal for the optimization gap.
#
# Solve the Center of Gravity problem from _A Deep Dive into Strategic Network Design Programming_
# https://bit.ly/3eorJrA
#
# Implement core functionality needed to achieve modularity.
# 1. Define the input data schema
# 2. Define the output data schema
# 3. Create a solve function that accepts a data set consistent with the input
#    schema and (if possible) returns a data set consistent with the output schema.
#
# Provides command line interface via ticdat.standard_main
# For example, typing
#   python cogmodel.py -i cog_sample_data.sql -o cog_solution.sql
# will read from a model stored in cog_sample_data.sql and
# write the solution to the cog_solution.sql directory.

# this version of the file uses ticdat.Model to access the xpress package
# if you don't have xpress installed, the code will still load and then fail on solve

import time
import datetime
import os
from ticdat import TicDatFactory, Progress, LogFile, Slicer, standard_main, Model

# ------------------------ define the input schema --------------------------------
# There are three input tables, with 4 primary key fields  and 4 data fields.
input_schema = TicDatFactory (
     sites      = [['Name'],['Demand', 'Center Status']],
     distance   = [['Source', 'Destination'],['Distance']],
     parameters = [["Parameter"], ["Value"]])

# add foreign key constraints
input_schema.add_foreign_key("distance", "sites", ['Source', 'Name'])
input_schema.add_foreign_key("distance", "sites", ['Destination', 'Name'])

# center_status is a flag field which can take one of two string values.
input_schema.set_data_type("sites", "Center Status", number_allowed=False,
                          strings_allowed=["Can Be Center", "Pure Demand Point"])
# The default type of non infinite, non negative works for distance and demand
input_schema.set_data_type("sites", "Demand")
input_schema.set_data_type("distance", "Distance")

input_schema.add_parameter("Number of Centroids", default_value=1, inclusive_min=False, inclusive_max=False, min=0,
                                max=float("inf"), must_be_int=True)
input_schema.add_parameter("MIP Gap", default_value=0.001, inclusive_min=False, inclusive_max=False, min=0,
                                max=float("inf"), must_be_int=False)
input_schema.add_parameter("Formulation", "Strong", number_allowed=False, strings_allowed=["Weak", "Strong"])
# ---------------------------------------------------------------------------------


# ------------------------ define the output schema -------------------------------
# There are three solution tables, with 2 primary key fields and 3
# data fields amongst them.
solution_schema = TicDatFactory(
    openings    = [['Site'],[]],
    assignments = [['Site', 'Assigned To'],[]],
    parameters  = [["Parameter"], ["Value"]])
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat, diagnostic_log, error_and_warning_log, progress):
    assert isinstance(progress, Progress)
    assert isinstance(diagnostic_log, LogFile) and isinstance(error_and_warning_log, LogFile)
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)
    diagnostic_log.write("COG output log\n%s\n\n" % time_stamp())
    error_and_warning_log.write("COG error log\n%s\n\n" % time_stamp())

    full_parameters = input_schema.create_full_parameters_dict(dat)
    def get_distance(x,y):
        if (x,y) in dat.distance:
            return dat.distance[x,y]["Distance"]
        if (y,x) in dat.distance:
            return dat.distance[y,x]["Distance"]
        return float("inf")

    def can_assign(x, y):
        return dat.sites[y]["Center Status"] == "Can Be Center" \
               and get_distance(x,y)<float("inf")


    unassignables = [n for n in dat.sites if not
                     any(can_assign(n,y) for y in dat.sites) and
                     dat.sites[n]["Demand"] > 0]
    if unassignables:
        # Infeasibility detected. Generate an error table and return None
        error_and_warning_log.write("The following sites have demand, but can't be " +
                  "assigned to anything.\n")
        error_and_warning_log.log_table("Un-assignable Demand Points",
                                        [["Site"]] + [[_] for _ in unassignables])
        return

    useless = [n for n in dat.sites if not any(can_assign(y,n) for y in dat.sites) and
                                             dat.sites[n]["Demand"] == 0]
    if useless:
        # Log in the error table as a warning, but can still try optimization.
        error_and_warning_log.write("The following sites have no demand, and can't serve as the " +
                  "center point for any assignments.\n")
        error_and_warning_log.log_table("Useless Sites", [["Site"]] + [[_] for _ in useless])

    progress.numerical_progress("Feasibility Analysis" , 100)

    mdl = Model(model_type="xpress", model_name="cog")

    assign_vars = {(n, assigned_to): mdl.add_var(type='binary', name="%s_%s"%(n,assigned_to))
                    for n in dat.sites for assigned_to in dat.sites
                    if can_assign(n, assigned_to)}
    open_vars = {n : mdl.add_var(type='binary', name = "open_%s"%n)
                     for n in dat.sites
                     if dat.sites[n]["Center Status"] == "Can Be Center"}
    if not open_vars:
        error_and_warning_log.write("Nothing can be a center!\n") # Infeasibility detected.
        return

    progress.numerical_progress("Core Model Creation", 50)

    # using ticdat.Slicer instead of tuplelist simply as a matter of taste/vanity
    assign_slicer = Slicer(assign_vars)

    for n, r in dat.sites.items():
        if r["Demand"] > 0:
            mdl.add_constraint(mdl.sum(assign_vars[n, assign_to]
                                    for _, assign_to in assign_slicer.slice(n, "*"))
                        == 1,
                        name = "must_assign_%s"%n)

    crippledfordemo = full_parameters["Formulation"] == "Weak"
    for assigned_to, r in dat.sites.items():
        if r["Center Status"] == "Can Be Center":
            _assign_vars = [[n, assign_vars[n, assigned_to]]
                            for n,_ in assign_slicer.slice("*", assigned_to)]
            if crippledfordemo:
                mdl.add_constraint(mdl.sum(_[1] for _ in _assign_vars) <=
                            len(_assign_vars) * open_vars[assigned_to],
                            name="weak_force_open_%s"%assigned_to)
            else:
                for n, var in _assign_vars:
                    mdl.add_constraint(var <= open_vars[assigned_to],
                                name = f"strong_force_open_{assigned_to}_{n}")

    number_of_centroids = full_parameters["Number of Centroids"]

    mdl.add_constraint(mdl.sum(v for v in open_vars.values()) == number_of_centroids,
                name="numCentroids")

    mdl.set_parameters(MIP_Gap=full_parameters["MIP Gap"])

    progress.numerical_progress("Core Model Creation", 100)

    mdl.set_objective(mdl.sum(var * get_distance(n,assigned_to) * dat.sites[n]["Demand"] for (n, assigned_to), var in
                              assign_vars.items()), "minimize")

    progress.add_xpress_callback("COG Optimization", mdl.core_model) # handle LB/UB progress in Foresta compliant way

    def xpress_solve_stdout_redicted(prob, object, msgtype, msg):
        if msgtype is not None: # I think its an xpress bug that msgtype has everything, but living with it.
            diagnostic_log.write(f"{msgtype}\n")
    mdl.core_model.addcbmessage(xpress_solve_stdout_redicted, None, 0)
    mdl.core_module.setOutputEnabled(False) # call xpress.setOutputEnabled to quiet stdout

    if mdl.optimize():
        progress.numerical_progress("Core Optimization", 100)

        sln = solution_schema.TicDat()
        results = mdl.get_mip_results()
        sln.parameters["Lower Bound"] = results.best_bound
        sln.parameters["Upper Bound"] = results.objective_value
        diagnostic_log.write('Upper Bound: %g\n' % sln.parameters["Upper Bound"]["Value"])
        diagnostic_log.write('Lower Bound: %g\n' % sln.parameters["Lower Bound"]["Value"])

        def almostone(var) :
            return abs(mdl.get_solution_value(var) - 1) < 0.0001

        for (n, assigned_to), var in assign_vars.items() :
            if almostone(var) :
                sln.assignments[n,assigned_to] = {}
        for n,var in open_vars.items() :
            if almostone(var) :
                sln.openings[n]={}
        diagnostic_log.write('Number Centroids: %s\n' % len(sln.openings))
        progress.numerical_progress("Full Cog Solve",  100)
        return sln

def time_stamp() :
    ts = time.time()
    return datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
def percent_error(lb, ub):
    assert lb<=ub
    return "%.2f"%(100.0 * (ub-lb) / ub) + "%"

# when run from the command line, will read/write json/xls/csv/db/mdb files
if __name__ == "__main__":
    if os.path.exists("cog.stop"):
        print("Removing the cog.stop file so that solve can proceed.")
        print("Add cog.stop whenever you want to stop the optimization")
        os.remove("cog.stop")

    class CogStopProgress(Progress):
        def mip_progress(self, theme, lower_bound, upper_bound):
            super(CogStopProgress, self).mip_progress(theme, lower_bound, upper_bound)
            print("%s:%s:%s"%(theme.ljust(30), "Percent Error".ljust(20),
                              percent_error(lower_bound, upper_bound)))
            # return False (to stop optimization) if the cog.stop file exists
            return not os.path.exists("cog.stop")

    # creating a single argument version of solve to pass to standard_main
    def _solve(dat):
        # create local text files for logging
        with LogFile("output.txt") as out :
            with LogFile("error.txt") as err :
                solution = solve(dat, out, err, CogStopProgress())
                if solution :
                    print('\n\nUpper Bound   : %g' % solution.parameters["Upper Bound"]["Value"])
                    print('Lower Bound   : %g' % solution.parameters["Lower Bound"]["Value"])
                    print('Percent Error : %s' % percent_error(solution.parameters["Lower Bound"]["Value"],
                                                               solution.parameters["Upper Bound"]["Value"]))
                    return solution
                else :
                    print('\nNo solution')

    standard_main(input_schema, solution_schema, _solve)
# ---------------------------------------------------------------------------------




