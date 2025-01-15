# testing the various ticdat.Progress utility routines for progress
# also a good ticdat.Model version of the COG sample model. Can show the superiority of Gurobi to CPLEX
from math import isnan
import os
from ticdat import TicDatFactory, Progress, LogFile, Slicer, standard_main, Model

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
# not using a the "strong v weak" parameter, testing will always use the weak formulation
input_schema.add_parameter("Core Model Type", "cplex", number_allowed=False,
                           strings_allowed=["gurobi", "cplex", "xpress"])

# testing parameter
input_schema.add_parameter("Maximize Subtract", None, nullable=True, inclusive_max=False, inclusive_min=False)


# data fields amongst them.
solution_schema = TicDatFactory(
    openings    = [['Site'],[]],
    assignments = [['Site', 'Assigned To'],[]],
    parameters  = [["Parameter"], ["Value"]])

def solve(dat, progress):
    assert isinstance(progress, Progress)
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)


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

    m = Model(model_name="cog", model_type=full_parameters["Core Model Type"])

    assign_vars = {(n, assigned_to) : m.add_var(type="binary",
                                        name="%s_%s"%(n,assigned_to))
                    for n in dat.sites for assigned_to in dat.sites
                    if can_assign(n, assigned_to)}
    open_vars = {n : m.add_var(type="binary", name = "open_%s"%n)
                     for n in dat.sites
                     if dat.sites[n]["Center Status"] == "Can Be Center"}
    assert open_vars, "nothing can be center"

    assign_slicer = Slicer(assign_vars)

    for n, r in dat.sites.items():
        if r["Demand"] > 0:
            m.add_constraint(m.sum(assign_vars[n, assign_to]
                                    for _, assign_to in assign_slicer.slice(n, "*")) ==1,
                             name="must_assign_%s"%n)

    for assigned_to, r in dat.sites.items():
        if r["Center Status"] == "Can Be Center":
            _assign_vars = [assign_vars[n, assigned_to]
                            for n,_ in assign_slicer.slice("*", assigned_to)]
            # this is the weak formulation
            m.add_constraint(m.sum(_assign_vars) <= len(_assign_vars) * open_vars[assigned_to],
                             name="weak_force_open%s"%assigned_to)

    number_of_centroids = full_parameters["Number of Centroids"]

    m.add_constraint(m.sum(v for v in open_vars.values())==number_of_centroids, name="numCentroids")

    m.set_parameters(MIP_Gap=full_parameters["MIP Gap"])

    if full_parameters["Maximize Subtract"] is None:
        m.set_objective(m.sum(var * get_distance(n,assigned_to) * dat.sites[n]["Demand"]
                     for (n, assigned_to),var in assign_vars.items()), sense="minimize")
    else:
        m.set_objective(full_parameters["Maximize Subtract"] -
                    m.sum(var * get_distance(n,assigned_to) * dat.sites[n]["Demand"]
                     for (n, assigned_to),var in assign_vars.items()), sense="maximize")
    sense_kwargs = {"sense": "minimize" if full_parameters["Maximize Subtract"] is None else "maximize"}
    if full_parameters["Core Model Type"] == "cplex":
        progress.add_cplex_listener("COG Optimization", m.core_model, **sense_kwargs)
    if full_parameters["Core Model Type"] == "xpress":
        progress.add_xpress_callback("COG Optimization", m.core_model, **sense_kwargs)

    worked = m.optimize(*([progress.gurobi_call_back_factory("COG Optimization", m.core_model, **sense_kwargs)]
                 if full_parameters["Core Model Type"] == "gurobi" else []))

    assert worked, "testing model set up only for success"

    sln = solution_schema.TicDat()
    results = m.get_mip_results()
    sln.parameters["Lower Bound"] = results.best_bound
    sln.parameters["Upper Bound"] = results.objective_value
    if full_parameters["Maximize Subtract"] is not None:
        fix = lambda x: -x + full_parameters["Maximize Subtract"]
        for k in ["Lower Bound", "Upper Bound"]:
            sln.parameters[k] = fix(sln.parameters[k]["Value"])

    def almostone(x) :
        return abs(x-1) < 0.0001

    for (n, assigned_to), var in assign_vars.items() :
        if almostone(m.get_solution_value(var)) :
            sln.assignments[n,assigned_to] = {}
    for n,var in open_vars.items() :
        if almostone(m.get_solution_value(var)):
            sln.openings[n]={}
    return sln