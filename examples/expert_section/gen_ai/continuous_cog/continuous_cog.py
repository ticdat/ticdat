import gurobipy as gu
from ticdat import TicDatFactory, standard_main, gurobi_env

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory(
    # Each site has a demand and a (latitude, longitude) location
    sites=[["Name"], ["Demand", "Latitude", "Longitude"]],
    # Two parameters: "Number of Centroids" and "MIP Gap"
    parameters=[["Parameter"], ["Value"]]
)

# Data types
input_schema.set_data_type("sites", "Demand", min=0, max=float("inf"))
input_schema.set_data_type("sites", "Latitude", min=-90, max=90)
input_schema.set_data_type("sites", "Longitude", min=-180, max=180)

input_schema.add_parameter(
    "Number of Centroids", default_value=3, must_be_int=True,
    min=1, max=float("inf")
)
input_schema.add_parameter(
    "MIP Gap", default_value=0.001, must_be_int=False,
    min=0, max=float("inf")
)

# ------------------------ define the output schema -------------------------------
# We'll store:
#  1) "openings" : the chosen centroid coordinates (indexed by centroid number)
#  2) "assignments": for each site, the centroid to which it is assigned
#  3) "parameters" : objective information
solution_schema = TicDatFactory(
    openings=[["Centroid"], ["Latitude", "Longitude"]],
    assignments=[["Site", "Assigned To"], []],
    parameters=[["Parameter"], ["Value"]]
)

def solve(dat):
    """
    Solve the continuous center-of-gravity problem using second-order cone constraints.
    Minimize sum_{i} Demand_i * distance_to_closest_centroid.
    We place 'Number of Centroids' centroids anywhere in the plane.
    """
    # Basic validation
    assert input_schema.good_tic_dat_object(dat)
    # (No foreign keys here; skipping that check)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # Grab parameters
    full_parameters = input_schema.create_full_parameters_dict(dat)
    K = full_parameters["Number of Centroids"]
    mip_gap = full_parameters["MIP Gap"]

    if not dat.sites:
        print("No sites found. Nothing to optimize.\n")
        return None

    # Build Model
    m = gu.Model("continuous_cog", env=gurobi_env())
    # Even though we might not strictly need an integer MIP gap for a continuous model,
    # we set it per instructions.
    m.Params.MIPGap = mip_gap

    # Create indexing for convenience
    site_list = list(dat.sites)
    n_sites = len(site_list)

    name_ = lambda s: "".join([_ if (_.lower() in "0123456789qwertyuiopasdfghjklzxcvbnm") else "_" for _ in s][:254])

    # Decision variables for each of the K centroids: (cx[k], cy[k])
    # No integrality needed; these are free continuous within some large bounds.
    # You might set them to smaller bounds if appropriate.
    cx = {k: m.addVar(lb=-1e5, ub=1e5, name=name_(f"cx_{k}")) for k in range(K)}
    cy = {k: m.addVar(lb=-1e5, ub=1e5, name=name_(f"cy_{k}")) for k in range(K)}

    # For each site i and centroid k, define d_{i,k} >= Euclidian distance.
    # We'll then define D_i to be the min_{k} d_{i,k}.
    d_ik = {}
    for i, site_name in enumerate(site_list):
        lat_i = dat.sites[site_name]["Latitude"]
        lon_i = dat.sites[site_name]["Longitude"]
        for k in range(K):
            # d_{i,k} >= 0
            d_ik[i, k] = m.addVar(lb=0, name=name_(f"d_{site_name}_{k}"))

    # For each site i, define obj_i to track the distance to the nearest centroid.
    obj_i = {i: m.addVar(lb=0, name=name_(f"D_{site_list[i]}")) for i in range(n_sites)}

    # Add second-order cone constraints:
    # d_{i,k} >= sqrt((cx[k] - lat_i)^2 + (cy[k] - lon_i)^2)
    # Gurobi provides a built-in general constraint for norms: Norm(x, 2) <= t
    # But we want d_{i,k} >= Norm(...) so we'll do the reversed version:
    #   d_{i,k}^2 >= (cx[k] - lat_i)^2 + (cy[k] - lon_i)^2
    # The simplest is to use addQConstr with a square on the left side:
    for i, site_name in enumerate(site_list):
        lat_i = dat.sites[site_name]["Latitude"]
        lon_i = dat.sites[site_name]["Longitude"]
        for k in range(K):
            m.addQConstr(
                d_ik[i, k] * d_ik[i, k] >= (cx[k] - lat_i) * (cx[k] - lat_i)
                                         + (cy[k] - lon_i) * (cy[k] - lon_i),
                name=name_(f"soc_{site_name}_{k}")
            )

    # For each site i, we want D_i <= d_{i,k} for all k so that
    # D_i = min_{k} d_{i,k}
    for i in range(n_sites):
        for k in range(K):
            m.addConstr(obj_i[i] <= d_ik[i, k], name=name_(f"minDist_{i}_{k}"))

    # Objective: Minimize sum_{i} Demand_i * D_i
    obj_expr = gu.LinExpr()
    for i, site_name in enumerate(site_list):
        demand = dat.sites[site_name]["Demand"]
        obj_expr.addTerms(demand, obj_i[i])

    m.setObjective(obj_expr, gu.GRB.MINIMIZE)

    # Optimize
    m.optimize()

    # Check for solution status
    if m.status in [gu.GRB.INF_OR_UNBD, gu.GRB.INFEASIBLE, gu.GRB.UNBOUNDED]:
        print(f"Model infeasible or unbounded. Status code = {m.status}")
        return None
    if m.status != gu.GRB.OPTIMAL and m.status != gu.GRB.INTERRUPTED:
        print(f"Unexpected solve status = {m.status}")
        return None

    # Build solution
    sln = solution_schema.TicDat()
    # Parameter reporting
    sln.parameters["Upper Bound"] = getattr(m, "objVal", float("inf"))
    # For a continuous QCP, Gurobi will report an 'objBound' attribute that might
    # be the same as objVal if truly optimal. We'll store it if available.
    lower_bound = getattr(m, "objBound", m.objVal)
    sln.parameters["Lower Bound"] = lower_bound

    print(f"Upper Bound: {m.objVal}")
    print(f"Lower Bound: {lower_bound}")

    # Record the centroid openings
    for k in range(K):
        sln.openings[str(k)] = {
            "Latitude": cx[k].X,
            "Longitude": cy[k].X
        }

    # Assign each site to whichever centroid yields the smallest d_ik in the solution
    for i, site_name in enumerate(site_list):
        # find the centroid with minimal d_ik
        best_k = None
        best_val = float("inf")
        for k in range(K):
            val = d_ik[i, k].X
            if val < best_val:
                best_val = val
                best_k = k
        # Record that assignment
        sln.assignments[site_name, str(best_k)] = {}

    print(f"Number of centroids placed: {K}")
    return sln


# ---------------------------------- Stand-alone main -----------------------------
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
