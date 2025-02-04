import gurobipy as gu
from ticdat import TicDatFactory, standard_main, gurobi_env

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory(
    # Each site has a demand, plus (latitude, longitude) location
    sites=[["Name"], ["Demand", "Latitude", "Longitude"]],

    # Two parameters: "Number of Centroids" and "MIP Gap"
    parameters=[["Parameter"], ["Value"]]
)

# Basic data type checks
input_schema.set_data_type("sites", "Demand", min=0, max=float("inf"))
input_schema.set_data_type("sites", "Latitude", min=-90, max=90)
input_schema.set_data_type("sites", "Longitude", min=-180, max=180)

# Parameter definitions
input_schema.add_parameter(
    "Number of Centroids", default_value=3, must_be_int=True,
    min=1, max=float("inf")
)
input_schema.add_parameter(
    "MIP Gap", default_value=0.001, must_be_int=False,
    min=0, max=float("inf")
)

# ------------------------ define the output schema -------------------------------
solution_schema = TicDatFactory(
    # Which centroids were opened? We store their continuous coordinates.
    openings=[["Centroid"], ["Latitude", "Longitude"]],

    # Assignments from each site to a chosen centroid
    assignments=[["Site", "Assigned To"], []],

    # Basic reporting info for objective and bounds
    parameters=[["Parameter"], ["Value"]]
)

def solve(dat):
    """
    Solve a continuous k-median / center of gravity style problem using:
      - Binary assignment variables x[i,k]
      - Continuous centroid coordinates (Cx[k], Cy[k])
      - Quadratic constraints for Euclidean distance
      - NonConvex=2 to allow binary*continuous coupling

    Objective: Minimize sum_{i,k} Demand_i * Dist[i,k], where Dist[i,k] is
               the distance if x[i,k] = 1, or effectively 0 if x[i,k] = 0.
    We also enforce exactly 'Number of Centroids' many centroids to be used.
    """
    # Validate input
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # Extract parameters
    full_params = input_schema.create_full_parameters_dict(dat)
    K = full_params["Number of Centroids"]
    mip_gap = full_params["MIP Gap"]

    site_list = list(dat.sites)
    if not site_list:
        print("No sites found. Nothing to optimize.")
        return None

    n_sites = len(site_list)

    # Create Model
    m = gu.Model("continuous_cog", env=gurobi_env())
    # We need NonConvex=2 to allow bilinear terms of form x[i,k]*(distance^2)
    m.Params.NonConvex = 2
    # MIP gap
    m.Params.MIPGap = mip_gap

    name_ = lambda s: "".join([_ if (_.lower() in "0123456789qwertyuiopasdfghjklzxcvbnm") else "_" for _ in s][:254])
    # ----- Decision Variables -----
    # 1) For each centroid k, a binary z[k] indicating if that centroid is used
    z = {k: m.addVar(vtype=gu.GRB.BINARY, name=name_(f"z_{k}")) for k in range(K)}

    # 2) Coordinates of each centroid (Cx[k], Cy[k]), continuous
    #    Here we allow them to vary within some bounding box.
    #    Adjust as needed if you have prior knowledge of location bounds.
    big_coord = 1e6
    Cx = {k: m.addVar(lb=-big_coord, ub=big_coord, name=name_(f"Cx_{k}")) for k in range(K)}
    Cy = {k: m.addVar(lb=-big_coord, ub=big_coord, name=name_(f"Cy_{k}")) for k in range(K)}

    # 3) For each site i and centroid k:
    #    x[i,k] in {0,1} indicating if site i is assigned to centroid k
    x_ik = {}
    for i, site_name in enumerate(site_list):
        for k in range(K):
            x_ik[i, k] = m.addVar(vtype=gu.GRB.BINARY, name=name_(f"x_{site_name}_{k}"))

    # 4) Dist[i,k] >= 0 capturing the distance from site i to centroid k
    Dist = {}
    for i, site_name in enumerate(site_list):
        for k in range(K):
            Dist[i, k] = m.addVar(lb=0, name=name_(f"Dist_{site_name}_{k}"))

    # ----- Constraints -----

    # a) Exactly K centroids are used
    #    sum_k z[k] == K
    m.addConstr(gu.quicksum(z[k] for k in range(K)) == K, name="use_exactly_K")

    # b) Assign each site to exactly one centroid
    #    sum_k x[i,k] = 1,  for each site i
    for i, site_name in enumerate(site_list):
        m.addConstr(gu.quicksum(x_ik[i, k] for k in range(K)) == 1,
                    name=name_(f"assign_{site_name}"))

    # c) If centroid k is not used (z[k] = 0), then x[i,k] must be 0
    #    x[i,k] <= z[k]
    for i, site_name in enumerate(site_list):
        for k in range(K):
            m.addConstr(x_ik[i, k] <= z[k], name=name_(f"link_assign_{site_name}_{k}"))

    # d) Second-order cone (SOC) style constraints to tie Dist[i,k] to the
    #    Euclidean distance from site i to centroid k, but only if x[i,k] = 1.
    #
    #    We use a *bilinear* approach:
    #       Dist[i,k]^2 >= x[i,k] * ((Lat_i - Cx[k])^2 + (Lon_i - Cy[k])^2)
    #    If x[i,k] = 0, the right side is 0 => Dist[i,k]^2 >= 0 => Dist[i,k] >= 0
    #    If x[i,k] = 1, Dist[i,k]^2 >= (Lat_i - Cx[k])^2 + (Lon_i - Cy[k])^2
    #
    #    Minimizing Dist[i,k] in the objective will force Dist[i,k] to
    #    "match" the actual distance if x[i,k] = 1.  This is a nonconvex constraint
    #    because of the x[i,k]*(...) product, so we must use NonConvex=2.
    for i, site_name in enumerate(site_list):
        lat_i = dat.sites[site_name]["Latitude"]
        lon_i = dat.sites[site_name]["Longitude"]
        for k in range(K):
            # Dist[i,k]^2 >= x[i,k] * ( (lat_i - Cx[k])^2 + (lon_i - Cy[k])^2 )
            # We'll build the LHS and RHS as expressions
            lhs = Dist[i, k]*Dist[i, k]
            dx = lat_i - Cx[k]
            dy = lon_i - Cy[k]
            rhs = x_ik[i, k] * (dx*dx + dy*dy)
            m.addQConstr(lhs >= rhs, name=name_(f"soc_site_{site_name}_centroid_{k}"))

    # ----- Objective -----
    # Minimize the sum of (Demand_i * Dist[i,k]) for whichever centroid is chosen
    # We do that as sum_{i,k} Demand_i * Dist[i,k]. But we only pay for Dist[i,k]
    # if x[i,k] = 1 *and* it is forced to match the actual distance in that case.
    # If x[i,k] = 0, Dist[i,k] can be small or zero, but it won't matter because
    # x[i,k] * Dist[i,k] effectively is handled by the bilinear constraint above.
    #
    # Simpler approach in code: Just do sum_{i,k} Demand_i * Dist[i,k].
    # Because Dist[i,k] won't blow up or help to reduce cost artificially—
    # it's pinned to zero if x[i,k]=0 by the constraint Dist[i,k]^2 >= 0.
    # And when x[i,k]=1, Dist[i,k] is forced to the "true" distance or bigger.
    obj = gu.quicksum(
        dat.sites[site_list[i]]["Demand"] * Dist[i, k]
        for i in range(n_sites)
        for k in range(K)
    )

    m.setObjective(obj, gu.GRB.MINIMIZE)

    # Optimize
    m.optimize()

    # Handle solve status
    if m.status in [gu.GRB.INF_OR_UNBD, gu.GRB.INFEASIBLE, gu.GRB.UNBOUNDED]:
        print(f"Model infeasible or unbounded. (status: {m.status})")
        return None
    elif m.status not in [gu.GRB.OPTIMAL, gu.GRB.INTERRUPTED]:
        print(f"Unexpected solve status: {m.status}")
        return None

    # ----- Build solution -----
    sln = solution_schema.TicDat()

    # Parameter reporting
    sln.parameters["Upper Bound"] = getattr(m, "objVal", float("inf"))
    sln.parameters["Lower Bound"] = getattr(m, "objBound", sln.parameters["Upper Bound"]["Value"])

    print(f"Upper Bound = {sln.parameters['Upper Bound']['Value']}")
    print(f"Lower Bound = {sln.parameters['Lower Bound']['Value']}")

    # Record opened centroids and their coordinates
    used_count = 0
    for k in range(K):
        if z[k].X > 0.5:  # "opened" centroid
            sln.openings[str(k)] = {
                "Latitude": Cx[k].X,
                "Longitude": Cy[k].X
            }
            used_count += 1

    print(f"Number of centroids used = {used_count}")

    # Assign each site to the centroid for which x[i,k]=1
    for i, site_name in enumerate(site_list):
        for k in range(K):
            if x_ik[i, k].X > 0.5:
                sln.assignments[site_name, str(k)] = {}
                break  # move to next site once assigned

    return sln


if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
