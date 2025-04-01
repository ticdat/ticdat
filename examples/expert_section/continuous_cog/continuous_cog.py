import math
import gurobipy as gu
from ticdat import TicDatFactory, standard_main, gurobi_env

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory(
    # Each site has a demand and a (latitude, longitude) location
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
    # We store each of the K centroid coordinates
    openings=[["Centroid"], ["Latitude", "Longitude"]],

    # Assignments from each site to exactly one centroid
    assignments=[["Site", "Assigned To"], []],

    # Reporting info: objective, bounds, etc.
    parameters=[["Parameter"], ["Value"]]
)

def solve(dat):
    """
    Solve a continuous k-median / center of gravity problem WITHOUT any z[k].
    We have exactly K centroids, each used.

    Key constraints:
      1) sum_k x[i,k] = 1  (each site must be assigned to exactly one centroid)
      2) Dist[i,k]^2 + M*(1 - x[i,k]) >= (Cx[k] - lat_i)^2 + (Cy[k] - lon_i)^2
         (forces Dist[i,k] to match the Euclidian distance if x[i,k]=1,
          or allows Dist[i,k] to be 0 if x[i,k]=0)
      3) Dist[i,k] <= M*x[i,k]  (Dist must be 0 if x[i,k]=0)
      4) Minimizes sum_{i,k} Demand_i * Dist[i,k}

    We set NonConvex=2 because of the difference-of-quadratics gating.
    """
    # Validate input
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    # Grab parameters
    full_params = input_schema.create_full_parameters_dict(dat)
    K = full_params["Number of Centroids"]  # exact number of centroids
    mip_gap = full_params["MIP Gap"]

    site_list = list(dat.sites)
    if not site_list:
        print("No sites found. Nothing to optimize.")
        return None
    n_sites = len(site_list)

    # Create model
    m = gu.Model("continuous_cog_no_z", env=gurobi_env())
    m.Params.NonConvex = 2   # needed for difference-of-quadratics gating
    m.Params.MIPGap = mip_gap

    name_ = lambda s: "".join([_ if (_.lower() in "0123456789qwertyuiopasdfghjklzxcvbnm") else "_" for _ in s][:254])
    # ----- Decision Variables -----
    # 1) Centroid coordinates (Cx[k], Cy[k]) for each of the K centroids
    lat_vals = [dat.sites[s]["Latitude"] for s in site_list]
    lon_vals = [dat.sites[s]["Longitude"] for s in site_list]
    min_lat, max_lat = min(lat_vals), max(lat_vals)
    min_lon, max_lon = min(lon_vals), max(lon_vals)

    # Expand bounding box if degenerate
    if abs(min_lat - max_lat) < 1e-6:
        min_lat -= 1
        max_lat += 1
    if abs(min_lon - max_lon) < 1e-6:
        min_lon -= 1
        max_lon += 1
    margin = 0.1
    lat_lb = min_lat - margin
    lat_ub = max_lat + margin
    lon_lb = min_lon - margin
    lon_ub = max_lon + margin

    Cx = {k: m.addVar(lb=lat_lb, ub=lat_ub, name=name_(f"Cx_{k}")) for k in range(K)}
    Cy = {k: m.addVar(lb=lon_lb, ub=lon_ub, name=name_(f"Cy_{k}"))for k in range(K)}

    # 2) Binary assignment x[i,k] in {0,1}
    x_ik = {}
    for i, site_name in enumerate(site_list):
        for k in range(K):
            x_ik[i, k] = m.addVar(vtype=gu.GRB.BINARY, name=name_(f"x_{site_name}_{k}"))

    # 3) Dist[i,k] >= 0 is the actual distance if x[i,k]=1, or 0 if x[i,k]=0
    Dist = {}
    for i, site_name in enumerate(site_list):
        for k in range(K):
            Dist[i, k] = m.addVar(lb=0, name=name_(f"Dist_{site_name}_{k}"))

    # ----- Constraints -----

    # a) Each site assigned to exactly 1 centroid
    for i, site_name in enumerate(site_list):
        m.addConstr(
            gu.quicksum(x_ik[i, k] for k in range(K)) == 1,
            name=name_(f"assign_{site_name}")
        )

    # b) Gating constraints with difference of quadratics:
    #    Dist[i,k]^2 + M(1 - x[i,k]) >= (Cx[k] - lat_i)^2 + (Cy[k] - lon_i)^2
    #    Dist[i,k] <= M*x[i,k]
    # We'll pick M as the diagonal of the bounding box to handle worst-case distances.
    box_width = lat_ub - lat_lb
    box_height = lon_ub - lon_lb
    bigM = math.sqrt(box_width**2 + box_height**2) + 1

    for i, site_name in enumerate(site_list):
        lat_i = dat.sites[site_name]["Latitude"]
        lon_i = dat.sites[site_name]["Longitude"]
        for k in range(K):
            lhs = Dist[i, k]*Dist[i, k] + (bigM**2)*(1 - x_ik[i, k])
            rhs = (Cx[k] - lat_i)*(Cx[k] - lat_i) + (Cy[k] - lon_i)*(Cy[k] - lon_i)
            m.addQConstr(lhs >= rhs, name=name_(f"distGating_{site_name}_{k}"))

            m.addConstr(Dist[i, k] <= bigM * x_ik[i, k],
                        name=name_(f"distBigM_{site_name}_{k}"))

    # ----- Objective -----
    # Minimize sum of Demand_i * Dist[i,k]
    obj = gu.quicksum(
        dat.sites[site_list[i]]["Demand"] * Dist[i, k]
        for i in range(n_sites) for k in range(K)
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
    ub = getattr(m, "objVal", float("inf"))
    lb = getattr(m, "objBound", ub)
    sln.parameters["Upper Bound"] = ub
    sln.parameters["Lower Bound"] = lb

    print(f"Upper Bound = {ub}")
    print(f"Lower Bound = {lb}")

    # Record all K centroids (since there's no "z[k]"—they're all used)
    for k in range(K):
        sln.openings[str(k)] = {
            "Latitude": Cx[k].X,
            "Longitude": Cy[k].X
        }
    print(f"Number of centroids used = {K}")

    # Assign each site to whichever centroid x[i,k]=1
    for i, site_name in enumerate(site_list):
        for k in range(K):
            if x_ik[i, k].X > 0.5:
                sln.assignments[site_name, str(k)] = {}
                break

    return sln


if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)