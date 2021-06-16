#
# Assign workers to shifts; each worker may or may not be available on a
# particular day. We use lexicographic optimization to solve the model:
# first, we minimize the linear sum of the slacks. Then, we constrain
# the sum of the slacks, and minimize the total payment to workers.
# Finally, we minimize a quadratic objective that
# tries to balance the workload among the workers.
#

import gurobipy as gp
from ticdat import TicDatFactory,  standard_main, gurobi_env

# ------------------------ define the input schema --------------------------------
input_schema = TicDatFactory(
    workers=[["Name"], ["Payment"]],
    shifts=[["Name"], ["Requirement"]],
    availability=[["Worker", "Shift"], []]
)
# Define the foreign key relationships
input_schema.add_foreign_key("availability", "workers", ['Worker', 'Name'])
input_schema.add_foreign_key("availability", "shifts", ['Shift', 'Name'])

# Define the data types
input_schema.set_data_type("workers", "Payment", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
input_schema.set_data_type("shifts", "Requirement", min=0, max=float("inf"),
                           inclusive_min=True, inclusive_max=True)
# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = TicDatFactory(
    assignments=[["Worker", "Shift"], []],
    slacks = [["Shift"], ["Slack"]],
    total_shifts=[["Worker"], ["Total Number Of Shifts"]],
    parameters=[["Parameter"], ["Value"]]
)
# ---------------------------------------------------------------------------------

# ------------------------ solving section-----------------------------------------
def solve(dat):
    """
    core solving routine
    :param dat: a good ticdat for the input_schema
    :return: a good ticdat for the solution_schema, or None
    """
    assert input_schema.good_tic_dat_object(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    m = gp.Model("assignment", env=gurobi_env())

    # Assignment variables: x[w,s] == 1 if worker w is assigned to shift s.
    x = m.addVars(dat.availability, vtype=gp.GRB.BINARY, name="x")

    # Slack variables for each shift constraint
    slacks = m.addVars(dat.shifts, name="Slack")
    totSlack = m.addVar(name="totSlack")

    # Variables to count the total shifts worked by each worker
    totShifts = m.addVars(dat.workers, name="TotShifts")

    # Constraint: assign exactly shiftRequirements[s] workers to each shift s,
    # plus the slack
    m.addConstrs((slacks[s] + x.sum('*', s) == dat.shifts[s]["Requirement"]
                          for s in dat.shifts), "_")

    m.addConstr(totSlack == slacks.sum(), "totSlack")

    m.addConstrs((totShifts[w] == x.sum(w) for w in dat.workers), "totShifts")

    # Objective: minimize the total slack
    m.setObjective(totSlack)
    def _solve():
        m.optimize()
        if m.status in [gp.GRB.Status.INF_OR_UNBD, gp.GRB.Status.INFEASIBLE, gp.GRB.Status.UNBOUNDED]:
            print('The model cannot be solved because it is infeasible or unbounded')
        elif m.status != gp.GRB.Status.OPTIMAL:
            print('Optimization was stopped with status %d' % m.status)
        else:
            return True
    if _solve():
        # Constrain the slack by setting its upper and lower bounds
        totSlack.ub = totSlack.lb = totSlack.x

        totalPayments = m.addVar(name="totalPayments")
        m.addConstr(totalPayments == gp.quicksum(dat.workers[w]["Payment"] * x[w, s]
                                                 for w,s in dat.availability))
        m.setObjective(totalPayments)
        if _solve():
            totalPayments.ub = totalPayments.lb = totalPayments.x

            # Variable to count the average number of shifts worked
            avgShifts = m.addVar(name="avgShifts")

            # Variables to count the difference from average for each worker;
            # note that these variables can take negative values.
            diffShifts = m.addVars(dat.workers, lb=-gp.GRB.INFINITY, name="Diff")

            # Constraint: compute the average number of shifts worked
            m.addConstr(len(dat.workers) * avgShifts == totShifts.sum(), "avgShifts")

            # Constraint: compute the difference from the average number of shifts
            m.addConstrs((diffShifts[w] == totShifts[w] - avgShifts for w in dat.workers),
                         "Diff")

            # Objective: minimize the sum of the square of the difference from the
            # average number of shifts worked
            m.setObjective(gp.quicksum(diffShifts[w] * diffShifts[w] for w in dat.workers))
            if _solve():
                sln = solution_schema.TicDat()
                for (w,s),x_var in x.items():
                    if abs(x_var.x - 1) < 1e-5:
                        sln.assignments[w,s] = {}
                for s,x_var in slacks.items():
                    if x_var.x > 0:
                        sln.slacks[s] = x_var.x
                for w,x_var in totShifts.items():
                    if x_var.x > 0:
                        sln.total_shifts[w] = x_var.x
                sln.parameters["Total Slack"] = totSlack.x
                sln.parameters["Total Payments"] = totalPayments.x
                sln.parameters["Variance of Total Shifts"] = float(m.objVal) / len(dat.workers)
                return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------