#
# Assign workers to shifts; each worker may or may not be available on a
# particular day. We use lexicographic optimization to solve the model:
# first, we minimize the linear sum of the slacks. Then, we constrain
# the sum of the slacks, and minimize the total payment to workers.
# Finally, we minimize a quadratic objective that
# tries to balance the workload among the workers.
#

from ticdat import PanDatFactory, standard_main
from amplpy import AMPL

# ------------------------ define the input schema --------------------------------
input_schema = PanDatFactory(
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
solution_schema = PanDatFactory(
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
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_duplicates(dat)
    assert not input_schema.find_foreign_key_failures(dat)
    assert not input_schema.find_data_type_failures(dat)

    # build the AMPL math model
    ampl = AMPL()
    ampl.setOption('solver', 'gurobi')

    ampl.eval("""
    set Workers;
    set Shifts;
    set Availability within {Workers,Shifts};

    param Pay{Workers};
    param Shift_Require{Shifts};

    var x{Availability} binary;
    var slack{Shifts}>=0;
    var totShifts{Workers};
    var totSlack;

    minimize Total_slack: totSlack;

    subject to reqCts{s in Shifts}: slack[s]+sum{(w,s) in Availability} x[w,s]== Shift_Require[s];
    subject to TotalSlack:totSlack==sum{s in Shifts}slack[s];
    subject to TotalShifts{w in Workers}:totShifts[w]==sum{(w,s) in Availability} x[w,s];
    """)

    # copy the tables to amplpy.DataFrame objects, renaming the data fields as needed
    dat = input_schema.copy_to_ampl(dat, field_renamings={
        ("shifts", "Requirement"): "Shift_Require",
        ("workers", "Payment"): "Pay"})
    # load the amplpy.DataFrame objects into the AMPL model, explicitly identifying how to populate the AMPL sets
    input_schema.set_ampl_data(dat, ampl, {"workers": "Workers", "shifts": "Shifts",
                                           "availability": "Availability"})

    ampl.solve()

    if ampl.getValue("solve_result") != "infeasible":
        # lexicographical solve 2 - minimize Total Payments while restricting Total Slack to be as small as possible
        totalSlack = ampl.getValue("totSlack")
        ampl.eval("""
        param maxSlack;
        subject to MaximumSlack: totSlack <= maxSlack;
        var totPayments;
        subject to TotalPayments: totPayments = sum{(w,s) in Availability}x[w,s]*Pay[w];
        minimize Total_payments: totPayments;
        objective Total_payments;
        """)
        ampl.param["maxSlack"] = totalSlack
        ampl.solve()
        if ampl.getValue("solve_result") != "infeasible":
            # lexicographical solve 2 - minimize imbalance among workers while restricting
            # Total Slack and Total Payments to be as small as possible
            totalPayments = ampl.getValue("Total_payments")
            ampl.eval("""
            param maxTotalPayments;
            subject to MaximumTotalPayments: totPayments <= maxTotalPayments;

            var avgShifts;
            var diffShifts{Workers};
            subject to avgShiftsC: card(Workers)*avgShifts==sum{w in Workers} totShifts[w];
            subject to Diff{w in Workers}: diffShifts[w]==totShifts[w]-avgShifts;

            minimize Total_Imbalance: sum{w in Workers}diffShifts[w] * diffShifts[w];
            objective Total_Imbalance;
            """)
            ampl.param["maxTotalPayments"] = totalPayments
            ampl.solve()
            if ampl.getValue("solve_result") != "infeasible":
                sln = solution_schema.copy_from_ampl_variables(
                        {('assignments' ,''): (ampl.getVariable("x"), lambda x: abs(x-1) < 1e-5),
                         ('slacks', 'Slack'): ampl.getVariable("slack"),
                         ('total_shifts', 'Total Number Of Shifts'): ampl.getVariable("totShifts")
                        })
                sln.parameters.loc[0] = ['Total Slack', ampl.getValue("Total_slack")]
                sln.parameters.loc[1] = ['Total Payments', ampl.getValue("Total_payments")]
                sln.parameters.loc[2] = ['Variance of Total Shifts', ampl.getValue("Total_Imbalance/card(Workers)")]
                return sln
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/db/sql/mdb files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------