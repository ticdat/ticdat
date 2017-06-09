/*********************************************
 * OPL job scheduling problem organized into tabular, ticdat compliant format
 * See https://ibm.co/2rGVyet for reference problem and sample data.
 *********************************************/

/* ------------------------ begin data initialization section ---------------------- */
include "ticdat_jobs.mod";

Jobs = {j | <j,m1,d1,m2,d2> in inp_jobs};
{string} Tasks  = inp_tasks;

{string} Machines = inp_machines;
{string} States = Machines union inp_areas;

int Index[s in States] = ord(States, s);

tuple jobRecord {
    string machine1;
    int    durations1;
    string machine2;
    int    durations2;
};
jobRecord job[Jobs] = [j: <m1,d1,m2,d2> | <j,m1,d1,m2,d2> in inp_jobs];

/* this might need to be an int
{string} inputParameterNames = {k | <k,v> in inp_parameters};
float parameters[inputParameterNames] = [k:v | <k,v> in inp_parameters];
float loadDuration = parameters["Load Duration"];
/* ------------------------ end data initialization section ------------------------ */

/* ------------------------ begin core mathematics section ------------------------- */
dvar interval act[Jobs][Tasks];

stateFunction trolleyPosition;

minimize max(j in Jobs) endOf(act[j]["unloadS"]);
subject to {
45:   // durations
46:   forall(j in Jobs) {
47:     lengthOf(act[j]["loadA"])    == loadDuration;
48:     lengthOf(act[j]["unload1"])  == loadDuration;
49:     lengthOf(act[j]["process1"]) == job[j].durations1;
50:     lengthOf(act[j]["load1"])    == loadDuration;
51:     lengthOf(act[j]["unload2"])  == loadDuration;
52:     lengthOf(act[j]["process2"]) == job[j].durations2;
53:     lengthOf(act[j]["load2"])    == loadDuration;
54:     lengthOf(act[j]["unloadS"])  == loadDuration;
55:   };
56:
57:   // precedence
58:   forall(j in Jobs)
59:     forall(ordered t1, t2 in Tasks)
60:       endBeforeStart(act[j][t1], act[j][t2]);
61:
62:   // no-overlap on machines
63:   forall (m in Machines) {
64:     noOverlap( append(
65:               all(j in Jobs: job[j].machine1==m) act[j]["process1"],
66:               all(j in Jobs: job[j].machine2==m) act[j]["process2"])
67:            );
68:   }
69:
70:    // state constraints
71:    forall(j in Jobs) {
72:      alwaysEqual(trolleyPosition, act[j]["loadA"],   Index["areaA"]);
73:      alwaysEqual(trolleyPosition, act[j]["unload1"], Index[job[j].machine1]);
74:      alwaysEqual(trolleyPosition, act[j]["load1"],   Index[job[j].machine1]);
75:      alwaysEqual(trolleyPosition, act[j]["unload2"], Index[job[j].machine2]);
76:      alwaysEqual(trolleyPosition, act[j]["load2"],   Index[job[j].machine2]);
77:      alwaysEqual(trolleyPosition, act[j]["unloadS"], Index["areaS"]);
78:    };
79: };

/* ------------------------ end core mathematics section --------------------------- */


/* ------------------------ begin ticdat output section ---------------------------- */
include "ticdat_jobs_output.mod";

execute {

   writeOutputToFile();
}
/* ------------------------ end ticdat output section ------------------------------ */