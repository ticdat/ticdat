import os, subprocess

if 'TICDAT_AMPL_PATH' not in os.environ:
    raise Exception("Please set the TICDAT_AMPL_PATH environment variable to be the location of ampl")
assert os.path.isfile(os.environ['TICDAT_AMPL_PATH']), "ampl not found"

if 'TICDAT_CPLEX_AMPL_PATH' not in os.environ:
    raise Exception("Please set the TICDAT_CPLEX_AMPL_PATH environment variable to be the location of cplexamp")
assert os.path.isfile(os.environ['TICDAT_CPLEX_AMPL_PATH']), "cplexamp not found"

output =''

with open("myscript.run", "w") as f:
    f.write("model diet.mod;\n")
    f.write("data tic_diet.dat;\n")
    f.write("option solver \"" + os.environ['TICDAT_CPLEX_AMPL_PATH'] + "\";\n")
    f.write("solve;\n")
    f.write("option display_1col INFINITY;\n")  # Ugg formatting
    f.write("option display_width INFINITY;\n")
    f.write("option display_precision 0;\n")
    f.write("display Buy > results.dat;\n")
    f.write("display Total_Cost > results.dat;\n")

try:
    output = subprocess.check_output([os.environ['TICDAT_AMPL_PATH'], 'myscript.run'])
except subprocess.CalledProcessError as err:
    with open("err.txt", "w") as f:
        print err
        f.write(str(err))
        raise Exception("Error occured while running ampl, check err.txt for details")

assert os.path.isfile("results.dat"), "blah was not generated"
with open("results.dat") as f:
    assert f.read()[-19:-12] == '11.8288'

print "Output completed successfully, check output.txt or see results above"