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
    f.write("data diet.dat;\n")
    f.write("option solver \""+os.environ['TICDAT_CPLEX_AMPL_PATH']+'\";\n')
    f.write("solve;\n")
    f.write("display Buy > output.txt;\n")
    f.write("display Total_Cost > output.txt;\n")
    f.write("close output.txt;")

try:
    output = subprocess.check_output([os.environ['TICDAT_AMPL_PATH'], 'myscript.run'])
except subprocess.CalledProcessError as err:
    with open("err.txt", "w") as f:
        print err
        f.write(err)
        raise Exception("Error occured while running ampl, check err.txt for details")

assert os.path.isfile("output.txt"), "blah was not generated"
with open("output.txt") as f:
    assert f.read()[-9:-2] == '11.8289'

print "Output completed successfully, check output.txt or see results above"