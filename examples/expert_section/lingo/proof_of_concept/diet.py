#  A Python programming example of interfacing with LINGO API.
import os,subprocess

assert os.environ["TICDAT_LINGO_PATH"] and os.path.isfile(os.environ["TICDAT_LINGO_PATH"]), "TICDAT_LINGO_PATH must be set"

# Run the script
commands = ["TAKE diet.lng",
            "GO",
            "QUIT"]
with open("run.ltf", "w") as f:
    f.write("\n".join(commands))

try:
    d = subprocess.check_output([os.environ["TICDAT_LINGO_PATH"],"run.ltf"])
except subprocess.CalledProcessError as err:
    raise Exception(err)

assert os.path.isfile("OBJ.dat"), "OBJ.dat not created"

with open("OBJ.dat") as f:
    assert f.read().strip() == "11.82886"


