import os, subprocess

if 'LINGO_PATH' not in os.environ:
    raise Exception("Please set the LINGO_PATH environment variable to be the location of runlingo")
assert os.path.isfile(os.environ['LINGO_PATH']), "runlingo not found"

output =''

try:
    output = subprocess.check_output(os.environ['LINGO_PATH', 'blend.lng'])
except subprocess.CalledProcessError as err:
    with open("err.txt", "w") as f:
        f.write(err)
        raise Exception("Error during runlingo, check err.txt for details")

with open("output.txt", "w") as f:
    f.write(output)

print output
print "Output completed successfully, check output.txt or see results above"
