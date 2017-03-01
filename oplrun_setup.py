print "This script configures a oplrun_path.txt file in order to enable use with oplrun."
print "You need to run this script from the directory containing oplrun "
print "(or oplrun.exe on windows)"
print "This is a one-time post 'pip install' setup step."

import ticdat.testing.ticdattestutils as tdu
tdu.configure_oplrun_path()

