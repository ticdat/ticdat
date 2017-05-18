print "This script configures a lingorun_path.txt file in order to enable use with runlingo."
print "You need to run this script from the directory containing runlingo "
print "(or runlingo.exe on windows)"
print "This is a one-time post 'pip install' setup step."

import ticdat.testing.ticdattestutils as tdu
tdu.configure_runlingo_path()

