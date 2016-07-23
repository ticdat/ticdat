print "This script configures a blank.accdb file in order to enable accdb file creation."
print "You need to run this script from a directory that contains an empty Access"
print "database file called blank.accdb."
print "This is a one-time post 'pip install' setup step."

import ticdat.testing.ticdattestutils as tdu
tdu.configure_blank_accdb()

