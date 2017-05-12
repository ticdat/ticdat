#  A Python programming example of interfacing with LINGO API.
from pyLingo import lingo, const
import numpy


with open("/opt/opalytics/lenticular/sams-stuff/lingo/install/lndlng16.lic") as f:
    l = f.read()

fptrs = numpy.array([l],dtype=numpy.str)
fptrs2 = numpy.array([0.],dtype=numpy.double)
try:
    pEnv = lingo.pyLScreateEnvLicenseLng(fptrs, fptrs2)
except:
    print ("something went wrong")

assert pEnv, "cannot create LINGO environment!"

def check_error(errorcode):
    assert errorcode == const.LSERR_NO_ERROR_LNG, "errorcode = " + errorcode


check_error(lingo.pyLSopenLogFileLng(pEnv,'diet.log'))

# pointers
pnPointersNow = numpy.array([0],dtype=numpy.int32)

# AQL_1 = N.array([AQL],dtype=N.double)


n = numpy.array([-1.0],dtype=numpy.double)
check_error(lingo.pyLSsetDouPointerLng(pEnv, n, pnPointersNow))

x = numpy.array([-1.0,-1.0,-1.0,-1.0],dtype=numpy.double)
check_error(lingo.pyLSsetDouPointerLng(pEnv, x, pnPointersNow))

# Run the script
commands = [ "SET ECHOIN 1", "TAKE Diet2.lng" ,"GO", "QUIT"]
# commands = [ "SET ECHOIN 1",
#             "TAKE DietSchema.lng",
#             "TAKE DietIn.lng",
#             "TAKE Diet.lng",
#             "TAKE DietOut.lng",
#             "GO",
#             "QUIT"]
cScript = "\n".join(commands)+"\n";
check_error(lingo.pyLSexecuteScriptLng(pEnv, cScript))

# Close the log file
check_error(lingo.pyLScloseLogFileLng(pEnv))

# delete Lingo enviroment object
check_error(lingo.pyLSdeleteEnvLng(pEnv))
