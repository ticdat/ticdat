from ticdat.utils import verify
import os

def opl_run(mod_file, input_tdf, input_dat, soln_tdf):
    """
    solve an optimization problem using an OPL .mod file
    :param mod_file: An OPL .mod file.
    :param input_tdf: A TicDatFactory defining the input schema
    :param input_dat: A TicDat object consistent with input_tdf
    :param soln_tdf: A TicDatFactory defining the solution schema
    :return: a TicDat object consistent with soln_tdf, or None if no solution found
    """
    verify(os.path.isfile(mod_file), "mod_file %s is not a valid file."%mod_file)
    msg  = []
    verify(input_tdf.good_tic_dat_object(input_dat, msg.append),
           "tic_dat not a good object for the input_tdf factory : %s"%"\n".join(msg))
    verify(False, "!!!!Under Construction!!!!")