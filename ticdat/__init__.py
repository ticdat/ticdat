"""
ticdat is a Python package that provides lightweight, ORM style functionality around either a dict-of-dicts or
pandas.DataFrame representation of tables. Although ticDat was specifically designed with Mixed Integer Programming
data sets in mind, it can be used for rapidly developing a wide variety of mathematical engines. It facilitates
creating one definition of your input data schema and one solve module, and reusing this same code, unchanged,
on data from different sources. This "separation of model from data" enables a user to move easily from toy,
hard coded data to larger, more realistic data sets.
The ticDat library is distributed under the BSD2 open source license.
"""

from ticdat.ticdatfactory import TicDatFactory, freeze_me
from ticdat.utils import Sloc, LogFile, Progress, Slicer, standard_main, \
                         gurobi_env, ampl_format, verify, faster_df_apply
from ticdat.opl import opl_run, create_opl_mod_text, create_opl_mod_output_text
from ticdat.model import Model
from ticdat.pandatfactory import PanDatFactory
__version__ = '0.2.24'
__all__ = ["TicDatFactory", "PanDatFactory", "freeze_me", "LogFile", "Sloc", "Slicer", "Progress", "standard_main",
           "Model", "opl_run", "create_opl_mod_text", "create_opl_mod_output_text", "ampl_format",
           "gurobi_env", "verify", "faster_df_apply"]
