import ticdat.utils as utils
from ticdat.utils import verify

try:
    import docplex.mp.model as cplex
except Exception as e:
    cplex = "docplex"

try:
    import gurobipy as gurobi
except Exception as e:
    gurobi = "gurobipy"

try:
    import xpress
except:
    xpress = "xpress"

class Model(object):
    """
    Mixed Integer Programming modeling object. Provides pass-through functionality to one
    of the three main commercial MIP Python APIs (CPLEX, Gurobi, and XPRESS).
    """
    def __init__(self, model_type = 'gurobi', model_name = "model"):
        """
        or another
        :param model_type: either gurobi, cplex or xpress
        :return: a Model object that encapsulates the appropriate engine model object
        """
        engines = {"gurobi":gurobi, "cplex":cplex, "xpress":xpress}
        verify(model_type in engines,
               "engine_type needs to be one of 'gurobi', cplex', 'xpress'")
        verify(not utils.stringish(engines[model_type]),
               "You need to have the %s package installed to build this model type."%
               engines[model_type])
        self._core_model = getattr(engines[model_type],
                            {"gurobi":"Model", "cplex":"Model","xpress":"problem"}[model_type])(model_name)
        self._model_type = model_type
        self._sum = ({"gurobi":lambda : gurobi.quicksum, "cplex": lambda : self.core_model.sum,
                     "xpress":lambda : xpress.Sum}[model_type])()

    @property
    def core_model(self):
        return self._core_model

    @property
    def model_type(self):
        return self._model_type

    def add_var(self, lb=0, ub=float("inf"), type="continuous", name=""):
        """
        Add a variable to the model.
        :param lb: The lower bound of the variable.
        :param ub: The upper bound of the variable.
        :param type: either 'binary' or 'continuous'
        :param name: The name of the variable. (Ignored if falsey).
        :return: The variable object associated with the model_type engine API
        """
        verify(type in ["continuous", "binary"], "type needs to be 'continuous' or 'binary'")
        verify(utils.numericish(lb) and utils.numericish(ub), "lb, ub need to be numbers")
        verify(ub>=lb, "lb cannot be bigger than ub")
        verify(lb < float("inf"), "lb cannot be positive infinity")
        verify(ub > -float("inf"), "ub cannot be negative infinity")
        if type == "binary":
            ub = 1 if ub == float("inf") else ub
            verify(lb in [0,1] and ub in [0,1], "lb,ub need to be 0 or 1 when type = 'binary'")
        name_dict = {"name":name} if name else {}
        if self.model_type == "gurobi":
            vtype = {"continuous":gurobi.GRB.CONTINUOUS, "binary":gurobi.GRB.BINARY}[type]
            rtn =  self.core_model.addVar(lb=lb, ub=ub, vtype=vtype, **name_dict)
            return rtn
        if self.model_type == "cplex":
            if type == "continuous":
                return self.core_model.continuous_var(lb=lb, ub=ub, **name_dict)
            rtn = self.core_model.binary_var(**name_dict)
            rhs = ub if ub == 0 else (lb if lb == 1 else None)
            if utils.numericish(rhs):
                self.core_model.add_constraint(rtn == rhs)
            return rtn
        if self.model_type == "xpress":
            vtype = {"continuous":xpress.continuous, "binary":xpress.binary}[type]
            rtn = xpress.var(lb=lb, ub=ub, vartype=vtype,  **name_dict)
            self.core_model.addVariable(rtn)
            return rtn

    def add_constraint(self, constraint, name=""):
        """
        Add a constraint to the model.
        :param constraint: A constraint created by via linear or quadratic combination of
                           variables and numbers. Be sure to use Model.sum for summing
                           over iterables.
        :param name: The name of the constraint. Ignored if falsey.
        :return: The constraint object associated with the model_type engine API
        """
        if self.model_type == "gurobi":
            return self.core_model.addConstr(constraint, **({"name":name} if name else {}))
        if self.model_type == "cplex":
            return self.core_model.add_constraint(constraint,
                                                  **({"ctname":name} if name else {}))
        if self.model_type == "xpress":
            rtn = xpress.constraint(constraint, **({"name":name} if name else {}))
            self.core_model.addConstraint(rtn)
            return rtn

    def set_objective(self, expression, sense="minimize"):
        """
        Set the objective for the model.
        :param expression: A linear or quadratic combination of variables and numbers.
                           Be sure to use Model.sum for summing over iterables.
        :param sense: Either 'minimize' or 'maximize'
        :return: None
        """
        verify(sense in ["maximize", "minimize"],
               "sense needs to be 'maximize' or 'minimize")
        if self.model_type == "gurobi":
            self.core_model.setObjective(expression, sense=
                {"maximize":gurobi.GRB.MAXIMIZE, "minimize":gurobi.GRB.MINIMIZE}[sense])
        if self.model_type == "cplex":
            ({"maximize":self.core_model.maximize,
              "minimize":self.core_model.minimize}[sense])(expression)
        if self.model_type == "xpress":
            self.core_model.setObjective(expression, sense=
                {"maximize":xpress.maximize, "minimize":xpress.minimize}[sense])

    def set_parameters(self, **kwargs):
        """
        Set one or more parameters in the core model
        :param kwargs: A mapping of parameter keyword to parameter value.
                       The keywords need to be on the list of known parameters, as follows.
                       MIP_Gap : set the MIP optimization tolerance
        :return: None
        """
        known_parameters = ("MIP_Gap",)
        for k,v in kwargs.items():
            verify(k in known_parameters,
                   "set_parameter does not yet know how to set %s.\n"%k +
                   "The list of known parameters is %s\n"%list(known_parameters) +
                   "Feel free to set this parameter directly using core_model.")
            if k == "MIP_Gap":
                verify(self.model_type != "xpress",
                       "MIP_Gap parameter not yet implemented for xpress.")
                if self.model_type == "gurobi":
                    self.core_model.Params.MIPGap = v
                elif self.model_type == "cplex":
                    self.core_model.parameters.mip.tolerances.mipgap = v

    def optimize(self, *args, **kwargs):
        """
        Optimize the model.
        :param args: Optional engine-specific arguments to pass to the model_type API
        :param kwargs: Optional engine-specific arguments to pass to the model_type API
        :return: True if the model solves successfully, False otherwise.
        """
        if self.model_type == "gurobi":
            self.core_model.optimize(*args, **kwargs)
            if ((self.core_model.status == gurobi.GRB.OPTIMAL) or
                ((self.core_model.status in [gurobi.GRB.INTERRUPTED,
                    gurobi.GRB.TIME_LIMIT, gurobi.GRB.ITERATION_LIMIT]) and
                 (all(hasattr(var, "x") for var in self.core_model.getVars())))):
                return True
        if self.model_type == "cplex":
            rtn = self.core_model.solve(*args, **kwargs)
            if rtn :
                self._cplex_soln = self.core_model.solution
            return rtn
        if self.model_type == "xpress":
            self.core_model.solve(*args, **kwargs)
            return self.core_model.getProbStatus() in [xpress.lp_optimal, xpress.mip_optimal]

    def get_solution_value(self, var):
        """
        Get the value for a variable in the solution. Only call after a successful optimize() call.
        :param var: The variable returned from a pervious call to add_var()
        :return: The value of this variable in the optimal solution.
        """
        if self.model_type == "gurobi":
           return var.x
        if self.model_type == "cplex":
           return self._cplex_soln.get_value(var)
        if self.model_type == "xpress":
            return self.core_model.getSolution(var)

    @property
    def sum(self):
        return self._sum









