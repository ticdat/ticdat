"""
Simple logging override for ticdat
PEP8
"""

class _Logfile(object) :
    def __init__(self, path):
        self._f = open(path, "w")
    def write(self, *args, **kwargs):
        self._f.write(*args, **kwargs)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    def close(self):
        self._f.close()
    def long_sequence(self, seq, formatter = lambda _ : "%s"%_, max_write = 10) :
        if len(seq) > max_write:
          self.write("(Showing first %s entries out of %s in total)\n"%(max_write, len(seq)))
        for b in list(seq)[:max_write]:
            self.write(formatter(b) + "\n")
        self.write("\n")

class _GurobiCallBackAndLog(object):
    def __init__(self, file_path = None, call_back_handler = lambda l,u : True):
        self.call_back_handler = call_back_handler
        if file_path:
            self._log_file = _Logfile(file_path)
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    def close(self):
        if hasattr(self, "_log_file"):
            self._log_file.close()
    def create_gurobi_callback(self, node_cnt_per_call = 10):
        import gurobipy as gu
        assert callable(self.call_back_handler)
        last_node = [0]
        def gurobi_call_back(model, where) :
            if where == gu.GRB.callback.MIP:
                nodecnt = model.cbGet(gu.GRB.callback.MIP_NODCNT)
                if nodecnt - last_node[0] >= node_cnt_per_call:
                    last_node[0] = nodecnt
                    ub = model.cbGet(gu.GRB.callback.MIP_OBJBST)
                    lb = model.cbGet(gu.GRB.callback.MIP_OBJBND)
                    keep_going = self.call_back_handler(lb, ub)
                    if not keep_going :
                        model.terminate()
            elif where == gu.GRB.callback.MESSAGE and hasattr(self, "_log_file"):
                msg = model.cbGet(gu.GRB.callback.MSG_STRING)
                self._log_file.write(msg)
        return gurobi_call_back

class LogFactory(object):
    @property
    def LogFile(self):
        return _Logfile
    @property
    def GurobiCallBackAndLog(self):
        return _GurobiCallBackAndLog

