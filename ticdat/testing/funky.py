from ticdat import TicDatFactory
solution_schema = input_schema = TicDatFactory(table=[['field'],[]])
def solve(dat):
    if "speak" in dat.table:
        return ("spoken", dat)
    if "fail" in dat.table:
        return ("failed message", None)
    return dat
