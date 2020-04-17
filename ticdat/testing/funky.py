from ticdat import TicDatFactory
solution_schema = input_schema = TicDatFactory(table=[['field'],[]])
def solve(dat):
    return dat
def an_action(dat):
    dat.table['a']={}
    return dat
def another_action(dat, sln):
    dat.table['e'] = sln.table['e'] = {}
    return {"dat": dat, "sln": sln}
