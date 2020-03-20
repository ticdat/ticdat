# example problem of how to create more sophisticated data row objects without monkey chaining
import diet # put the ticdat diet example in this directory
import ticdat
from ticdat.utils import verify
class CustomizedTicDatRow(object):
    '''
    general utility class that allows you to effectively roll-your-own TicDatRow objects. I would think
    this is not really needed because monkey-chaining is fine for the rare use cases where you want this, but
    if you really don't want to monkey-chain than do something like this.
    '''
    def __init__(self, tic_dat_row):
        self._tic_dat_row = tic_dat_row
    # this is just encapsulation masquerading as inheritance. Can't really inherit from a tic_dat_row since its
    # not designed to be inherited-from. Note that
    def __getitem__(self, item):
        return self._tic_dat_row[item]
    def __setitem__(self, key, value):
        self._tic_dat_row[key] = value
    def keys(self):
        return self._tic_dat_row.keys()
    def values(self):
        return self._tic_dat_row.values()
    def items(self):
        return self._tic_dat_row.items()
    def __contains__(self, item):
        return item in self._tic_dat_row
    def __iter__(self):
        return iter(self._tic_dat_row)
    def __len__(self):
        return len(self.self._tic_dat_row)
    def __repr__(self):
        return "_ctd:" + {k:v for k,v in self.items()}.__repr__()

class CategoryRow(CustomizedTicDatRow):
    def __init__(self, tic_dat_row, fake_or_real="real"):
        verify(ticdat.utils.dictish(tic_dat_row) and
               set(tic_dat_row) == set(diet.input_schema.data_fields["categories"]),
               "the normal ticdat magic for row creation not enabled")
        super().__init__(tic_dat_row)
        self.fake_or_real = fake_or_real

class CategoryTable(ticdat.utils.FreezeableDict): # you could probably get away with just a normal dict, but I wouldn't.
    def __init__(self, *_args, **_kwargs):
        super().__init__(*_args, **_kwargs)
    def __setitem__(self, key, value):
        verify(not ticdat.utils.containerish(key), "inconsistent key length for category")
        return super().__setitem__(key, value if isinstance(value, CategoryRow) else CategoryRow(value))
    def __getitem__(self, item):
        return super().__getitem__(item)

class CustomizedDat(diet.input_schema.TicDat):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        old_categories = self.categories
        self.categories = CategoryTable()
        for k, row in old_categories.items():
            self.categories[k] = CategoryRow(row, fake_or_real="fake" if k == "fat" else "real")
    @staticmethod
    def create_from_normal(dat, freeze_it=False):
        assert diet.input_schema.good_tic_dat_object(dat)
        rtn = CustomizedDat(**{t:getattr(dat, t) for t in diet.input_schema.all_tables})
        if freeze_it:
            return diet.input_schema.freeze_me(rtn)
        assert diet.input_schema.good_tic_dat_object(rtn)
        return rtn

def monkey_chain_finisher(dat):
    # this is the way Pete would do it
    assert diet.input_schema.good_tic_dat_object(dat)
    for k, row in dat.categories.items():
        row.fake_or_real = "fake" if k == "fat" else "real"
    # its input data so just freeze it after you've monkey_chained whatever you need
    return diet.input_schema.freeze_me(dat)




