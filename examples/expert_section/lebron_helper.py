# Example code for people that want to generate data integrity reports in table format from the command
# line. There is more than one way to tabluarize data integrity failures, and reporting data integrity failures
# from the command line goes  a bit against the grain of expected usage of ticdat. It is for these reasons that
# functionality like this is not part of ticdat proper. That said, this example code will work with any ticdat-consitent
# package playing the role of lebron. The lebron package is itself not provided - it is simply a placeholder.
# This example code should work if lebron.input_schema is a TicDatFactory or PanDatFactory.

from ticdat import PanDatFactory, standard_main
import lebron
input_schema = lebron.input_schema.clone(clone_factory=PanDatFactory) # requires 0.2.20.4 or later.
from collections import defaultdict
_pks = {t: pks for t, (pks, dfs) in input_schema.schema().items()}
_longest_pk =  max(len(pks) for pks in _pks.values())
_fld_names = [f"PK Field {_ + 1}" for _ in range(_longest_pk)]
solution_schema = PanDatFactory(duplicate_rows=[["Table Name"] + _fld_names, []],
                                data_type_failures=[["Table Name", "Field Name"] + _fld_names, []],
                                data_row_failures=[["Table Name", "Predicate Name"] + _fld_names, []],
                                foreign_key_failures =[["Native Table", "Foreign Table", "Mapping"] + _fld_names, []])

def solve(dat):
    dups = input_schema.find_duplicates(dat)
    duplicate_rows = defaultdict(list)
    for table, dup_df in dups.items():
        for row in dup_df.itertuples(index=False):
            duplicate_rows["Table Name"].append(table)
            for f, c in zip(_fld_names, row[:len(_pks[table])]):
                duplicate_rows[f].append(c)
            for i in range(len(_pks[table]), _longest_pk):
                duplicate_rows[_fld_names[i]].append(None)
    if duplicate_rows:
        return solution_schema.PanDat(duplicate_rows=duplicate_rows)

    dt_fails = input_schema.find_data_type_failures(dat)
    data_type_failures = defaultdict(list)
    for (table, field), dt_fail_df in dt_fails.items():
        for row in dt_fail_df.itertuples(index=False):
            data_type_failures["Table Name"].append(table)
            data_type_failures["Field Name"].append(field)
            for f, c in zip(_fld_names, row[:len(_pks[table])]):
                data_type_failures[f].append(c)
            for i in range(len(_pks[table]), _longest_pk):
                data_type_failures[_fld_names[i]].append(None)
    if data_type_failures:
        return solution_schema.PanDat(data_type_failures=data_type_failures)

    dr_fails = input_schema.find_data_row_failures(dat)
    data_row_failures = defaultdict(list)
    for (table, predicate), dr_fail_df in dr_fails.items():
        for row in dr_fail_df.itertuples(index=False):
            data_row_failures["Table Name"].append(table)
            data_row_failures["Predicate Name"].append(predicate)
            for f, c in zip(_fld_names, row[:len(_pks[table])]):
                data_row_failures[f].append(c)
            for i in range(len(_pks[table]), _longest_pk):
                data_row_failures[_fld_names[i]].append(None)
    if data_row_failures:
        return solution_schema.PanDat(data_row_failures=data_row_failures)

    fk_fails = input_schema.find_foreign_key_failures(dat, verbosity="Low")
    foreign_key_failures = defaultdict(list)
    for (native_table, foreign_table, mapping), fk_fail_df in fk_fails.items():
        for row in fk_fail_df.itertuples(index=False):
            foreign_key_failures["Native Table"].append(native_table)
            foreign_key_failures["Foreign Table"].append(foreign_table)
            foreign_key_failures["Mapping"].append(str(mapping))
            for f, c in zip(_fld_names, row[:len(_pks[native_table])]):
                foreign_key_failures[f].append(c)
            for i in range(len(_pks[native_table]), _longest_pk):
                foreign_key_failures[_fld_names[i]].append(None)
    if foreign_key_failures:
        return solution_schema.PanDat(foreign_key_failures=foreign_key_failures)
    print("\nlebron_helper won't create a solution, because there are no basic data integrity problems.\n")
    print("Go ahead and run lebron on this input data.\n")

if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)