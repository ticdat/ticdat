## ticdat examples

When building an engine with `ticdat`, the first decision to make is how to represent the input data
tables in Python. There are two choices.

 1. dict-of-dicts. Some people find this convenient for the idioms associated with Mixed Integer Programming. 
 In this case, use `TicDatFactory`.
 1. `pandas` is an incredibly popular and powerful data library. If you want a 
 `DataFrame` to be the default representation of a table, then use `PanDatFactory`.
 
 Here is a quick guide to examples for each approach.
 
 * `TicDatFactory`. 
   * See the `diet.py`, `netflow.py`, `fantop.py`, `metrorail.py` and `workforce.py` files
 in the `gurobipy` directory.
 * `PanDatFactory`. 
   * For a MIP example, see `gurobipy/netflow/netflow_pd.py`. 
   * The `iris/iris.py` example shows `PanDatFactory` connecting through to the widely used `sklearn` package 
   for machine learning.
   * The `datetime/simple_datetime_solver.py` engine demonstrates how `PanDatFactory` can validate and munge timestamp
   data.
   
   Bear in mind that you don't need to spend a lot of mental energy choosing between `TicDatFactory` and 
   `PanDatFactory`. `ticdat` also makes it very easy to convert the data tables between `DataFrame` and  
   dict-of-dicts. See `TicDatFactory.copy_to_pandas` and `PanDatFactory.copy_to_tic_dat`.
   
   The `beginner_ticdat` directory contains the files relevant to the 
   [beginner ticdat](https://github.com/ticdat/ticdat/wiki/1-Beginner-ticdat-intro) training material.
   
   The `expert_section` directory contains a wide variety of additional examples. This is the place to go if you're 
   curious about other MIP engines, like `docplex` and `pulp`. Since `ticdat` is just a MIP facilitator, 
   those examples are very similar to `gurobipy`. 
   
   The `expert_section/amplpy` directory demonstrates the `ticdat`  convenience functions that can help you
   use the AMPL programming language within a Python based engine. Unfortunately, such an approach is not currently 
   recommended due to the primitive state of the AMPL licensing technology.