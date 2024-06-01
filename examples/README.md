## ticdat examples

The examples files here are meant to demonstrate `ticdat` functionality, and to 
illustrate the principles of 
[Tidy, Tested, Safe](https://github.com/ticdat/tidy_tested_safe/wiki/What-is-Tidy,-Tested,-Safe%3F). 
As a rule, I strongly recommend you build your engines as properly versioned Python packages, as opposed to 
the free standing .py files you see here. Any one of these example files could trivially be
organized into a dedicated repository [consistent](https://docs.python-guide.org/writing/structure/) with Python best 
practices. For example, see 
[tts_diet](https://github.com/ticdat/tts_diet), 
[tts_netflow_a](https://github.com/ticdat/tts_netflow_a) and 
[tts_netflow_b](https://github.com/ticdat/tts_netflow_b).

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
   
The `xpress_model` examples are a convenient set of examples that access the `xpress` package indirectly, via
the `ticdat.Model` pacakge. These models can also be directed to use `docplex` or `gurobipy` based on an optional
parameter.