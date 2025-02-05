### LLM best practices for ticdat and Foresta

Our experience is that the mainstream LLMs are aware of `ticdat` and tend to understand the 
basic mechanics of how to use the `dat` objects created by `TicDatFactory` and `PanDatFactory`.
This makes sense, since the latter is just `pandas.DataFrame` objects and the former just 
"dict-of-dicts". These two concepts are broadly used and well understood by the Python community 
and thus the LLMs are on solid footing when they translate `dat` objects to math modeling.

The `ticdat` template (which is also the template for the Foresta platform) is somewhat niche, and
thus the LLMs need a bit of coaching to generate Foresta ready applications. That said, our
experience is that simply providing some standard, boilerplate prompts alongside a plain
English description of the specific problem at hand tends to yield excellent results.

Our suggestion is thus to initiate your chat session with a single long prompt that follows the 
following format.  **Be advised** - this prompt assumes you are using `TicDatFactory`. To use
`PanDatFactory`, just replace `TicDat` with `PanDat` in the prompt below, and then copy
the `pan_dat_template.py` file into the prompt instead of the `tic_dat_template.py` file. 

> Please create a .py file to solve the following problem.
>  <fill in your plain English description of the problem>
> Please use ticdat as part of your solution. Please follow the template in the 
> code I am providing below. Your solution should fill in tables and fields for the 
> input data by completing the input_schema object. It should do the same for the 
> solution data by completing the solution_schema object. Your solution should also
> complete the solve function, which takes a dat object consistent with input_schema 
> as an argument. The solve function should return None if no feasible solution can
> be found, otherwise it should create a solution_schema.TicDat object, populate that 
> object with the solution report data, and then return that object.
>

If your input data requires parameters (for example, the number of centroids to locate for a 
center of gravity model), then you should also include the following text in your initial 
prompt, subsequent to the text above and preceding the template code that will end your prompt.

> Please note that the input data set needs parameters to control things like <fill in appropriately>.
> Please follow the standard ticdat protocol for input parameters.
>  - have an input table named parameters with one primary key field and one text field.
>  - have one line that calls input_schema.add_parameter for each parameter.
>  - have a line like full_params = input_schema.create_full_parameters_dict(dat) inside 
>    the solve function to read the parameters into a dictionary which can then be referenced
>    by the rest of the solve logic.

Finally, copy either `pan_dat_template.py`  or `tic_dat_template.py` into the prompt, 
depending on whether you want to use `DataFrame` objects or dict-of-dict objects to 
represent your input and solution tables. 




