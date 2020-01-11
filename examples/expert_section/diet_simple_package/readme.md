#### This directory implements a simple pacakge named diet_simple_package.

You can zip this up and upload it directly to Enframe in lieu of a `diet.py` file.

Note that the name of the package is `diet_simple_package`, because that's the name of the folder.

Of course this example is so small that splitting it into seperate files is a bit silly. But some
engines will be large enough that putting all the code into one file will be troublesome, and a package
is the standard Python tool for multi-file solutions. 

Don't forget to use the -m argument when running from the command line. E.g.

` python -m diet_simple_package -i diet_sample_data -o diet_solution_data`