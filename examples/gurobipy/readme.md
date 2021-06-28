Repository for `gurobipy` `ticdat` examples. The bulk of these examples use `TicDatFactory`, 
and thus render input tables in dict-of-dicts format. `netflow_pd` demonstrates an engine that accepts
input tables as `pandas.DataFrame` objects, and thus uses `PanDatFactory`.

 * diet - standard diet example.
 * netflow - standard multicommodity network flow.
 * fantop - fantasy football example. See [here](https://www.linkedin.com/pulse/fantasy-footballers-nerds-too-peter-cacioppi/) for more information.
 * metrorail - optimize the number of times you visit the fare kiosk when riding the metro. See [here](https://www.linkedin.com/pulse/miami-metrorail-meets-python-peter-cacioppi/) for more information. This is a good example of constructing a comprehensive solution from a series of sub-problems.
 * workforce - Demonstrates lexicographical optimization for a workforce scheduling problem.
 * simplest_examples - examples shrunk down by removing the data integrity checking functionality. 
