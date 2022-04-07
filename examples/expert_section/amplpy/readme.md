Repository for `amplpy` examples.

These examples use `PanDatFactory` to connect `amplpy` seamlessly with `pandas`. 

Note that these examples validate input data integrity with both AMPL and `ticdat` code. `ticdat` is used to make data 
integrity problems apparent to another Python programmer. AMPL is used to simply double check that the `ticdat` 
checks are comprehensive.  
 * diet - standard diet example. Easiest example.
 * netflow - standard multi-commodity network flow problem. Second easiest example.
 * fantop - fantasy football example. See [here](https://www.linkedin.com/pulse/fantasy-footballers-nerds-too-peter-cacioppi/) for more information. This is a good example of using `pandas` for pre and post solve data munging.
 * metrorail - optimize the number of times you visit the fare kiosk when riding the metro. See [here](https://www.linkedin.com/pulse/miami-metrorail-meets-python-peter-cacioppi/) for more information. This is a good example of constructing a comprehensive solution from a series of sub-problems.
 * workforce - Demonstrates lexicographical optimization for a workforce scheduling problem.
 * simplest_examples - examples shrunk down by removing the data integrity checking functionality. 
