Repository for amplpy examples.

These examples use `PanDatFactory` to connect `amplpy` seamlessly with `pandas`. This is our recommended best practice for OR practitioners looking to combine optimization with Python.

 * diet - standard diet example. Easiest example.
 * netflow - standard multi-commodity network flow problem. Second easiest example.
 * fantop - fantasy football example. See [here](https://www.linkedin.com/pulse/fantasy-footballers-nerds-too-peter-cacioppi/) for more information.
 * metrorail - optimize the number of times you visit the fare kiosk when riding the metro. See [here](https://www.linkedin.com/pulse/miami-metrorail-meets-python-peter-cacioppi/) for more information.
 * simplest_examples - examples shrunk down by removing the data integrity checking functionality. Industrial engines are much improved by using this functionality, and I don't recommend skipping pre-condition checking for your engines. That said, most Python-MIP example codes don't include pre-condition checking of the input data, and for this reason I made comparable example codes to facilitate apples-to-apples comparisons.
