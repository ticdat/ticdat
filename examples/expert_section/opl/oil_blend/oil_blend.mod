/*********************************************
 * OPL oil_blend problem organized into tabular, ticdat compliant format
 * See https://goo.gl/kqXmQE for reference problem and sample data.
 *********************************************/

/* creates inp_oil, inp_gas, inp_parameters
include "ticdat_oil_blend.mod";

/* use the inp_ data to populate the original data structures */

{string} Gasolines = {n | <n,d,s,o,l> in inp_gas};
{string} Oils = {n | <n,s,p,o,l> in inp_oil};
tuple gasType {
  float demand;
  float price;
  float octane;
  float lead;
}

tuple oilType {
  float capacity;
  float price;
  float octane;
  float lead;
}

/* next 4 lines are probably wrong - please review */
gasType Gas[Gasolines] = ...;
oilType Oil[Oils] = ...;
float MaxProduction = ...;
float ProdCost = ...;

dvar float+ a[Gasolines];
dvar float+ Blend[Oils][Gasolines];


maximize
  sum( g in Gasolines , o in Oils )
    (Gas[g].price - Oil[o].price - ProdCost) * Blend[o][g]
    - sum(g in Gasolines) a[g];
subject to {
  forall( g in Gasolines )
    ctDemand:
      sum( o in Oils )
        Blend[o][g] == Gas[g].demand + 10*a[g];
  forall( o in Oils )
    ctCapacity:
      sum( g in Gasolines )
        Blend[o][g] <= Oil[o].capacity;
  ctMaxProd:
    sum( o in Oils , g in Gasolines )
      Blend[o][g] <= MaxProduction;
  forall( g in Gasolines )
    ctOctane:
      sum( o in Oils )
        (Oil[o].octane - Gas[g].octane) * Blend[o][g] >= 0;
  forall( g in Gasolines )
    ctLead:
      sum( o in Oils )
        (Oil[o].lead - Gas[g].lead) * Blend[o][g] <= 0;
}

execute DISPLAY_REDUCED_COSTS{
  for( var g in Gasolines ) {
    writeln("a[",g,"].reducedCost = ",a[g].reducedCost);
  }
}


include "ticdat_oil_blend_output.mod";

