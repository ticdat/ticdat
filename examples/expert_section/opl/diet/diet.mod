/*********************************************
 * OPL 12.6.0.0 Model
 * Author: Joshua
 * Creation Date: Jan 19, 2017 at 6:14:30 PM
 *********************************************/
include "ticdat_diet.mod";

{string} foodItems = {f | <f,c> in foods};
float foodCost[foodItems] = [f: c | <f,c> in foods];

dvar float+ purchase[foodItems];

minimize
  sum(f in foodItems) foodCost[f] * purchase[f];

subject to {

  forall (<category,min_nutrition,max_nutrition> in categories){
    sum(<f,category,quantity> in nutrition_quantities) quantity * purchase[f] >= min_nutrition;
    sum(<f,category,quantity> in nutrition_quantities) quantity * purchase[f] <= max_nutrition;
  }

}