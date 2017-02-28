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

  forall (<category,minNutrition,maxNutrition> in categories){
    sum(<f,category,qty> in nutrition_quantities) qty * purchase[f] >= minNutrition;
    sum(<f,category,qty> in nutrition_quantities) qty * purchase[f] <= maxNutrition;
  }

}
include "ticdat_diet_output.mod";
float nutrition_consumed[c in categories] = sum(nq in nutrition_quantities: nq.category == c.name) nq.qty * purchase[nq.food];

float total_cost = sum(f in foodItems) foodCost[f] * purchase[f];
execute {
   for (var f in foodItems){
      buy_food.add(f,purchase[f]);   
   }
   
   for (var c in categories){
      consume_nutrition.add(c.name,nutrition_consumed[c]);     
   }

   parameters.add("Total Cost",total_cost);
   var ofile = new IloOplOutputFile("results.dat");
   ofile.writeln("parameters = ",parameters);
   ofile.writeln("buy_food = ", buy_food);
   ofile.writeln("consume_nutrition = ",consume_nutrition);
}  