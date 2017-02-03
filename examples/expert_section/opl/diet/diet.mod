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
float objValue = sum(f in foodItems) foodCost[f] * purchase[f];
{parameters_type} parameters = {};
{buy_food_type} buy_food = {<f,purchase[f]> | f in foodItems: purchase[f] > 0};
float nutrition[<category,minN,maxN> in categories] = sum(<f,category,qty> in nutrition_quantities) qty * purchase[f];
{consume_nutrition_type} consume_nutrition = {<category,nutrition[<category,minN,maxN>]> | <category,minN,maxN> in categories};

execute {
  parameters.add("total_cost",objValue);
  writeln("parameters = ",parameters);
  writeln(buy_food);
  writeln(consume_nutrition);
}