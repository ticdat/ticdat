#
# Perform KMeans-clustering on the Iris data set. Number of clusters can be controlled via an optional
# parameters table.
#
# Command line interface works like this
#    python iris.py -i sample_data -o solution_directory
#
from ticdat import PanDatFactory, standard_main
from sklearn.preprocessing import scale
from sklearn.cluster import KMeans

# ------------------------ define the input schema --------------------------------
_core_numeric_fields = ['Sepal Length', 'Sepal Width', 'Petal Length', 'Petal Width']
input_schema = PanDatFactory(parameters=[['Name'], ['Value']],
    iris=[[], _core_numeric_fields + ['Species']])

# the core data fields should be positive, non-infinite numbers
for fld in _core_numeric_fields:
    input_schema.set_data_type("iris", fld, inclusive_min=False, inclusive_max=False, min=0, max=float("inf"))
input_schema.set_data_type("iris", 'Species', number_allowed=False, strings_allowed='*')

# the number of clusters is our only parameter, but using a parameters table makes it easy to add more as needed
input_schema.add_parameter("Number of Clusters", default_value=4, inclusive_min=False, inclusive_max=False, min=0,
                                max=float("inf"), must_be_int=True)
# ---------------------------------------------------------------------------------

# ------------------------ define the output schema -------------------------------
solution_schema = PanDatFactory(iris=[[],['Sepal Length', 'Sepal Width', 'Petal Length', 'Petal Width', 'Species',
                                          'Cluster ID']])
# generally setting the data type not required for a report table, but for string fields, it can be helpful
solution_schema.set_data_type("iris", 'Species', number_allowed=False, strings_allowed='*')
# ---------------------------------------------------------------------------------

# ------------------------ create a solve function --------------------------------
def solve(dat):
    assert input_schema.good_pan_dat_object(dat)
    assert not input_schema.find_data_type_failures(dat)
    assert not input_schema.find_data_row_failures(dat)

    full_parameters = input_schema.create_full_parameters_dict(dat)

    # standardize data
    scaled_data = scale(dat.iris[_core_numeric_fields])

    # run K-means for optimal size
    kmeans = KMeans(n_clusters=full_parameters["Number of Clusters"], random_state=0, max_iter=1000, n_init=100).\
             fit(scaled_data)

    # attach clusterid's column back to original data
    result_iris = dat.iris.copy()
    result_iris["Cluster ID"]=kmeans.labels_
    return solution_schema.PanDat(iris=result_iris)
# ---------------------------------------------------------------------------------

# ------------------------ provide stand-alone functionality ----------------------
# when run from the command line, will read/write json/xls/csv/SQLite files
if __name__ == "__main__":
    standard_main(input_schema, solution_schema, solve)
# ---------------------------------------------------------------------------------
