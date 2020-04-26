To create the sample data files, do the following.

```
import dietdata
dietdata.input_schema.xls.write_file(dietdata.dat, "diet.xls")
dietdata.input_schema.sql.write_db_data(dietdata.dat, "diet.sql")
```