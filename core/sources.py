from datetime import date

import pyspark.sql.types as T

cdc = {
    "employees": {
        "path": "s3://sprkcdc-bucket/dms/employees/employees/",
        "primary_keys": ["emp_no"],
        "partition_date": "",
        "cdc_start_date": date(2020, 11, 13),
        "schema": T.StructType()
        .add("emp_no", "integer")
        .add("birth_date", "date")
        .add("first_name", "string")
        .add("last_name", "string")
        .add("gender", "string")
        .add("hire_date", "date")
    }
}
