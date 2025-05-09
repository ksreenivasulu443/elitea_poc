import os

def test_file_to_file_equality(spark):
    elitea_poc = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    source_path =  os.path.join(elitea_poc, "data", "source.csv")
    target_path =  os.path.join(elitea_poc, "data", "target.csv")

    df_source = spark.read.option("header", True).csv(source_path)
    df_target = spark.read.option("header", True).csv(target_path)

    df_source.show()
    df_target.show()

    # Basic data quality check: data should match
    assert df_source.count() == df_target.count(), "count do not match"