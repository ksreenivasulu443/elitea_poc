import os

def test_file_to_file_equality(spark):
    source_path = os.path.abspath("data/source.csv")
    target_path = os.path.abspath("data/target.csv")

    df_source = spark.read.option("header", True).csv(source_path)
    df_target = spark.read.option("header", True).csv(target_path)

    # Basic data quality check: data should match
    assert df_source.schema == df_target.schema, "Schemas do not match"
    assert df_source.subtract(df_target).count() == 0, "Source has extra rows"
    assert df_target.subtract(df_source).count() == 0, "Target has extra rows"
