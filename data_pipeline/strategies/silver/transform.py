"""Silver transform strategy with normalization and type handling."""

import unicodedata

from pyspark.sql.functions import (
    col,
    initcap,
    lit,
    regexp_replace,
    to_date,
    trim,
    when,
)

from data_pipeline.shared.logger import StructuredLogger
from data_pipeline.strategies.interfaces import Strategy


class SilverTransform(Strategy):
    def __init__(self):
        """Initialize strategy logger."""
        self.logger = StructuredLogger("strategy-silver-transform")

    def treat_string_cols(self, df, cols):
        """Normalize text columns using trim and title-case transformation.

        Args:
            df: Input Spark DataFrame.
            cols: Column names to normalize.

        Returns:
            Transformed DataFrame.
        """
        for c in cols:
            df = df.withColumn(c, initcap(trim(col(c))))
        return df

    def treat_integer_cols(self, df, cols):
        """Validate and cast columns to integer values.

        Args:
            df: Input Spark DataFrame.
            cols: Column names expected to be integer.

        Returns:
            DataFrame with cleaned integer columns.
        """
        for c in cols:
            cleaned_col = trim(col(c).cast("string"))
            df = df.withColumn(
                c,
                when(cleaned_col.isNull() | (cleaned_col == ""), None)
                .when(cleaned_col.rlike(r"^-?\d+$"), cleaned_col.cast("long"))
                .otherwise(None),
            )
        return df

    def treat_float_cols(self, df, cols):
        """Validate and cast columns to floating-point values.

        Args:
            df: Input Spark DataFrame.
            cols: Column names expected to be floating-point.

        Returns:
            DataFrame with cleaned float columns.
        """
        for c in cols:
            cleaned_col = regexp_replace(trim(col(c).cast("string")), ",", ".")
            df = df.withColumn(
                c,
                when(cleaned_col.isNull() | (cleaned_col == ""), None)
                .when(
                    cleaned_col.rlike(r"^-?\d+(\.\d+)?$"),
                    cleaned_col.cast("double"),
                )
                .otherwise(None),
            )
        return df

    def normalize_columns_names(self, df):
        """Normalize DataFrame column names to lowercase snake case.

        Args:
            df: Input Spark DataFrame.

        Returns:
            DataFrame with normalized column names.
        """
        for column in df.columns:
            self.logger.info("Normalizing column", column=column)
            new_col = unicodedata.normalize("NFD", column)
            new_col = new_col.strip()
            new_col = new_col.replace(" ", "_")
            new_col = new_col.lower()
            df = df.withColumnRenamed(column, new_col)
            self.logger.info(
                "Column normalized",
                original_column=column,
                normalized_column=new_col,
            )
        return df

    def clean_postal_code(self, df, col_name):
        """Remove non-alphanumeric characters

        Args:
            df: Input Spark DataFrame.
            col_name: Postal code column name.

        Returns:
            DataFrame with cleaned postal code.
        """
        return df.withColumn(
            col_name, regexp_replace(col(col_name), "[^0-9A-Za-z]", "")
        )

    def deduplicate(self, df, subset_cols):
        """Drop duplicate rows based on business keys.

        Args:
            df: Input Spark DataFrame.
            subset_cols: Columns used as uniqueness keys.

        Returns:
            Deduplicated DataFrame.
        """
        return df.dropDuplicates(subset=subset_cols)

    def execute(self, df, context):
        """Run the complete silver transformation pipeline.

        Args:
            df: Input Spark DataFrame from bronze layer.
            context: Runtime context with execution metadata.

        Returns:
            Transformed Spark DataFrame ready for silver load.
        """
        self.logger.info("Silver transform started")
        df = self.normalize_columns_names(df)
        df = self.clean_postal_code(df, "postal_code")
        df = self.deduplicate(df, ["id"])
        string_columns = [
            "id",
            "name",
            "brewery_type",
            "address_1",
            "address_2",
            "address_3",
            "city",
            "postal_code",
            "state_province",
            "country",
            "phone",
            "website_url",
            "state",
            "street",
        ]
        df = self.treat_string_cols(df, string_columns)
        float_columns = ["longitude", "latitude"]
        df = self.treat_float_cols(df, float_columns)

        execution_date = context["execution_date"]
        df = df.withColumn("_execution_date", to_date(lit(execution_date)))

        df.show(truncate=False)
        self.logger.info(
            "Silver transform finished", execution_date=execution_date
        )

        return df
