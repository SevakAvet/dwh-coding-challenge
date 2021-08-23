from argparse import Namespace, ArgumentParser
from time import sleep
from typing import Callable, Optional, List

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, coalesce, last, row_number, lag, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, FloatType


class TransactionProcessor:
    def __init__(self, opts: Namespace):
        self.opts = opts
        self.spark = self._init_spark_session()

    def _init_spark_session(self):
        return SparkSession \
            .builder \
            .appName("Transaction processor") \
            .master("local[*]") \
            .config("spark.default.parallelism", 8) \
            .config("spark.sql.shuffle.partitions", 8) \
            .getOrCreate()

    def _read_files(self, input_dir: str, schema: StructType,
                    post_read: Optional[Callable[[DataFrame], DataFrame]] = None) -> DataFrame:
        """
        Read json files from specified input directory.

        :param input_dir: input directory
        :param schema: schema definition (StructType)
        :param post_read: a function to call on a dataframe after all data is read
        :return:
        """
        df = self.spark.read.option("multiline", "true").schema(schema).json(input_dir)
        if post_read:
            df = post_read(df)
        return df

    def _base_schema(self, data_schema: Callable[[bool], StructType]) -> StructType:
        """
        Create a schema definition for an input data.
        Field like id, op, ts are marked as non-nullable (= mandatory).
        data_schema is a callable that generates data schema for a particular dataset.
        It accepts boolean parameter (nullable), in case of 'data' field (almost) all columns in data_schema are mandatory,
        as it describes input record upon creation. 'set' field represents update operation, thus none of the fields is mandatory,
        e.g. only certain fields can be present in the row.

        :param data_schema: callable that generates data schema for a particular dataset
        :return: schema definition
        """
        return StructType([
            StructField("id", StringType(), False),
            StructField("op", StringType(), False),
            StructField("ts", LongType(), False),
            StructField("data", data_schema(False), True),
            StructField("set", data_schema(True), True),
        ])

    def _account_schema(self, nullable: bool = False) -> StructType:
        return StructType([
            StructField("account_id", StringType(), nullable),
            StructField("name", StringType(), nullable),
            StructField("address", StringType(), nullable),
            StructField("phone_number", StringType(), nullable),
            StructField("email", StringType(), nullable),
            StructField("savings_account_id", StringType(), True),
            StructField("card_id", StringType(), True),
        ])

    def _card_schema(self, nullable: bool = False) -> StructType:
        return StructType([
            StructField("card_id", StringType(), nullable),
            StructField("card_number", StringType(), nullable),
            StructField("credit_used", LongType(), nullable),
            StructField("monthly_limit", LongType(), nullable),
            StructField("status", StringType(), nullable),
        ])

    def _saving_account_schema(self, nullable: bool = False) -> StructType:
        return StructType([
            StructField("savings_account_id", StringType(), nullable),
            StructField("balance", LongType(), nullable),
            StructField("interest_rate_percent", FloatType(), nullable),
            StructField("status", StringType(), nullable),
        ])

    def _convert_timestamp(self, df: DataFrame, timestamp_column: str = "ts") -> DataFrame:
        """
        Converts timestamp column from a long type to timestamp. Input value is in milliseconds,
        thus it needs to be converted to seconds (divided by 1000).

        :param df: input dataframe
        :param timestamp_column: name of the timestamp column
        :return: dataframe with update timestamp column
        """
        return df.withColumn(timestamp_column, (col(timestamp_column) / 1000).cast(TimestampType()))

    def _read_accounts(self) -> DataFrame:
        schema = self._base_schema(data_schema=self._account_schema)
        return self._read_files(self.opts.input_accounts, schema, self._convert_timestamp)

    def _read_cards(self) -> DataFrame:
        schema = self._base_schema(data_schema=self._card_schema)
        return self._read_files(self.opts.input_cards, schema, self._convert_timestamp)

    def _read_saving_cards(self) -> DataFrame:
        schema = self._base_schema(data_schema=self._saving_account_schema)
        return self._read_files(self.opts.input_saving_accounts, schema, self._convert_timestamp)

    def _calculate_history(self, df: DataFrame) -> DataFrame:
        """
        Calculate historical dataset based on create/update operations.
        Each data column (all columns except "op", "id", "ts") is calculated as
        the last non-null value up to `ts` timestamp.

        :param df: input dataframe with all create/update operations
        :return: historical dataframe
        """

        base_columns = {"op", "id", "ts"}
        data_columns = list(set(df.schema.names) - base_columns)

        window_spec = Window.partitionBy("id").orderBy("ts")
        agg_columns = [last(column, ignorenulls=True).over(window_spec).alias(column)
                       for column in data_columns]

        return df.select([col(column) for column in base_columns] + agg_columns)

    def _flatten(self, df: DataFrame) -> DataFrame:
        """
        Flatten nested dataframe. Nested columns are "data" and "set", e.g.:
        before: op, id, ts, data.a, data.b, set.a, set.b
        after:  op, id, ts, a, b

        Final value of the column is based on data.{column} and set.{column} values, first non-null value is taken.

        :param df: nested dataframe
        :return: flattened dataframe
        """
        column_names = [col(column) for column in set(df.schema.names) - {"data", "set"}]
        nested_column_names = [coalesce(col(f"data.{column}"), f"set.{column}").alias(column)
                               for column in df.schema["data"].dataType.names]

        return df.select(column_names + nested_column_names)

    def _denormalized_join(self, accounts: DataFrame, cards: DataFrame, saving_cards: DataFrame) -> DataFrame:
        """
        Join accounts, cards, saving cards dataframes
        :param accounts: accounts dataframe
        :param cards: cards dataframe
        :param saving_cards: saving cards dataframe
        :return: denormalized joined table (accounts, cards, saving cards)
        """

        def join(df: DataFrame, other_df: DataFrame,
                 df_alias: str, other_df_alias: str,
                 on: str, how: str = "left") -> DataFrame:
            """
            Join base dataframe (accounts) with either of cards/saving cards.
            Left join is performed by default as foreign keys to cards/saving cards might be missing.
            There might be more than one record matching to a single account record.

            Deduplication is done in a following way:
            the last card/saving card record, that happened not later than account record is chosen.

            :param df: accounts dataframe
            :param other_df: either of cards/saving cards
            :param df_alias: "accounts"
            :param other_df_alias: alias of other_df
            :param on: which column to join on
            :param how: type of a join (inner, left, right), left is default.
            :return: df joined with other_df
            """

            base_df_ts = col(f"{df_alias}.ts")
            other_df_ts = col(f"{other_df_alias}.ts")
            window_spec = Window.partitionBy(on, base_df_ts).orderBy(other_df_ts.desc())

            return df.join(other_df, how=how, on=on) \
                .filter(other_df_ts.isNull() | (other_df_ts <= base_df_ts)) \
                .withColumn("rn", row_number().over(window_spec)) \
                .filter(col("rn") == 1) \
                .drop("rn")

        accounts_with_saving_cards = join(accounts, saving_cards, "accounts", "saving_cards", "savings_account_id")
        return join(accounts_with_saving_cards, cards, "accounts", "cards", "card_id")

    def _find_transactions(self, df: DataFrame, transaction_column: str, status: Optional[List[str]] = None) -> DataFrame:
        """
        Find all transactions.
        Transaction is defined as an activity which change the value in 'transaction_column'.
        Note, transactions can optionally be calculated for cards/accounts in specific 'status' only.

        :param df: input dataframe
        :param transaction_column: column that defines transaction.
        :param status: list of statuses to filter by.
        :return: dataframe with all transactions.
        """
        if status:
            df = df.filter(col("status").isin(status))

        window_spec = Window.partitionBy("id").orderBy("ts")
        return df \
            .withColumn("transaction", col(transaction_column) - lag(transaction_column, 1).over(window_spec)) \
            .filter(col("transaction") != lit(0)) \
            .select("ts", "transaction")

    def run(self):
        accounts = self._read_accounts()
        cards = self._read_cards()
        saving_cards = self._read_saving_cards()

        flattened_accounts = self._flatten(accounts)
        flattened_cards = self._flatten(cards)
        flattened_saving_cards = self._flatten(saving_cards)

        accounts_history = self._calculate_history(flattened_accounts).alias("accounts")
        cards_history = self._calculate_history(flattened_cards).alias("cards")
        saving_cards_history = self._calculate_history(flattened_saving_cards).alias("saving_cards")

        accounts_history.orderBy("accounts.ts").show(truncate=False)
        cards_history.orderBy("cards.ts").show(truncate=False)
        saving_cards_history.orderBy("saving_cards.ts").show(truncate=False)

        denormalized = self._denormalized_join(accounts_history, cards_history, saving_cards_history)
        denormalized.orderBy("accounts.ts").show(truncate=False)

        card_transactions = self._find_transactions(cards_history, "credit_used")
        active_card_transactions = self._find_transactions(cards_history, "credit_used", ["ACTIVE"])
        saving_card_transactions = self._find_transactions(saving_cards_history, "balance")
        active_saving_card_transactions = self._find_transactions(saving_cards_history, "balance", ["ACTIVE"])

        print(f"Card transactions: {card_transactions.count()}")
        card_transactions.orderBy("ts").show(truncate=False)

        print(f"Active card transactions: {active_card_transactions.count()}")
        active_card_transactions.orderBy("ts").show(truncate=False)

        print(f"Saving card transactions: {saving_card_transactions.count()}")
        saving_card_transactions.orderBy("ts").show(truncate=False)

        print(f"Active saving card transactions: {active_saving_card_transactions.count()}")
        active_saving_card_transactions.orderBy("ts").show(truncate=False)

        sleep(self.opts.wait_for)

def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument("--input-accounts", required=False, help="path to an input directory with account jsons",
                        default="../../data/accounts/")
    parser.add_argument("--input-cards", required=False, help="path to an input directory with cards jsons",
                        default="../../data/cards/")
    parser.add_argument("--input-saving-accounts", required=False, help="path to an input directory with cards jsons",
                        default="../../data/savings_accounts/")
    parser.add_argument("--wait-for", required=False, default=0, type=int, help="For how long to sleep before quitting")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    processor = TransactionProcessor(args)
    processor.run()


if __name__ == '__main__':
    main()
