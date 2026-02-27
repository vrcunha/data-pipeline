"""Test stubs for external dependencies not available in local environment."""

import sys
import types
from typing import Any


class _Expr:
    """Minimal expression stub used by fake Spark functions."""

    def __init__(self, value=None) -> Any:
        self.value = value

    def cast(self, _dtype) -> Any:
        """Mimic Spark cast API and keep chainability."""
        return self

    def isNull(self) -> Any:
        """Mimic Spark isNull API and keep chainability."""
        return self

    def rlike(self, _pattern) -> Any:
        """Mimic Spark rlike API and keep chainability."""
        return self

    def __eq__(self, _other) -> Any:
        """Mimic Spark column comparison expressions."""
        return self

    def __or__(self, _other) -> Any:
        """Mimic Spark boolean expression OR operator."""
        return self


class _WhenExpr:
    """Minimal `when` expression chain stub."""

    def when(self, _condition, _value) -> Any:
        """Mimic chained `when` API."""
        return self

    def otherwise(self, _value) -> Any:
        """Mimic terminal `otherwise` API."""
        return _Expr()


class _CountExpr:
    """Minimal count expression stub."""

    def alias(self, _name) -> Any:
        """Mimic Spark alias API."""
        return _Expr()


def _install_pyspark_stub() -> None:
    """Install lightweight `pyspark` stubs into `sys.modules`."""
    if "pyspark" in sys.modules:
        return

    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    types_mod = types.ModuleType("pyspark.sql.types")

    class SparkSession:  # pragma: no cover - stub only
        pass

    class DataFrame:  # pragma: no cover - stub only
        pass

    class StructField:
        def __init__(self, name, data_type, nullable) -> Any:
            self.name = name
            self.dataType = data_type
            self.nullable = nullable

    class StructType:
        def __init__(self, fields) -> Any:
            self.fields = fields

    class StringType:
        pass

    class DoubleType:
        pass

    class LongType:
        pass

    def col(name) -> Any:
        return _Expr(name)

    def trim(expr) -> Any:
        return expr

    def initcap(expr) -> Any:
        return expr

    def regexp_replace(expr, _pattern, _replacement) -> Any:
        return expr

    def lit(value) -> Any:
        return _Expr(value)

    def to_date(expr) -> Any:
        return expr

    def count(_expr) -> Any:
        return _CountExpr()

    def when(_condition, _value) -> Any:
        return _WhenExpr()

    def coalesce(*_exprs) -> Any:
        return _Expr()

    def concat_ws(_sep, *_exprs) -> Any:
        return _Expr()

    def sha2(_expr, _num_bits) -> Any:
        return _Expr()

    sql.SparkSession = SparkSession
    sql.DataFrame = DataFrame
    functions.col = col
    functions.trim = trim
    functions.initcap = initcap
    functions.regexp_replace = regexp_replace
    functions.lit = lit
    functions.to_date = to_date
    functions.count = count
    functions.when = when
    functions.coalesce = coalesce
    functions.concat_ws = concat_ws
    functions.sha2 = sha2
    types_mod.StructType = StructType
    types_mod.StructField = StructField
    types_mod.StringType = StringType
    types_mod.DoubleType = DoubleType
    types_mod.LongType = LongType

    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions
    sys.modules["pyspark.sql.types"] = types_mod


def _install_pydeequ_stub() -> None:
    """Install lightweight `pydeequ` stubs into `sys.modules`."""
    if "pydeequ" in sys.modules:
        return

    pydeequ = types.ModuleType("pydeequ")
    checks = types.ModuleType("pydeequ.checks")
    verification = types.ModuleType("pydeequ.verification")

    class CheckLevel:
        Error = "Error"

    class ConstrainableDataTypes:
        Fractional = "Fractional"
        Integral = "Integral"
        String = "String"

    class Check:
        def __init__(self, spark, level, name) -> Any:
            self.spark = spark
            self.level = level
            self.name = name

        def hasSize(self, _fn) -> Any:
            return self

        def isComplete(self, _column) -> Any:
            return self

        def hasUniqueness(self, _columns, _fn) -> Any:
            return self

        def hasDataType(self, _column, _dtype, _fn) -> Any:
            return self

        def isNonNegative(self, _column) -> Any:
            return self

        def isContainedIn(self, _column, _allowed) -> Any:
            return self

        def satisfies(self, _constraint, _name) -> Any:
            return self

    class _Result:
        status = "Success"

    class VerificationSuite:
        def __init__(self, spark) -> Any:
            self.spark = spark

        def onData(self, _data) -> Any:
            return self

        def addCheck(self, _check) -> Any:
            return self

        def run(self) -> Any:
            return _Result()

    class VerificationResult:
        @staticmethod
        def checkResultsAsDataFrame(_spark, _result) -> Any:
            class _ResultDataFrame:
                def show(self, truncate=False) -> Any:
                    return None

            return _ResultDataFrame()

    checks.Check = Check
    checks.CheckLevel = CheckLevel
    checks.ConstrainableDataTypes = ConstrainableDataTypes
    verification.VerificationSuite = VerificationSuite
    verification.VerificationResult = VerificationResult

    sys.modules["pydeequ"] = pydeequ
    sys.modules["pydeequ.checks"] = checks
    sys.modules["pydeequ.verification"] = verification


def _install_delta_stub() -> None:
    """Install lightweight `delta` stubs into `sys.modules`."""
    if "delta.tables" in sys.modules:
        return

    delta = types.ModuleType("delta")
    delta_tables = types.ModuleType("delta.tables")

    class DeltaTable:
        @staticmethod
        def isDeltaTable(_spark, _path) -> Any:
            return False

        @staticmethod
        def forPath(_spark, _path) -> Any:
            class _Target:
                def alias(self, _name) -> Any:
                    return self

                def merge(self, _source, _condition) -> Any:
                    return self

                def whenMatchedUpdateAll(self) -> Any:
                    return self

                def whenNotMatchedInsertAll(self) -> Any:
                    return self

                def execute(self) -> Any:
                    return None

            return _Target()

    delta_tables.DeltaTable = DeltaTable
    delta.tables = delta_tables
    sys.modules["delta"] = delta
    sys.modules["delta.tables"] = delta_tables


def _install_misc_stubs() -> None:
    """Install stubs for network and cloud SDK dependencies."""
    if "aiohttp" not in sys.modules:
        aiohttp = types.ModuleType("aiohttp")

        class ClientError(Exception):
            pass

        class ClientSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def get(self, *args, **kwargs) -> Any:
                class _Response:
                    async def __aenter__(self):
                        return self

                    async def __aexit__(self, exc_type, exc, tb):
                        return False

                    def raise_for_status(self) -> Any:
                        return None

                    async def json(self):
                        return []

                return _Response()

        aiohttp.ClientError = ClientError
        aiohttp.ClientSession = ClientSession
        sys.modules["aiohttp"] = aiohttp

    if "backoff" not in sys.modules:
        backoff = types.ModuleType("backoff")

        def on_exception(*args, **kwargs) -> Any:
            def _decorator(fn) -> Any:
                return fn

            return _decorator

        backoff.on_exception = on_exception
        backoff.expo = object()
        backoff.full_jitter = object()
        sys.modules["backoff"] = backoff

    if "requests" not in sys.modules:
        requests = types.ModuleType("requests")

        class _Resp:
            def json(self) -> Any:
                return {"total": 0}

        def get(*args, **kwargs) -> Any:
            return _Resp()

        requests.get = get
        sys.modules["requests"] = requests

    if "boto3" not in sys.modules:
        boto3 = types.ModuleType("boto3")

        def client(*args, **kwargs) -> Any:
            class _Client:
                def put_object(self, **kwargs) -> Any:
                    return None

            return _Client()

        boto3.client = client
        sys.modules["boto3"] = boto3

    if "botocore.config" not in sys.modules:
        botocore = types.ModuleType("botocore")
        config_mod = types.ModuleType("botocore.config")

        class Config:
            def __init__(self, **kwargs) -> Any:
                self.kwargs = kwargs

        config_mod.Config = Config
        sys.modules["botocore"] = botocore
        sys.modules["botocore.config"] = config_mod

    if "dotenv" not in sys.modules:
        dotenv = types.ModuleType("dotenv")

        def load_dotenv(*args, **kwargs) -> Any:
            return None

        dotenv.load_dotenv = load_dotenv
        sys.modules["dotenv"] = dotenv


def install_test_stubs() -> None:
    """Install all test stubs used by unit tests."""
    _install_pyspark_stub()
    _install_pydeequ_stub()
    _install_delta_stub()
    _install_misc_stubs()
