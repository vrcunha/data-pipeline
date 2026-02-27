"""Test stubs for external dependencies not available in local environment."""

import sys
import types


class _Expr:
    """Minimal expression stub used by fake Spark functions."""

    def __init__(self, value=None):
        self.value = value

    def cast(self, _dtype):
        """Mimic Spark cast API and keep chainability."""
        return self

    def isNull(self):
        """Mimic Spark isNull API and keep chainability."""
        return self

    def rlike(self, _pattern):
        """Mimic Spark rlike API and keep chainability."""
        return self

    def __eq__(self, _other):
        """Mimic Spark column comparison expressions."""
        return self

    def __or__(self, _other):
        """Mimic Spark boolean expression OR operator."""
        return self


class _WhenExpr:
    """Minimal `when` expression chain stub."""

    def when(self, _condition, _value):
        """Mimic chained `when` API."""
        return self

    def otherwise(self, _value):
        """Mimic terminal `otherwise` API."""
        return _Expr()


class _CountExpr:
    """Minimal count expression stub."""

    def alias(self, _name):
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

    class StructField:
        def __init__(self, name, data_type, nullable):
            self.name = name
            self.dataType = data_type
            self.nullable = nullable

    class StructType:
        def __init__(self, fields):
            self.fields = fields

    class StringType:
        pass

    class DoubleType:
        pass

    class LongType:
        pass

    def col(name):
        return _Expr(name)

    def trim(expr):
        return expr

    def initcap(expr):
        return expr

    def regexp_replace(expr, _pattern, _replacement):
        return expr

    def lit(value):
        return _Expr(value)

    def to_date(expr):
        return expr

    def count(_expr):
        return _CountExpr()

    def when(_condition, _value):
        return _WhenExpr()

    sql.SparkSession = SparkSession
    functions.col = col
    functions.trim = trim
    functions.initcap = initcap
    functions.regexp_replace = regexp_replace
    functions.lit = lit
    functions.to_date = to_date
    functions.count = count
    functions.when = when
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
        def __init__(self, spark, level, name):
            self.spark = spark
            self.level = level
            self.name = name

        def hasSize(self, _fn):
            return self

        def isComplete(self, _column):
            return self

        def hasUniqueness(self, _columns, _fn):
            return self

        def hasDataType(self, _column, _dtype, _fn):
            return self

        def isNonNegative(self, _column):
            return self

    class _Result:
        status = "Success"

    class VerificationSuite:
        def __init__(self, spark):
            self.spark = spark

        def onData(self, _data):
            return self

        def addCheck(self, _check):
            return self

        def run(self):
            return _Result()

    class VerificationResult:
        @staticmethod
        def checkResultsAsDataFrame(_spark, _result):
            class _ResultDataFrame:
                def show(self, truncate=False):
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
        def isDeltaTable(_spark, _path):
            return False

        @staticmethod
        def forPath(_spark, _path):
            class _Target:
                def alias(self, _name):
                    return self

                def merge(self, _source, _condition):
                    return self

                def whenMatchedUpdateAll(self):
                    return self

                def whenNotMatchedInsertAll(self):
                    return self

                def execute(self):
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

            def get(self, *args, **kwargs):
                class _Response:
                    async def __aenter__(self):
                        return self

                    async def __aexit__(self, exc_type, exc, tb):
                        return False

                    def raise_for_status(self):
                        return None

                    async def json(self):
                        return []

                return _Response()

        aiohttp.ClientError = ClientError
        aiohttp.ClientSession = ClientSession
        sys.modules["aiohttp"] = aiohttp

    if "backoff" not in sys.modules:
        backoff = types.ModuleType("backoff")

        def on_exception(*args, **kwargs):
            def _decorator(fn):
                return fn

            return _decorator

        backoff.on_exception = on_exception
        backoff.expo = object()
        backoff.full_jitter = object()
        sys.modules["backoff"] = backoff

    if "requests" not in sys.modules:
        requests = types.ModuleType("requests")

        class _Resp:
            def json(self):
                return {"total": 0}

        def get(*args, **kwargs):
            return _Resp()

        requests.get = get
        sys.modules["requests"] = requests

    if "boto3" not in sys.modules:
        boto3 = types.ModuleType("boto3")

        def client(*args, **kwargs):
            class _Client:
                def put_object(self, **kwargs):
                    return None

            return _Client()

        boto3.client = client
        sys.modules["boto3"] = boto3

    if "botocore.config" not in sys.modules:
        botocore = types.ModuleType("botocore")
        config_mod = types.ModuleType("botocore.config")

        class Config:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        config_mod.Config = Config
        sys.modules["botocore"] = botocore
        sys.modules["botocore.config"] = config_mod

    if "dotenv" not in sys.modules:
        dotenv = types.ModuleType("dotenv")

        def load_dotenv(*args, **kwargs):
            return None

        dotenv.load_dotenv = load_dotenv
        sys.modules["dotenv"] = dotenv


def install_test_stubs() -> None:
    """Install all test stubs used by unit tests."""
    _install_pyspark_stub()
    _install_pydeequ_stub()
    _install_delta_stub()
    _install_misc_stubs()
