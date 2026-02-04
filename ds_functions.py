import decimal
from datetime import datetime
from io import UnsupportedOperation

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType, DateType, StructType, StructField, BooleanType


def ds_ereplace(expression, substring, replacement, occurrence=0, begin=1):
    """
    Mimics the behavior of IBM DataStage EREPLACE function.
Use the EREPLACE function to replace substring in expression with another substring. If you do not specify occurrence, each occurrence of substring is replaced.
occurrence specifies the number of occurrences of substring to replace. To replace all occurrences, specify occurrence as a number less than 1.
begin specifies the first occurrence to replace. If begin is omitted or less than 1, it defaults to 1.
If substring is an empty string, replacement is prefixed to expression. If replacement is an empty string, all occurrences of substring are removed.
If expression evaluates to the null value, null is returned. If substring, replacement, occurrence, or begin evaluates to the null value, the EREPLACE function fails and the program terminates with a run-time error message.
The EREPLACE function behaves like the CHANGE function except when substring evaluates to an empty string.

    Parameters:
    expression (str): The string to search within.
    substring (str): The substring to replace.
    replacement (str): The substring to replace with.
    occurrence (int, optional): The number of occurrences to replace. Default is 0 (all occurrences).
    begin (int, optional): The first occurrence to replace. Default is 1.

    Returns:
    str: The modified string.
    """
    if expression is None:
        return None
    if substring is None or replacement is None or occurrence is None or begin is None:
        raise ValueError(
            "EREPLACE function failed: Null value provided for substring, replacement, occurrence, or begin.")

    if substring == "":
        return replacement + expression

    if replacement == "":
        return expression.replace(substring, "", occurrence if occurrence > 0 else 1000000)

    if begin < 1:
        begin = 1

    parts = expression.split(substring)
    result = []
    count = 0
    for i, part in enumerate(parts):
        if i == 0:
            result.append(part)
        elif i <= begin - 1:
            result.append(substring + part)
        else:
            if count < occurrence or occurrence <= 0:
                result.append(replacement + part)
                count += 1
            else:
                result.append(substring + part)

    return "".join(result)

def ds_alpha(expression:str):
    if expression is None:
        return None
    return 1 if expression.isalpha() else 0

def ds_alphanum(expression:str):
    if expression is None:
        return None
    return 1 if expression.isalnum() else 0

def ds_bitnot(expression, bit=None):
    if expression is None:
        return None
    if bit  and (bit <= 0 or bit) > 63:
        raise ValueError("bit# must be a non-negative integer")

    if bit is not None:
        # Invert only the specified bit
        return expression ^ (1 << (bit) - 1)
    else:
        # Invert all bits
        return ~expression


def ds_change(expression, substring, replacement, occurrence=0, begin=1):
    """
    Replace a substring in expression with another substring.
Use the CHANGE function to replace a substring in expression with another substring. If you do not specify occurrence, each occurrence of the substring is replaced.

occurrence specifies the number of occurrences of substring to replace. To change all occurrences, specify occurrence as a number less than 1.

begin specifies the first occurrence to replace. If begin is omitted or less than 1, it defaults to 1.

If substring is an empty string, the value of expression is returned. If replacement is an empty string, all occurrences of substring are removed.

If expression evaluates to the null value, null is returned. If substring, replacement, occurrence, or begin evaluates to the null value, the CHANGE function fails and the program terminates with a run-time error message.

The CHANGE function behaves like the EREPLACE function except when substring evaluates to an empty string.
    :param expression: The original string.
    :param substring: The substring to be replaced.
    :param replacement: The substring to replace with.
    :param occurrence: The number of occurrences to replace. If less than 1, replace all.
    :param begin: The first occurrence to replace. Defaults to 1.
    :return: The modified string.
    """
    if expression is None:
        return None
    if substring is None or replacement is None or occurrence is None or begin is None:
        raise ValueError(
            "EREPLACE function failed: Null value provided for substring, replacement, occurrence, or begin.")

    if substring == "":
        return expression

    if replacement == "":
        return expression.replace(substring, "", occurrence if occurrence > 0 else 1000000)

    if begin < 1:
        begin = 1

    parts = expression.split(substring)
    result = []
    count = 0
    for i, part in enumerate(parts):
        if i == 0:
            result.append(part)
        elif i <= begin - 1:
            result.append(substring + part)
        else:
            if count < occurrence or occurrence <= 0:
                result.append(replacement + part)
                count += 1
            else:
                result.append(substring + part)

    return "".join(result)

import re
def ds_compare(string1, string2, justification='L'):
    if justification not in ['L', 'R']:
        return 0

    if justification == 'L':
        if string1 < string2:
            return -1
        elif string1 == string2:
            return 0
        else:
            return 1
    elif justification == 'R':
        # Split the strings into parts and compare numerically

        parts1 = re.findall(r'[\d]+$', string1)
        parts2 = re.findall(r'[\d]+$', string2)

        # Assuming the strings are well-formed and have the same structure
        for part1, part2 in zip(parts1, parts2):
            num1 = float(part1) if part1.isdigit() else part1
            num2 = float(part2) if part2.isdigit() else part2

            if num1 < num2:
                return -1
            elif num1 > num2:
                return 1
        return 0

def ds_count(string, substring):
    if substring is None:
        raise ValueError("Substring cannot be null")
    if string is None:
        return None
    if substring == "":
        return len(string)
    count = 0
    start = 0
    while start < len(string):
        index = string.find(substring, start)
        if index == -1:
            break
        count += 1
        start = index + len(substring)
    return count


def ds_field(string, delimiter:str, occurrence, num_substr=1):
    if string is None :
        return None
    if delimiter is None or occurrence is None or num_substr is None:
        raise ValueError("Delimiter, occurrence, and num_substr cannot be null")

    if occurrence < 1:
        occurrence = 1

    if num_substr < 1:
        num_substr = 1

    if delimiter == "":
        return string

    if len(delimiter) > 1:
        delimiter = delimiter[0:1]

    parts = string.split(delimiter)

    if occurrence > len(parts) + 1:
        return ""

    start_index = sum(len(parts[i]) + len(delimiter) for i in range(occurrence - 1))
    end_index = start_index + sum(
        len(parts[i]) + len(delimiter) for i in range(occurrence - 1, min(len(parts), occurrence - 1 + num_substr)))

    if occurrence == 1 and delimiter not in string:
        return string

    return string[start_index:end_index].rstrip(delimiter)


def ds_fieldstore(string, delimiter, start, n, new_string):
    if string is None:
        return None
    if delimiter is None or start is None or n is None or new_string is None:
        raise ValueError("Delimiter, start, n, or new_string cannot be null")

    fields = string.split(delimiter)
    new_fields = new_string.split(delimiter)

    if start > len(fields):
        fields.extend([''] * (start - len(fields)))

    if n == 0:
        fields = fields[:start - 1] + new_fields + fields[start - 1:]
    elif n > 0:
        fields = fields[:start - 1] + new_fields[:n] + fields[start - 1 + n:]
    elif n < 0:
        fields = fields[:start - 1] + new_fields + fields[start - 1:]

    return delimiter.join(fields)


def ds_index(string, substring, occurrence):
    if string is None or substring is None or occurrence <= 0:
        return 0

    if len(substring) == 0:
        return 1

    start = 0
    count = 0
    while start < len(string):
        found_index = string.find(substring, start)
        if found_index == -1:
            break
        count += 1
        if count == occurrence:
            return found_index + 1  # +1 to convert to 1-based index
        start = found_index + 1

    return 0


def ds_decimaltostring(decimal_value, option=None):
    # if not isinstance(decimal_value, decimal.Decimal):
    #     return None

    if option == "suppress_zero":
        # Remove leading zeros and trailing zeros after the decimal point
        decimal_str = str(decimal_value)
        dot_index = decimal_str.find(".")
        if dot_index > 0:
            zero_index = decimal_str.find("0",dot_index )
            if (zero_index >= 0):
                return decimal_str[:zero_index].rstrip(".")
            else:
                return decimal_str
        elif dot_index == 0:
            return "0" + decimal_str
        else:
            return decimal_str
    else:
        # Return the decimal value as a fixed-length string with trailing zeros
        # TODO: fix code later
        return str(decimal_value).zfill(34)


from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, ROUND_DOWN, getcontext, InvalidOperation
import math


def ds_stringtodecimal(string_val, rtype="trunc_zero"):
    """
    以十进制表示形式返回给定的字符串，支持不同的取整类型。

    Args:
        string_val (str): 要转换的字符串
        rtype (str, optional): 取整类型 - "ceil", "floor", "round_inf", "trunc_zero"

    Returns:
        Decimal: 转换后的十进制数值

    Raises:
        ValueError: 如果输入字符串不是有效的数字
    """

    if not isinstance(string_val, str):
        raise ValueError("输入必须是字符串")

    string_val = string_val.strip()
    if not string_val:
        raise ValueError("输入字符串不能为空")

    try:
        # 将字符串转换为Decimal
        decimal_val = Decimal(string_val)
    except InvalidOperation:
        raise ValueError(f"无法将字符串 '{string_val}' 转换为十进制数字")

    # 根据不同的取整类型进行处理
    if rtype == "ceil":
        # 向正无穷方向取整
        return decimal_val.to_integral_value(rounding=ROUND_CEILING)

    elif rtype == "floor":
        # 向负无穷方向取整
        return decimal_val.to_integral_value(rounding=ROUND_FLOOR)

    elif rtype == "round_inf":
        # 向最接近的可表示值取整（四舍五入）
        return decimal_val.to_integral_value(rounding=ROUND_HALF_UP)

    elif rtype == "trunc_zero":
        # 截断小数位（向零方向取整）
        return decimal_val.to_integral_value(rounding=ROUND_DOWN)

    else:
        raise ValueError(f"不支持的取整类型: {rtype}")


# 更通用的函数，支持指定目标精度和小数位数
def ds_stringtodecimal(string_val, precision=None, scale=None, rtype="trunc_zero"):
    """
    以十进制表示形式返回给定的字符串，支持指定精度、小数位数和取整类型。

    Args:
        string_val (str): 要转换的字符串
        precision (int, optional): 总精度（数字总位数）
        scale (int, optional): 小数位数
        rtype (str, optional): 取整类型

    Returns:
        Decimal: 转换后的十进制数值
    """

    if not isinstance(string_val, str):
        raise ValueError("输入必须是字符串")

    string_val = string_val.strip()
    if not string_val:
        raise ValueError("输入字符串不能为空")

    try:
        decimal_val = Decimal(string_val)
    except InvalidOperation:
        raise ValueError(f"无法将字符串 '{string_val}' 转换为十进制数字")

    # 如果没有指定精度和小数位数，直接返回原始值
    if precision is None and scale is None:
        return decimal_val

    # 设置精度和小数位数约束
    if precision is not None and scale is not None:
        # 计算整数部分的最大位数
        integer_digits = precision - scale

        # 分离整数和小数部分
        integer_part = decimal_val.to_integral_value(rounding=ROUND_DOWN)
        fractional_part = decimal_val - integer_part

        # 检查整数部分是否超出范围
        if integer_part != 0 and math.log10(abs(float(integer_part))) + 1 > integer_digits:
            raise ValueError(f"整数部分超出精度限制: {integer_digits} 位")

        # 根据取整类型处理小数部分
        if rtype == "ceil":
            # 向正无穷方向取整
            rounded_fractional = fractional_part.to_integral_value(rounding=ROUND_CEILING)
            result = integer_part + rounded_fractional

        elif rtype == "floor":
            # 向负无穷方向取整
            rounded_fractional = fractional_part.to_integral_value(rounding=ROUND_FLOOR)
            result = integer_part + rounded_fractional

        elif rtype == "round_inf":
            # 四舍五入
            # 使用量化来保留指定的小数位数
            result = decimal_val.quantize(Decimal('1.' + '0' * scale), rounding=ROUND_HALF_UP)

        elif rtype == "trunc_zero":
            # 截断小数位
            # 使用量化来截断到指定的小数位数
            result = decimal_val.quantize(Decimal('1.' + '0' * scale), rounding=ROUND_DOWN)

        else:
            raise ValueError(f"不支持的取整类型: {rtype}")

        return result

    return decimal_val

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType, DateType, \
    ArrayType
import re

import re
import datetime
from decimal import Decimal, InvalidOperation


def DS_IsValid(type_str, teststring, format_str=None):
    """
    Validates if a string is valid for the given data type.

    Args:
        type_str (str): The data type to validate against
        teststring (str): The string to validate
        format_str (str, optional): Format string for date/time types

    Returns:
        int: 1 if valid, 0 if invalid
    """

    if not isinstance(teststring, str) or teststring.strip() == "":
        return 0

    teststring = teststring.strip()

    # Handle decimal type with precision and scale
    decimal_match = re.match(r'decimal\s*\[\s*(\d+)\s*,\s*(\d+)\s*\]', type_str.lower())
    if decimal_match:
        return _is_valid_decimal(teststring, int(decimal_match.group(1)), int(decimal_match.group(2)))

    type_lower = type_str.lower()

    type_handlers = {
        'date': lambda: _is_valid_datetime(teststring, format_str or '%Y-%m-%d', 'date'),
        'time': lambda: _is_valid_datetime(teststring, format_str or '%H:%M:%S', 'time'),
        'timestamp': lambda: _is_valid_datetime(teststring, format_str or '%Y-%m-%d %H:%M:%S', 'timestamp'),
        'dfloat': lambda: _is_valid_float(teststring, True),
        'sfloat': lambda: _is_valid_float(teststring, False),
        'int8': lambda: _is_valid_int(teststring, 8, True),
        'uint8': lambda: _is_valid_int(teststring, 8, False),
        'int16': lambda: _is_valid_int(teststring, 16, True),
        'uint16': lambda: _is_valid_int(teststring, 16, False),
        'int32': lambda: _is_valid_int(teststring, 32, True),
        'uint32': lambda: _is_valid_int(teststring, 32, False),
        'int64': lambda: _is_valid_int(teststring, 64, True),
        'uint64': lambda: _is_valid_int(teststring, 64, False),
        'raw': lambda: 1,  # Raw data is always valid
        'string': lambda: 1,  # String is always valid
        'ustring': lambda: 1,  # Unicode string is always valid
    }

    if type_lower in type_handlers:
        try:
            return type_handlers[type_lower]()
        except (ValueError, TypeError):
            return 0

    return 0


def _is_valid_decimal(teststring, precision, scale):
    """Validate decimal with specified precision and scale"""
    try:
        # Check if it's a valid decimal number
        decimal_val = Decimal(teststring)

        # Check if it's finite
        if not decimal_val.is_finite():
            return 0

        # Convert to string to check precision and scale
        parts = str(decimal_val).split('.')

        # Calculate total digits (precision)
        integer_digits = len(parts[0].lstrip('-+'))
        decimal_digits = len(parts[1]) if len(parts) > 1 else 0
        total_digits = integer_digits + decimal_digits

        # Check precision and scale
        if total_digits > precision or decimal_digits > scale:
            return 0

        return 1
    except (InvalidOperation, ValueError):
        return 0


def _is_valid_datetime(teststring, format_pattern, dtype):
    """Validate date, time, or timestamp with given format"""
    try:
        dt = datetime.datetime.strptime(teststring, format_pattern)

        # Additional validation based on type
        if dtype == 'date':
            # For date, we don't care about time components
            return 1
        elif dtype == 'time':
            # For time, we don't care about date components
            return 1
        elif dtype == 'timestamp':
            # For timestamp, both date and time should be valid
            return 1

        return 1
    except (ValueError, TypeError):
        return 0


def _is_valid_float(teststring, is_double):
    """Validate float (single or double precision)"""
    try:
        float_val = float(teststring)

        # Check if it's finite (not NaN or infinity)
        if not float.isfinite(float_val):
            return 0

        # For double float, no additional constraints
        # For single float, check if it can be represented as float32
        if not is_double:
            # Try to convert to float32 and back to check precision
            float32_val = float(float_val)
            # Simple check - if conversion to float and back changes value significantly
            if abs(float_val - float32_val) > 1e-6 * abs(float_val):
                return 0

        return 1
    except (ValueError, TypeError):
        return 0


def _is_valid_int(teststring, bits, signed):
    """Validate integer with specified bit size and signedness"""
    try:
        int_val = int(teststring)

        # Check range based on bit size and signedness
        if signed:
            min_val = -(2 ** (bits - 1))
            max_val = (2 ** (bits - 1)) - 1
        else:
            min_val = 0
            max_val = (2 ** bits) - 1

        if int_val < min_val or int_val > max_val:
            return 0

        return 1
    except (ValueError, TypeError):
        return 0

def ds_spark_schema_from_text(text:str) -> str:
    """
    Parse the input text and generate a PySpark StructType.
    - `nullable` is an optional field.
    - Fields like 'string[]' or 'ustring[]' are translated to StringType.
    """
    # Regex to match field definitions with optional 'nullable'
    field_pattern = re.compile(
        r'^\s*(?P<name>\w+):\s*(?P<nullable>nullable\s+)?(?P<type>\w+)(?:\[(?P<params>[^\]]*)\])?(?:\s*;\s*)?$',
        re.MULTILINE | re.IGNORECASE
    )

    # Initialize an empty StructType
    schema = StructType()

    for match in field_pattern.finditer(text):
        name = match.group('name')
        is_nullable = bool(match.group('nullable'))  # True if 'nullable' exists
        data_type = match.group('type').lower()
        params = match.group('params')

        # Map data type to PySpark type
        spark_type = None
        if data_type == 'string':
            spark_type = StringType()
        elif data_type == 'ustring':
            spark_type = StringType()
        elif data_type == 'decimal':
            if params:
                precision, scale = map(int, params.split(','))
                spark_type = DecimalType(precision, scale)
            else:
                spark_type = DecimalType(10, 2)  # Default
        elif data_type == 'timestamp':
            spark_type = TimestampType()
        elif data_type == 'date':
            spark_type = DateType()
        elif data_type == 'array':
            # Only map to ArrayType if the element type is not string/ustring
            if params and params.lower() in ['string', 'ustring']:
                spark_type = StringType()
            else:
                spark_type = ArrayType(StringType())  # Fallback for other array types
        else:
            spark_type = StringType()  # Fallback

        # Add the field to the schema
        schema.add(StructField(name, spark_type, nullable=is_nullable))

    return schema.json()

    # Example usage
    input_text = """
    MUREX_INTRNL_TRAN_ID:nullable decimal[10,0];
    BUY_SELL_IND:ustring[1];
    AS_OF_DT:timestamp;
    Output_Text:string[];
    """

    schema = parse_text_to_struct(input_text)
    print(schema)


# def ds_decimal_to_date(basedec, format_str="%yyyy%mm%dd"):
#     """
#     Converts a packed decimal to a date based on the specified format.
#
#     :param basedec: Decimal number to convert.
#     :param format_str: Format string specifying how the date is stored in the decimal number.
#     :return: Converted date.
#     """
#     # Remove sign and scale from the decimal number
#     basedec = abs(int(basedec))
#
#     # Replace format tokens with regex patterns
#     format_patterns = {
#         "%yyyy": r'(\d{4})',
#         "%yy": r'(\d{2})',
#         "%NNNNyy": r'(\d{2})',  # Assuming NNNN is a placeholder for year cutoff
#         "%mm": r'(\d{2})',
#         "%dd": r'(\d{2})',
#         "%ddd": r'(\d{3})'
#     }
#
#     for token, pattern in format_patterns.items():
#         format_str = format_str.replace(token, pattern)
#
#     # Extract date components using regex
#     match = re.match(format_str, str(basedec))
#     if not match:
#         raise ValueError(f"Invalid decimal format for date conversion: {basedec}")
#
#     # Map format tokens to date components
#     format_map = {
#         "%yyyy": "year",
#         "%yy": "year",
#         "%NNNNyy": "year",
#         "%mm": "month",
#         "%dd": "day",
#         "%ddd": "day"
#     }
#
#     components = {}
#     for i, token in enumerate(format_patterns.keys()):
#         if token in format_map and match.group(i + 1):
#             components[format_map[token]] = match.group(i + 1)
#
#     # Construct date string
#     year = components.get('year', '1900')
#     month = components.get('month', '01')
#     day = components.get('day', '01')
#
#     # Handle two-digit year
#     if len(year) == 2:
#         year = '19' + year if int(year) > 50 else '20' + year
#
#     date_str = f"{year}-{month}-{day}"
#
#     # Convert to date
#     return datetime.strptime(date_str, "%Y-%m-%d").date()
def spark_register_ds_common_functions(spark: SparkSession):
    spark.udf.register("ds_ereplace", ds_ereplace, StringType())
    spark.udf.register("ds_alpha", ds_alpha, IntegerType())
    spark.udf.register("ds_alphanum", ds_alphanum, IntegerType())
    spark.udf.register("ds_bitnot", ds_bitnot, IntegerType())
    spark.udf.register("ds_change", ds_change, StringType())
    spark.udf.register("ds_compare", ds_compare, IntegerType())
    spark.udf.register("ds_count", ds_count, IntegerType())
    spark.udf.register("ds_field", ds_field,StringType())
    spark.udf.register("ds_fieldstore", ds_fieldstore,StringType())
    spark.udf.register("ds_index", ds_index, IntegerType())
    spark.udf.register("ds_decimaltostring", ds_decimaltostring, StringType())
    spark.udf.register("ds_spark_schema_from_text", ds_spark_schema_from_text, StringType())
    spark.udf.register("ds_isvalid", DS_IsValid, BooleanType())
    spark.udf.register("ds_stringtodecimal", ds_decimaltostring, DecimalType())



    # spark.udf.register("ds_decimal_to_date", ds_decimal_to_date, DateType())
