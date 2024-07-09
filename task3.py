import sys
from pyspark import SparkContext


def mapper(line: str) -> tuple[str, tuple[int, float]]:
    parts = line.split(',')
    if len(parts) < 17:
        return None
    try:
        pickup_datetime = parts[2]
        travel_distance = float(parts[5])
        surcharge_amount = float(parts[12])
        hour = int(pickup_datetime.split()[1].split(':')[0])
        if travel_distance > 0:
            profit_ratio = surcharge_amount / travel_distance
            return hour, profit_ratio
    except BaseException:  # pylint: disable=broad-exception-caught
        return None
    return None


def clean_data(x: tuple):
    return x is not None


def reducer(cum: tuple[float, int], value: float) -> tuple[float, int]:
    return (cum[0] + value, cum[1] + 1)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: <file> <output> ", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    n = 3

    profit_ratios = lines \
        .map(mapper) \
        .filter(clean_data) \
        .mapValues(lambda value: (value, 1)) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda sum_count: sum_count[0] / sum_count[1])
    
    top_n_hours = profit_ratios.takeOrdered(3, key=lambda x: -x[1])

    sc.parallelize(top_n).saveAsTextFile(sys.argv[2])
    sc.stop()

    # Print top_n
    print(f"Top {n} medallions with highest earnings per minute:")
    for hour, profit_ratio in top_n_hours:
        print(f"Hour: {hour}, Profit Ratio: {profit_ratio}")


# import sys
# from pyspark import SparkContext
# from datetime import datetime

# def mapper(line: str) -> tuple[str, tuple[float, float]]:
#     """
#     Returns hour, (surcharge, distance)
#     """
#     parts = line.split(',')
#     if len(parts) < 18:
#         return None
#     try:
#         # Extract datetime and surcharge amount and distance
#         datetime_str = parts[3]
#         surcharge = float(parts[15])
#         distance = float(parts[9])
#         hour = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S').hour
#         return hour, (surcharge, distance)
#     except BaseException:  # pylint: disable=broad-exception-caught
#         return None

# def clean_data(x: tuple):
#     if not x or len(x) != 2:
#         return False
#     _, (surcharge, distance) = x
#     return distance > 0 and surcharge >= 0

# def reducer(cum: tuple[float, float], value: tuple[float, float]) -> tuple[float, float]:
#     return (cum[0] + value[0], cum[1] + value[1])

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Usage: <file> <output> ", file=sys.stderr)
#         sys.exit(-1)

#     sc = SparkContext(appName="ProfitRatioPerHour")
#     lines = sc.textFile(sys.argv[1], 1)
#     n = 3

#     profit_ratio = lines \
#         .map(mapper) \
#         .filter(clean_data) \
#         .reduceByKey(reducer) \
#         .mapValues(lambda i: i[0] / i[1])  # Profit ratio = Surcharge / Distance
#     top_n = profit_ratio.takeOrdered(n, key=lambda x: -x[1])  # Negate x[1] to get in descending order

#     sc.parallelize(top_n).saveAsTextFile(sys.argv[2])
#     sc.stop()

#     # Print top_n
#     print(f"Top {n} hours with highest profit ratio:")
#     for hour, ratio in top_n:
#         print(f"Hour: {hour}, Profit Ratio: {ratio}")
