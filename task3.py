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
