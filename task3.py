import sys
import os
import shutil
from pyspark import SparkContext


def mapper(line: str) -> tuple[str, tuple[int, float]] | None:
    parts = line.split(',')
    if len(parts) < 17:
        return None
    try:
        pickup_datetime = parts[2]
        travel_distance = float(parts[5])
        surcharge_amount = float(parts[12])
        hour = int(pickup_datetime.split()[1].split(':')[0])
        if travel_distance > 0 and surcharge_amount >= 0:
            return hour, (surcharge_amount, travel_distance)
    except BaseException:  # pylint: disable=broad-exception-caught
        return None
    return None


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: <file> <output> ", file=sys.stderr)
        sys.exit(-1)

    if os.path.exists(sys.argv[2]):
        shutil.rmtree(sys.argv[2])

    sc = SparkContext(appName="ProfitRatios")
    lines = sc.textFile(sys.argv[1], 1)
    n = 3

    profit_ratios = lines \
        .map(mapper) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
        .mapValues(lambda sum_count: sum_count[0] / sum_count[1])

    top_n_hours = profit_ratios.takeOrdered(3, key=lambda x: -x[1])

    sc.parallelize(top_n_hours).saveAsTextFile(sys.argv[2])
    sc.stop()

    # Print top_n
    print(f"Top {n} hours with highest profit ratio:")
    for h, pr in top_n_hours:
        print(f"Hour: {h}, Profit Ratio: {pr}")
