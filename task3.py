import sys
from pyspark import SparkContext


def mapper(line: str) -> tuple[str, tuple[int, int]]:
    """
    Returns driver_id, time, earnings
    """
    parts = line.split(',')
    if len(parts) < 17:
        return None
    try:
        return parts[1], (float(parts[4]) / 60, float(parts[16]))
    except BaseException:  # pylint: disable=broad-exception-caught
        return None


def clean_data(x: tuple):
    if not x or len(x) != 2:
        return False
    _, (time, earnings) = x
    return time > 0 and earnings >= 0


def reducer(cum: tuple[float, float], value: tuple[float, float]) -> tuple[float, float]:
    return (cum[0] + value[0], cum[1] + value[1])


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: <file> <output> ", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    n = 10

    epm = lines \
        .map(mapper) \
        .filter(clean_data) \
        .reduceByKey(reducer) \
        .mapValues(lambda i: i[1] / i[0])
    top_n = epm.takeOrdered(n, key=lambda x: -x[1])  # Negate x[1] to get in descending order

    sc.parallelize(top_n).saveAsTextFile(sys.argv[2])
    sc.stop()

    # Print top_n
    print(f"Top {n} medallions with highest earnings per minute:")
    for medallion, count in top_n:
        print(f"Medallion: {medallion}, Earnings Per Minute: {count}")
