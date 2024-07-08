import sys
from pyspark import SparkContext


def seqOp(acc: set, value: str) -> set:
    acc.add(value)
    return acc


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: wordcount <file> <output> ", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="PythonWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    n = 10

    counts = lines \
        .map(lambda line: tuple(line.split(',')[:2])) \
        .aggregateByKey(set(), seqOp, lambda a, b: a.union(b)) \
        .mapValues(len)
    top_n = counts.takeOrdered(n, key=lambda x: -x[1])  # Negate x[1] to get in descending order

    sc.parallelize(top_n).saveAsTextFile(sys.argv[2])
    sc.stop()

    # Print top_n
    print(f"Top {n} medallions with the most drivers:")
    for medallion, count in top_n:
        print(f"Medallion: {medallion}, Unique Drivers: {count}")
