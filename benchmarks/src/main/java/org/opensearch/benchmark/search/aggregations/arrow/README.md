Running locally


```
./gradlew :benchmarks:run --args="ArrowBenchmark -p uniqueTerms=1000000 -p timeout:
avgKeyLength=100" -Dverify.results=true
```

```
  | Benchmark               | Time (μs) | Difference  |
  |-------------------------|-----------|-------------|
  | combined_arrowStyle     | 10,576    | Baseline    |
  | combined_bigArraysStyle | 22,168    | 2.1x slower |
```


```
  | Benchmark                     | Time (μs) | Difference |
  |-------------------------------|-----------|------------|
  | theory1_primitiveAccumulation | 11,284    | Baseline   |
  | theory1_heavyObjectAllocation | 19,771    | 75% slower |
```

```
  | Benchmark                         | Time (μs) | Difference  |
  |-----------------------------------|-----------|-------------|
  | theory2_interleavedColumnarAccess | 132       | Fastest     |
  | theory2_columnarAccess            | 167       | 27% slower  |
  | theory2_rowOrientedAccess         | 267       | 102% slower |
```

```
  | Benchmark                     | Time (μs) | Difference |
  |-------------------------------|-----------|------------|
  | theory3_arraysCompareUnsigned | 65        | Fastest    |
  | theory3_bytesRefComparator    | 95        | 46% slower |
  | theory3_genericComparator     | 97        | 49% slower |
```

```
  | Benchmark                     | Time (μs) | Difference |
  |-------------------------------|-----------|------------|
  | theory4_minimalReductionLogic | 10,977    | Baseline   |
  | theory4_fullReductionLogic    | 20,072    | 83% slower |
```

```
  | Benchmark                | Time (μs) | Difference |
  |--------------------------|-----------|------------|
  | theory5_arrayIndexAccess | 60        | Fastest    |
  | theory5_enhancedForArray | 62        | 4% slower  |
  | theory5_listIndexAccess  | 93        | 56% slower |
  | theory5_iteratorAccess   | 94        | 57% slower |
```


Changing shard allocation
```
  | Benchmark                     | Time (ms) | vs Arrow Style          |
  |-------------------------------|-----------|-------------------------|
  | combined_arrowStyle           | 40.7      | Baseline                |
  | theory1_primitiveAccumulation | 41.1      | ~Same (just primitives) |
  | theory1_heavyObjectAllocation | 71.4      | 75% slower              |
  | combined_bigArraysStyle       | 91.5      | 125% slower             |
```

```
  | Benchmark                   | 10K terms | 100K terms | Scale Factor |
  |-----------------------------|-----------|------------|--------------|
  | arrowStyleWithSubAggs       | 3.2 ms    | 34.4 ms    | 10.7x        |
  | bigArraysStyleWithSubAggs   | 8.9 ms    | 101.5 ms   | 11.4x        |
  | arrowVectorStyleWithSubAggs | 19.7 ms   | 189.1 ms   | 9.6x         
```

```
  | Benchmark                     | Time (ms) | vs Plain Arrays |
  |-------------------------------|-----------|-----------------|
  | microBenchmarkArrayRead       | 1.31      | Baseline        |
  | microBenchmarkArrowZeroCopy   | 6.60      | 5x slower       |
  | microBenchmarkArrowVectorRead | 14.58     | 11x slower      |
```

```
  | Benchmark                   | Time (ms) | vs Real StringTerms.reduce |
  |-----------------------------|-----------|----------------------------|
  | arrowStyleWithSubAggs       | 38.9      | 5.5x faster                |
  | arrowVectorStyleWithSubAggs | 203.6     | Similar (~5% faster)       |
  | bigArraysStyleWithSubAggs   | 214.7     | Baseline (real code)       |
```


```
  Summary by Distribution

  PARTITIONED (No term overlap - best case for Arrow)

  | Benchmark       | Time (ms) | vs BigArrays |
  |-----------------|-----------|--------------|
  | arrowReduceSIMD | 1.053     | 2.1x faster  |
  | arrowReduce     | 1.507     | 1.5x faster  |
  | bigArraysReduce | 2.259     | baseline     |

  UNIFORM (Full overlap - realistic)

  | Benchmark       | Time (ms) | vs BigArrays |
  |-----------------|-----------|--------------|
  | bigArraysReduce | 9.655     | baseline     |
  | arrowReduceSIMD | 11.246    | 1.2x slower  |
  | arrowReduce     | 16.489    | 1.7x slower  |

  ZIPF (Full overlap - realistic with skew)

  | Benchmark       | Time (ms) | vs BigArrays |
  |-----------------|-----------|--------------|
  | bigArraysReduce | 9.636     | baseline     |
  | arrowReduceSIMD | 11.349    | 1.2x slower  |
  | arrowReduce     | 16.826    | 1.7x slower  |
```