Running locally


```
./gradlew :benchmarks:run --args="ArrowBenchmark -p uniqueTerms=1000000 -p timeout:
avgKeyLength=100" -Dverify.results=true
```