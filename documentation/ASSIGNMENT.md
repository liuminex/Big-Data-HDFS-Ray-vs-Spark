## Comparison between Python scaling frameworks for big data analysis and ML

Ray provides a high-performance data processing API for large-scale datasets.
It offers many advantages for working efficiently with datasets.
You are required to compare Ray with Apache Spark.

Specifically you must do the following:

1) Install and setup the two systems (Ray and Spark) in local or okeanos-based resources.

2) Generate or discover real data and load them to the two DBs. Ideally, loaded data should not able to fit in main memory (eg: grater then 8GB - or whatever the memory is) and have millions/billions of records. Use the same data (loaded) in all tests.

3) Write (or find) a set of python scripts (workflows) to test the performance of the two systems in scaling it.
Scripts should target both ML-related and standard data-management operations (ETL) that are common over different cluster resources.
Each team is free to improvise but you can test both individual tasks (e.g., graph operators like PageRank, triangleCount, popular ML operations such as prediction, clustering, etc.) as well as more complex jobs (you can use [kdnuggets](https://www.kdnuggets.com/) and [kaggle](https://www.kaggle.com/) for example on this).
These scripts should be posed over
    - a different number of nodes/workers,
    - different input data size/type. Teams should be careful and compare meaningful statistics in this important step.

Besides the aforementioned project aspects, you are free to improvise in order to best demonstrate the relative strengths and weaknesses of each system (strengths in particular operations, scalability with memory/cores, etc).

