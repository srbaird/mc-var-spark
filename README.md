# Value at Risk using Monte Carlo Simulation on the Spark platform


The aim of this project was to build a system capable of accurately estimating VaR figures for a portfolio of financial assets using the Monte Carlo simulation method whilst taking advantage of the parallel processing capabilities of the Apache Spark framework. 

Value at Risk is a measurement of the potential losses that a portfolio of financial assets may experience as a result of movements in market factors and is expressed as a confidence level over a time horizon. An example value is 95% probability over a 10 day period where the loss would represent the drop in value of the portfolio in 10 days time. The probability value implies that the predicted loss would be exceeded by the observed loss only once in every 20 occurrences. The 10 day period here is commonly referred to as the h-day value.

Monte Carlo simulation is based on the notion of solving a problem by random sampling. For Value at Risk it is applied by generating a number of random scenarios that might affect an equity value and apply them to a model of that equity in order to estimate the change in the asset value.  The aggregated value of all the estimates represent the predicted variance of the portfolio and the set of these aggregations form the hypothetical distribution of possible outcomes. Selecting the appropriate quantile from this distribution yields the corresponding risk value. This has the advantage of being able to predict situations which have not occurred before, a draw back of alternative methodologies which rely on historical data only.

The following illustrates the improvements in accuracy obtained by using the Monte Carlo Simulation method compared to the Variance-Covariance approach for portfolio over a 14 month period. It shows the predicted figure is only exceeded twice in this period (it should only experience a break once in every 100 days) whereas the alternative method showed an unacceptably high number of 8 breaks.


<p align="center">
<img src="https://github.com/srbaird/mc-var-spark/blob/master/documents/VaRMethodsComparison.jpg" alt="Var Methods Comparison"  >
</p>

___

Running the application requires the installation of Apache Spark and Hadoop FileSystem (HDFS). To support Scala 2.11 a version of Spark later then 2.0 is required and, although not strictly required, Spark should be configured to inherit the Hadoop configuration files using the spark-env.sh file. If required an alternative filesystem location may be supplied using the application configuration file.
The general form of running a valuation on a command line is
```
spark-submit --class main.scala.application.command  <spark configuration commands> path-to-jar/mc-var-spark-n.n.n.jar <application parameters>
```

where  mc-var-spark-n.n.n.jar refers to the version of the application artefact and command is one of the following Scala objects

|        **Command**       |         **<application parameters>**        |
| ------------------------ | ------------------------------------------- |
|CovarianceVar             |config-file portfolio-name at-date           |
|GenerateModels            |config-file asset-code from-date to-date     |
|GenerateModelsForPortfolio|config-file portfolio-name from-date to-date |
|GenerateObservations      |config-file portfolio-name at-date           |
|MonteCarloVar             |config-file portfolio-name at-date           |


All file names should refer to locations within the Hadoop filesystem. An example configuration file may be found [here](https://github.com/srbaird/releases/blob/master/mcs-var-spark/1.2.0/applicationContext).

___

The following is a high level illustration of the project structure and the composition can be best described in the [Spring context file](https://github.com/srbaird/releases/blob/master/mcs-var-spark/1.2.0/application-context.xml)

<p align="center">
<img src="https://github.com/srbaird/mc-var-spark/blob/master/documents/VaRMethodsComparison.jpg" alt="High Level View"  >
</p>

___

Example input data files may be found [here](https://github.com/srbaird/mc-var-spark-resources/tree/master/data). 



