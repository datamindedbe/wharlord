# Data Minded: Wharlord <img src="https://raw.githubusercontent.com/datamindedbe/wharlord/master/logo/dataminded_avatar.png" alt="Logo" height="25">
## Description

Wharlord is a small library for checking constraints on Spark Data Frames. It can be used to assure a certain data quality, especially when continuous imports happen.

The project is a fork from [Drunken Data Quality or DDQ](https://github.com/FRosner/drunken-data-quality)


## Using Wharlord
### Getting Started

Create two example tables to play with (or use your existing ones).

```scala
import org.apache.spark.sql._
val sparkSession = SparkSession.builder().getOrCreate()
import sparkSession.implicits._

case class Customer(id: Int, name: String)
case class Contract(id: Int, customerId: Int, duration: Int)

val customers = sc.parallelize(List(
  Customer(0, "Frank"),
  Customer(1, "Alex"),
  Customer(2, "Slavo")
)).toDF

val contracts = sc.parallelize(List(
  Contract(0, 0, 5),
  Contract(1, 0, 10),
  Contract(0, 1, 6)
)).toDF
```

Run some checks and see the results on the console.

```scala
import be.dataminded.wharlord._
Check(customers)
  .hasNumRows(_ >= 3)
  .hasUniqueKey("id")
  .run()

Check(contracts)
  .hasNumRows(_ > 0)
  .hasUniqueKey("id", "customerId")
  .satisfies("duration > 0")
  .hasForeignKey(customers, "customerId" -> "id")
  .run()
```

### Custom Reporters

By default the check result will be printed to stdout using ANSI escape codes to highlight the output. To have a report in another format, you can specify one or more custom reporters.

```scala
import be.dataminded.wharlord.reporters.MarkdownReporter
import be.dataminded.wharlord._

Check(customers).hasNumRows(_ >= 3)
                .hasUniqueKey("id")
                .run(MarkdownReporter(System.err))
```

### Running multiple checks

You can use a runner to generate reports for multiple checks at once. It will execute all the checks and report the results to the specified reporters.

```scala
import be.dataminded.wharlord.reporters.ConsoleReporter
import be.dataminded.wharlord.reporters.MarkdownReporter
import be.dataminded.wharlord._
import java.io.{PrintStream, File}

val check1 = Check(customers).hasNumRows(_ >= 3)
                             .hasUniqueKey("id")

val check2 = Check(contracts).hasNumRows(_ > 0)
                             .hasUniqueKey("id", "customerId")
                             .satisfies("duration > 0")
                             .hasForeignKey(customers, "customerId" -> "id")

val consoleReporter = ConsoleReporter(System.out)
val reportStream = new PrintStream(new File("report.md"))
val markdownReporter = MarkdownReporter(reportStream)

Runner.run(Seq(check1, check2), Seq(consoleReporter, markdownReporter))

reportStream.close()
```

### Unit Tests

You can also use Wharlord to write automated quality tests for your data. After running a check or a series of checks, you can inspect the results programmatically.

```scala
def allConstraintsSatisfied(checkResult: CheckResult): Boolean =
  checkResult.constraintResults.map {
    case (constraint, ConstraintSuccess(_)) => true
    case (constraint, ConstraintFailure(_)) => false
  }.reduce(_ && _)

val results = Runner.run(Seq(check1, check2), Seq.empty)
assert(allConstraintsSatisfied(results(check1)))
assert(allConstraintsSatisfied(results(check2)))
assert(allConstraintsSatisfied(results(check2)))
```

If you want to fail the data load if the number of rows and the unique key constraints are not satisfied, but the duration constraint can be violated, you can write individual assertions for each constraint result.

```scala
val numRowsConstraint = Check.hasNumRows(_ >= 3)
val uniqueKeyConstraint = Check.hasUniqueKey("id", "customerId")
val durationConstraint = Check.satisfies("duration > 0")

val check = Check(contracts).addConstraint(numRowsConstraint)
  .addConstraint(uniqueKeyConstraint)
  .addConstraint(durationConstraint)

val results = Runner.run(Seq(check), Seq.empty)
val constraintResults = results(check).constraintResults
assert(constraintResults(numRowsConstraint).isInstanceOf[ConstraintSuccess])
assert(constraintResults(uniqueKeyConstraint).isInstanceOf[ConstraintSuccess])
```

## Documentation
For a comprehensive list of available constraints, please refer to the [Wiki](https://github.com/FRosner/drunken-data-quality/wiki).

## License
This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
