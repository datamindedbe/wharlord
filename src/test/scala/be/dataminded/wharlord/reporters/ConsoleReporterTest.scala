package be.dataminded.wharlord.reporters

import java.io.{ByteArrayOutputStream, PrintStream}

import be.dataminded.wharlord.{Check, CheckResult}
import be.dataminded.wharlord.constraints._
import be.dataminded.wharlord.test.{DummyConstraint, DummyConstraintResult}
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class ConsoleReporterTest extends FlatSpec with Matchers with MockitoSugar {

  "A Console reporter" should "produce correct output for a check with constraints" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = ConsoleReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."

    val message1 = "1"
    val status1 = ConstraintSuccess
    val constraint1 = DummyConstraint(message1, status1)
    val result1 = constraint1.fun(df)

    val message2 = "2"
    val status2 = ConstraintFailure
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df)

    val message3 = "3"
    val status3 = ConstraintError(new IllegalArgumentException())
    val constraint3 = DummyConstraint(message3, status3)
    val result3 = DummyConstraintResult(constraint3, message3, status3)

    val constraints = Map[Constraint, ConstraintResult[Constraint]](
      constraint1 -> result1,
      constraint2 -> result2,
      constraint3 -> result3
    )
    val check = Check(df, "", "", displayName, Option.empty, constraints.keys.toSeq)

    consoleReporter.report(CheckResult(constraints, check, dfCount))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.GREEN}- ${result1.message}${Console.RESET}
${Console.RED}- ${result2.message}${Console.RESET}
${Console.YELLOW}- ${result3.message}${Console.RESET}

"""

    baos.toString shouldBe expectedOutput
  }

  it should "produce correct output for a check without constraint" in {
    val baos = new ByteArrayOutputStream()
    val consoleReporter = ConsoleReporter(new PrintStream(baos))

    val df = mock[DataFrame]
    val displayName = "myDf"
    val dfColumns = Array("1", "2")
    val dfCount = 5
    when(df.columns).thenReturn(dfColumns)

    val header = s"Checking $displayName"
    val prologue = s"It has a total number of ${dfColumns.length} columns and $dfCount rows."
    val check = Check(df, "", "", displayName, Option.empty, Seq.empty)

    consoleReporter.report(CheckResult(Map.empty, check, dfCount))
    val expectedOutput = s"""${Console.BLUE}$header${Console.RESET}
${Console.BLUE}$prologue${Console.RESET}
${Console.BLUE}Nothing to check!${Console.RESET}

"""

    baos.toString shouldBe expectedOutput
  }

}
