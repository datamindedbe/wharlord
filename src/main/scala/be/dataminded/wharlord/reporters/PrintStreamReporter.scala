package be.dataminded.wharlord.reporters

import java.io.PrintStream

import be.dataminded.wharlord.CheckResult

abstract class PrintStreamReporter extends Reporter {

  val stream: PrintStream

  override def report(checkResult: CheckResult): Unit = {
    val check = checkResult.check
    val df = check.dataFrame
    report(
      checkResult = checkResult,
      header = s"Checking ${check.name}",
      prologue = s"It has a total number of ${df.columns.length} columns " +
        s"and ${checkResult.numRows} rows."
    )
  }

  protected def report(checkResult: CheckResult, header: String, prologue: String)

}
