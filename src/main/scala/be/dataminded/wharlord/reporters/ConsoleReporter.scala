package be.dataminded.wharlord.reporters

import java.io.PrintStream

import be.dataminded.wharlord.CheckResult
import be.dataminded.wharlord.constraints.{ConstraintError, ConstraintFailure, ConstraintSuccess}

/**
  * A class which produces a console report of [[CheckResult]].
  *
  * @param stream The [[java.io.PrintStream]] to put the output. The stream will not be closed internally and can
  *               be reused.
**/
case class ConsoleReporter(stream: PrintStream = Console.out) extends PrintStreamReporter {

  /**
    * Output console report of a given checkResult to the stream passed to the constructor
    * @param checkResult The [[CheckResult]] to be reported
    */
  override def report(checkResult: CheckResult, header: String, prologue: String): Unit = {
    stream.println(Console.BLUE + header + Console.RESET)
    stream.println(Console.BLUE + prologue + Console.RESET)
    if (checkResult.constraintResults.nonEmpty) {
      checkResult.constraintResults.foreach {
        case (_, constraintResult) =>
          val color = constraintResult.status match {
            case ConstraintSuccess  => Console.GREEN
            case ConstraintFailure  => Console.RED
            case ConstraintError(_) => Console.YELLOW
          }
          stream.println(color + "- " + constraintResult.message + Console.RESET)
      }
    } else {
      stream.println(Console.BLUE + "Nothing to check!" + Console.RESET)
    }
    stream.println("")
  }

}
