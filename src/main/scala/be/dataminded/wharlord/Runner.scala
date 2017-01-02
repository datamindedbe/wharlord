package be.dataminded.wharlord

import be.dataminded.wharlord.reporters.Reporter

/**
  * An object responsible for running checks and producing reports
  */
object Runner {

  /**
    * Run checks and then report to the reporters. Each check will be reported by every reporter.
    *
    * @param checks    An iterable of check objects to be reported
    * @param reporters An iterable of reporters
    * @return Result for every check passed as an argument
    */
  def run(checks: Iterable[Check], reporters: Iterable[Reporter]): Map[Check, CheckResult] = {
    checks.map { check =>
      val potentiallyPersistedDf = check.cacheMethod.map(check.dataFrame.persist).getOrElse(check.dataFrame)
      val constraintResults = check.constraints.map(c => (c, c.fun(potentiallyPersistedDf))).toMap
      val numRows = potentiallyPersistedDf.count
      val checkResult = CheckResult(constraintResults, check, numRows)
      if (check.cacheMethod.isDefined) potentiallyPersistedDf.unpersist()

      reporters.foreach(_.report(checkResult))
      (check, checkResult)
    }.toMap
  }
}
