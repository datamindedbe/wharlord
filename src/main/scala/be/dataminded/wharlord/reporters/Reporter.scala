package be.dataminded.wharlord.reporters

import be.dataminded.wharlord.CheckResult

trait Reporter {

  def report(checkResult: CheckResult): Unit

}
