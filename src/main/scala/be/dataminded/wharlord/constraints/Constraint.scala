package be.dataminded.wharlord.constraints

import be.dataminded.wharlord.constraints.Constraint.ConstraintFunction
import org.apache.spark.sql.DataFrame

trait Constraint {

  val fun: ConstraintFunction

}

object Constraint {

  type ConstraintFunction = (DataFrame) => ConstraintResult[Constraint]

}
