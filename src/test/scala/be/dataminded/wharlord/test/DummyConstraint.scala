package be.dataminded.wharlord.test

import be.dataminded.wharlord.constraints.{Constraint, ConstraintStatus}
import org.apache.spark.sql.DataFrame

case class DummyConstraint(message: String, status: ConstraintStatus) extends Constraint {

  override val fun: (DataFrame) => DummyConstraintResult = (df: DataFrame) => DummyConstraintResult(this, message, status)

}
