package be.dataminded.wharlord.constraints

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Column, DataFrame}

case class NumberOfRowsConstraint(expected: Column) extends Constraint {

  val fun: (DataFrame) => NumberOfRowsConstraintResult = (df: DataFrame) => {
    val countDf = df.agg(count(new Column("*")).as(NumberOfRowsConstraint.countKey))
    val actual = countDf.collect().map(_.getLong(0)).apply(0)
    val satisfied = countDf.select(expected).collect().map(_.getBoolean(0)).apply(0)
    NumberOfRowsConstraintResult(
      constraint = this,
      actual = actual,
      status = if (satisfied) ConstraintSuccess else ConstraintFailure
    )
  }

}

object NumberOfRowsConstraint {

  private[constraints] val countKey: String = "count"

  def apply(expected: Column => Column): NumberOfRowsConstraint = {
    new NumberOfRowsConstraint(expected(new Column(countKey)))
  }

}

case class NumberOfRowsConstraintResult(constraint: NumberOfRowsConstraint,
                                        actual: Long,
                                        status: ConstraintStatus) extends ConstraintResult[NumberOfRowsConstraint] {

  val message: String = {
    val expected = constraint.expected
    status match {
      case ConstraintSuccess => s"The number of rows satisfies $expected."
      case ConstraintFailure => s"The actual number of rows $actual does not satisfy $expected."
      case _ => throw IllegalConstraintResultException(this)
    }
  }

}
