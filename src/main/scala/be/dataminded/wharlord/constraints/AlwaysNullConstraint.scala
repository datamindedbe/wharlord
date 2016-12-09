package be.dataminded.wharlord.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class AlwaysNullConstraint(columnName: String) extends Constraint {

  override val fun: (DataFrame) => AlwaysNullConstraintResult = (df: DataFrame) => {
    val tryNotNullCount = Try(df.filter(new Column(columnName).isNotNull).count)
    AlwaysNullConstraintResult(
      constraint = this,
      status = tryToStatus[Long](tryNotNullCount, _ == 0),
      data = tryNotNullCount.toOption.map(AlwaysNullConstraintResultData)
    )
  }

}

case class AlwaysNullConstraintResult(constraint: AlwaysNullConstraint,
                                      status: ConstraintStatus,
                                      data: Option[AlwaysNullConstraintResultData])
    extends ConstraintResult[AlwaysNullConstraint] {

  val message: String = {
    val columnName = constraint.columnName
    val maybeNonNullRows = data.map(_.nonNullRows)
    val maybePluralS = maybeNonNullRows.map(n => if (n == 1) "" else "s")
    (status, maybeNonNullRows, maybePluralS) match {
      case (ConstraintError(throwable), None, None) =>
        s"Checking column $columnName for being always null failed: $throwable"
      case (ConstraintSuccess, Some(0), Some(_)) =>
        s"Column $columnName is always null."
      case (ConstraintFailure, Some(nonNullRows), Some(pluralS)) =>
        s"Column $columnName contains $nonNullRows non-null row$pluralS (should always be null)."
      case _ => throw IllegalConstraintResultException(this)
    }
  }

}

case class AlwaysNullConstraintResultData(nonNullRows: Long)
