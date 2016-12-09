package be.dataminded.wharlord.constraints

import org.apache.spark.sql.{Column, DataFrame}

import scala.util.Try

case class ColumnColumnConstraint(constraintColumn: Column) extends Constraint {

  val fun = (df: DataFrame) => {
    val maybeFailingRows = Try {
      val succeedingRows = df.filter(constraintColumn).count
      df.count - succeedingRows
    }
    ColumnColumnConstraintResult(
      constraint = this,
      data = maybeFailingRows.toOption.map(ColumnColumnConstraintResultData),
      status = tryToStatus[Long](maybeFailingRows, _ == 0)
    )
  }

}

case class ColumnColumnConstraintResult(constraint: ColumnColumnConstraint,
                                        data: Option[ColumnColumnConstraintResultData],
                                        status: ConstraintStatus)
    extends ConstraintResult[ColumnColumnConstraint] {

  val message: String = createColumnConstraintMessage(
    status = status,
    constraintResult = this,
    constraintString = constraint.constraintColumn.toString,
    maybeViolatingRows = data.map(_.failedRows)
  )

}

case class ColumnColumnConstraintResultData(failedRows: Long)
