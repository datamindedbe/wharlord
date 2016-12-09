package be.dataminded.wharlord.constraints

sealed trait ConstraintStatus {
  val stringValue: String
}

object ConstraintSuccess extends ConstraintStatus {
  val stringValue = "Success"
}

object ConstraintFailure extends ConstraintStatus {
  val stringValue = "Failure"
}

case class ConstraintError(throwable: Throwable) extends ConstraintStatus {
  val stringValue = "Error"
}
