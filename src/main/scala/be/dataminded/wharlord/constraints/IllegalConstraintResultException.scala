package be.dataminded.wharlord.constraints

case class IllegalConstraintResultException(constraintResult: ConstraintResult[Constraint]) extends Exception(
  s"Constraint result is in an illegal state: $constraintResult"
)
