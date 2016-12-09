package be.dataminded.wharlord.test

import be.dataminded.wharlord.constraints.{ConstraintResult, ConstraintStatus}


case class DummyConstraintResult(constraint: DummyConstraint,
                                 message: String,
                                 status: ConstraintStatus) extends ConstraintResult[DummyConstraint]
