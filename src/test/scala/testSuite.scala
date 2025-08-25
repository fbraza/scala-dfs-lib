import org.scalatest._

class MasterTestSuite extends Stepwise(
  Sequential(
    // FileOps tests
    new TestTouch,
    new TestMkdir,
    new TestMv,
    new TestMvInto,
    new TestMvOver,
    new TestRm,
    new TestRmr,
    // StatOps tests  
    new TestSize,
    new TestReplication,
    new TestBlockSize,
    new TestGetPath,
    new TestStat
  )
)