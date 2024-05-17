package utils.itemset
import org.junit.Assert.assertEquals
import org.junit.Test

class RiskRatioTest {

  @Test
  def simpleRatioTest(): Unit = {
    assertEquals(1.0, RiskRatio.compute(10,10,100,100).get(), 0.01)
    assertEquals(6, RiskRatio.compute(10, 10, 1000, 100).get(), 0.01)
    assertEquals(900.082, RiskRatio.compute(10, 99, 1000, 100).get(), 0.01)
  }

  @Test
  def testRatioBoundaryConditions(): Unit = {
    // Test with no exposure
    assertEquals(0, RiskRatio.compute(0, 0, 100, 100).get(), 0)

    // Test with all exposed
    assertEquals(0, RiskRatio.compute(100, 100, 100, 100).get(), 0)

    // Test with event only found in exposed
    assertEquals(Double.PositiveInfinity, RiskRatio.compute(0, 100, 100, 100).get(), 0)
    assertEquals(Double.PositiveInfinity, RiskRatio.compute(-1.0, 100, 100, 100).get(), 0)

    // Test with event never found in exposed
    assertEquals(0, RiskRatio.compute(100, 0, 1000, 100).get(), 0)
    assertEquals(0, RiskRatio.compute(100, -1.0, 1000, 100).get(), 0)

    // Test the handling of nulls and all zeros
    assertEquals(0, RiskRatio.compute(-1.0, -1.0, -1.0, -1.0).get(), 0)
  }
}
