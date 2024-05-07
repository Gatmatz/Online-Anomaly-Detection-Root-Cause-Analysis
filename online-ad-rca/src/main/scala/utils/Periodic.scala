package utils

/**
 * Auxiliary class used for Macrobase' functionalities.
 * @param periodType Tuple or Time based periods.
 * @param periodLength The length of the Period
 * @param task A Runnable task to be executed on certain Periods
 */
class Periodic(periodType: Types.PeriodType,
               periodLength: Double,
               @transient task: Runnable) extends Serializable {

  private var previousPeriod: Double = _
  private var numCalls: Int = _
  private var elapsed: Double = _

  if (periodType == "TIME_BASED")
  {
    previousPeriod = System.currentTimeMillis()
  }

  def runIfNecessary(): Unit = {
    numCalls += 1

    if (periodLength < 0) {
      return
    }

    elapsed = {
      if (periodType == "TIME_BASED") {
        System.currentTimeMillis()
      }
      else {
        numCalls
      }
    }

    while (previousPeriod + periodLength < elapsed)
    {
      task.run()
      previousPeriod += periodLength
    }
  }
}
