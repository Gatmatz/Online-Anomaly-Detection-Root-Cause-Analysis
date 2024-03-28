package utils
class Periodic(periodType: Types.PeriodType,
               periodLength: Double,
               task: Runnable) {

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
