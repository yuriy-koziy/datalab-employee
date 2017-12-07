
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.datalab.employee.features.Features
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

/**
  * Created by Yuriy Koziy on 12/7/17.
  */
class FeatureTest extends FunSuite with Checkers {

    test("run") {
        Features.main(Array("-i", "/Users/yura/Downloads/BD_LAB_EXAMPLE_SAMPLE.csv", "-o", "/tmp"))
    }
}
