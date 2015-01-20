import com.twitter.scalding.typed.TypedPipe
import com.twitter.scalding._

/**
 * @author tomerb
 * on 20/01/15.
 */
class WordCountJob(args : Args) extends Job(args) {
  val stat = Stat("alice-counter")
  TypedPipe.from("is alice really alice ?".split(" "))
    .flatMap {
      line => {
        stat.inc
        line.split("""\s+""") }
     }
    .groupBy { word => word }
    .size
    .toTypedPipe
    .write(TypedTsv(args("output")))

}

object MyUniqueID {
  val value = UniqueID.getRandom
}