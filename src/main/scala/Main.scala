import zio.*
import zio.http.*
import zio.json.*
import zio.stream.*

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.util.Try

// Define the Trade case class
case class Trade(
                  symbol: String,
                  date: String,
                  hour: Int,
                  openbid: Double,
                  highbid: Double,
                  lowbid: Double,
                  closebid: Double,
                  openask: Double,
                  highask: Double,
                  lowask: Double,
                  closeask: Double,
                  totalticks: Int
                )

object Trade {
  def fromArray(arr: List[Any]): Either[String, Trade] = {
    Try {
      Trade(
        symbol = arr(0).toString,
        date = arr(1).toString,
        hour = arr(2).toString.toDouble.toInt,  // Handle hour as a double and convert to int
        openbid = arr(3).toString.toDouble,
        highbid = arr(4).toString.toDouble,
        lowbid = arr(5).toString.toDouble,
        closebid = arr(6).toString.toDouble,
        openask = arr(7).toString.toDouble,
        highask = arr(8).toString.toDouble,
        lowask = arr(9).toString.toDouble,
        closeask = arr(10).toString.toDouble,
        totalticks = arr(11).toString.toDouble.toInt
      )
    }.toEither.left.map(_.getMessage)
  }

  implicit val encoder: JsonEncoder[Trade] = DeriveJsonEncoder.gen[Trade]
}

// Define Datatable and DatatableContainer
case class Datatable(data: List[List[Any]])

object Datatable {

  // Custom decoder for `Any`
  implicit val anyDecoder: JsonDecoder[Any] = JsonDecoder.string.map(_.asInstanceOf[Any])
    .orElse(JsonDecoder.double.map(_.asInstanceOf[Any]))
    .orElse(JsonDecoder.int.map(_.asInstanceOf[Any]))
    .orElse(JsonDecoder.boolean.map(_.asInstanceOf[Any]))

  // Custom decoder for List[List[Any]]
  implicit val listOfListDecoder: JsonDecoder[List[List[Any]]] = JsonDecoder.list(JsonDecoder.list(anyDecoder))

  // Custom decoder for Datatable
  implicit val decoder: JsonDecoder[Datatable] = DeriveJsonDecoder.gen[Datatable]
}

case class DatatableContainer(datatable: Datatable)

object DatatableContainer {
  implicit val decoder: JsonDecoder[DatatableContainer] = DeriveJsonDecoder.gen[DatatableContainer]
}

// Process Trades based on the provided JSON structure
def processTrades(jsonString: String): ZIO[Any, Throwable, List[Trade]] = {
  ZIO.fromEither(jsonString.fromJson[DatatableContainer].left.map(e => new Exception(e)))
    .flatMap { container =>
      ZIO.foreach(container.datatable.data) { data =>
        ZIO.fromEither(Trade.fromArray(data)).mapError(new Exception(_))
      }
    }
}

// Filtering stream of trades between date ranges
def filterTradesStream(startDate: LocalDate, endDate: LocalDate): ZStream[Any, Throwable, Trade] = {
  val path = Paths.get("src/main/resources/TradesData.json")

  ZStream.scoped {
    ZIO.fromAutoCloseable(
      ZIO.attemptBlocking(Files.newBufferedReader(path))
    )
  }.flatMap { reader =>
    ZStream
      .fromReader(reader)  // Work directly with Char stream
      .mapChunks(ch => Chunk.single(ch.mkString))  // Combine Char chunks into a single String
      .via(ZPipeline.splitLines)  // Now we can split the lines correctly
      .mapZIO { (line: String) =>
        processTrades(line).map(_.headOption)  // Parsing the trades from each line
      }
      .collect { case Some(trade) => trade }
      .filter { trade =>
        val tradeDate = LocalDate.parse(trade.date)
        (tradeDate.isEqual(startDate) || tradeDate.isAfter(startDate)) &&
          (tradeDate.isEqual(endDate) || tradeDate.isBefore(endDate))
      }
  }
}

// Main entry point to run the HTTP server
object Main extends ZIOAppDefault {

  val app = Http.collectZIO[Request] {
    case req @ Method.GET -> Root / "trades" =>
      (for {
        startDate <- ZIO.fromOption(req.url.queryParams.get("startDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption))
          .orElseFail(Response.text("Missing or invalid startDate").withStatus(Status.BadRequest))
        endDate <- ZIO.fromOption(req.url.queryParams.get("endDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption))
          .orElseFail(Response.text("Missing or invalid endDate").withStatus(Status.BadRequest))
        tradesStream = filterTradesStream(startDate, endDate)
        filteredTrades <- tradesStream.runCollect.map(_.toList)
        response = Response.json(filteredTrades.toJson)
      } yield response).catchAll { error =>
        ZIO.succeed(Response.text(s"Error: ${error.toString}").withStatus(Status.InternalServerError))
      }
  }

  override def run =
    Server.serve(app).provide(Server.default)
}
