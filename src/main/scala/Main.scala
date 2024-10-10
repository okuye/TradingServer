import zio.*
import zio.http.*
import zio.json.*
import zio.stream.*

import java.nio.file.{Files, Paths}
import java.time.LocalDate
import scala.math.Ordered.orderingToOrdered
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
        hour = arr(2) match {
          case d: Double => d.toInt
          case i: Int => i
          case other => other.toString.toDouble.toInt
        },
        openbid = arr(3).toString.toDouble,
        highbid = arr(4).toString.toDouble,
        lowbid = arr(5).toString.toDouble,
        closebid = arr(6).toString.toDouble,
        openask = arr(7).toString.toDouble,
        highask = arr(8).toString.toDouble,
        lowask = arr(9).toString.toDouble,
        closeask = arr(10).toString.toDouble,
        totalticks = arr(11) match {
          case d: Double => d.toInt
          case i: Int => i
          case other => other.toString.toDouble.toInt
        }
      )
    }.toEither.left.map(_.getMessage)
  }

  // Add JSON Encoder for Trade
  implicit val encoder: JsonEncoder[Trade] = DeriveJsonEncoder.gen[Trade]
}

// Case class for response columns
case class Column(name: String, `type`: String)

object Column {
  implicit val encoder: JsonEncoder[Column] = DeriveJsonEncoder.gen[Column]
}

// Modify DataTableResponse to use List[List[String]] for specific typing
case class DataTableResponse(data: List[List[String]], columns: List[Column])

object DataTableResponse {
  implicit val encoder: JsonEncoder[DataTableResponse] = DeriveJsonEncoder.gen[DataTableResponse]
}

// Case class for the final response structure
case class TradeResponse(datatable: DataTableResponse, meta: MetaData)

object TradeResponse {
  implicit val encoder: JsonEncoder[TradeResponse] = DeriveJsonEncoder.gen[TradeResponse]
}

// Case class for metadata
case class MetaData(next_cursor_id: Option[String])

object MetaData {
  implicit val encoder: JsonEncoder[MetaData] = DeriveJsonEncoder.gen[MetaData]
}

// Case class for the container of trades
case class Datatable(data: List[List[Any]])

object Datatable {
  implicit val anyDecoder: JsonDecoder[Any] = JsonDecoder.string.map(_.asInstanceOf[Any])
    .orElse(JsonDecoder.double.map(_.asInstanceOf[Any]))
    .orElse(JsonDecoder.int.map(_.asInstanceOf[Any]))
    .orElse(JsonDecoder.boolean.map(_.asInstanceOf[Any]))

  implicit val listOfListDecoder: JsonDecoder[List[List[Any]]] = JsonDecoder.list(JsonDecoder.list(anyDecoder))

  implicit val decoder: JsonDecoder[Datatable] = DeriveJsonDecoder.gen[Datatable]
}

// Case class for the container response
case class DatatableContainer(datatable: Datatable)

object DatatableContainer {
  implicit val decoder: JsonDecoder[DatatableContainer] = DeriveJsonDecoder.gen[DatatableContainer]
}

// Function to process trades from the JSON string
def processTrades(jsonString: String): ZIO[Any, Throwable, List[Trade]] = {
  ZIO.fromEither(jsonString.fromJson[DatatableContainer].left.map(e => new Exception(e)))
    .flatMap { container =>
      ZIO.foreach(container.datatable.data) { data =>
        ZIO.fromEither(Trade.fromArray(data)).mapError(new Exception(_))
      }
    }
}

// Function to convert a list of trades to the response format
def convertTradesToResponse(trades: List[Trade]): TradeResponse = {
  val columns = List(
    Column("symbol", "String"),
    Column("date", "Date"),
    Column("hour", "Integer"),
    Column("openbid", "double"),
    Column("highbid", "double"),
    Column("lowbid", "double"),
    Column("closebid", "double"),
    Column("openask", "double"),
    Column("highask", "double"),
    Column("lowask", "double"),
    Column("closeask", "double"),
    Column("totalticks", "Integer")
  )

  // Convert Trade data to List[List[String]] for specific typing
  val tradeData = trades.map { trade =>
    List(
      trade.symbol,
      trade.date,
      trade.hour.toString,
      trade.openbid.toString,
      trade.highbid.toString,
      trade.lowbid.toString,
      trade.closebid.toString,
      trade.openask.toString,
      trade.highask.toString,
      trade.lowask.toString,
      trade.closeask.toString,
      trade.totalticks.toString
    )
  }

  TradeResponse(
    datatable = DataTableResponse(tradeData, columns),
    meta = MetaData(next_cursor_id = None)
  )
}

// Function to filter trades within a date range
def filterTradesStream(startDate: LocalDate, endDate: LocalDate): ZStream[Any, Throwable, Trade] = {
  val path = Paths.get("src/main/resources/TradesData.json")

  ZStream.scoped {
    ZIO.fromAutoCloseable(ZIO.attemptBlocking(Files.newBufferedReader(path)))
  }.flatMap { reader =>
    ZStream
      .fromReader(reader)
      .mapChunks(ch => Chunk.single(ch.mkString))  // Combine Char chunks into a single String
      .via(ZPipeline.splitLines)  // Split the lines correctly
      .mapZIO { (line: String) =>
        ZIO.logInfo(s"Processing line: $line") *>
          processTrades(line)  // Process all trades from each line
      }
      .mapConcat(identity)  // Flatten the List[Trade] into individual Trade objects
      .filter { trade =>
        val tradeDate = LocalDate.parse(trade.date)
        tradeDate >= startDate && tradeDate <= endDate  // Apply date filtering
      }
      .tap(trade => ZIO.logInfo(s"Filtered trade: $trade"))  // Log filtered trades
  }
}

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
        response = convertTradesToResponse(filteredTrades)
      } yield Response.json(response.toJson)).catchAll { error =>
        ZIO.succeed(Response.text(s"Error: ${error.toString}").withStatus(Status.InternalServerError))
      }
  }

  override def run =
    Server.serve(app).provide(Server.default)
}
