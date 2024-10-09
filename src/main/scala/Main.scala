import zio.*
import zio.http.*
import zio.json.*
import zio.stream.*

import java.nio.file.Paths
import java.nio.file.Files
import java.time.LocalDate
import scala.util.Try

// Define Trade case class and Json Decoder/Encoder
case class Trade(symbol: String, date: String, hour: Int, openbid: Double, highbid: Double, lowbid: Double, closebid: Double, openask: Double, highask: Double, lowask: Double, closeask: Double, totalticks: Int)

object Trade {
  // Custom decoder to map JSON array to Trade object
  def fromArray(arr: List[String]): Either[String, Trade] = {
    Try {
      Trade(
        symbol = arr(0),
        date = arr(1),
        hour = arr(2).toInt,
        openbid = arr(3).toDouble,
        highbid = arr(4).toDouble,
        lowbid = arr(5).toDouble,
        closebid = arr(6).toDouble,
        openask = arr(7).toDouble,
        highask = arr(8).toDouble,
        lowask = arr(9).toDouble,
        closeask = arr(10).toDouble,
        totalticks = arr(11).toInt
      )
    }.toEither.left.map(_.getMessage)
  }
}

given JsonDecoder[Trade] = DeriveJsonDecoder.gen[Trade]
given JsonEncoder[Trade] = DeriveJsonEncoder.gen[Trade]

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
        // Parse the line assuming it's JSON
        line.fromJson[List[String]] match {
          case Right(array) =>
            Trade.fromArray(array) match {
              case Right(trade) => ZIO.succeed(trade)
              case Left(error) => ZIO.fail(new Exception(s"Failed to parse trade from array: $error"))
            }
          case Left(error) =>
            ZIO.fail(new Exception(s"Failed to decode JSON: $error, line: $line"))
        }
      }
      .filter { trade =>
        val tradeDate = LocalDate.parse(trade.date)
        (tradeDate.isEqual(startDate) || tradeDate.isAfter(startDate)) &&
          (tradeDate.isEqual(endDate) || tradeDate.isBefore(endDate))
      }
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
        response = Response.json(filteredTrades.toJson)
      } yield response).catchAll { error =>
        ZIO.succeed(Response.text(s"Error: ${error.toString}").withStatus(Status.InternalServerError))
      }
  }

  override def run =
    Server.serve(app).provide(Server.default)
}
