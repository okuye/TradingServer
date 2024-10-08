import zio.*
import zio.http.*
import zio.json.*
import zio.stream.*

import java.io.FileNotFoundException
import java.time.LocalDate
import scala.util.Try


case class Trade(symbol: String, date: String, hour: Int, openbid: Double, highbid: Double, lowbid: Double, closebid: Double, openask: Double, highask: Double, lowask: Double, closeask: Double, totalticks: Int)
case class TradesData(data: List[Trade])

given JsonDecoder[Trade] = DeriveJsonDecoder.gen[Trade]
given JsonEncoder[Trade] = DeriveJsonEncoder.gen[Trade]

def filterTradesStream(startDate: LocalDate, endDate: LocalDate): ZStream[Any, Throwable, Trade] = {
  ZStream.fromZIO(ZIO.attemptBlockingIO {
    Option(getClass.getResourceAsStream("/TradesData.json")).getOrElse(
      throw new FileNotFoundException("TradesData.json not found in resources")
    )
  }).flatMap { inputStream =>
    ZStream.fromInputStream(inputStream)
      .via(ZPipeline.utf8Decode)
      .via(ZPipeline.splitLines)
      .mapZIO { line =>
        ZIO.fromEither(line.fromJson[Trade].left.map(new Exception(_)))
      }
      .filter { trade =>
        val tradeDate = LocalDate.parse(trade.date)
        (tradeDate.isEqual(startDate) || tradeDate.isAfter(startDate)) &&
          (tradeDate.isEqual(endDate) || tradeDate.isBefore(endDate))
      }
  }
}

object Main extends ZIOAppDefault:


  val app = Http.collectZIO[Request] {
    case req@Method.GET -> Root / "trades" =>
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


//  val app = Http.collectZIO[Request] {
//    case req@Method.GET -> Root / "trades" =>
//      (for {
//        startDate <- ZIO.fromOption(req.url.queryParams.get("startDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption))
//          .orElseFail(Response.text("Missing or invalid startDate").withStatus(Status.BadRequest))
//        endDate <- ZIO.fromOption(req.url.queryParams.get("endDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption))
//          .orElseFail(Response.text("Missing or invalid endDate").withStatus(Status.BadRequest))
//        tradesStream = filterTradesStream(startDate, endDate)
//        filteredTrades <- tradesStream.runCollect.map(_.toList)
//        response = Response.json(filteredTrades.toJson)
//      } yield response).catchAll { error =>
//        ZIO.succeed(Response.text(s"Error: ${error.toString}").withStatus(Status.InternalServerError))
//      }
//  }

  override def run =
    Server.serve(app).provide(Server.default)

