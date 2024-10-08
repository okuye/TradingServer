import zio.*
import zio.http.*
import zio.json.*
import zio.stream.*

import java.time.LocalDate
import scala.io.Source
import scala.util.Try

case class Trade(symbol: String, date: String, hour: Int, openbid: Double, highbid: Double, lowbid: Double, closebid: Double, openask: Double, highask: Double, lowask: Double, closeask: Double, totalticks: Int)
case class TradesData(data: List[Trade])

given JsonDecoder[Trade] = DeriveJsonDecoder.gen[Trade]
given JsonDecoder[TradesData] = DeriveJsonDecoder.gen[TradesData]
given JsonEncoder[Trade] = DeriveJsonEncoder.gen[Trade]
given JsonEncoder[TradesData] = DeriveJsonEncoder.gen[TradesData]

def filterTrades(startDate: LocalDate, endDate: LocalDate, trades: List[Trade]): List[Trade] =
  trades.filter { trade =>
    val tradeDate = LocalDate.parse(trade.date)
    (tradeDate.isEqual(startDate) || tradeDate.isAfter(startDate)) &&
      (tradeDate.isEqual(endDate) || tradeDate.isBefore(endDate))
  }

object Main extends ZIOAppDefault:

  val app = Http.collectZIO[Request] {
    case req @ Method.GET -> Root / "trades" =>
      for {
        startDate <- ZIO.fromOption(
          req.url.queryParams.get("startDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption)
        ).orElseFail(Response.text("Missing or invalid startDate").withStatus(Status.BadRequest))

        endDate <- ZIO.fromOption(
          req.url.queryParams.get("endDate").flatMap(_.headOption).flatMap(s => Try(LocalDate.parse(s)).toOption)
        ).orElseFail(Response.text("Missing or invalid endDate").withStatus(Status.BadRequest))

        jsonString <- ZIO.attempt(Source.fromResource("TradesData.json").mkString)
        tradesData <- ZIO.fromEither(jsonString.fromJson[TradesData])
        filteredTrades = filterTrades(startDate, endDate, tradesData.data)
        response = Response.json(TradesData(filteredTrades).toJson)
      } yield response
  }.catchAllCauseZIO { cause =>
    ZIO.succeed(
      Response
        .text(s"Error: ${cause.failureOption.fold("Unknown error")(_.toString)}")
        .withStatus(Status.InternalServerError)
    )
  }
  override def run =
    Server.serve(app).provide(Server.default)
