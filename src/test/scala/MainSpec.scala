import zio.*
import zio.http.*
import zio.test.*
import zio.test.Assertion.*
import zio.Chunk

object MainSpec extends ZIOSpecDefault {

  def testApp(req: Request): ZIO[Any, Throwable, Response] =
    Main.app.runZIO(req).flatMap {
      case response: Response => ZIO.succeed(response)
    }.orElseFail(new RuntimeException("Request returned None"))

  def spec = suite("MainSpec")(
    test("filter trades within date range") {
      val req = Request.get(URL(Root / "trades").withQueryParams(Map(
        "startDate" -> Chunk("2023-01-02"),
        "endDate"   -> Chunk("2023-01-03")
      )))
      for {
        response <- testApp(req)
        body <- response.body.asString
      } yield {
        assertTrue(response.status == Status.Ok) &&
          assertTrue(body.contains("EUR/USD")) &&
          assertTrue(body.contains("2023-01-02") || body.contains("2023-01-03"))
      }
    },
    test("return empty list for dates outside range") {
      val req = Request.get(URL(Root / "trades").withQueryParams(Map(
        "startDate" -> Chunk("2025-01-01"),
        "endDate"   -> Chunk("2025-01-02")
      )))
      for {
        response <- testApp(req)
        body <- response.body.asString
      } yield assertTrue(body == "[]")
    },
    test("handle missing or invalid date parameters") {
      val req = Request.get(URL(Root / "trades").withQueryParams(Map(
        "startDate" -> Chunk("invalid-date"),
        "endDate"   -> Chunk("2023-01-02")
      )))
      for {
        response <- testApp(req)
      } yield assertTrue(response.status == Status.BadRequest)
    }
  )
}
