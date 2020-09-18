package org.renci.cam

import java.util.Properties

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._
import io.circe.generic.auto._
import io.circe.{Decoder, Encoder, Printer}
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.Location
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.{Logger, _}
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.Utilities._
import org.renci.cam.domain._
import sttp.tapir.docs.openapi._
import sttp.tapir.json.circe._
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.server.http4s.ztapir._
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import sttp.tapir.ztapir._
import zio.config.typesafe.TypesafeConfig
import zio.config.{ZConfig, _}
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio.{config => _, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Server extends App {

  object LocalTapirJsonCirce extends TapirJsonCirce {
    override def jsonPrinter: Printer = Printer.spaces2.copy(dropNullValues = true)
  }

  import LocalTapirJsonCirce._

//  val predicatesEndpoint: ZEndpoint[Unit, String, String] = endpoint.get.in("predicates").errorOut(stringBody).out(jsonBody[String])
//
//  val predicatesRouteR: URIO[ZConfig[AppConfig], HttpRoutes[Task]] = predicatesEndpoint.toRoutesR { case () =>
//    val program = for {
//      response <- Task.effect("")
//    } yield response
//    program.mapError(error => error.getMessage)
//  }

  val queryEndpointZ: URIO[Has[PrefixesMap], ZEndpoint[(Option[Int], TRAPIQueryRequestBody), String, TRAPIMessage]] = {
    for {
      prefixes <- biolinkPrefixes
    } yield {
      implicit val iriDecoder: Decoder[IRI] = IRI.makeDecoder(prefixes.prefixesMap)
      implicit val iriEncoder: Encoder[IRI] = IRI.makeEncoder(prefixes.prefixesMap)
      endpoint.post
        .in("query")
        .in(query[Option[Int]]("limit"))
        .in(jsonBody[TRAPIQueryRequestBody])
        .errorOut(stringBody)
        .out(jsonBody[TRAPIMessage])
    }
  }

  def queryRouteR(queryEndpoint: ZEndpoint[(Option[Int], TRAPIQueryRequestBody), String, TRAPIMessage])
    : URIO[ZConfig[AppConfig] with HttpClient with Has[Biolink], HttpRoutes[Task]] =
    queryEndpoint.toRoutesR { case (limit, body) =>
      val program = for {
        queryGraph <-
          ZIO
            .fromOption(body.message.query_graph)
            .orElseFail(new InvalidBodyException("A query graph is required, but hasn't been provided."))
        message <- QueryService.run(queryGraph, limit)
      } yield message
      program.mapError(error => error.getMessage)
    }

  val server: RIO[ZConfig[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Unit] =
    ZIO.runtime[Any].flatMap { implicit runtime =>
      for {
        appConfig <- config[AppConfig]
        queryEndpoint <- queryEndpointZ
        //predicatesRoute <- predicatesRouteR
        queryRoute <- queryRouteR(queryEndpoint)
        routes = queryRoute //<+> predicatesRoute
        // will be available at /docs
        openAPI = List(queryEndpoint).toOpenAPI("SPARQL-KP API", "0.1").toYaml
        docsRoute =
          if (appConfig.subdirectory.isEmpty) new SwaggerHttp4s(openAPI).routes[Task]
          else swaggerRoutes(openAPI, appConfig.subdirectory)
        httpApp = Router("" -> (routes <+> docsRoute)).orNotFound
        httpAppWithLogging = Logger.httpApp(true, false)(httpApp)
        result <-
          BlazeServerBuilder[Task](runtime.platform.executor.asEC)
            .bindHttp(appConfig.port, appConfig.host)
            .withHttpApp(CORS(httpAppWithLogging))
            .withResponseHeaderTimeout(120.seconds)
            .withIdleTimeout(180.seconds)
            .serve
            .compile
            .drain
      } yield result
    }

  val configLayer: Layer[Throwable, ZConfig[AppConfig]] = TypesafeConfig.fromDefaultLoader(AppConfig.config)

  val biolinkLayer: ZLayer[HttpClient with Has[PrefixesMap], Throwable, Has[Biolink]] = Biolink.getBiolinkModel.toLayer

  val prefixesLayer: ZLayer[HttpClient, Throwable, Has[PrefixesMap]] = Utilities.makePrefixesLayer

  override def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
    (for {
      httpClientLayer <- HttpClient.makeHttpClientLayer
      providedPrefixesLayer = httpClientLayer >>> prefixesLayer
      appLayer = httpClientLayer ++ providedPrefixesLayer ++ configLayer ++ ((httpClientLayer ++ providedPrefixesLayer) >>> biolinkLayer)
      out <- server.provideLayer(appLayer)
    } yield out).exitCode

  // hack using SwaggerHttp4s code to handle running in subdirectory
  private def swaggerRoutes(yaml: String, subdirectory: String): HttpRoutes[Task] = {
    val dsl = Http4sDsl[Task]
    import dsl._
    val contextPath = "docs"
    val yamlName = "docs.yaml"
    HttpRoutes.of[Task] {
      case path @ GET -> Root / `subdirectory` / `contextPath` =>
        val queryParameters = Map("url" -> Seq(s"${path.uri}/$yamlName"))
        Uri
          .fromString(s"${path.uri}/index.html")
          .map(uri => uri.setQueryParams(queryParameters))
          .map(uri => PermanentRedirect(Location(uri)))
          .getOrElse(NotFound())
      case GET -> Root / `contextPath` / `yamlName` =>
        Ok(yaml)
      case GET -> Root / `contextPath` / swaggerResource =>
        StaticFile
          .fromResource[Task](
            s"/META-INF/resources/webjars/swagger-ui/$swaggerVersion/$swaggerResource",
            Blocker.liftExecutionContext(ExecutionContext.global)
          )
          .getOrElseF(NotFound())
    }
  }

  private val swaggerVersion = {
    val p = new Properties()
    val pomProperties = getClass.getResourceAsStream("/META-INF/maven/org.webjars/swagger-ui/pom.properties")
    try p.load(pomProperties)
    finally pomProperties.close()
    p.getProperty("version")
  }

}
