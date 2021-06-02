package org.renci.cam

import java.util.Properties
import cats.effect.Blocker
import cats.implicits._
import io.circe.generic.auto._
import io.circe.yaml.syntax._
import io.circe._
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
import sttp.tapir.Endpoint
import sttp.tapir.docs.openapi._
import sttp.tapir.json.circe._
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.openapi.{Contact, Info, License}
import sttp.tapir.server.http4s.ztapir._
import sttp.tapir.ztapir._
import zio.config.typesafe.TypesafeConfig
import zio.interop.catz._
import zio.interop.catz.implicits._
import zio._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object Server extends App {

  object LocalTapirJsonCirce extends TapirJsonCirce {
    override def jsonPrinter: Printer = Printer.spaces2.copy(dropNullValues = true)
  }

  import LocalTapirJsonCirce._

  val predicatesEndpoint: ZEndpoint[Unit, String, Map[BiolinkTerm, Map[BiolinkTerm, List[BiolinkTerm]]]] =
    endpoint.get
      .in("predicates")
      .errorOut(stringBody)
      .out(jsonBody[Map[BiolinkTerm, Map[BiolinkTerm, List[BiolinkTerm]]]])
      .summary("Get predicates used at this service")

  val predicatesRouteR: ZIO[Has[AppConfig] with HttpClient with Has[Biolink], Throwable, HttpRoutes[Task]] =
    for {
      cachedResponse <- PredicatesService.run
      ret <- predicatesEndpoint.toRoutesR { case () =>
        ZIO.effect(cachedResponse).mapError(error => error.getMessage)
      }
    } yield ret

  val metaKnowlegeGraphEndpoint: URIO[Has[PrefixesMap], ZEndpoint[Unit, String, MetaKnowledgeGraph]] = {
    for {
      prefixes <- biolinkPrefixes
    } yield {
      implicit val iriDecoder: Decoder[IRI] = IRI.makeDecoder(prefixes.prefixesMap)
      implicit val iriEncoder: Encoder[IRI] = IRI.makeEncoder(prefixes.prefixesMap)
      implicit val iriKeyDecoder: KeyDecoder[IRI] = IRI.makeKeyDecoder(prefixes.prefixesMap)
      implicit val iriKeyEncoder: KeyEncoder[IRI] = IRI.makeKeyEncoder(prefixes.prefixesMap)
      endpoint.get
        .in("meta_knowledge_graph")
        .errorOut(stringBody)
        .out(jsonBody[MetaKnowledgeGraph])
        .summary("Meta knowledge graph representation of this TRAPI web service.")
    }
  }

  def metaKnowledgeGraphRouteR(endpoint: ZEndpoint[Unit, String, MetaKnowledgeGraph])
    : ZIO[Has[AppConfig] with HttpClient with Has[Biolink] with Has[PrefixesMap], Throwable, HttpRoutes[Task]] =
    for {
      cachedResponse <- MetaKnowledgeGraphService.run
      ret <- endpoint.toRoutesR { case () =>
        ZIO.effectTotal(cachedResponse)
      }
    } yield ret

  val queryEndpointZ: URIO[Has[PrefixesMap], ZEndpoint[(Option[Int], TRAPIQueryRequestBody), String, TRAPIResponse]] = {
    for {
      prefixes <- biolinkPrefixes
    } yield {
      implicit val iriDecoder: Decoder[IRI] = IRI.makeDecoder(prefixes.prefixesMap)
      implicit val iriEncoder: Encoder[IRI] = IRI.makeEncoder(prefixes.prefixesMap)
      implicit val iriKeyDecoder: KeyDecoder[IRI] = IRI.makeKeyDecoder(prefixes.prefixesMap)
      implicit val iriKeyEncoder: KeyEncoder[IRI] = IRI.makeKeyEncoder(prefixes.prefixesMap)
      endpoint.post
        .in("query")
        .in(query[Option[Int]]("limit"))
        .in(jsonBody[TRAPIQueryRequestBody])
        .errorOut(stringBody)
        .out(jsonBody[TRAPIResponse])
        .summary("Submit a TRAPI question graph and retrieve matching solutions")
    }
  }

  def queryRouteR(queryEndpoint: ZEndpoint[(Option[Int], TRAPIQueryRequestBody), String, TRAPIResponse])
    : URIO[Has[AppConfig] with HttpClient with Has[Biolink], HttpRoutes[Task]] =
    queryEndpoint.toRoutesR { case (limit, body) =>
      val program = for {
        queryGraph <-
          ZIO
            .fromOption(body.message.query_graph)
            .orElseFail(InvalidBodyException("A query graph is required, but hasn't been provided."))
        message <- QueryService.run(queryGraph, limit)
      } yield message
      program.mapError(error => error.getMessage)
    }

  val openAPIInfo: Info = Info(
    "Ontology-KP API",
    "0.1",
    Some("TRAPI interface to integrated ontology knowledgebase"),
    Some("https://opensource.org/licenses/MIT"),
    Some(Contact(Some("Jim Balhoff"), Some("balhoff@renci.org"), None)),
    Some(License("MIT License", Some("https://opensource.org/licenses/MIT")))
  )

  val server: RIO[Has[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Unit] =
    ZIO.runtime[Any].flatMap { implicit runtime =>
      for {
        appConfig <- config.getConfig[AppConfig]
        biolink <- ZIO.service[Biolink]
        queryEndpoint <- queryEndpointZ
        metaKGEndpoint <- metaKnowlegeGraphEndpoint
        predicatesRoute <- predicatesRouteR
        metaKnowledgeGraphRoute <- metaKnowledgeGraphRouteR(metaKGEndpoint)
        queryRoute <- queryRouteR(queryEndpoint)
        routes = queryRoute <+> predicatesRoute <+> metaKnowledgeGraphRoute
        // will be available at /docs
        openAPI = List(queryEndpoint, predicatesEndpoint, metaKGEndpoint)
          .toOpenAPI("Ontology-KP API", "0.1")
          .copy(info = openAPIInfo)
          .copy(tags = List(sttp.tapir.openapi.Tag("translator")))
          .servers(List(sttp.tapir.openapi.Server(appConfig.location)))
          .toYaml
        openAPIJson <- ZIO.fromEither(io.circe.yaml.parser.parse(openAPI))
        info: String = s"""
             {
                "info": {
                  "x-translator": {
                    "component": "KP",
                    "team": [ "Standards Reference Implementation Team" ],
                    "biolink-version": "${biolink.version}"
                  },
                  "x-trapi": {
                    "version": "1.1.0"
                  }
                }
             }
          """
        infoJson <- ZIO.fromEither(io.circe.parser.parse(info))
        openAPIExtended = infoJson.deepMerge(openAPIJson).asYaml.spaces2
        docsRoute = swaggerRoutes(openAPIExtended)
        httpApp = Router("" -> (routes <+> docsRoute)).orNotFound
        httpAppWithLogging = Logger.httpApp(true, false)(httpApp)
        result <-
          BlazeServerBuilder[Task](runtime.platform.executor.asEC)
            .bindHttp(appConfig.port, appConfig.host)
            .withHttpApp(CORS(httpAppWithLogging))
            .withResponseHeaderTimeout(800.seconds)
            .withIdleTimeout(900.seconds)
            .serve
            .compile
            .drain
      } yield result
    }

  val configLayer: Layer[Throwable, Has[AppConfig]] = TypesafeConfig.fromDefaultLoader(AppConfig.config)

  val biolinkLayer: ZLayer[HttpClient with Has[PrefixesMap], Throwable, Has[Biolink]] = Biolink.getBiolinkModel.toLayer

  val prefixesLayer: ZLayer[HttpClient, Throwable, Has[PrefixesMap]] = Utilities.makePrefixesLayer

  override def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
    (for {
      httpClientLayer <- HttpClient.makeHttpClientLayer
      appLayer = httpClientLayer >+> prefixesLayer >+> biolinkLayer ++ configLayer
      out <- server.provideLayer(appLayer)
    } yield out).exitCode

  // hack using SwaggerHttp4s code to handle running in subdirectory
  private def swaggerRoutes(yaml: String): HttpRoutes[Task] = {
    val dsl = Http4sDsl[Task]
    import dsl._
    val contextPath = "docs"
    val yamlName = "docs.yaml"
    HttpRoutes.of[Task] {
      case path @ GET -> Root / `contextPath` =>
        val queryParameters = Map("url" -> Seq(s"$yamlName"))
        Uri
          .fromString(s"$contextPath/index.html")
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
