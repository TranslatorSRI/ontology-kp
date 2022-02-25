package org.renci.cam

import cats.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.yaml.syntax._
import org.http4s._
import org.http4s.implicits._
import org.http4s.server.Router
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.middleware._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.Utilities._
import org.renci.cam.domain._
import sttp.tapir.docs.openapi._
import sttp.tapir.json.circe._
import sttp.tapir.openapi.circe.yaml._
import sttp.tapir.openapi.{Contact, Info, License}
import sttp.tapir.server.http4s.ztapir._
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import sttp.tapir.ztapir._
import zio._
import zio.config.typesafe.TypesafeConfig
import zio.interop.catz._
import zio.interop.catz.implicits._

import scala.concurrent.duration._

object Server extends App {

  object LocalTapirJsonCirce extends TapirJsonCirce {
    override def jsonPrinter: Printer = Printer.spaces2.copy(dropNullValues = true)
  }

  import LocalTapirJsonCirce._

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

  private val queryExample: TRAPIQuery = {
    val n0Node = TRAPIQueryNode(Some(List(BiolinkTerm("BiologicalProcess", IRI(s"${BiolinkTerm.namespace}BiologicalProcess")))), None)
    val n1Node = TRAPIQueryNode(Some(List(BiolinkTerm("BiologicalProcess", IRI(s"${BiolinkTerm.namespace}BiologicalProcess")))), None)
    val e0Edge = TRAPIQueryEdge(Some(List(BiolinkTerm("part_of", IRI(s"${BiolinkTerm.namespace}part_of")))), "n1", "n0")
    val queryGraph = TRAPIQueryGraph(Map("n0" -> n0Node, "n1" -> n1Node), Map("e0" -> e0Edge))
    val message = TRAPIMessage(Some(queryGraph), None, None)
    TRAPIQuery(message, None)
  }

  val queryEndpointZ: URIO[Has[PrefixesMap], ZEndpoint[(Option[Int], TRAPIQuery), String, TRAPIResponse]] = {
    for {
      prefixes <- biolinkPrefixes
    } yield {
      implicit val iriDecoder: Decoder[IRI] = IRI.makeDecoder(prefixes.prefixesMap)
      implicit val iriEncoder: Encoder[IRI] = IRI.makeEncoder(prefixes.prefixesMap)
      implicit val iriKeyDecoder: KeyDecoder[IRI] = IRI.makeKeyDecoder(prefixes.prefixesMap)
      implicit val iriKeyEncoder: KeyEncoder[IRI] = IRI.makeKeyEncoder(prefixes.prefixesMap)
      endpoint.post
        .in("query")
        .in(query[Option[Int]]("limit").example(Some(10)))
        .in(jsonBody[TRAPIQuery].example(queryExample))
        .errorOut(stringBody)
        .out(jsonBody[TRAPIResponse])
        .summary("Submit a TRAPI question graph and retrieve matching solutions")
    }
  }

  def queryRouteR(queryEndpoint: ZEndpoint[(Option[Int], TRAPIQuery), String, TRAPIResponse])
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

  private val cors = CORS.policy.withAllowOriginAll.withAllowCredentials(false)

  val server: RIO[Has[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Unit] =
    ZIO.runtime[Any].flatMap { implicit runtime =>
      for {
        appConfig <- config.getConfig[AppConfig]
        biolink <- ZIO.service[Biolink]
        queryEndpoint <- queryEndpointZ
        metaKGEndpoint <- metaKnowlegeGraphEndpoint
        metaKnowledgeGraphRoute <- metaKnowledgeGraphRouteR(metaKGEndpoint)
        queryRoute <- queryRouteR(queryEndpoint)
        routes = queryRoute <+> metaKnowledgeGraphRoute
        // will be available at /docs
        openAPI = List(queryEndpoint, metaKGEndpoint)
          .toOpenAPI("Ontology-KP API", "0.1")
          .copy(info = openAPIInfo)
          .copy(tags = List(sttp.tapir.openapi.Tag("translator"), sttp.tapir.openapi.Tag("trapi")))
          .servers(List(sttp.tapir.openapi.Server(appConfig.basePath)))
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
                    "version": "1.2.0",
                    "operations": [ "lookup" ]
                  }
                }
             }
          """
        infoJson <- ZIO.fromEither(io.circe.parser.parse(info))
        openAPIExtended = infoJson.deepMerge(openAPIJson).asYaml.spaces2
        docsRoute = new SwaggerHttp4s(openAPIExtended).routes[Task]
        httpApp = Router("" -> (routes <+> docsRoute)).orNotFound
        httpAppWithLogging = Logger.httpApp(true, false)(httpApp)
        result <-
          BlazeServerBuilder[Task](runtime.platform.executor.asEC)
            .bindHttp(appConfig.port, appConfig.host)
            .withHttpApp(cors(httpAppWithLogging))
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

}
