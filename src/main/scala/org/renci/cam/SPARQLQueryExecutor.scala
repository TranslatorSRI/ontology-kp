package org.renci.cam

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.jena.query.{Query, QuerySolution, ResultSet, ResultSetFactory}
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._
import org.phenoscape.sparql.FromQuerySolution
import org.renci.cam.HttpClient.HttpClient
import zio.ZIO.ZIOAutoCloseableOps
import zio.interop.catz._
import zio._

import scala.jdk.CollectionConverters._

object SPARQLQueryExecutor extends LazyLogging {

  implicit val jsonDecoder: EntityDecoder[Task, ResultSet] =
    EntityDecoder.decodeBy(mediaType"application/sparql-results+json") { media =>
      DecodeResult(
        EntityDecoder
          .decodeText(media)
          .map(jsonText => IOUtils.toInputStream(jsonText, StandardCharsets.UTF_8))
          .bracketAuto(input => Task.effect(ResultSetFactory.fromJSON(input)))
          .mapError[DecodeFailure](e => MalformedMessageBodyFailure("Invalid JSON for SPARQL results", Some(e)))
          .either
      )
    }

  implicit val queryEncoder: EntityEncoder[Task, Query] = EntityEncoder[Task, String]
    .withContentType(`Content-Type`(MediaType.application.`sparql-query`))
    .contramap(_.toString)

  def runSelectQueryAs[T: FromQuerySolution](query: Query): RIO[Has[AppConfig] with HttpClient, List[T]] =
    for {
      resultSet <- runSelectQuery(query)
      results = resultSet.solutions.map(FromQuerySolution.mapSolution[T])
      validResults <- ZIO.foreach(results)(ZIO.fromTry(_))
    } yield validResults

  def runSelectQuery(query: Query): RIO[Has[AppConfig] with HttpClient, SelectResult] =
    for {
      appConfig <- config.getConfig[AppConfig]
      client <- HttpClient.client
      uri = appConfig.sparqlEndpoint
      _ = logger.debug("query: {}", query)
      request = Request[Task](Method.POST, uri).withEntity(query)
      response <- client.expect[ResultSet](request)
      vars = response.getResultVars.asScala.to(List)
      results = response.asScala.to(List)
    } yield SelectResult(vars, results)

  final case class SelectResult(vars: List[String], solutions: List[QuerySolution])

}
