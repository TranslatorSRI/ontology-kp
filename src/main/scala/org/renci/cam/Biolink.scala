package org.renci.cam

import io.circe.generic.auto._
import io.circe.{yaml, Decoder}
import org.apache.commons.text.CaseUtils
import org.http4s.implicits._
import org.http4s.{Method, Request}
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.Utilities.{biolinkPrefixes, PrefixesMap}
import org.renci.cam.domain.IRI
import zio._
import zio.interop.catz._

final case class BiolinkTerm(is_a: Option[String],
                             mixins: Option[List[String]],
                             subclass_of: Option[IRI],
                             mappings: Option[List[IRI]],
                             exact_mappings: Option[List[IRI]],
                             narrow_mappings: Option[List[IRI]])

final case class Biolink(classes: Map[String, BiolinkTerm], slots: Map[String, BiolinkTerm])

object Biolink {

  def getBiolinkModel: ZIO[HttpClient with Has[PrefixesMap], Throwable, Biolink] =
    for {
      prefixes <- biolinkPrefixes
      httpClient <- HttpClient.client
      uri = uri"https://biolink.github.io/biolink-model/biolink-model.yaml"
      request = Request[Task](Method.GET, uri)
      text <- httpClient.expect[String](request)
      biolinkJson <- ZIO.fromEither(yaml.parser.parse(text))
      biolink <- {
        implicit val iriDecoder: Decoder[IRI] = IRI.makeDecoder(prefixes.prefixesMap)
        ZIO.fromEither(biolinkJson.as[Biolink])
      }
    } yield biolink

  /** Map from a Biolink term name to all external IRIs for it and its descendants
    */
  def mappingsClosure(biolink: Biolink): Map[String, Set[IRI]] = {
    val allTerms = biolink.classes.map { case (key, value) =>
      (CaseUtils.toCamelCase(key, true, ' '), value)
    } ++ biolink.slots.map { case (key, value) =>
      (key.replaceAllLiterally(" ", "_"), value)
    }
    def ancestors(term: String): Set[String] = allTerms.get(term).toSet[BiolinkTerm].flatMap { t =>
      val parents = (t.is_a.toList ::: t.mixins.toList.flatten).toSet
      if (parents.isEmpty) parents
      else parents ++ parents.flatMap(ancestors)
    }
    val reflexiveTermsToDescendantsPairs = for {
      name <- allTerms.keys
      ancestor <- ancestors(name) + name
    } yield ancestor -> name
    val reflexiveTermsToDescendants = reflexiveTermsToDescendantsPairs.groupBy(_._1).mapValues(_.map(_._2).toSet).toList.toMap
    (for {
      (term, descendants) <- reflexiveTermsToDescendants
      descendantBiolinkTerms = descendants.flatMap(d => allTerms.get(d))
      mappings = descendantBiolinkTerms.flatMap(t =>
        t.subclass_of.toList
          ::: t.mappings.toList.flatten
          ::: t.exact_mappings.toList.flatten
          ::: t.narrow_mappings.toList.flatten)
    } yield term -> mappings).toMap
  }

}
