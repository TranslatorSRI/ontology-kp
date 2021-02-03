package org.renci.cam

import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.QueryService.RDFSSubClassOf
import org.renci.cam.Utilities.ListOps
import org.renci.cam.domain.IRI
import zio._
import zio.config.ZConfig
//import zio.config.ZConfig
//import scala.concurrent.duration._

object PredicatesService {

  final case class Triple(subject: IRI, predicate: IRI, `object`: IRI)

  def run: ZIO[ZConfig[AppConfig] with HttpClient with Has[Biolink],
               Throwable,
               Map[domain.BiolinkTerm, Map[domain.BiolinkTerm, List[domain.BiolinkTerm]]]] =
    for {
      biolink <- ZIO.service[Biolink]
      slots = mappedSlots(biolink)
      classes = mappedClasses(biolink)
      predicates = slots.values.flatten.to(Set)
      termsToClasses = invert(classes)
      termsToSlots = invert(slots)
      predicateResults <- ZIO.foreachParN(10)(predicates.to(List))(queryPredicate)
      triples = predicateResults.flatten.to(Set)
    } yield {
      val allBiolinkTriples = triples.flatMap { case Triple(subject, predicate, obj) =>
        for {
          subjectClass <- termsToClasses.getOrElse(subject, Nil)
          objectClass <- termsToClasses.getOrElse(obj, Nil)
          predicateSlot <- termsToSlots.getOrElse(predicate, Nil)
        } yield (makeBiolinkClass(subjectClass), makeBiolinkSlot(predicateSlot), makeBiolinkClass(objectClass))
      }
      allBiolinkTriples
        .groupMap(_._1)(t => (t._2, t._3))
        .view
        .mapValues { predicatesAndObjects =>
          predicatesAndObjects.to(List).groupMap(_._2)(_._1)
        }
        .to(Map)
    }

  private def makeBiolinkClass(name: String): domain.BiolinkTerm = {
    val localPart = Biolink.classNameToLocalPart(name)
    domain.BiolinkTerm(localPart, IRI(s"${domain.BiolinkTerm.namespace}$localPart"))
  }

  private def makeBiolinkSlot(name: String): domain.BiolinkTerm = {
    val localPart = Biolink.slotNameToLocalPart(name)
    domain.BiolinkTerm(localPart, IRI(s"${domain.BiolinkTerm.namespace}$localPart"))
  }

  def queryPredicate(predicate: IRI): ZIO[ZConfig[AppConfig] with HttpClient with Has[Biolink], Throwable, List[Triple]] =
    for {
      biolink <- ZIO.service[Biolink]
      classes = mappedClasses(biolink).values.flatten.to(Set)
      classesFilterList = classes.to(List).map(iri => sparql"$iri").intersperse(sparql",").fold(sparql"")(_ + _)
      _ = println(s"Started: $predicate")
      query =
        sparql"""
              SELECT DISTINCT ?subject ($predicate AS ?predicate) ?object
              WHERE {
                ?sub $RDFSSubClassOf ?subject .                                                            
                ?sub $predicate ?object .
                FILTER (?subject IN ($classesFilterList))
                FILTER (?object IN ($classesFilterList))
              }
              """
      parsedQuery <- ZIO.effect(query.toQuery)
      triples <- SPARQLQueryExecutor.runSelectQueryAs[Triple](parsedQuery)
      _ = println(s"Done: $predicate")
    } yield triples

  def mappedClasses(biolink: Biolink): Map[String, List[IRI]] =
    biolink.classes
      .map { case (label, cls) =>
        label -> (cls.exact_mappings.to(List).flatten ++ cls.narrow_mappings.to(List).flatten ++ cls.mappings.to(List).flatten)
      }
      .to(Map)

  def mappedSlots(biolink: Biolink): Map[String, List[IRI]] =
    biolink.slots
      .map { case (label, slot) =>
        label -> (slot.exact_mappings.to(List).flatten ++ slot.narrow_mappings.to(List).flatten ++ slot.mappings.to(List).flatten)
      }
      .to(Map)

  def termToBiolinkIRI(localPart: String): IRI = IRI(s"${domain.BiolinkTerm.namespace}$localPart")

  private def invert[K, V](map: Map[K, List[V]]): Map[V, List[K]] = {
    val pairs = map.to(List).flatMap { case (k, vs) =>
      vs.map(v => k -> v)
    }
    pairs.groupMap(_._2)(_._1)
  }

}
