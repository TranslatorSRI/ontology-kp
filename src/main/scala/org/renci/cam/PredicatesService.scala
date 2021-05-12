package org.renci.cam

import org.apache.jena.vocabulary.{RDF, RDFS, SKOS}
import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.domain.IRI
import zio._

object PredicatesService {

  private val rdfType = IRI(RDF.`type`.getURI)
  private val rdfsSubClassOf = IRI(RDFS.subClassOf.getURI)
  private val skosMappingRelation = IRI(SKOS.mappingRelation.getURI)
  private val skosExactMatch = IRI(SKOS.exactMatch.getURI)
  private val skosNarrowMatch = IRI(SKOS.narrowMatch.getURI)
  private val ClassDefinition = IRI("https://w3id.org/linkml/ClassDefinition")

  final case class Triple(subject: IRI, predicate: IRI, `object`: IRI)

  def run: ZIO[Has[AppConfig] with HttpClient with Has[Biolink],
               Throwable,
               Map[domain.BiolinkTerm, Map[domain.BiolinkTerm, List[domain.BiolinkTerm]]]] =
    for {
      biolink <- ZIO.service[Biolink]
      slots = biolink.slots.keys.map(makeBiolinkSlot).map(_.iri)
      predicateResults <- ZIO.foreach(slots.to(List))(queryPredicate)
      triples = predicateResults.flatten.to(Set)
    } yield {
      val allBiolinkTriples = triples.map { case Triple(subject, predicate, obj) =>
        (makeBiolinkTerm(subject), makeBiolinkTerm(predicate), makeBiolinkTerm(obj))
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

  private def makeBiolinkTerm(iri: IRI): domain.BiolinkTerm = {
    val localPart = iri.value.replace(domain.BiolinkTerm.namespace, "")
    domain.BiolinkTerm(localPart, iri)
  }

  private def makeBiolinkSlot(name: String): domain.BiolinkTerm = {
    val localPart = Biolink.slotNameToLocalPart(name)
    domain.BiolinkTerm(localPart, IRI(s"${domain.BiolinkTerm.namespace}$localPart"))
  }

  def queryPredicate(slot: IRI): ZIO[Has[AppConfig] with HttpClient with Has[Biolink], Throwable, List[Triple]] = {
    val query = sparql"""
              SELECT DISTINCT (?subjectClass AS ?subject) ($slot AS ?predicate) (?objectClass AS ?object)
              WHERE {
                ?subjectClass $rdfType $ClassDefinition .
                ?subjectClass ($skosMappingRelation|$skosExactMatch|$skosNarrowMatch) ?subjectTerm .
                ?objectClass $rdfType $ClassDefinition .  
                ?objectClass ($skosMappingRelation|$skosExactMatch|$skosNarrowMatch) ?objectTerm .
                $slot ($skosMappingRelation|$skosExactMatch|$skosNarrowMatch) ?pred .
                ?subjectNode $rdfsSubClassOf ?subjectTerm .
                ?subjectNode ?pred ?objectTerm .
                FILTER(isIRI(?subjectClass))
                FILTER(isIRI(?subjectNode))
                FILTER(isIRI(?subjectTerm))
                FILTER(isIRI(?objectClass))
                FILTER(isIRI(?objectTerm))
              }
              """
    for {
      parsedQuery <- ZIO.effect(query.toQuery)
      _ = println(s"Started: $slot")
      triples <- SPARQLQueryExecutor.runSelectQueryAs[Triple](parsedQuery)
      _ = println(s"Done: $slot")
    } yield triples
  }

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
