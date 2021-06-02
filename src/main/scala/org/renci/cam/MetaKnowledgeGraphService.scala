package org.renci.cam

import org.apache.jena.vocabulary.{OWL, RDF, SKOS}
import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.QueryService.{BiolinkIsA, BiolinkMixins}
import org.renci.cam.Utilities.PrefixesMap
import org.renci.cam.domain.{IRI, MetaEdge, MetaKnowledgeGraph, MetaNode}
import zio._

object MetaKnowledgeGraphService {

  private val rdfType = IRI(RDF.`type`.getURI)
  private val owlClass = IRI(OWL.Class.getURI)
  private val skosMappingRelation = IRI(SKOS.mappingRelation.getURI)
  private val skosExactMatch = IRI(SKOS.exactMatch.getURI)
  private val skosNarrowMatch = IRI(SKOS.narrowMatch.getURI)
  private val ClassDefinition = IRI("https://w3id.org/linkml/ClassDefinition")
  private val SlotDefinition = IRI("https://w3id.org/linkml/SlotDefinition")

  final case class Triple(subject: IRI, predicate: IRI, `object`: IRI)

  def run: ZIO[Has[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Throwable, MetaKnowledgeGraph] =
    for {
      (nodesResult, edgesResult) <- nodes.zipPar(edges)
    } yield MetaKnowledgeGraph(nodesResult, edgesResult)

  private def nodes
    : ZIO[Has[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Throwable, Map[domain.BiolinkTerm, MetaNode]] = {
    final case class Result(blcategory: IRI, prefix: String)
    for {
      biolink <- ZIO.service[Biolink]
      prefixesMap <- ZIO.service[PrefixesMap]
      prefixes = prefixesMap.prefixesMap
      biolinkClasses = biolink.classes.keySet.map(makeBiolinkClass)
      biolinkClassValues = biolinkClasses.map(_.iri).map(iri => sparql" $iri ").reduceOption(_ + _).getOrElse(sparql"")
      prefixExpansionValues = prefixes
        .map { case (prefix, expansion) => sparql" ($prefix $expansion) " }
        .reduceOption(_ + _)
        .getOrElse(sparql"")
      query =
        sparql"""
            SELECT DISTINCT ?blcategory ?prefix
            WHERE {
              VALUES ?blcategory { $biolinkClassValues }
              VALUES (?prefix ?expansion) { $prefixExpansionValues }
              ?term ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?blcategory .
              ?term $rdfType $owlClass .
              FILTER(STRSTARTS(STR(?term), ?expansion))
            }
            """
      parsedQuery <- ZIO.effect(query.toQuery)
      results <- SPARQLQueryExecutor.runSelectQueryAs[Result](parsedQuery)
    } yield results
      .groupMap(result => MetaKnowledgeGraphService.makeBiolinkTerm(result.blcategory))(_.prefix)
      .map(kv => kv._1 -> MetaNode(kv._2))
  }

  private def edges: ZIO[Has[AppConfig] with HttpClient, Throwable, List[MetaEdge]] = {
    val query =
      sparql"""
            SELECT DISTINCT (?blSubjectClass AS ?subject) (?slot AS ?predicate) (?blObjectClass AS ?object)
            WHERE {
  	          ?subject_term ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?blSubjectClass .
              ?object_term ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?blObjectClass .
              ?relation ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?slot .
              ?slot $rdfType $SlotDefinition .
              ?blSubjectClass $rdfType $ClassDefinition .
              ?blObjectClass $rdfType $ClassDefinition .
              ?subject_term ?relation ?object_term .
            }
            """
    for {
      parsedQuery <- ZIO.effect(query.toQuery)
      edgeResults <- SPARQLQueryExecutor.runSelectQueryAs[Triple](parsedQuery)
    } yield edgeResults.map(t => MetaEdge(makeBiolinkTerm(t.subject), makeBiolinkTerm(t.predicate), makeBiolinkTerm(t.`object`)))
  }

  private def makeBiolinkTerm(iri: IRI): domain.BiolinkTerm = {
    val localPart = iri.value.replace(domain.BiolinkTerm.namespace, "")
    domain.BiolinkTerm(localPart, iri)
  }

  private def makeBiolinkClass(name: String): domain.BiolinkTerm = {
    val localPart = Biolink.classNameToLocalPart(name)
    domain.BiolinkTerm(localPart, IRI(s"${domain.BiolinkTerm.namespace}$localPart"))
  }

}
