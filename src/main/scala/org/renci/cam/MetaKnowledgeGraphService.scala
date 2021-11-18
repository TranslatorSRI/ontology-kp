package org.renci.cam

import org.apache.jena.vocabulary.{OWL, RDF, RDFS, SKOS}
import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.QueryService.{BiolinkIsA, BiolinkMixins}
import org.renci.cam.Utilities.PrefixesMap
import org.renci.cam.domain.{IRI, MetaEdge, MetaKnowledgeGraph, MetaNode}
import zio._

object MetaKnowledgeGraphService {

  private val rdfsSubClassOf = IRI(RDFS.subClassOf.getURI)
  private val owlClass = IRI(OWL.Class.getURI)
  private val skosMappingRelation = IRI(SKOS.mappingRelation.getURI)
  private val skosExactMatch = IRI(SKOS.exactMatch.getURI)
  private val skosNarrowMatch = IRI(SKOS.narrowMatch.getURI)
  private val ClassDefinition = IRI("https://w3id.org/linkml/ClassDefinition")
  private val SlotDefinition = IRI("https://w3id.org/linkml/SlotDefinition")
  private val biolinkCategory = IRI("https://w3id.org/biolink/vocab/category")

  final case class Triple(subject: IRI, predicate: IRI, `object`: IRI)

  def run: ZIO[Has[AppConfig] with HttpClient with Has[PrefixesMap] with Has[Biolink], Throwable, MetaKnowledgeGraph] =
    for {
      (nodesResult, edgesResult) <- nodes.zipPar(edges)
    } yield MetaKnowledgeGraph(nodesResult, edgesResult)

  private def nodes: RIO[Has[AppConfig] with HttpClient, Map[domain.BiolinkTerm, MetaNode]] = {
    final case class Result(blcategory: IRI, ontology: IRI)
    val query =
      sparql"""
            SELECT DISTINCT ?blcategory ?ontology
            WHERE {
              ?blcategory ^$biolinkCategory/${RDFS.isDefinedBy} ?ontology .
              FILTER EXISTS {
                ?blcategory ${RDF.`type`} $ClassDefinition .
              }
            }
            """
    for {
      parsedQuery <- ZIO.effect(query.toQuery)
      results <- SPARQLQueryExecutor.runSelectQueryAs[Result](parsedQuery)
    } yield results
      .groupMap(result => MetaKnowledgeGraphService.makeBiolinkTerm(result.blcategory))(result => oboPrefixFor(result.ontology))
      .map(kv => kv._1 -> MetaNode(kv._2))
  }

  private def oboPrefixFor(iri: IRI): String =
    iri.value.replace("http://purl.obolibrary.org/obo/", "").replace(".owl", "").toUpperCase()

  private def edges: RIO[Has[AppConfig] with HttpClient, List[MetaEdge]] = {
    val query =
      sparql"""
            SELECT DISTINCT (?blSubjectClass AS ?subject) (?slot AS ?predicate) (?blObjectClass AS ?object)
            WHERE {
  	          ?subject_term ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?blSubjectClass .
              ?object_term ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?blObjectClass .
              ?relation ^($skosMappingRelation|$skosExactMatch|$skosNarrowMatch)/($BiolinkIsA|$BiolinkMixins)* ?slot .
              ?slot ${RDF.`type`} $SlotDefinition .
              ?blSubjectClass ${RDF.`type`} $ClassDefinition .
              ?blObjectClass ${RDF.`type`} $ClassDefinition .
              ?subject_term ?relation ?object_term .
            }
            """
    for {
      parsedQuery <- ZIO.effect(query.toQuery)
      edgeResults <- SPARQLQueryExecutor.runSelectQueryAs[Triple](parsedQuery)
    } yield edgeResults.map(t => MetaEdge(makeBiolinkTerm(t.subject), makeBiolinkTerm(t.predicate), makeBiolinkTerm(t.`object`)))
  }

  def makeBiolinkTerm(iri: IRI): domain.BiolinkTerm = {
    val localPart = iri.value.replace(domain.BiolinkTerm.namespace, "")
    domain.BiolinkTerm(localPart, iri)
  }

  private def makeBiolinkClass(name: String): domain.BiolinkTerm = {
    val localPart = Biolink.classNameToLocalPart(name)
    domain.BiolinkTerm(localPart, IRI(s"${domain.BiolinkTerm.namespace}$localPart"))
  }

}
