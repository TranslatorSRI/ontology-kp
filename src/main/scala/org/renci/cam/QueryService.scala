package org.renci.cam

import org.apache.jena.query.Query
import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.SPARQLQueryExecutor.SelectResult
import org.renci.cam.domain._
import zio._
import zio.config.ZConfig

object QueryService {

  val RDFSSubClassOf: IRI = IRI("http://www.w3.org/2000/01/rdf-schema#subClassOf")
  val RDFSLabel: IRI = IRI("http://www.w3.org/2000/01/rdf-schema#label")

  type NodeMap = Map[String, (TRAPIQueryNode, String, QueryText)]
  type EdgeMap = Map[String, (TRAPIQueryEdge, String, QueryText)]

  def run(queryGraph: TRAPIQueryGraph, limit: Option[Int]): RIO[ZConfig[AppConfig] with HttpClient with Has[Biolink], TRAPIMessage] =
    for {
      knownPredicates <- getKnownPredicates
      biolink <- ZIO.service[Biolink]
      (nodeMap, edgeMap, query) = makeSPARQL(queryGraph, biolink, limit, knownPredicates)
      result <- SPARQLQueryExecutor.runSelectQuery(query)
    } yield makeResultMessage(result, queryGraph, nodeMap, edgeMap)

  def makeResultMessage(results: SelectResult, queryGraph: TRAPIQueryGraph, nodeMap: NodeMap, edgeMap: EdgeMap): TRAPIMessage = {
    val (trapiResults, nodes, edges) = results.solutions.map { solution =>
      val (trapiNodeBindings, trapiNodes) = queryGraph.nodes
        .toSet[TRAPIQueryNode]
        .map { queryNode =>
          val (_, queryVar, _) = nodeMap(queryNode.id)
          val nodeIRI = IRI(solution.getResource(queryVar).getURI)
          val nameOpt = Option(solution.getLiteral(s"${queryVar}_label")).map(_.getLexicalForm)
          val trapiNode = TRAPINode(nodeIRI, nameOpt, queryNode.`type`.toList)
          val trapiNodeBinding = TRAPINodeBinding(Some(queryNode.id), nodeIRI)
          (trapiNodeBinding, trapiNode)
        }
        .unzip
      val (trapiEdgeBindings, trapiEdges) =
        queryGraph.edges
          .toSet[TRAPIQueryEdge]
          .map { queryEdge =>
            val (_, sourceVar, _) = nodeMap(queryEdge.source_id)
            val (_, targetVar, _) = nodeMap(queryEdge.target_id)
            val (_, predicateVar, _) = edgeMap(queryEdge.id)
            val sourceIRI = IRI(solution.getResource(sourceVar).getURI)
            val targetIRI = IRI(solution.getResource(targetVar).getURI)
            val predicateIRI = IRI(solution.getResource(predicateVar).getURI)
            val edgeKGID = s"${sourceIRI.value}${predicateIRI.value}${targetIRI.value}"
            val trapiEdge = TRAPIEdge(edgeKGID, sourceIRI, targetIRI, queryEdge.`type`)
            val trapiEdgeBinding = TRAPIEdgeBinding(Some(queryEdge.id), edgeKGID)
            (trapiEdgeBinding, trapiEdge)
          }
          .unzip
      val trapiResult = TRAPIResult(trapiNodeBindings.toList, trapiEdgeBindings.toList)
      (trapiResult, trapiNodes, trapiEdges)
    }.unzip3
    val kg = TRAPIKnowledgeGraph(nodes.toSet.flatten.toList, edges.toSet.flatten.toList)
    TRAPIMessage(Some(queryGraph), Some(kg), Some(trapiResults))
  }

  def makeSPARQL(queryGraph: TRAPIQueryGraph,
                 biolink: Biolink,
                 limit: Option[Int],
                 knownPredicates: Set[IRI]): (NodeMap, EdgeMap, Query) = {
    val mappingsClosure = Biolink.mappingsClosure(biolink)
    val nodesToVariables = queryGraph.nodes.zipWithIndex.map { case (node, index) =>
      val nodeVarName = s"n$index"
      val nodeVar = QueryText(s"?$nodeVarName")
      val nodeLabelVar = QueryText(s"?${nodeVarName}_label")
      val nodeSPARQL = if (node.curie.isEmpty) {
        node.`type`
          .map { blt =>
            if (blt.shorthand == "named_thing") sparql"$nodeVar $RDFSLabel $nodeLabelVar ."
            else {
              val nodeSuperVar = QueryText(s"?${nodeVarName}_super")
              val mappings = mappingsClosure.get(blt.shorthand.replaceAllLiterally("_", " ")).toSet.flatten //FIXME this is hacky
              val values = mappings.map(term => sparql"$term ").reduceOption(_ + _).getOrElse(sparql"")
              sparql"""
                $nodeVar $RDFSLabel $nodeLabelVar . 
                FILTER EXISTS {
                  $nodeVar $RDFSSubClassOf $nodeSuperVar .
                  VALUES $nodeSuperVar { $values }
                }
              """
            }
          }
          .getOrElse(sparql"")
      } else
        node.curie
          .map { c =>
            sparql"""
              $nodeVar $RDFSLabel $nodeLabelVar . 
              VALUES $nodeVar { $c }
            """
          }
          .getOrElse(sparql"")
      node.id -> (node, nodeVarName, nodeSPARQL)
    }.toMap
    val nodeSPARQL = nodesToVariables.values.map(_._3).reduceOption(_ + _).getOrElse(sparql"")
    val edgesToVariables = queryGraph.edges.zipWithIndex.map { case (edge, index) =>
      val pred = s"e$index"
      val predVar = QueryText(s"?$pred")
      val edgeType = edge.`type`.map(_.shorthand).getOrElse("related_to")
      //FIXME replacement is hacky
      val edgeValues = mappingsClosure
        .getOrElse(edgeType.replaceAllLiterally("_", " "), Set.empty)
        .filter(knownPredicates)
        .map(prop => sparql"$prop ")
        .reduceOption(_ + _)
        .getOrElse(sparql"")
      val sparql = (for {
        subj <- nodesToVariables.get(edge.source_id).map(_._2)
        subjVar = QueryText(s"?$subj")
        obj <- nodesToVariables.get(edge.target_id).map(_._2)
        objVar = QueryText(s"?$obj")
      } yield sparql"""
                $subjVar $predVar $objVar .
                VALUES $predVar { $edgeValues }
              """).getOrElse(sparql"")
      edge.id -> (edge, pred, sparql)
    }.toMap
    val edgeSPARQL = edgesToVariables.values.map(_._3).reduceOption(_ + _).getOrElse(sparql"")
    val nodeVariables = nodesToVariables.values.map(_._2)
    val nodeLabelVariables = nodeVariables.map(v => s"${v}_label")
    val allVariables = nodeVariables ++ nodeLabelVariables ++ edgesToVariables.values.map(_._2)
    val mainVariablesProjection = allVariables.map(v => QueryText(s"?$v ")).reduceOption(_ + _).getOrElse(sparql"")
    val projection = mainVariablesProjection
    val limitSPARQL = limit.map(l => sparql"LIMIT $l").getOrElse(sparql"")
    val query = sparql"""
      SELECT DISTINCT $projection
      WHERE {
        $nodeSPARQL
        $edgeSPARQL
      }
      $limitSPARQL
          """
    (nodesToVariables, edgesToVariables, query.toQuery)
  }

  // This is an optimization for the main query, which runs much faster with fewer possible predicates
  def getKnownPredicates: ZIO[ZConfig[AppConfig] with HttpClient, Throwable, Set[IRI]] = {
    val query =
      sparql"""
         SELECT DISTINCT ?p
         WHERE { 
           ?s ?p ?o
         }
      """.toQuery
    for {
      result <- SPARQLQueryExecutor.runSelectQuery(query)
    } yield result.solutions.map(qs => IRI(qs.getResource("p").getURI)).toSet
  }

}
