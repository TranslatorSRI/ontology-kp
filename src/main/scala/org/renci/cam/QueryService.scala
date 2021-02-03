package org.renci.cam

import java.nio.charset.StandardCharsets

import org.apache.commons.codec.digest.DigestUtils
import org.apache.jena.query.{Query, QuerySolution}
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

  def run(queryGraph: TRAPIQueryGraph, limit: Option[Int]): RIO[ZConfig[AppConfig] with HttpClient with Has[Biolink], TRAPIResponse] =
    for {
      knownPredicates <- getKnownPredicates
      biolink <- ZIO.service[Biolink]
      (nodeMap, edgeMap, query) = makeSPARQL(queryGraph, biolink, limit, knownPredicates)
      result <- SPARQLQueryExecutor.runSelectQuery(query)
    } yield TRAPIResponse(makeResultMessage(result, queryGraph, nodeMap, edgeMap))

  def makeResultMessage(results: SelectResult, queryGraph: TRAPIQueryGraph, nodeMap: NodeMap, edgeMap: EdgeMap): TRAPIMessage = {
    val (trapiResults, nodes, edges) = results.solutions.map { solution =>
      val (trapiNodeBindings, trapiNodes) = queryGraph.nodes.map { case (nodeLocalID, queryNode) =>
        responseForQueryNode(nodeLocalID, queryNode, solution, nodeMap)
      }.unzip
      val (trapiEdgeBindings, trapiEdges) =
        queryGraph.edges.map { case (edgeLocalID, queryEdge) =>
          responseForQueryEdge(edgeLocalID, queryEdge, solution, nodeMap, edgeMap)
        }.unzip
      val trapiResult = TRAPIResult(trapiNodeBindings.toMap, trapiEdgeBindings.toMap)
      (trapiResult, trapiNodes, trapiEdges)
    }.unzip3
    val kg = TRAPIKnowledgeGraph(nodes.flatten.toMap, edges.flatten.toMap)
    TRAPIMessage(Some(queryGraph), Some(kg), Some(trapiResults))
  }

  private def responseForQueryNode(nodeLocalID: String,
                                   queryNode: TRAPIQueryNode,
                                   solution: QuerySolution,
                                   nodeMap: NodeMap): ((String, List[TRAPINodeBinding]), (IRI, TRAPINode)) = {
    val (_, queryVar, _) = nodeMap(nodeLocalID)
    val nodeIRI = IRI(solution.getResource(queryVar).getURI)
    val nameOpt = Option(solution.getLiteral(s"${queryVar}_label")).map(_.getLexicalForm)
    val trapiNode = TRAPINode(nameOpt, queryNode.category.to(List))
    val trapiNodeBinding = TRAPINodeBinding(nodeIRI)
    (nodeLocalID -> List(trapiNodeBinding), nodeIRI -> trapiNode)
  }

  private def responseForQueryEdge(edgeLocalID: String,
                                   queryEdge: TRAPIQueryEdge,
                                   solution: QuerySolution,
                                   nodeMap: NodeMap,
                                   edgeMap: EdgeMap): ((String, List[TRAPIEdgeBinding]), (String, TRAPIEdge)) = {
    val (_, sourceVar, _) = nodeMap(queryEdge.subject)
    val (_, targetVar, _) = nodeMap(queryEdge.`object`)
    val (_, predicateVar, _) = edgeMap(edgeLocalID)
    val sourceIRI = IRI(solution.getResource(sourceVar).getURI)
    val targetIRI = IRI(solution.getResource(targetVar).getURI)
    val predicateIRI = IRI(solution.getResource(predicateVar).getURI)
    val edgeKGID =
      DigestUtils.sha1Hex(s"${sourceIRI.value}${predicateIRI.value}${targetIRI.value}".getBytes(StandardCharsets.UTF_8))
    val trapiEdge = TRAPIEdge(queryEdge.predicate, sourceIRI, targetIRI)
    val trapiEdgeBinding = TRAPIEdgeBinding(edgeKGID)
    ((edgeLocalID, List(trapiEdgeBinding)), (edgeKGID, trapiEdge))
  }

  def makeSPARQL(queryGraph: TRAPIQueryGraph,
                 biolink: Biolink,
                 limit: Option[Int],
                 knownPredicates: Set[IRI]): (NodeMap, EdgeMap, Query) = {
    val mappingsClosure = Biolink.mappingsClosure(biolink)
    val nodesToVariables = queryGraph.nodes.zipWithIndex
      .map { case ((nodeLocalID, node), index) =>
        val nodeVarName = s"n$index"
        val nodeVar = QueryText(s"?$nodeVarName")
        val nodeLabelVar = QueryText(s"?${nodeVarName}_label")
        val nodeSPARQL = if (node.id.isEmpty) {
          node.category
            .map { blt =>
              if (blt.shorthand == "NamedThing") sparql"$nodeVar $RDFSLabel $nodeLabelVar ."
              else {
                val nodeSuperVar = QueryText(s"?${nodeVarName}_super")
                val mappings = mappingsClosure.get(blt.shorthand).to(Set).flatten
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
          node.id
            .map { c =>
              sparql"""
              $nodeVar $RDFSLabel $nodeLabelVar . 
              VALUES $nodeVar { $c }
            """
            }
            .getOrElse(sparql"")
        nodeLocalID -> (node, nodeVarName, nodeSPARQL)
      }
      .to(Map)
    val nodeSPARQL = nodesToVariables.values.map(_._3).reduceOption(_ + _).getOrElse(sparql"")
    val edgesToVariables = queryGraph.edges.zipWithIndex
      .map { case ((edgeLocalID, edge), index) =>
        val pred = s"e$index"
        val predVar = QueryText(s"?$pred")
        val edgeType = edge.predicate.map(_.shorthand).getOrElse("related_to")
        //FIXME replacement is hacky
        val edgeValues = mappingsClosure
          .getOrElse(edgeType, Set.empty)
          .filter(knownPredicates)
          .map(prop => sparql"$prop ")
          .reduceOption(_ + _)
          .getOrElse(sparql"")
        val sparql = (for {
          subj <- nodesToVariables.get(edge.subject).map(_._2)
          subjVar = QueryText(s"?$subj")
          obj <- nodesToVariables.get(edge.`object`).map(_._2)
          objVar = QueryText(s"?$obj")
        } yield sparql"""
                $subjVar $predVar $objVar .
                VALUES $predVar { $edgeValues }
              """).getOrElse(sparql"")
        edgeLocalID -> (edge, pred, sparql)
      }
      .to(Map)
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
    } yield result.solutions.map(qs => IRI(qs.getResource("p").getURI)).to(Set)
  }

}
