package org.renci.cam

import java.nio.charset.StandardCharsets
import org.apache.commons.codec.digest.DigestUtils
import org.apache.jena.query.{Query, QuerySolution}
import org.phenoscape.sparql.SPARQLInterpolation._
import org.renci.cam.HttpClient.HttpClient
import org.renci.cam.MetaKnowledgeGraphService.makeBiolinkTerm
import org.renci.cam.SPARQLQueryExecutor.SelectResult
import org.renci.cam.domain._
import zio._

object QueryService {

  val RDFSLabel: IRI = IRI("http://www.w3.org/2000/01/rdf-schema#label")
  val IAODefinition: IRI = IRI("http://purl.obolibrary.org/obo/IAO_0000115")
  val XSDString: IRI = IRI("http://www.w3.org/2001/XMLSchema#string")
  val BiolinkModelGraph: IRI = IRI("https://biolink.github.io/biolink-model/")
  val BiolinkIsA: IRI = IRI("https://w3id.org/linkml/is_a")
  val BiolinkCategory: IRI = IRI("https://w3id.org/biolink/vocab/category")
  val BiolinkDescription: IRI = IRI("https://w3id.org/biolink/vocab/description")
  val BiolinkMixins: IRI = IRI("https://w3id.org/linkml/mixins")
  val BiolinkRelatedTo: BiolinkTerm = BiolinkTerm("related_to", IRI("https://w3id.org/biolink/vocab/related_to"))

  val BiolinkInformationResource: BiolinkTerm =
    BiolinkTerm("InformationResource", IRI("https://w3id.org/biolink/vocab/InformationResource"))

  val BiolinkAggregatorKnowledgeSource: BiolinkTerm =
    BiolinkTerm("aggregator_knowledge_source", IRI("https://w3id.org/biolink/vocab/aggregator_knowledge_source"))

  val SkosExactMatch: IRI = IRI("http://www.w3.org/2004/02/skos/core#exactMatch")
  val SkosNarrowMatch: IRI = IRI("http://www.w3.org/2004/02/skos/core#narrowMatch")
  val SkosMappingRelation: IRI = IRI("http://www.w3.org/2004/02/skos/core#mappingRelation")

  val EDAMHierarchy: IRI = IRI("http://edamontology.org/data_2589")
  val NonRedundantGraph: IRI = IRI("http://reasoner.renci.org/nonredundant")
  val RedundantGraph: IRI = IRI("http://reasoner.renci.org/redundant")
  val BGPrior: IRI = IRI("http://www.bigdata.com/queryHints#Prior")
  val BGRunFirst: IRI = IRI("http://www.bigdata.com/queryHints#runFirst")

  private val StandardProvenance = TRAPIAttribute(
    attribute_source = Some("infores:sri-ontology"),
    attribute_type_id = BiolinkAggregatorKnowledgeSource.iri,
    original_attribute_name = None,
    value = "infores:sri-ontology",
    value_type_id = Some(BiolinkInformationResource.iri),
    value_url = Some("https://ontology-kp.apps.renci.org"),
    description = None,
    attributes = None
  )

  type NodeMap = Map[String, (TRAPIQueryNode, String, QueryText, QueryText)]
  type EdgeMap = Map[String, (TRAPIQueryEdge, String, QueryText, QueryText, String)]

  def run(queryGraph: TRAPIQueryGraph, limit: Option[Int]): RIO[Has[AppConfig] with HttpClient with Has[Biolink], TRAPIResponse] = {
    val (nodeMap, edgeMap, query) = makeSPARQL(queryGraph, limit)
    for {
      result <- SPARQLQueryExecutor.runSelectQuery(query)
    } yield TRAPIResponse(makeResultMessage(result, queryGraph, nodeMap, edgeMap))
  }

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
    TRAPIMessage(Some(queryGraph), Some(kg), Some(trapiResults.distinct))
  }

  private def responseForQueryNode(nodeLocalID: String,
                                   queryNode: TRAPIQueryNode,
                                   solution: QuerySolution,
                                   nodeMap: NodeMap): ((String, List[TRAPINodeBinding]), (IRI, TRAPINode)) = {
    val (_, queryVar, _, _) = nodeMap(nodeLocalID)
    val nodeIRI = IRI(solution.getResource(queryVar).getURI)
    val nameOpt = Option(solution.getLiteral(s"${queryVar}_label_first")).map(_.getLexicalForm)
    val definitionOpt = Option(solution.getLiteral(s"${queryVar}_definition_first")).map(_.getLexicalForm)
    val definitionAttribute = definitionOpt.map { defn =>
      List(
        TRAPIAttribute(
          attribute_source = None,
          attribute_type_id = BiolinkDescription,
          original_attribute_name = None,
          value = defn,
          value_type_id = Some(XSDString),
          value_url = None,
          description = None,
          attributes = None
        )
      )
    }
    val trapiNode = TRAPINode(nameOpt, queryNode.categories, definitionAttribute)
    val trapiNodeBinding = TRAPINodeBinding(nodeIRI)
    (nodeLocalID -> List(trapiNodeBinding), nodeIRI -> trapiNode)
  }

  private def responseForQueryEdge(edgeLocalID: String,
                                   queryEdge: TRAPIQueryEdge,
                                   solution: QuerySolution,
                                   nodeMap: NodeMap,
                                   edgeMap: EdgeMap): ((String, List[TRAPIEdgeBinding]), (String, TRAPIEdge)) = {
    val (_, sourceVar, _, _) = nodeMap(queryEdge.subject)
    val (_, targetVar, _, _) = nodeMap(queryEdge.`object`)
    val (_, predicateVar, _, _, _) = edgeMap(edgeLocalID)
    val (_, _, _, _, categoryVar) = edgeMap(edgeLocalID)
    val sourceIRI = IRI(solution.getResource(sourceVar).getURI)
    val targetIRI = IRI(solution.getResource(targetVar).getURI)
    val predicateIRI = IRI(solution.getResource(predicateVar).getURI)
    val category = makeBiolinkTerm(
      Option(solution.getResource(s"${categoryVar}_first")).map(_.getURI).map(IRI(_)).getOrElse(BiolinkRelatedTo.iri))
    val edgeKGID =
      DigestUtils.sha1Hex(s"${sourceIRI.value}${predicateIRI.value}${targetIRI.value}".getBytes(StandardCharsets.UTF_8))
    val trapiEdge = TRAPIEdge(Some(category), sourceIRI, targetIRI, Some(List(StandardProvenance)))
    val trapiEdgeBinding = TRAPIEdgeBinding(edgeKGID)
    ((edgeLocalID, List(trapiEdgeBinding)), (edgeKGID, trapiEdge))
  }

  def makeSPARQL(queryGraph: TRAPIQueryGraph, limit: Option[Int]): (NodeMap, EdgeMap, Query) = {
    val nodesToVariables: Map[String, (TRAPIQueryNode, String, QueryText, QueryText)] = queryGraph.nodes.zipWithIndex
      .map { case ((nodeLocalID, node), index) =>
        val nodeVarName = s"n$index"
        val nodeVar = QueryText(s"?$nodeVarName")
        val nodeLabelVar = QueryText(s"?${nodeVarName}_label")
        val nodeDefinitionVar = QueryText(s"?${nodeVarName}_definition")
        val nodeSPARQL = if (node.ids.getOrElse(Nil).isEmpty) {
          val blterms = node.categories.getOrElse(Nil)
          if (blterms.isEmpty || ((blterms.size == 1) && (blterms.head.shorthand == "NamedThing"))) {
            (
              sparql"""$nodeVar $RDFSLabel $nodeLabelVar .
                     OPTIONAL { $nodeVar $IAODefinition $nodeDefinitionVar . }
                  """,
              sparql"""FILTER(isIRI($nodeVar))"""
            )
          } else {
            val (biolinkClassNode, biolinkClassConstraints) =
              if (blterms.size == 1) (sparql"${blterms.head.iri}", sparql"")
              else {
                val blVar = QueryText(s"?${nodeVarName}_biolink")
                val blIRIs = blterms.map(t => sparql" ${t.iri} ").reduceOption(_ + _).getOrElse(sparql"")
                (blVar, sparql"VALUES $blVar { $blIRIs }")
              }
            (sparql"""
                $nodeVar $RDFSLabel $nodeLabelVar . 
                OPTIONAL { $nodeVar $IAODefinition $nodeDefinitionVar . } 
              """,
             sparql"""
                FILTER(isIRI($nodeVar))
                GRAPH $BiolinkModelGraph {
                  $biolinkClassConstraints
                  $nodeVar $BiolinkCategory $biolinkClassNode .
                }
              """)
          }
        } else {
          node.ids
            .map { ids =>
              val idsList = ids.map(i => sparql" $i ").reduceOption(_ + _).getOrElse(sparql"")
              (sparql"""
              $nodeVar $RDFSLabel $nodeLabelVar . 
              OPTIONAL { $nodeVar $IAODefinition $nodeDefinitionVar . }
            """,
               sparql"""
                  FILTER(isIRI($nodeVar))
                  VALUES $nodeVar { $idsList }
                """)
            }
            .getOrElse((sparql"", sparql""))
        }
        nodeLocalID -> (node, nodeVarName, nodeSPARQL._1, nodeSPARQL._2)
      }
      .to(Map)
    val nodeOuterSPARQL = nodesToVariables.values.map(_._3).reduceOption(_ + _).getOrElse(sparql"")
    val nodeSubQuerySPARQL = nodesToVariables.values.map(_._4).reduceOption(_ + _).getOrElse(sparql"")
    val edgesToVariables = queryGraph.edges.zipWithIndex
      .map { case ((edgeLocalID, edge), index) =>
        val directEdges = edge.attribute_constraints.toList.flatten.exists { constraint =>
          (constraint.id == EDAMHierarchy) && (constraint.value.toLowerCase == "direct") && (constraint.operator == "==")
        }
        val edgeGraph = if (directEdges) NonRedundantGraph else RedundantGraph
        val pred = s"e$index"
        val predVar = QueryText(s"?$pred")
        val edgeTypes = edge.predicates match {
          case None => List(BiolinkRelatedTo)
          case Some(Nil) => List(BiolinkRelatedTo)
          case Some(predicates) => predicates
        }
        val categoryVarName = s"${pred}_category"
        val categoryVar = QueryText(s"?$categoryVarName")
        val (outerPredicateSPARQL, subQueryPredicateSPARQL) = {
          val category2Var = QueryText(s"?${pred}_category2")
          // allow any edge to match related_to
          if (edgeTypes == List(BiolinkRelatedTo)) {
            (sparql"""
                  GRAPH $BiolinkModelGraph {
                    $predVar $BiolinkCategory $categoryVar .
                    FILTER NOT EXISTS {
                      $predVar $BiolinkCategory $category2Var .
                      FILTER($categoryVar != $category2Var)
                      $category2Var ($BiolinkIsA|$BiolinkMixins)+ $categoryVar .
                    }
                  }
                  """,
             sparql"")
          } else {
            val (predicateNode, predicateConstraints) =
              if (edgeTypes.size == 1) (sparql"${edgeTypes.head.iri}", sparql"")
              else {
                val predicateVar = QueryText(s"?${pred}_predicate")
                val edgeTypeIRIs = edgeTypes.map(t => sparql" ${t.iri} ").reduceOption(_ + _).getOrElse(sparql"")
                (predicateVar, sparql"VALUES $predicateVar { $edgeTypeIRIs }")
              }
            (sparql"""
                   GRAPH $BiolinkModelGraph {
                     $predVar $BiolinkCategory $categoryVar .
                     $categoryVar ($BiolinkIsA|$BiolinkMixins)* $predicateNode .
                     FILTER NOT EXISTS {
                       $predVar $BiolinkCategory $category2Var .
                       FILTER($categoryVar != $category2Var)
                       $category2Var ($BiolinkIsA|$BiolinkMixins)+ $categoryVar .
                     }
                   }
                  """,
             sparql"""
                    GRAPH $BiolinkModelGraph {
                      $predicateConstraints
                      $predVar $BiolinkCategory $predicateNode .
                    }
              """)
          }
        }
        val (outerEdgeSPARQL, subQueryEdgeSPARQL) = (for {
          subj <- nodesToVariables.get(edge.subject).map(_._2)
          subjVar = QueryText(s"?$subj")
          obj <- nodesToVariables.get(edge.`object`).map(_._2)
          objVar = QueryText(s"?$obj")
        } yield (outerPredicateSPARQL,
                 sparql"""
                GRAPH $edgeGraph { $subjVar $predVar $objVar . }
                $subQueryPredicateSPARQL
              """)).getOrElse((sparql"", sparql""))
        edgeLocalID -> (edge, pred, outerEdgeSPARQL, subQueryEdgeSPARQL, categoryVarName)
      }
      .to(Map)
    val outerEdgeSPARQLs = edgesToVariables.values.map(_._3).reduceOption(_ + _).getOrElse(sparql"")
    val subQueryEdgeSPARQLs = edgesToVariables.values.map(_._4).reduceOption(_ + _).getOrElse(sparql"")
    val categoryVarNames = edgesToVariables.values.map(_._5)
    val nodeVariables = nodesToVariables.values.map(_._2)
    val nodeLabelVariables = nodeVariables.map(v => s"${v}_label")
    val nodeDefinitionVariables = nodeVariables.map(v => s"${v}_definition")
    val groupingVariablesText =
      (nodeVariables ++ edgesToVariables.values.map(_._2)).map(v => QueryText(s" ?$v ")).reduceOption(_ + _).getOrElse(sparql"")
    val takeFirstVariables =
      (nodeLabelVariables ++ nodeDefinitionVariables ++ categoryVarNames)
        .map(v => QueryText(s" (MIN(?$v) AS ?${v}_first) "))
        .reduceOption(_ + _)
        .getOrElse(sparql"")
    val projection = groupingVariablesText + takeFirstVariables
    val limitValue = limit.getOrElse(10000) //TODO report in TRAPI if max was encountered?
    val limitSPARQL = sparql"LIMIT $limitValue"
    val query = sparql"""
      SELECT $projection
      WHERE {
        $nodeOuterSPARQL
        $outerEdgeSPARQLs
        {
          SELECT DISTINCT $groupingVariablesText
          WHERE {
            $subQueryEdgeSPARQLs
            $nodeSubQuerySPARQL
          }
          $limitSPARQL
        }
        $BGPrior $BGRunFirst true .
      }
      GROUP BY $groupingVariablesText
          """
    (nodesToVariables, edgesToVariables, query.toQuery)
  }

}
