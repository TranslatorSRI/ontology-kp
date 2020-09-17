package org.renci.cam

import contextual.Case
import io.circe.Decoder.Result
import io.circe._
import org.apache.commons.text.CaseUtils
import org.apache.jena.query.ParameterizedSparqlString
import org.phenoscape.sparql.SPARQLInterpolation.SPARQLInterpolator.SPARQLContext
import org.phenoscape.sparql.SPARQLInterpolation._

package object domain {

  final case class BiolinkTerm(shorthand: String, iri: IRI)

  object BiolinkTerm {

    val namespace: String = "https://w3id.org/biolink/vocab/"

    implicit val decoder: Decoder[BiolinkTerm] = Decoder.decodeString.map { s =>
      val local = CaseUtils.toCamelCase(s, true, '_')
      BiolinkTerm(s, IRI(s"$namespace$local"))
    }

//    def makeDecoder(biolinkModel: Map[String, IRI]): Decoder[BiolinkTerm] = new Decoder[BiolinkTerm] {
//
//      override def apply(c: HCursor): Result[BiolinkTerm] = for {
//        value <- c.value.as[String]
//        iri <- biolinkModel.get(value).toRight(DecodingFailure(s"No Biolink IRI found for $value", Nil))
//      } yield BiolinkTerm(value, iri)
//
//    }

    implicit val encoder: Encoder[BiolinkTerm] = Encoder.encodeString.contramap(blTerm => blTerm.shorthand)

  }

  final case class IRI(value: String)

  object IRI {

    private val Curie = "^([^:]*):(.*)$".r

    def makeDecoder(prefixesMap: Map[String, String]): Decoder[IRI] = new Decoder[IRI] {

      private val protocols = Set("http", "https", "ftp", "file", "mailto")

      override def apply(c: HCursor): Result[IRI] = for {
        value <- c.value.as[String]
        Curie(prefix, local) = value
        namespace <-
          if (protocols(prefix)) Right(prefix)
          else prefixesMap.get(prefix).toRight(DecodingFailure(s"No prefix expansion found for $prefix:$local", Nil))
      } yield IRI(s"$namespace$local")

    }

    def makeEncoder(prefixesMap: Map[String, String]): Encoder[IRI] = Encoder.encodeString.contramap { iri =>
      prefixesMap.values
        .find(v => iri.value.startsWith(v))
        .map { namespace =>
          s"$namespace${iri.value.drop(namespace.length)}"
        }
        .getOrElse(iri.value)
    }

    implicit val embedInSPARQL = SPARQLInterpolator.embed[IRI](Case(SPARQLContext, SPARQLContext) { iri =>
      val pss = new ParameterizedSparqlString()
      pss.appendIri(iri.value)
      pss.toString
    })

  }

  final case class TRAPIQueryNode(id: String, `type`: Option[BiolinkTerm], curie: Option[IRI])

  final case class TRAPIQueryEdge(id: String, source_id: String, target_id: String, `type`: Option[BiolinkTerm])

  final case class TRAPIQueryGraph(nodes: List[TRAPIQueryNode], edges: List[TRAPIQueryEdge])

  final case class TRAPINode(id: IRI, name: Option[String], `type`: List[BiolinkTerm])

  final case class TRAPIEdge(id: String, source_id: IRI, target_id: IRI, `type`: Option[BiolinkTerm])

  final case class TRAPIKnowledgeGraph(nodes: List[TRAPINode], edges: List[TRAPIEdge])

  final case class TRAPINodeBinding(qg_id: Option[String], kg_id: IRI)

  final case class TRAPIEdgeBinding(qg_id: Option[String], kg_id: String)

  final case class TRAPIResult(node_bindings: List[TRAPINodeBinding], edge_bindings: List[TRAPIEdgeBinding])

  final case class TRAPIMessage(query_graph: Option[TRAPIQueryGraph],
                                knowledge_graph: Option[TRAPIKnowledgeGraph],
                                results: Option[List[TRAPIResult]])

  final case class TRAPIQueryRequestBody(message: TRAPIMessage)

}
