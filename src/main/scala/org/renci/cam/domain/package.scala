package org.renci.cam

import contextual.Case
import io.circe.Decoder.Result
import io.circe._
import org.apache.commons.text.CaseUtils
import org.apache.jena.query.{ParameterizedSparqlString, QuerySolution}
import org.phenoscape.sparql.FromQuerySolution
import org.phenoscape.sparql.SPARQLInterpolation.SPARQLInterpolator.SPARQLContext
import org.phenoscape.sparql.SPARQLInterpolation._

import scala.util.Try

package object domain {

  private val Curie = "^([^:]*):(.*)$".r
  private val Protocols = Set("http", "https", "ftp", "file", "mailto")

  private def expandCURIEString(curieString: String, prefixesMap: Map[String, String]): Either[DecodingFailure, IRI] =
    for {
      curie <- curieString match {
        case Curie(p, l) => Right((p, l))
        case _ => Left(DecodingFailure(s"CURIE is malformed: $curieString", Nil))
      }
      (prefix, local) = curie
      namespace <-
        if (Protocols(prefix)) Right(s"$prefix:")
        else prefixesMap.get(prefix).toRight(DecodingFailure(s"No prefix expansion found for $prefix:$local", Nil))
    } yield IRI(s"$namespace$local")

  final case class BiolinkTerm(shorthand: String, iri: IRI)

  object BiolinkTerm {

    val namespace: String = "https://w3id.org/biolink/vocab/"

    //FIXME would be good to check that this is a known Biolink term rather than just accepting
    implicit val decoder: Decoder[BiolinkTerm] = IRI.makeDecoder(Map("biolink" -> namespace)).map { iri =>
      BiolinkTerm(iri.value.replace(namespace, ""), iri)
    }

    implicit val keyDecoder: KeyDecoder[BiolinkTerm] = IRI.makeKeyDecoder(Map("biolink" -> namespace)).map { iri =>
      BiolinkTerm(iri.value.replace(namespace, ""), iri)
    }

    implicit val encoder: Encoder[BiolinkTerm] = Encoder.encodeString.contramap(blTerm => s"biolink:${blTerm.shorthand}")

    implicit val keyEncoder: KeyEncoder[BiolinkTerm] = KeyEncoder.encodeKeyString.contramap { term =>
      s"biolink:${term.shorthand}"
    }

  }

  final case class IRI(value: String)

  object IRI {

    def makeDecoder(prefixesMap: Map[String, String]): Decoder[IRI] = new Decoder[IRI] {

      override def apply(c: HCursor): Result[IRI] = for {
        value <- c.value.as[String]
        iri <- expandCURIEString(value, prefixesMap)
      } yield iri

    }

    def makeKeyDecoder(prefixesMap: Map[String, String]): KeyDecoder[IRI] = new KeyDecoder[IRI] {
      override def apply(key: String): Option[IRI] = expandCURIEString(key, prefixesMap).toOption
    }

    def makeEncoder(prefixesMap: Map[String, String]): Encoder[IRI] = Encoder.encodeString.contramap { iri =>
      val startsWith = prefixesMap.filter { case (prefix, namespace) => iri.value.startsWith(namespace) }
      if (startsWith.nonEmpty) {
        val (prefix, namespace) = startsWith.maxBy(_._2.length)
        s"$prefix:${iri.value.drop(namespace.length)}"
      } else iri.value
    }

    def makeKeyEncoder(prefixesMap: Map[String, String]): KeyEncoder[IRI] = KeyEncoder.encodeKeyString.contramap { iri =>
      val startsWith = prefixesMap.filter { case (prefix, namespace) => iri.value.startsWith(namespace) }
      if (startsWith.nonEmpty) {
        val (prefix, namespace) = startsWith.maxBy(_._2.length)
        s"$prefix:${iri.value.drop(namespace.length)}"
      } else iri.value
    }

    implicit val embedInSPARQL: SPARQLEmbedder[IRI] = SPARQLInterpolator.embed[IRI](Case(SPARQLContext, SPARQLContext) { iri =>
      val pss = new ParameterizedSparqlString()
      pss.appendIri(iri.value)
      pss.toString
    })

    implicit object IRIFromQuerySolution extends FromQuerySolution[IRI] {

      def fromQuerySolution(qs: QuerySolution, variablePath: String = ""): Try[IRI] =
        getResource(qs, variablePath).map(r => IRI(r.getURI))

    }

  }

  final case class TRAPIQueryNode(category: Option[BiolinkTerm], id: Option[IRI])

  //BiolinkPredicate?
  final case class TRAPIQueryEdge(predicate: Option[BiolinkTerm], subject: String, `object`: String) //relation

  final case class TRAPIQueryGraph(nodes: Map[String, TRAPIQueryNode], edges: Map[String, TRAPIQueryEdge])

  //BiolinkClass
  final case class TRAPINode(name: Option[String], category: List[BiolinkTerm])

  final case class TRAPIEdge(predicate: Option[BiolinkTerm], subject: IRI, `object`: IRI)

  final case class TRAPIKnowledgeGraph(nodes: Map[IRI, TRAPINode], edges: Map[String, TRAPIEdge])

  final case class TRAPINodeBinding(id: IRI)

  final case class TRAPIEdgeBinding(id: String)

  final case class TRAPIResult(node_bindings: Map[String, List[TRAPINodeBinding]], edge_bindings: Map[String, List[TRAPIEdgeBinding]])

  final case class TRAPIMessage(query_graph: Option[TRAPIQueryGraph],
                                knowledge_graph: Option[TRAPIKnowledgeGraph],
                                results: Option[List[TRAPIResult]])

  final case class TRAPIQueryRequestBody(message: TRAPIMessage)

  final case class TRAPIResponse(message: TRAPIMessage)

}
