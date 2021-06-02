package org.renci.cam

import io.circe.Json
import io.circe.parser.parse
import org.renci.cam.HttpClient.HttpClient
import zio._

import scala.io.Source

object Utilities {

  def getBiolinkPrefixes: ZIO[HttpClient, Throwable, PrefixesMap] = {
    val sourceManaged = for {
      fileStream <- Managed.fromAutoCloseable(Task.effect(getClass.getResourceAsStream("/context.jsonld")))
      source <- Managed.fromAutoCloseable(Task.effect(Source.fromInputStream(fileStream)))
    } yield source
    for {
      prefixesStr <- sourceManaged.use(source => ZIO.effect(source.getLines().mkString("\n")))
      prefixesJson <- ZIO.fromEither(parse(prefixesStr))
      cursor = prefixesJson.hcursor
      contextValue <- ZIO.fromEither(cursor.downField("@context").as[Map[String, Json]])
      curies =
        contextValue
          .map { case (key, value) =>
            val idOpt = value.asString.orElse {
              for {
                obj <- value.asObject
                idVal <- obj.toMap.get("@id")
                id <- idVal.asString
              } yield id
            }
            (key, idOpt)
          }
          .collect {
            case (key, Some(value)) if !key.startsWith("@") => (key, value)
          }
    } yield PrefixesMap(curies)
  }

  def localPrefixes: ZIO[Any, Throwable, Map[String, String]] = {
    val sourceManaged = for {
      fileStream <- Managed.fromAutoCloseable(Task.effect(getClass.getResourceAsStream("/legacy_prefixes.json")))
      source <- Managed.fromAutoCloseable(Task.effect(Source.fromInputStream(fileStream)))
    } yield source
    for {
      prefixesStr <- sourceManaged.use(source => ZIO.effect(source.getLines().mkString))
      prefixesJson <- ZIO.fromEither(parse(prefixesStr))
      prefixes <- ZIO.fromEither(prefixesJson.as[Map[String, String]])
    } yield prefixes
  }

  def getPrefixes: ZIO[HttpClient, Throwable, PrefixesMap] =
    for {
      local <- localPrefixes
      biolink <- getBiolinkPrefixes //.orElse(getBiolinkPrefixesFromFile)
      combined = local ++ biolink.prefixesMap
    } yield PrefixesMap(combined)

  def makePrefixesLayer: ZLayer[HttpClient, Throwable, Has[PrefixesMap]] = getPrefixes.toLayer

  val biolinkPrefixes: URIO[Has[PrefixesMap], PrefixesMap] = ZIO.service

  final case class PrefixesMap(prefixesMap: Map[String, String])

  implicit final class ListOps[A](val self: List[A]) extends AnyVal {

    def intersperse(item: A): List[A] =
      self match {
        case Nil => Nil
        case _ :: Nil => self
        case first :: rest => first :: item :: rest.intersperse(item)
      }

  }

}
