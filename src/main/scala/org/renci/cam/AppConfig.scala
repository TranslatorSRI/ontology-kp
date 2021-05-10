package org.renci.cam

import org.http4s.Uri
import zio.config._
import zio.config.magnolia.DeriveConfigDescriptor._
import zio.config.magnolia.Descriptor

final case class AppConfig(host: String, location: String, port: Int, sparqlEndpoint: Uri)

object AppConfig {

  implicit val uriDescriptor: Descriptor[Uri] = zio.config.magnolia.getDescriptor(
    descriptor[String].transformOrFailLeft(s => Uri.fromString(s).left.map(_.getMessage))(_.toString)
  )

  val config: ConfigDescriptor[AppConfig] = descriptor[AppConfig].mapKey(toKebabCase)

}
