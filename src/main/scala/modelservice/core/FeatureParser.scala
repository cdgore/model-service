package modelservice.core

import org.json4s._
import org.json4s.jackson.JsonMethods._
import akka.actor._
import akka.dispatch.{BoundedMessageQueueSemantics, RequiresMessageQueue}
import spray.http.HttpEntity
import modelservice.core.prediction.{PredictionActors, PredictActor, TreePredictionActor}

/**
 * Actor to parse features
 * @param predictionActors Create actor using current the context.  This should be the prediction actor in
 *                              production or a probe actor for tests.  Defaults to tree prediction
 */
class FeatureParser(predictionActors: PredictionActors)
  extends Actor with ActorLogging with RequiresMessageQueue[BoundedMessageQueueSemantics] {
  import FeatureParser._
  import TreePredictionActor._

  def receive = {
    case ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef) =>
      try {
        // Start prediction tree
        val parsedContext = PredictTree(parse(record.asString).values.asInstanceOf[Map[String, Any]],
          modelKey, paramKey, modelStorage, client)
        predictionActors.predictionActor ! parsedContext
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case _ => log.info("Cannot parse feature request")
  }
}

object FeatureParser {
  case class ParseFeatures(record: HttpEntity, modelKey: Option[String], paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
}
