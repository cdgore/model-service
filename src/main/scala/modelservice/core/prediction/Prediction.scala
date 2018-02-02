package modelservice.core.prediction

import akka.actor._
import akka.routing.RoundRobinRouter
import modelservice.core.{Core, FeatureParser}

/**
 * Prediction layer
 */
trait PredictionActors {
  val predictionActor: ActorRef
  val treePredictionNodes: ActorRef
}

trait PredictionActorSet extends PredictionActors {
  this: Core =>

  val predictionActor = system actorOf Props(new TreePredictionActor(this)).withRouter(RoundRobinRouter(nrOfInstances = 8))
  val treePredictionNodes = system actorOf Props(new TreePredictionNode(this)).withRouter(RoundRobinRouter(nrOfInstances = 32))
}
