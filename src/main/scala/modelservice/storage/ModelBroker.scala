package modelservice.storage

import spray.http.{HttpEntity, HttpResponse}
import spray.http.HttpHeaders._
import spray.http.ContentTypes._

import scala.concurrent.duration._
import akka.actor._
import akka.util.Timeout
import akka.pattern.ask
import akka.pattern.pipe
import breeze.linalg.SparseVector
import modelservice.core.HashFeatureManager
import modelservice.storage.ParameterStorage.ParameterEntry
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization


/**
 * Coordinate model storage and retrieval with client
 */
class ModelBroker extends Actor with ActorLogging {
  import ModelBroker._
  import ModelStorage._

  implicit val timeout: Timeout = 2.second
  import context.dispatcher
  implicit val formats = DefaultFormats

  def receive = {
    case StoreFeatureManager(basicModel: BasicFeatureManager, modelStorage: ActorRef, client: ActorRef) => {
      val model = ModelFactory(basicModel)
      val key = model.hashCode().toString
      modelStorage ! Post(key, model, client)
    }

    case StoreFeatureManagerWithKey(FeatureManagerWithKey(key: String, basicModel: BasicFeatureManager), modelStorage: ActorRef, client: ActorRef) => {
      val model = ModelFactory(basicModel)

//      Post(key: String, featureManager: HashFeatureManager, client: ActorRef)
//      Put(modelKey: String, modelParameters: ParameterEntry, client: ActorRef)

      modelStorage ! Post(key, model, client)
    }

    case StoreModelParameters(modelKey: String, paramKey: Option[String], basicModelParameters: BasicSparseVector, modelStorage: ActorRef, client: ActorRef) => {
      val modelParameters = ModelFactory(basicModelParameters)

      modelStorage ! Put(modelKey, ParameterEntry(paramKey, modelParameters), client)
    }

    case GetAllKeysInStorage(modelStorage, client) => {
      val keysFuture = modelStorage ? GetAllKeys()
      keysFuture onSuccess {
        case results: Map[String, Set[String]] => try {
          client ! HttpResponse(
            200,
            entity = HttpEntity(
              `application/json`,
              Serialization.write(results.asInstanceOf[Map[String, Set[String]]])
            ),
            headers = List(Connection("close"))
          )
        } catch {
          case e: Exception => client ! HttpResponse(
            500,
            headers = List(Connection("close"))
          )
        }
      }
    }

    // TODO: serve models to HTTP client
    //    case GetLatestModel(modelStorage: ActorRef) =>
  }
}

object ModelBroker {
  final case class BasicSparseVector(index: Array[Int], data: Array[Double], maxFeatures: Int,
                                     numFeatures: Option[Int] = None)
  final case class BasicFeatureManager(k: Int, label: String, singleFeatures: List[String],
                                       quads: Option[Seq[Seq[Seq[String]]]] = None, numericLabelKey: Option[String])

  final case class BasicModel(basicWeights: BasicSparseVector, basicFeatureManager: BasicFeatureManager)
  final case class ModelWithKey(key: String, basicModel: BasicModel)

  final case class StoreModel(basicModel: BasicModel, modelStorage: ActorRef, client: ActorRef)
  final case class StoreModelWithKey(modelWithKey: ModelWithKey, modelStorage: ActorRef, client: ActorRef)

  final case class FeatureManagerWithKey(key: String, basicModel: BasicFeatureManager)

  final case class StoreFeatureManager(basicFeatureManager: BasicFeatureManager, modelStorage: ActorRef, client: ActorRef)
  final case class StoreFeatureManagerWithKey(featureManagerWithKey: FeatureManagerWithKey, modelStorage: ActorRef, client: ActorRef)

  final case class StoreModelParameters(modelKey: String, paramKey: Option[String],
                                        basicModelParameters: BasicSparseVector, modelStorage: ActorRef,
                                        client: ActorRef)

  final case class GetLatestModel(modelStorage: ActorRef)
  final case class GetModelByKey(key: String, modelStorage: ActorRef)

  final case class GetAllKeysInStorage(modelStorage: ActorRef, client: ActorRef)

  def createActor(actorRefFactory: ActorRefFactory): ActorRef = {
    actorRefFactory actorOf Props(classOf[ModelBroker])
  }
}

object ModelFactory {
  import ModelBroker._
  import ModelStorage._

  def createFeatureManager(bfM: BasicFeatureManager): HashFeatureManager = {
    bfM match {
      case BasicFeatureManager(k, label, singleFeatures, quads, numericLabel) => {
        val featureManager = (new HashFeatureManager)
          .withK(k)
          .withLabel(label)
          .withSingleFeatures(singleFeatures)
        val featureManagerWithQuads = quads match {
          case Some(q) => featureManager.withQuadraticFeatures(q)
          case None => featureManager
        }
        numericLabel match {
          case Some(nL) => featureManagerWithQuads.withNumericValue(nL)
          case None => featureManagerWithQuads
        }
      }
    }
  }

  def createWeights(bSV: BasicSparseVector): SparseVector[Double] = {
    bSV match {
      case BasicSparseVector(index, data, maxFeatures, _) => {
        new SparseVector[Double](index, data, index.length, maxFeatures)
      }
    }
  }

  def apply(basicSparseVector: BasicSparseVector) = {
    createWeights(basicSparseVector)
  }

  def apply(basicFeatureManager: BasicFeatureManager) = {
    createFeatureManager(basicFeatureManager)
  }
}
