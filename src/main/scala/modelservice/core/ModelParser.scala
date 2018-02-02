package modelservice.core

import akka.actor._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spray.http.HttpEntity

import modelservice.storage.{StorageActors, ModelBroker}

/**
 * Parse Models
 * @param storageActors Create actor using current the context.  This should be the model broker in production or a
 *                         probe actor for tests
 */
class ModelParser(storageActors: StorageActors)
  extends Actor with ActorLogging {
  import ModelBroker._
  import ModelParser._

  def simpleGetMap(m: Map[String, Any], key: String): Map[String, Any] = {
    m.getOrElse(key, Map[String, Any]()).asInstanceOf[Map[String, Any]]
  }

  def receive = {
    case ParseModelAndStore(rec: HttpEntity, modelKey: Option[String], modelStorage: ActorRef, client: ActorRef) =>
      try {
        log.info("Received HttpEntity")
        val basicFeatureManager = jsonToFeatureManager(parse(rec.asString).values.asInstanceOf[Map[String, Any]])
        log.info("Parsed feature manager!")
        log.info(basicFeatureManager.toString)

        modelKey match {
          case Some(k) => storageActors.modelBroker ! StoreFeatureManagerWithKey(FeatureManagerWithKey(k, basicFeatureManager), modelStorage, client)
          case None => storageActors.modelBroker ! StoreFeatureManager(basicFeatureManager, modelStorage, client)
        }
      } catch {
        case e: Exception => log.info(e.getLocalizedMessage)
      }
    case ParseParametersAndStore(rec: HttpEntity, modelKey: String, paramKey: Option[String], modelStorage: ActorRef, client: ActorRef) => {
      val basicModelParameters = jsonToModelParameters(parse(rec.asString).values.asInstanceOf[Map[String, Any]])
      storageActors.modelBroker ! StoreModelParameters(modelKey, paramKey, basicModelParameters, modelStorage, client)
    }
    case _ => log.info("Cannot parse model request")
  }

  // 2^k feature space
  def jsonToHashModel = jsonToModel(x => math.pow(2.toDouble, x.toDouble).toInt) _

  // k feature space
  def jsonToNumericModel = jsonToModel(x => x) _

  def jsonToModel(vectorSizeFunction: (Int) => Int)(rec: Map[String, Any]): BasicModel = {
    val featureManagerMap = simpleGetMap(rec, "feature_manager")
    val k = featureManagerMap.getOrElse("k", 0).asInstanceOf[BigInt].toInt
    val label = featureManagerMap.getOrElse("label", "").asInstanceOf[String]
    val singleFeatures = featureManagerMap.getOrElse("single_features", List[String]()).asInstanceOf[List[String]]
    val quadFeatures = featureManagerMap.getOrElse("quadratic_features", Seq[Seq[Seq[String]]]()).asInstanceOf[Seq[Seq[Seq[String]]]] match {
      case x: Seq[Seq[Seq[String]]] if x.length > 0 => Some(x)
      case _ => None
    }
    val numericLabelKey = rec.get("numeric_label") match {
      case Some(x: String) => Some(x.asInstanceOf[String])
      case _ => None
    }
    val basicFeatureManager = BasicFeatureManager(k, label, singleFeatures, quadFeatures, numericLabelKey)

    val modelMap = simpleGetMap(rec, "model_parameters")
    val modelData = modelMap.getOrElse("data", List[Double]()).asInstanceOf[List[Double]].toArray
    val modelIndex = modelMap.getOrElse("index", List[BigInt]()).asInstanceOf[List[BigInt]].toArray.map(_.toInt)
    val modelMaxFeatures = vectorSizeFunction(k)
    val modelNumFeatures = Some(modelIndex.length)
    val sparseModel = BasicSparseVector(modelIndex, modelData, modelMaxFeatures, modelNumFeatures)
    BasicModel(sparseModel, basicFeatureManager)
  }

  def jsonToFeatureManager(rec: Map[String, Any]): BasicFeatureManager = {
    val k = rec.getOrElse("k", 0).asInstanceOf[BigInt].toInt
    val label = rec.getOrElse("label", "").asInstanceOf[String]
    val singleFeatures = rec.getOrElse("single_features", List[String]()).asInstanceOf[List[String]]
    val quadFeatures = rec.getOrElse("quadratic_features", Seq[Seq[Seq[String]]]()).asInstanceOf[Seq[Seq[Seq[String]]]] match {
      case x: Seq[Seq[Seq[String]]] if x.length > 0 => Some(x)
      case _ => None
    }
    val numericLabelKey = rec.get("numeric_label") match {
      case Some(x: String) => Some(x.asInstanceOf[String])
      case _ => None
    }
    BasicFeatureManager(k, label, singleFeatures, quadFeatures, numericLabelKey)
  }

  def jsonToModelParameters(rec: Map[String, Any]): BasicSparseVector = {
    val modelData = rec.getOrElse("data", List[Double]()).asInstanceOf[List[Double]].toArray
    val modelIndex = rec.getOrElse("index", List[BigInt]()).asInstanceOf[List[BigInt]].toArray.map(_.toInt)
    val modelMaxFeatures = rec.getOrElse("length", 0).asInstanceOf[BigInt].toInt
    val modelNumFeatures = Some(modelIndex.length)
    BasicSparseVector(modelIndex, modelData, modelMaxFeatures, modelNumFeatures)
  }
}

object ModelParser {
  case class ParseModelAndStore(rec: HttpEntity, modelKey: Option[String], modelStorage: ActorRef, client: ActorRef)
  case class ParseParametersAndStore(rec: HttpEntity, modelKey: String, paramKey: Option[String], modelStorage: ActorRef, client: ActorRef)
}
