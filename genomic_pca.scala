//this code is to run in the adam-shell

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.bdgenomics.formats.avro.{Genotype, FlatGenotype, GenotypeAllele}
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.models.VariantContext._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._ 
import scala.collection.JavaConverters._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vectors._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.feature.VectorSlicer

//this was aws ec2 spark master in my case
val myhost = "hdfs://<MASTER IP>:9010"

//broadcast the sampleID to population map

val panelFile = "/PATH/TO/integrated_call_samples_v3.20130502.ALL.panel"
val populations = Set("CHB","ASW","GBR","YRI","CEU","JPT")

import scala.io.Source
def extract(file: String, filter: (String, String) => Boolean): Map [String, String] = { 
  Source.fromFile(panelFile).getLines() 
  .map(line => {  
  val tokens = line.split("\t").toList 
  tokens(0) -> tokens(1) 
  }) 
  .toMap 
  .filter(tuple => filter(tuple._1, tuple._2)) 
}

val panel : Map[String, String] = extract(panelFile, (sampleID: String, pop: String) => populations.contains(pop))
val bPanel = sc.broadcast(panel)


//read in the adam genotype data
val localAdam = myhost + "/user/ds/genomics/1kg/phase1/parquet/chr22"
val filteredGts : RDD[Genotype] = sc.loadGenotypes(localAdam).filter(genotype => {panel.contains(genotype.getSampleId)}).cache()

//make a case class of variants

case class SampleVariant(sampleId: String, variantId: Int, alternateAlleleCount: Int, otheralternateAlleleCount: Int, refAlleleCount: Int, noCallAlleleCount: Int,
allAllele: String)

def variantId(genotype: Genotype): String = {
  val name = genotype.getVariant.getContig.getContigName
  val start = genotype.getVariant.getStart
  val end = genotype.getVariant.getEnd
  s"$name:$start$end"
}

def alternateAlleleCount(genotype: Genotype): Int = {
genotype.getAlleles.asScala.count(_ == GenotypeAllele.Alt)
}
def otheralternateAlleleCount(genotype: Genotype): Int = {
genotype.getAlleles.asScala.count(_ == GenotypeAllele.OtherAlt)
}
def refAlleleCount(genotype: Genotype): Int = {
genotype.getAlleles.asScala.count(_ == GenotypeAllele.Ref)
}
def noCallAlleleCount(genotype: Genotype): Int = {
genotype.getAlleles.asScala.count(_ == GenotypeAllele.NoCall)
}
def allAllele(genotype: Genotype): String = {
genotype.getAlleles.toString()
}

def toVariant(genotype: Genotype): SampleVariant = {
new SampleVariant(genotype.getSampleId.intern(), variantId(genotype).hashCode(), alternateAlleleCount(genotype), otheralternateAlleleCount(genotype), refAlleleCount(genotype),
noCallAlleleCount(genotype), allAllele(genotype))
}
val variantsRDD: RDD[SampleVariant] = filteredGts.map(toVariant)

//sort the variants to get the number of variants per sample and filter on that

val variantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsRDD.groupBy(_.sampleId)
val sampleCount = variantsBySampleId.count()
val variantsByVariantId: RDD[(Int, Iterable[SampleVariant])] = variantsRDD.groupBy(_.variantId)
val filteredVariantsByVariantId = variantsByVariantId.filter{case (_, sampleVariants) => sampleVariants.size == sampleCount}

//filter on variant frequencies

import scala.collection.immutable.Range.inclusive
val variantFrequencies: collection.Map[Int, Int] = filteredVariantsByVariantId.map {
  case (variantId, sampleVariants) => (variantId, sampleVariants.count(_.alternateAlleleCount > 0))
}.collectAsMap()

val permittedRange = inclusive(30, 30)
val filteredVariantsBySampleId: RDD[(String, Iterable[SampleVariant])] = variantsBySampleId.map {
  case (sampleId, sampleVariants) =>
  val filteredSampleVariants = sampleVariants.filter(variant => permittedRange.contains(
  variantFrequencies.getOrElse(variant.variantId, -1)))
  (sampleId, filteredSampleVariants)
}

val sortedVariantsBySampleId: RDD[(String, Array[SampleVariant])] = filteredVariantsBySampleId.map {
  case (sampleId, variants) =>
  (sampleId, variants.toArray.sortBy(_.variantId))
}

//make dataframe
import org.apache.spark.sql.types.{StructType,StructField,StringType}
val header = StructType(Array(StructField("Region", StringType)) ++ sortedVariantsBySampleId.first()._2.map(variant => {StructField(variant.variantId.toString, IntegerType)}))

val rowRDD: RDD[Row] = sortedVariantsBySampleId.map {
  case (sampleId, sortedVariants) =>
  val region: Array[String] = Array(panel.getOrElse(sampleId, "Unknown"))
  val alternateCounts: Array[Int] = sortedVariants.map(_.alternateAlleleCount)
  Row.fromSeq(region ++ alternateCounts)
}

val df = sqlContext.applySchema(rowRDD,header)

//create input vector

var includedFeatureTerms = Array[String]()
includedFeatureTerms ++= df.schema.fields.map(_.name).filter(term => term.toString != "Region")
val assembler = new VectorAssembler()
.setInputCols(includedFeatureTerms)
.setOutputCol("features")
val output = assembler.transform(df) 

//run pca
val pca = new PCA()
.setInputCol("features")
.setOutputCol("pcaFeatures")
.setK(3) 
val pcaDF = pca.fit(output)

//project data onto PCs
val result = pcaDF.transform(output).select("Region","pcaFeatures")

//get the resulting vector of pcs into a format which can be read into SparkR

val slicer = new VectorSlicer().setInputCol("pcaFeatures").setOutputCol("PC1")
slicer.setIndices(Array(0))
val result1 = slicer.transform(result)

val slicer = new VectorSlicer().setInputCol("pcaFeatures").setOutputCol("PC2")
slicer.setIndices(Array(1))
val result2 = slicer.transform(result1)

val slicer = new VectorSlicer().setInputCol("pcaFeatures").setOutputCol("PC3")
slicer.setIndices(Array(2))
val result3 = slicer.transform(result2)

val trimdf = result3.select(result3("Region").as("Region"), result3("PC1").as("PC1"), result3("PC2").as("PC2"), result3("PC3").as("PC3"))

val trimrdd = trimdf.rdd
val stringRowRDD = trimrdd.map(r => Row(r(0).toString, r(1).toString, r(2).toString, r(3).toString))

case class res2(Result: String, pc1: String, pc2: String, pc3: String)
val schemaString = "Region PC1 PC2 PC3"
val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
val stringDF = sqlContext.createDataFrame(stringRowRDD, schema)

def sub = udf((s: String) => s.substring(1, s.length-2))
val resultDF = stringDF.select(stringDF("Region"), sub(stringDF("PC1")) as "PC1", sub(stringDF("PC2")) as "PC2", sub(stringDF("PC3")) as "PC3")

//write result dataframe to parquet
resultDF.write.parquet(myhost + "/user/ds/genomics/1kg/phase1/parquet/pcaresultsfinal/")
