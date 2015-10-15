# genomic_pca_with_spark_and_adam
# code for Seattle Spark Meetup Machine Learning Roundtable 10/14/2015

## Before we run the analysis code, we need to get our data into hdfs

We can currently get the 1000 genomes public dataset from aws s3. I used a command line tool called s3cmd. 
```
s3cmd ls s3://1000genomes/
s3cmd get s3://1000genomes/phase1/analysis_results/integrated_call_sets/ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz
```

We will also want the call samples list, which will tell us which population each sample ID belongs to:
```
s3cmd get s3://1000genomes/release/20130502/integrated_call_samples_v3.20130502.ALL.panel
```

We need to unzip our data, and get in onto hdfs:
```
gunzip ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz
./bin/hadoop fs -put /vol0/data/ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/vcf/
```

Theoretically we could do this in one line, though I had some issues with that and did it separately for now. Here is what I tried: 
```
gunzip ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz | ./bin/hadoop fs -put - /user/ds/genomics/1kg/vcf/
```

## Use ADAM-CLI to convert vcf to adam format. 
```
./bin/adam-submit vcf2adam /user/ds/genomics/1kg/vcf/ALL.chr22.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf /user/ds/genomics/1kg/parquet/chr22
```
## Data is ready for analysis












